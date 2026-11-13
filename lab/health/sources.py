from collections import Counter
from contextlib import closing
import json

import pandas as pd

from lab.phase2 import store as coinbase_store
from lab.phase13 import observation
from lab.phase14.outcomes import dated_schedule, funding_status, stores
from lab.phase17.collection import futures_stores, spot_store
from lab.storage import utc


def coinbase_interval(start, now):
    output = {}
    with coinbase_store.connect(readonly=True) as db:
        for seconds in (300,3600):
            step = pd.Timedelta(seconds=seconds)
            end = (now-pd.Timedelta(seconds=40)).floor(step)
            first = start.ceil(step)
            rows = db.execute("SELECT c.open_epoch,min(r.received_at),count(*) FROM candles c JOIN responses r ON c.batch=r.id "
                              "WHERE c.seconds=? AND c.open_epoch>=? AND c.open_epoch<? GROUP BY c.open_epoch ORDER BY c.open_epoch",
                              (seconds,int(first.timestamp()),int(end.timestamp()))).fetchall()
            delays = [(utc(r[1])-pd.Timestamp(r[0]+seconds,unit="s",tz="UTC")).total_seconds() for r in rows]
            errors = dict(db.execute("SELECT problem,count(*) FROM responses WHERE seconds=? AND received_at>=? AND problem IS NOT NULL GROUP BY problem",
                                     (seconds,start.isoformat())))
            output[str(seconds)] = {"start":first.isoformat(),"end_exclusive":end.isoformat(),"expected":max(0,int((end-first)//step)),
                                   "rows":len(rows),"missing":max(0,int((end-first)//step)-len(rows)),
                                   "revisions":sum(r[2]-1 for r in rows),"max_receipt_delay_seconds":max(delays,default=None),
                                   "delayed_over_120_seconds":sum(x>120 for x in delays),"failed_responses":errors}
    return output


def native_interval(amendment, native, start, now, *, funding_since=None):
    end = (now-pd.Timedelta(seconds=45)).floor("15min")
    funding_left = funding_since or start
    expected = pd.date_range(start.ceil("min"),end,freq="1min",inclusive="left")
    selected = [*stores(amendment), *futures_stores(native)]
    rows = {"trade":{},"mark":{}}
    qualified = {"trade": None, "mark": None}
    settled, metadata, queries, cycles = [], {}, [], []
    corrections = 0
    requests = Counter()
    for record in selected:
        if not (observation.directory(record)/"observations.sqlite3").exists():
            continue
        with closing(observation.db_open(record,readonly=True)) as db:
            for basis in qualified:
                latest = db.execute(
                    "SELECT source_close FROM item WHERE basis=? AND source_close<=? AND received_at<=? "
                    "AND timing='ON_CADENCE' ORDER BY source_at DESC LIMIT 1",
                    (basis, end.isoformat(), now.isoformat()),
                ).fetchone()
                if latest and (qualified[basis] is None or utc(latest[0]) > qualified[basis]):
                    qualified[basis] = utc(latest[0])
            for basis, source_at, close, receipt, body, timing in db.execute("SELECT basis,source_at,source_close,received_at,body,timing FROM item "
                    "WHERE received_at<=? AND ((basis IN ('trade','mark') AND source_at>=? AND source_at<?) "
                    "OR (basis='funding' AND source_at>=? AND source_at<?))",
                    (now.isoformat(),start.isoformat(),end.isoformat(),funding_left.isoformat(),end.isoformat())):
                if basis == "funding":
                    settled.append((utc(source_at),utc(receipt),json.loads(body)["rate"]))
                elif basis in rows:
                    rows[basis][utc(source_at)] = (utc(close),utc(receipt),timing)
            initial = db.execute("SELECT as_of,received_at,body FROM metadata WHERE as_of<=? ORDER BY as_of DESC LIMIT 1",(funding_left.isoformat(),)).fetchone()
            recent = db.execute("SELECT as_of,received_at,body FROM metadata WHERE as_of>? AND received_at<=? ORDER BY as_of",
                                (funding_left.isoformat(),now.isoformat())).fetchall()
            for as_of, receipt, body in ([initial] if initial else [])+recent:
                metadata.setdefault(utc(as_of),(utc(receipt),json.loads(body)))
            for basis,spec,receipt in db.execute("SELECT basis,spec,receipt FROM request WHERE reserved>=?",((funding_left-pd.Timedelta(hours=2)).isoformat(),)):
                row = json.loads(receipt) if receipt else {}
                if row.get("received_at"):
                    if utc(row["received_at"])>=start:
                        requests[row["status"]] += 1
                    if basis=="funding":
                        doc=json.loads(spec)
                        queries.append((utc(doc["start"]),utc(doc["end_exclusive"]),utc(row["received_at"]),row["status"]))
            cycles.extend((utc(boundary),state) for boundary,state in db.execute("SELECT boundary,state FROM cycle WHERE boundary>?",(start.isoformat(),)))
            corrections += db.execute("SELECT count(*) FROM correction WHERE source_at>=? AND source_at<?",(start.isoformat(),end.isoformat())).fetchone()[0]
    snapshots = [(stamp,*metadata[stamp]) for stamp in sorted(metadata)]
    funding = funding_status(dated_schedule(snapshots,funding_left,end),settled,queries,funding_left,end,now,pd.Timedelta(hours=2))
    prices = {}
    for basis, observed in rows.items():
        gaps = expected.difference(pd.DatetimeIndex(list(observed),tz="UTC"))
        prices[basis] = {"rows":len(observed),"missing":len(gaps),"missing_opens":[v.isoformat() for v in gaps[:30]],
                         "last_close":max((r[0] for r in observed.values()),default=None),
                         "last_receipt":max((r[1] for r in observed.values()),default=None),
                         "last_qualified_close":qualified[basis],
                         "catch_up":sum(r[2]=="CATCH_UP" for r in observed.values()),
                         "max_receipt_delay_seconds":max(((r[1]-r[0]).total_seconds() for r in observed.values()),default=None)}
    final = max((b for b,s in cycles if s in ("COMPLETE","PARTIAL_FINAL")),default=None)
    recent = expected[-15:]
    recent_qualified = bool(len(recent) == 15 and all(
        at in rows[basis] and rows[basis][at][2] == "ON_CADENCE" for at in recent for basis in rows))
    return {"start":start.isoformat(),"end_exclusive":end.isoformat(),"expected":len(expected),"prices":prices,
            "paired":len(rows["trade"].keys() & rows["mark"].keys()),"last_checkpoint":final,"corrections":corrections,
            "request_status":dict(requests),"cycles":dict(Counter(state for _,state in cycles)),"funding":funding,
            "recent_qualified":recent_qualified}


def spot_interval(native,start,now):
    source = spot_store(native)
    end = (now-pd.Timedelta(seconds=45)).floor("1h")
    first = max(start.ceil("1h"),utc(native["body"]["profiles"]["position"]["start"]))
    count = max(0,int((end-first)//pd.Timedelta(hours=1)))
    rows, requests, corrections, qualified = [], [], 0, None
    if (observation.directory(source)/"observations.sqlite3").exists():
        with closing(observation.db_open(source,readonly=True)) as db:
            rows = db.execute("SELECT source_at,source_close,received_at FROM item WHERE basis='trade' "
                              "AND source_at>=? AND source_at<? AND received_at<=? ORDER BY source_at",
                              (first.isoformat(),end.isoformat(),now.isoformat())).fetchall()
            corrections = db.execute("SELECT count(*) FROM correction WHERE source_at>=? AND source_at<?",(first.isoformat(),end.isoformat())).fetchone()[0]
            for close, receipt in db.execute("SELECT source_close,received_at FROM item WHERE basis='trade' "
                    "AND source_close<=? AND received_at<=? ORDER BY source_at DESC", (end.isoformat(),now.isoformat())):
                if pd.Timedelta(0) <= utc(receipt) - utc(close) <= pd.Timedelta(seconds=120):
                    qualified = close
                    break
            requests = [json.loads(row[0])["status"] for row in db.execute("SELECT receipt FROM request WHERE reserved>=? AND receipt IS NOT NULL",(first.isoformat(),))]
    delays = [(utc(receipt)-utc(close)).total_seconds() for _,close,receipt in rows]
    return {"start":first.isoformat(),"end_exclusive":end.isoformat(),"expected":count,"rows":len(rows),"missing":max(0,count-len(rows)),
            "last_close":rows[-1][1] if rows else None,"last_receipt":max((r[2] for r in rows),default=None),"corrections":corrections,
            "last_qualified_close":qualified,
            "max_receipt_delay_seconds":max(delays,default=None),"delayed_over_120_seconds":sum(d>120 for d in delays),
            "request_status":dict(Counter(requests)),"mark":"NOT_APPLICABLE","funding":"NOT_APPLICABLE"}
