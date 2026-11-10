from contextlib import closing
import json
import shutil
import time

import httpx
import pandas as pd

from lab.learning.features import build_features
from lab.phase9.data import candles
from lab.phase9.public import kline, load_payload, send
from lab.phase10.worker import cache_read
from lab.phase13 import observation
from lab.phase13.registry import checked
from lab.phase14.context import pointer as amendment_pointer
from lab.phase14.outcomes import dated_schedule, market_evidence, metadata_and_funding_queries, stores
from lab.phase15.catalog import QUARTER, ROOT, STEP, encode
from lab.storage import utc, utc_now


def capture_spot(db, body, start, end, now, transport=None):
    budget = body["spot_budget"]
    prior = db.execute("SELECT end,receipt FROM spot_request ORDER BY id DESC LIMIT 1").fetchone()
    previous = json.loads(prior[1]) if prior and prior[1] else {}
    if previous.get("status") == "ACCESS_RESTRICTED":
        return previous
    if previous.get("resume_after") and now < utc(previous["resume_after"]):
        return previous
    present = {utc(row[0]) for row in db.execute("SELECT open_at FROM spot_minute WHERE open_at>=? AND open_at<?",
                                                (start.isoformat(), end.isoformat()))}
    expected = pd.date_range(start, end, freq=STEP, inclusive="left")
    missing = expected.difference(pd.DatetimeIndex(sorted(present))) if present else expected
    if not len(missing):
        return {"status": "REUSED", "rows": len(present)}
    start = missing[0]
    if prior and utc(prior[0]) == end and (previous.get("status") == "RECEIVED" or not previous):
        return {"status": "WAITING_FOR_NEXT_BOUNDARY", "rows": len(present)}
    root = ROOT / "spot"
    root.mkdir(parents=True, exist_ok=True)
    if (db.execute("SELECT count(*) FROM spot_request").fetchone()[0] >= budget["requests"]
            or sum(path.stat().st_size for path in root.rglob("*") if path.is_file())+budget["response_bytes"] > budget["retained_bytes"]
            or shutil.disk_usage(root).free < budget["min_free_bytes"]):
        return {"status": "SPOT_RESOURCE_LIMIT"}
    spec = kline("phase15-spot", "BTC-USDT", "SPOT", "TRADE", "1min", start, end)
    reservation = db.execute("INSERT INTO spot_request(start,end,reserved_at) VALUES (?,?,?)",
                             (start.isoformat(), end.isoformat(), now.isoformat())).lastrowid
    db.commit()
    time.sleep(budget["spacing_seconds"])
    with httpx.Client(timeout=budget["timeout_seconds"], follow_redirects=False, transport=transport) as client:
        evidence = {**send(client, spec, budget["response_bytes"], root / "raw"), "request": spec}
    if evidence["status"] == "RECEIVED":
        try:
            frame = candles(load_payload(root, evidence), spec, utc(evidence["received_at"]))
            for row in frame.to_dict("records"):
                stamp = row["open_at"].isoformat()
                found = db.execute("SELECT body FROM spot_minute WHERE open_at=?", (stamp,)).fetchone()
                if not found:
                    db.execute("INSERT INTO spot_minute VALUES (?,?)", (stamp, encode({**row, "received_at": evidence["received_at"],
                                                                                       "raw_sha256": evidence["raw_sha256"]})))
                elif any(json.loads(found[0])[key] != row[key] for key in ("open", "high", "low", "close", "volume")):
                    db.execute("INSERT OR IGNORE INTO spot_correction VALUES (?,?)", (stamp, evidence["raw_sha256"]))
            evidence["rows"] = len(frame)
        except (KeyError, ValueError) as exc:
            evidence.update(status="INVALID_RESPONSE", error=str(exc))
    if evidence["status"] not in {"RECEIVED", "ACCESS_RESTRICTED"}:
        minimum = utc_now()+QUARTER
        evidence["resume_after"] = max(minimum, utc(evidence.get("resume_after", minimum))).isoformat()
    db.execute("UPDATE spot_request SET receipt=? WHERE id=?", (encode(evidence), reservation))
    db.commit()
    return evidence


def spot_bars(db, start, end):
    found = {}
    for at, raw in db.execute("SELECT open_at,body FROM spot_minute WHERE open_at>=? AND open_at<? ORDER BY open_at",
                              (start.isoformat(), end.isoformat())):
        found[utc(at)] = json.loads(raw)
    for digest in {row["raw_sha256"] for row in found.values()}:
        checked(ROOT / "spot" / "raw" / f"{digest}.json", digest)
    corrections = {utc(row[0]) for row in db.execute("SELECT open_at FROM spot_correction WHERE open_at>=? AND open_at<?",
                                                     (start.isoformat(), end.isoformat()))}
    return found, corrections


def futures_source(body):
    amendment = amendment_pointer("amendment")
    if (not amendment or amendment["id"] != body["phase14_amendment"]
            or amendment["body"]["original_protocol"] != body["source_protocol"]):
        raise ValueError("Public futures observation binding changed")
    return amendment


def futures_window(body, start, end, now, path_start=None):
    amendment = futures_source(body)
    bars = {}
    for basis in ("trade", "mark"):
        values = market_evidence(amendment, start, end, basis=basis)
        for path in {row[3] for row in values}:
            checked(path, path.stem)
        bars[basis] = {at: {**row, "open_at": at.isoformat(), "close_at": close.isoformat(), "received_at": receipt.isoformat(),
                           "raw_sha256": path.stem} for at,close,receipt,path,row in values if receipt <= now}
    metadata, _ = metadata_and_funding_queries(amendment, now)
    schedule = dated_schedule(metadata, start-STEP, end+STEP)
    funding_start = max(utc(body["start"]), start-STEP)
    found = market_evidence(amendment, funding_start, end+STEP, basis="funding")
    for path in {row[3] for row in found}:
        checked(path, path.stem)
    settled, flags = {}, {}
    nominal = set(schedule or [])
    verified = set()
    for at, _, receipt, path, row in found:
        slot = at.round("min")
        if receipt > now:
            continue
        if slot not in nominal or abs(at-slot) > pd.Timedelta(seconds=5) or slot in verified:
            flags[at.floor(STEP)] = ("Unexpected or duplicate public funding settlement", True)
            continue
        verified.add(slot)
        settled.setdefault(at.floor(STEP), []).append({"settlement_at": at.isoformat(), "received_at": receipt.isoformat(),
                                                      "rate": row["rate"], "raw_sha256": path.stem})
    for slot in nominal-verified:
        reason = "Expected public funding settlement is missing" if now >= slot+pd.Timedelta(seconds=body["funding_grace_seconds"]) else "Waiting for expected public funding settlement"
        flags.setdefault(slot-STEP, (reason, False))
        flags.setdefault(slot, (reason, False))
    for store in stores(amendment):
        with closing(observation.db_open(store, readonly=True)) as db:
            changed = db.execute("SELECT source_at FROM correction WHERE source_at>=? AND source_at<? AND received_at<=?",
                                 ((path_start or start).isoformat(), end.isoformat(), now.isoformat())).fetchall()
        for (stamp,) in changed:
            flags[max(start, utc(stamp).floor(STEP))] = ("A later public correction conflicts with the active evidence path", True)
    usable_metadata = [stamp for stamp, receipt, _ in metadata if stamp <= start and receipt <= now]
    if usable_metadata:
        valid_to = usable_metadata[-1]+pd.Timedelta(hours=24)
        for at in pd.date_range(start, end, freq=STEP, inclusive="left"):
            if at >= valid_to:
                renewed = [stamp for stamp, receipt, _ in metadata if stamp <= at and receipt <= now]
                if not renewed or at >= renewed[-1]+pd.Timedelta(hours=24):
                    flags.setdefault(at, ("Dated public funding schedule is stale", False))
                else:
                    valid_to = renewed[-1]+pd.Timedelta(hours=24)
    return bars, settled, flags, schedule is not None and bool(usable_metadata)


def swing_trails(lane, start, end, now):
    frame = cache_read("swing")
    if frame.empty:
        return {}
    usable = frame.loc[(frame.close_at <= end) & (frame.received_at <= now)].sort_values("open_at")
    if usable.empty:
        return {}
    profile = lane["profile_rules"]
    features, _ = build_features(usable.set_index("open_at"), profile, pd.Timedelta(profile["decision"]))
    receipts = usable.set_index("close_at").received_at.cummax()
    updates = {}
    for at, row in features.loc[(features.index >= start-pd.Timedelta(hours=4)) & features.atr.gt(0)].iterrows():
        received = max(at, receipts.loc[at]) + pd.Timedelta(seconds=lane["execution"]["processing_seconds"])
        available = received.floor(STEP)+STEP
        updates[available] = {"close_at": at.isoformat(), "available_at": available.isoformat(),
                              "close": float(row.close), "atr": float(row.atr)}
    return updates
