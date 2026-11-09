from collections import Counter
from contextlib import closing
from dataclasses import asdict
import json

import pandas as pd

from lab.data import OHLCV
from lab.learning.data import Market, validate_prices
from lab.phase9.ideas import current_rules
from lab.phase13 import observation, protocol
from lab.phase13.future import expected_funding
from lab.phase13.registry import checked
from lab.phase14.collection import tail_protocol
from lab.phase14.context import MINUTE, combine, decode, intervals, pointer
from lab.storage import utc, utc_now


def stores(amendment):
    original = protocol.active()
    if original["id"] != amendment["body"]["original_protocol"]:
        raise ValueError("Unexpected parent observation protocol")
    return [store for store in (original, tail_protocol(amendment, original)) if store and
            (observation.directory(store) / "observations.sqlite3").exists()]


def market_evidence(amendment, start, end, *, basis):
    if basis not in {"trade", "mark", "funding"} or start >= end:
        raise ValueError("Unsupported native observation request")
    output = []
    for store in stores(amendment):
        root = observation.directory(store)
        with closing(observation.db_open(store, readonly=True)) as db:
            cursor = db.execute("SELECT source_at,source_close,received_at,raw_sha,body FROM item WHERE basis=? "
                                "AND source_at>=? AND source_at<? ORDER BY source_at",
                                (basis, start.isoformat(), end.isoformat()))
            for stamp, close, receipt, digest, raw in cursor:
                value = json.loads(raw)
                output.append((utc(stamp), utc(close), utc(receipt), root / "raw" / (digest+".json"), value))
    return output


def price_frame(rows, basis, *, verify=False):
    data = []
    sources = set()
    for stamp, close, receipt, path, value in rows:
        if verify and path not in sources:
            checked(path, path.stem)
            sources.add(path)
        fields = {name: value[name] for name in (OHLCV if basis == "trade" else OHLCV[:4])}
        if basis == "mark":
            fields["volume"] = 0.
        data.append({**fields, "open_at": stamp, "close_at": close, "received_at": receipt, "evidence": str(path)})
    return decode(data)


def metadata_and_funding_queries(amendment, now):
    body = amendment["body"]
    metadata = [(utc(body["instrument"]["metadata_at_registration"]["as_of"]), utc(body["registered_at"]),
                 body["instrument"]["metadata_at_registration"])]
    requests = []
    for store in stores(amendment):
        with closing(observation.db_open(store, readonly=True)) as db:
            metadata.extend((utc(a), utc(r), json.loads(b)) for a,r,b in db.execute("SELECT as_of,received_at,body FROM metadata")
                            if utc(r) <= now)
            for spec, receipt in db.execute("SELECT spec,receipt FROM request WHERE basis='funding'"):
                req, rec = json.loads(spec), json.loads(receipt) if receipt else {}
                if rec.get("received_at") and utc(rec["received_at"]) <= now:
                    requests.append((utc(req["start"]), utc(req["end_exclusive"]), utc(rec["received_at"]), rec["status"]))
    snapshots = {}
    for stamp, receipt, record in sorted(metadata, key=lambda item: item[:2]):
        snapshots.setdefault(stamp, (receipt, record))
    return [(a,r,b) for a,(r,b) in snapshots.items()], requests


def dated_schedule(snapshots, start, end):
    expected = []
    if not snapshots or snapshots[0][0] > start:
        return None
    for index, (stamp, _, record) in enumerate(snapshots):
        next_change = snapshots[index+1][0] if index+1 < len(snapshots) else end
        left, right = max(start, stamp), min(end, next_change)
        if left < right:
            slots = expected_funding(record, left, right)
            if slots is None:
                return None
            expected.extend(slots)
    return expected


def funding_status(expected, settled, queries, start, end, now, grace):
    if expected is None:
        return {"state": "FUNDING_SCHEDULE_UNAVAILABLE", "slots": [], "complete": False}
    actual = {}
    unexpected = []
    for stamp, receipt, rate in settled:
        nominal = stamp.round("min")
        if nominal not in expected or abs(stamp-nominal) > pd.Timedelta(seconds=5) or nominal in actual:
            unexpected.append(stamp.isoformat())
        else:
            actual[nominal] = (stamp, receipt, rate)
    slots = []
    for slot in expected:
        rate = None
        row = {"scheduled_at": slot.isoformat()}
        if slot in actual:
            stamp, received, rate = actual[slot]
            state = "VERIFIED_ZERO_RATE" if rate == 0 else "VERIFIED_SETTLEMENT"
            row.update(source_at=stamp.isoformat(), received_at=received.isoformat(), late_receipt=received > slot+grace)
        else:
            covering = [item for item in queries if item[0] <= slot < item[1] and item[2] >= slot]
            latest = max(covering, key=lambda item: item[2], default=None)
            if latest and latest[3] != "RECEIVED":
                state = "FUNDING_QUERY_FAILED"
            elif now <= slot+grace:
                state = "EXPECTED_NOT_YET_PUBLISHED"
            elif latest and latest[2] >= slot+grace:
                state = "EXPECTED_SETTLEMENT_MISSING"
            else:
                state = "FUNDING_QUERY_UNCONFIRMED"
        slots.append({**row, "state": state, "rate": rate})
    if not expected:
        recent = [q for q in queries if q[0] <= start and q[1] >= end]
        state = "NO_SETTLEMENT_EXPECTED"
        if recent and max(recent, key=lambda q: q[2])[3] != "RECEIVED":
            state = "NO_SETTLEMENT_EXPECTED_QUERY_FAILED"
    else:
        state = "SETTLEMENTS_VERIFIED" if len(actual) == len(expected) and not unexpected else "INCOMPLETE_SETTLEMENT_EVIDENCE"
    return {"state": state, "slots": slots, "counts": dict(Counter(row["state"] for row in slots)),
            "unexpected_settlements": unexpected, "complete": len(actual) == len(expected) and not unexpected}


def audit(amendment=None, now=None):
    amendment = amendment or pointer("amendment")
    if not amendment:
        return {"state": "UNREGISTERED", "future_evaluations": 0}
    now = now if now is not None else utc_now()
    body = amendment["body"]
    start, end = utc(body["start"]), utc(body["end_exclusive"])
    grace = pd.Timedelta(seconds=body["acquisition"]["close_grace_seconds"])
    due = min(end, max(start, (now-grace).floor("15min")))
    observations = {}
    for basis in ("trade", "mark"):
        times = []
        if due > start:
            for store in stores(amendment):
                with closing(observation.db_open(store, readonly=True)) as db:
                    times.extend(utc(row[0]) for row in db.execute("SELECT source_at FROM item WHERE basis=? AND source_at>=? AND source_at<?",
                                                                 (basis, start.isoformat(), due.isoformat())))
        expected = intervals(start, due, MINUTE)
        actual = pd.DatetimeIndex(times, tz="UTC")
        observations[basis] = {"rows": len(actual), "missing": len(expected.difference(actual)),
                               "duplicates": int(actual.duplicated().sum())}
    snapshots, queries = metadata_and_funding_queries(amendment, now)
    funding_rows = [(stamp, received, payload["rate"]) for stamp, _, received, _, payload in
                    market_evidence(amendment, start, due, basis="funding") if received <= now] if due > start else []
    funding = funding_status(dated_schedule(snapshots, start, due), funding_rows, queries, start, due, now,
                             pd.Timedelta(seconds=body["rules"]["funding_receipt_grace_seconds"]))
    pinned = body["instrument"]["contract"]
    config = {"ideas": {"modeled_liquidation_fee": pinned["liquidation_fee_rate"]}}
    corrections = bridge_corrections = 0
    seed = pointer("seed")
    bridge_start = utc(seed["body"]["cutoff"]) if seed and seed["id"] == body["seed"] else start
    for store in stores(amendment):
        with closing(observation.db_open(store, readonly=True)) as db:
            corrections += db.execute("SELECT count(*) FROM correction WHERE source_at>=? AND source_at<?",
                                      (start.isoformat(), due.isoformat())).fetchone()[0]
            bridge_corrections += db.execute("SELECT count(*) FROM correction WHERE basis='trade' AND source_at>=? AND source_at<?",
                                             (bridge_start.isoformat(), start.isoformat())).fetchone()[0]
    compatible, age = True, 0.
    usable = sorted((stamp, receipt, record) for stamp, receipt, record in snapshots if receipt < due)
    for i, (stamp, receipt, record) in enumerate(usable):
        segment_end = usable[i+1][1] if i+1 < len(usable) else due
        if segment_end <= start:
            continue
        age = max(age, (segment_end-stamp).total_seconds())
        try:
            compatible &= asdict(current_rules("FUTURES", record, config)) == pinned
        except (KeyError, ValueError, StopIteration):
            compatible = False
    complete = all(not x["missing"] and not x["duplicates"] for x in observations.values()) and funding["complete"]
    settled_horizon = now >= end + grace and due == end
    metadata_ready = bool(usable and usable[0][1] <= start and compatible and age <= 86400)
    eligible = settled_horizon and complete and not corrections and not bridge_corrections and metadata_ready
    result = pointer("evaluation")
    state = ("QUALIFIED_FOR_OFFLINE_REPLAY" if eligible else "WAITING_FOR_EVIDENCE" if settled_horizon else
             "WAITING_FOR_PRIMARY_START" if now < start else "PRIMARY_HORIZON_OPEN")
    maturity = "HORIZON_ELAPSED" if settled_horizon else "NOT_STARTED" if now < start else "MATURING"
    return {"state": state,
            "cutoff": due.isoformat(), "collection_coverage": observations, "funding": funding,
            "metadata_age_max_seconds": age, "metadata_compatible": compatible, "metadata_ready": metadata_ready,
            "source_corrections": corrections, "feature_bridge_corrections": bridge_corrections,
            "scoring_availability": "AFTER_QUALIFICATION" if not eligible else "FROZEN_ARTIFACTS_ELIGIBLE",
            "outcome_maturity": maturity,
            "economic_path_complete": bool(due > start and complete), "statistical_evidence": "UNEVALUATED" if not result else "SEE_FROZEN_RESULT",
            "review_state": "DUE" if now >= utc(body["technical_review_due"]) else "PENDING_FIRST_WEEK",
            "future_evaluations": 1 if result else 0}


def execution_market(amendment, funding):
    start, end = (utc(amendment["body"][key]) for key in ("start", "end_exclusive"))
    trade, mark = (price_frame(market_evidence(amendment, start, end, basis=basis), basis, verify=True)
                   for basis in ("trade", "mark"))
    raw_funding = {stamp: (receipt, path, payload["rate"]) for stamp, _, receipt, path, payload in
                   market_evidence(amendment, start, end, basis="funding")}
    sources = []
    for row in funding["slots"]:
        if "source_at" not in row:
            raise ValueError("Execution funding must come from a published settlement")
        stamp = utc(row["source_at"])
        receipt, path, rate = raw_funding[stamp]
        if receipt != utc(row["received_at"]) or rate != row["rate"]:
            raise ValueError("Funding evidence changed after outcome qualification")
        checked(path, path.stem)
        sources.append({**row, "raw": str(path), "sha256": path.stem})
    return native_market(trade, mark, start, end, sources), sources


def native_market(trade, mark, start, end, funding):
    clock = intervals(start, end, MINUTE)
    for frame in (trade, mark):
        if not frame.index.equals(clock) or (frame.received_at < frame.close_at).any():
            raise ValueError("Execution requires contiguous post-start paired native minutes")
        validate_prices(frame, MINUTE)
    market = Market(trade, mark, {}, MINUTE, "HYPOTHETICAL_REPLAY")
    market.intrabar_funding = {}
    for row in funding:
        stamp, rate = utc(row["source_at"]), float(row["rate"])
        floor = stamp.floor(MINUTE)
        if stamp == floor:
            market.funding[stamp] = rate
        else:
            market.intrabar_funding.setdefault(floor, []).append((stamp, rate))
    return market
