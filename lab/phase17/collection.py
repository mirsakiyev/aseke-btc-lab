from contextlib import closing
import json
import shutil

import httpx
import pandas as pd

from lab.config import fingerprint
from lab.phase9.data import candles
from lab.phase9.public import kline, load_payload, send
from lab.phase13 import observation, protocol
from lab.phase14.collection import managed_observe, original_complete
from lab.phase14.context import pointer
from lab.phase17.registry import DAY, ROOT, registered
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


MINUTE = pd.Timedelta(minutes=1)
HOUR = pd.Timedelta(hours=1)
QUARTER = 15 * MINUTE
GRACE = pd.Timedelta(seconds=45)


def spot_store(record):
    return {"id": record["id"], "storage_root": ROOT / "spot"}


def futures_stores(record):
    body = record["body"]
    start = utc(body["profiles"]["day"]["start"])
    end = max(utc(body["profiles"][name]["end_exclusive"]) for name in ("day", "swing"))
    width = body["budget"]["futures_shard_days"] * DAY
    for left in pd.date_range(start, end, freq=width, inclusive="left"):
        right = min(end, left+width)
        identifier = fingerprint([record["id"], "XBTUSDTM-native", left.isoformat(), right.isoformat()])
        yield {"id": identifier, "pulse_id": body["parent_protocols"]["phase13"], "storage_root": ROOT,
               "body": {"start": left.isoformat(), "end_exclusive": right.isoformat(),
                        "instrument": {"contract": {"base_per_contract": .001}},
                        "acquisition": {**protocol.OBSERVATION, "max_requests": body["budget"]["futures_shard_requests"],
                                        "max_retained_bytes": body["budget"]["futures_shard_retained_bytes"]}}}


def price_observe(record, launcher, now=None):
    """The current authenticated observer keeps ownership of both protected closeouts."""
    now = utc_now() if now is None else now
    if not record or not protocol.control()["enabled"]:
        return managed_observe(launcher)
    marker = ROOT / "protected-closeout.json"
    if not marker.exists():
        result = managed_observe(launcher)
        scheduled = read_json(observation.PULSE) if observation.PULSE.exists() else {}
        # Phase 14 can park directly after its final funded request. A
        # near-term retry remains Phase 14's until it actually parks.
        terminal_receipt = (result["state"] == "FUNDING_CLOSEOUT_RECORDED" and scheduled.get("wake")
                            and utc(scheduled["wake"]) > now + 365*DAY)
        if result["state"] == "FUNDING_CLOSEOUT_FINISHED" or terminal_receipt:
            write_json(marker, {"registration": record["id"], "recorded_at": now.isoformat(),
                                "phase14": record["body"]["parent_protocols"]["phase14"], "protected_state": result["state"]})
            observation.schedule(protocol.active(), now.floor("1h") + HOUR + GRACE)
        return result
    if read_json(marker)["registration"] != record["id"]:
        raise ValueError("Protected closeout marker belongs to a different native registration")
    for source in futures_stores(record):
        start, end = (utc(source["body"][key]) for key in ("start", "end_exclusive"))
        if now < start + QUARTER + GRACE:
            break
        if now <= end + GRACE or not original_complete(source) and now <= end + 3*HOUR:
            return observation.observe(launcher=launcher, study=source, after_horizon=end+QUARTER+GRACE)
    observation.schedule(protocol.active(), now.floor("1h") + HOUR + GRACE)
    return {"state": "WAITING_FOR_NATIVE_FUTURES_WINDOW"}


def capture_spot(record, now, transport=None):
    cfg = record["body"]["profiles"]["position"]
    budget = record["body"]["budget"]
    start = utc(cfg["start"])
    end = min((now-GRACE).floor("1h"), utc(cfg["end_exclusive"]))
    if end <= start:
        return {"state": "WAITING_FOR_FIRST_SPOT_HOUR", "next_due": (start+HOUR+GRACE).isoformat()}
    source = spot_store(record)
    root = observation.directory(source)
    with writer_lock(root / ".capture.lock"), closing(observation.db_open(source)) as db:
        observation.save_metadata(db, now)
        prior = db.execute("SELECT max(boundary),count(*) FROM request WHERE basis='trade'").fetchone()
        stop = db.execute("SELECT receipt FROM request WHERE receipt IS NOT NULL ORDER BY id DESC LIMIT 1").fetchone()
        receipt = json.loads(stop[0]) if stop else {}
        if receipt.get("status") == "ACCESS_RESTRICTED" or receipt.get("resume_after") and now < utc(receipt["resume_after"]):
            return {"state": receipt["status"], "receipt": receipt}
        if prior[0] and utc(prior[0]) >= end:
            db.commit()
            return {"state": "SPOT_HOUR_ALREADY_RESERVED", "end_exclusive": end.isoformat()}
        if (prior[1] >= budget["spot_requests"] or shutil.disk_usage(root).free < budget["min_free_disk_mib"] * 1024**2
                or sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) + budget["spot_response_bytes"] >= budget["spot_retained_bytes"]):
            db.commit()
            return {"state": "SPOT_RESOURCE_LIMIT"}
        last_close = db.execute("SELECT max(source_close) FROM item WHERE basis='trade'").fetchone()[0]
        left = max(start, utc(last_close)-HOUR if last_close else start, end-3*HOUR)
        spec = kline("phase17-managed-spot", "BTC-USDT", "SPOT", "TRADE", "1hour", left, end)
        identifier = db.execute("INSERT INTO request(boundary,basis,spec,reserved) VALUES (?,?,?,?)",
                                (end.isoformat(), "trade", observation.encode(spec), now.isoformat())).lastrowid
        db.commit()
        with httpx.Client(timeout=5, follow_redirects=False, transport=transport) as client:
            evidence = send(client, spec, budget["spot_response_bytes"], root / "raw")
        if evidence["status"] == "RECEIVED":
            try:
                frame = candles(load_payload(root, evidence), spec, utc(evidence["received_at"]))
                observation.store_frame(db, "trade", frame, evidence, protocol.OBSERVATION)
                evidence["rows"] = len(frame)
            except (KeyError, ValueError) as exc:
                evidence.update(status="INVALID_RESPONSE", reason=str(exc))
        db.execute("UPDATE request SET receipt=? WHERE id=?", (observation.encode(evidence), identifier))
        db.commit()
        return {"state": evidence["status"], "left": left.isoformat(), "end_exclusive": end.isoformat(), "receipt": evidence}


def existing_sources(record):
    from lab.phase14.outcomes import stores

    amendment = pointer("amendment")
    if not amendment or amendment["id"] != record["body"]["parent_protocols"]["phase14"]:
        raise ValueError("Managed coverage source differs from Phase 17 registration")
    return stores(amendment)


def coverage(sources, start, end, step, now, *, mark_required=True):
    """Coverage is bounded by source times and the actual observation cutoff."""
    totals = {basis: {"rows": 0, "source_first": None, "source_last_close": None, "last_receipt": None}
              for basis in ("trade", "mark", "funding")}
    paired = corrections = 0
    for source in sources:
        if not (observation.directory(source) / "observations.sqlite3").exists():
            continue
        args = (start.isoformat(), end.isoformat(), now.isoformat())
        with closing(observation.db_open(source, readonly=True)) as db:
            for basis, count, first, last, receipt in db.execute(
                    "SELECT basis,count(*),min(source_at),max(source_close),max(received_at) FROM item "
                    "WHERE source_at>=? AND source_at<? AND received_at<=? GROUP BY basis", args):
                row = totals[basis]
                row["rows"] += count
                row["source_first"] = min(row["source_first"] or first, first)
                row["source_last_close"] = max(row["source_last_close"] or last, last)
                row["last_receipt"] = max(row["last_receipt"] or receipt, receipt)
            paired += db.execute("SELECT count(*) FROM item t JOIN item m ON t.source_at=m.source_at AND m.basis='mark' "
                                 "WHERE t.basis='trade' AND t.source_at>=? AND t.source_at<? AND t.received_at<=? AND m.received_at<=?",
                                 (*args, args[-1])).fetchone()[0]
            corrections += db.execute("SELECT count(*) FROM correction WHERE source_at>=? AND source_at<? AND received_at<=?", args).fetchone()[0]
    expected = max(0, int((end-start)//step))
    return {"start": start.isoformat(), "end_exclusive": end.isoformat(), "expected_per_price_basis": expected,
            "bases": totals, "paired": paired if mark_required else None, "corrections": corrections,
            "trade_gaps": max(0, expected-totals["trade"]["rows"]),
            "mark_gaps": max(0, expected-totals["mark"]["rows"]) if mark_required else None}
