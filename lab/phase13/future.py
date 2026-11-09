from contextlib import closing
from dataclasses import asdict
import gzip
import json
import shutil
from uuid import uuid4

import pandas as pd

from lab.config import fingerprint
from lab.phase9.ideas import current_rules
from lab.phase13.observation import db_open, directory, encode, target_boundary
from lab.phase13.protocol import active
from lab.phase13.registry import ROOT, checked
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


def expected_funding(metadata, start, end):
    contract = metadata.get("contract", {})
    ms = contract.get("currentFundingRateGranularity") or contract.get("fundingRateGranularity")
    anchor = contract.get("nextFundingRateDateTime")
    if not ms or not anchor or int(ms) <= 0:
        return None
    cycle = pd.Timedelta(milliseconds=int(ms))
    reference = pd.Timestamp(int(anchor), unit="ms", tz="UTC")
    offset = (start - reference) % cycle
    first = start if not offset else start + cycle - offset
    return pd.date_range(first, end, freq=cycle, inclusive="left")


def technical(now=None):
    protocol = active()
    if not protocol:
        return {"state": "UNREGISTERED"}
    now = now if now is not None else utc_now()
    body = protocol["body"]
    start, end = utc(body["start"]), utc(body["end_exclusive"])
    due = max(start, target_boundary(now, protocol))
    expected = int((due - start).total_seconds() // 60)
    evaluation_allowed = due >= end
    report = {"protocol": protocol["id"], "at": now.isoformat(), "expected_through": due.isoformat(),
              "first_week_review_due": body["technical_review_due"], "primary_end": body["end_exclusive"],
              "technical_review": "DUE" if now >= utc(body["technical_review_due"]) else "WAITING_FOR_FIRST_WEEK",
              "state": "WAITING_FOR_PRIMARY_HORIZON" if not evaluation_allowed else "WAITING_FOR_QUALIFIED_OBSERVATIONS",
              "required_price_minutes_each": expected, "new_future_evaluations": 0,
              "historical_archive_requests_reused": 0, "sealed_holdout_accessed": False,
              "profile_limits": {"day": "Needs frozen causal warm-up and mature outcomes",
                                 "swing": "Needs 201 complete daily context bars. Registered primary window is 90 days."}}
    database = directory(protocol) / "observations.sqlite3"
    if not database.exists():
        return report
    with closing(db_open(protocol, readonly=True)) as db:
        now_iso, stop_iso = start.isoformat(), due.isoformat()
        report["observed_price_minutes"] = {basis: db.execute(
            "SELECT count(*) FROM item WHERE basis=? AND source_at>=? AND source_at<?", (basis, now_iso, stop_iso)).fetchone()[0]
            for basis in ("trade", "mark")}
        report["price_gaps"] = {key: expected - value for key, value in report["observed_price_minutes"].items()}
        observed = [utc(row[0]) for row in db.execute("SELECT source_at FROM item WHERE basis='funding' AND "
                                                        "source_at>=? AND source_at<? ORDER BY source_at", (now_iso, stop_iso))]
        corrections = db.execute("SELECT count(*) FROM correction WHERE source_at>=? AND source_at<?", (now_iso, stop_iso)).fetchone()[0]
        snapshots = [{"record": body["instrument"]["metadata_at_registration"], "first_available": body["registered_at"]}]
        snapshots += [{"record": json.loads(row[0]), "first_available": row[1]} for row in db.execute(
            "SELECT body,received_at FROM metadata ORDER BY as_of")]
    report["corrections"] = corrections
    report["observed_settlements"] = len(observed)
    # Audit the registration cadence against every subsequently received dated rule.
    registered_slots = expected_funding(snapshots[0]["record"], start, due)
    changed = [item["record"].get("as_of") for item in snapshots
               if registered_slots is not None and not registered_slots.equals(expected_funding(item["record"], start, due))]
    eligible_metadata = [item for item in snapshots if utc(item["first_available"]) < due]
    available = [utc(item["first_available"]) for item in eligible_metadata]
    stale_gap = max(((b - utc(item["record"]["as_of"])).total_seconds()
                     for item, b in zip(eligible_metadata, available[1:] + [due])), default=0.)
    report["metadata_snapshots"] = len(snapshots)
    report["metadata_availability_max_age_seconds"] = stale_gap
    report["funding_regime_changes_requiring_review"] = changed
    pinned = body["instrument"]["contract"]
    config = {"ideas": {"modeled_liquidation_fee": pinned["liquidation_fee_rate"]}}
    mismatched = []
    for item in snapshots:
        try:
            matches = asdict(current_rules("FUTURES", item["record"], config)) == pinned
        except (KeyError, ValueError, StopIteration):
            matches = False
        if not matches:
            mismatched.append(item["record"].get("as_of"))
    report["contract_changes_requiring_review"] = mismatched
    if registered_slots is not None:
        actual = pd.DatetimeIndex(observed, tz="UTC")
        nominal = actual.round("min")
        offsets_ok = (abs(actual - nominal) <= pd.Timedelta(seconds=5)).all()
        missing = registered_slots.difference(nominal)
        extra = nominal.difference(registered_slots)
        report["expected_funding_slots"] = len(registered_slots)
        report["missing_funding_slots"] = [row.isoformat() for row in missing]
        report["unexpected_funding_slots"] = [row.isoformat() for row in extra]
        report["funding_slots_aligned"] = bool(offsets_ok and not len(missing) and not len(extra) and nominal.is_unique)
    if evaluation_allowed:
        flags = []
        if any(report["price_gaps"].values()):
            flags.append("Complete paired native price accounting has not been observed")
        if not report.get("funding_slots_aligned") or changed:
            flags.append("Dated funding slot coverage needs verification")
        if corrections:
            flags.append("Point-in-time source corrections require explicit reconciliation")
        if mismatched:
            flags.append("Observed contract rules depart from the fixed scenario")
        if stale_gap > 86400:
            flags.append("Metadata receipt coverage exceeds the frozen 24-hour freshness interval")
        report["remaining_requirements"] = flags
        if not flags:
            report["state"] = "READY_FOR_FROZEN_OFFLINE_SNAPSHOT"
    return report


def prepare_evaluation():
    report = technical()
    if report["state"] != "READY_FOR_FROZEN_OFFLINE_SNAPSHOT":
        return report
    protocol = active()
    root = directory(protocol)
    bounds = protocol["body"]
    with writer_lock(root / ".capture.lock"), closing(db_open(protocol, readonly=True)) as db:
        report = technical()
        if report["state"] != "READY_FOR_FROZEN_OFFLINE_SNAPSHOT":
            return report
        pointers = {}
        dest = ROOT / "future-snapshots" / protocol["id"]
        pointer = dest / "completed.json"
        if pointer.exists():
            existing = read_json(pointer)
            saved = checked(existing["path"], existing["sha256"])
            for asset in read_json(saved)["body"]["assets"].values():
                checked(asset["file"], asset["sha256"])
            return {**existing, "reused": True}
        if shutil.disk_usage(root).free < bounds["acquisition"]["min_free_disk_bytes"]:
            return {**report, "state": "BLOCKED_FREE_DISK"}
        dest.mkdir(parents=True, exist_ok=True)
        checked_raw = set()
        for basis in ("trade", "mark", "funding"):
            source = db.execute("SELECT source_at,source_close,received_at,raw_sha,body,timing FROM item "
                                "WHERE basis=? AND source_at>=? AND source_at<? ORDER BY source_at",
                                (basis, bounds["start"], bounds["end_exclusive"]))
            temporary = dest / f".{uuid4().hex}.tmp"
            count = 0
            try:
                with temporary.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as output:
                    for at, close, receipt, digest, payload, timing in source:
                        if digest not in checked_raw:
                            checked(root / "raw" / f"{digest}.json", digest)
                            checked_raw.add(digest)
                        entry = {"source_at": at, "source_close": close, "received_at": receipt, "raw_sha256": digest,
                                 "observation": json.loads(payload), "timing": timing}
                        output.write((encode(entry) + "\n").encode())
                        count += 1
                digest = file_hash(temporary)
                path = dest / f"{basis}-{digest}.jsonl.gz"
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
            pointers[basis] = {"file": str(path), "sha256": digest, "rows": count}
        body = {"protocol": protocol["id"], "registry": bounds["registry"], "interval": [bounds["start"], bounds["end_exclusive"]],
                "scope": "REGISTERED_RECEIPT_OFFLINE_REPLAY", "assets": pointers,
                "metadata": [{"as_of": a, "received_at": b, "record": json.loads(c)}
                             for a,b,c in db.execute("SELECT as_of,received_at,body FROM metadata ORDER BY as_of")],
                "funding_slots": report["expected_funding_slots"], "future_evaluations": 0}
        identifier = fingerprint(body)
        path = dest / f"snapshot-{identifier}.json"
        if not path.exists():
            write_json(path, {"id": identifier, "body": body})
        result = {"state": "FROZEN_OFFLINE_SNAPSHOT", "snapshot": identifier, "path": str(path),
                  "sha256": file_hash(path), "future_evaluations": 0}
        write_json(pointer, result)
        return result
