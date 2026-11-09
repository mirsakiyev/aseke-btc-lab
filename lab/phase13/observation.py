from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sqlite3
import time

import httpx
import pandas as pd

from lab.phase9.data import candles, settled_rates
from lab.phase9.public import funding, kline, load_payload, send
from lab.phase13.protocol import active, control, current_metadata
from lab.phase13.registry import ROOT
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


PULSE = ROOT / "observation-schedule.json"
SCHEMA = """
CREATE TABLE IF NOT EXISTS request(id INTEGER PRIMARY KEY, boundary TEXT, basis TEXT, spec TEXT, reserved TEXT, receipt TEXT);
CREATE TABLE IF NOT EXISTS cycle(boundary TEXT PRIMARY KEY, lo TEXT, attempt INTEGER, state TEXT, body TEXT);
CREATE TABLE IF NOT EXISTS item(basis TEXT, source_at TEXT, source_close TEXT, received_at TEXT, raw_sha TEXT,
    body TEXT, timing TEXT, PRIMARY KEY(basis,source_at));
CREATE TABLE IF NOT EXISTS correction(basis TEXT, source_at TEXT, received_at TEXT, raw_sha TEXT, body TEXT,
    UNIQUE(basis,source_at,raw_sha));
CREATE TABLE IF NOT EXISTS metadata(as_of TEXT PRIMARY KEY, received_at TEXT, body TEXT);
"""


def encode(data):
    return json.dumps(data, default=str, sort_keys=True, allow_nan=False, separators=(",", ":"))


def directory(protocol):
    return protocol.get("storage_root", ROOT) / "observations" / protocol["id"]


def db_open(protocol, readonly=False):
    target = directory(protocol) / "observations.sqlite3"
    if readonly:
        return sqlite3.connect(target.resolve().as_uri() + "?mode=ro", uri=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(target)
    db.executescript(SCHEMA)
    return db


def target_boundary(now, protocol):
    body = protocol["body"]
    policy = body["acquisition"]
    ready = now - pd.Timedelta(seconds=policy["close_grace_seconds"])
    end = utc(body["end_exclusive"])
    return end if ready >= end else ready.floor(f"{policy['step_minutes']}min")


def scheduled_due(now=None):
    if not control()["enabled"] or not (protocol := active()):
        return False
    now = now if now is not None else utc_now()
    if target_boundary(now, protocol) <= utc(protocol["body"]["start"]):
        return False
    pulse = read_json(PULSE) if PULSE.exists() else {}
    if pulse.get("protocol") == protocol["id"] and pulse.get("wake"):
        return now >= utc(pulse["wake"])
    return True


def schedule(protocol, wake):
    write_json(PULSE, {"protocol": protocol.get("pulse_id", protocol["id"]), "wake": wake.isoformat()})


def counters(db):
    return {"requests": db.execute("SELECT count(*) FROM request").fetchone()[0],
            "cycles": dict(db.execute("SELECT state,count(*) FROM cycle GROUP BY state")),
            "observations": dict(db.execute("SELECT basis,count(*) FROM item GROUP BY basis")),
            "corrections": db.execute("SELECT count(*) FROM correction").fetchone()[0],
            "metadata_snapshots": db.execute("SELECT count(*) FROM metadata").fetchone()[0]}


def store_frame(db, basis, frame, evidence, policy):
    funding_rows = basis == "funding"
    for row in frame.to_dict("records"):
        stamp = row["settlement_at" if funding_rows else "open_at"]
        close = stamp if funding_rows else row["close_at"]
        receipt = evidence["received_at"]
        if close > utc(receipt):
            raise ValueError("Source event arrived before its declared close")
        elapsed = (utc(receipt) - close).total_seconds()
        timing = ("SETTLED_RECEIPT" if funding_rows else "ON_CADENCE" if
                  elapsed <= policy["step_minutes"] * 60 + policy["decision_max_receipt_delay_seconds"] else "CATCH_UP")
        text = encode(row)
        found = db.execute("SELECT body FROM item WHERE basis=? AND source_at=?", (basis, stamp.isoformat())).fetchone()
        if not found:
            db.execute("INSERT INTO item VALUES (?,?,?,?,?,?,?)",
                       (basis, stamp.isoformat(), close.isoformat(), receipt, evidence["raw_sha256"], text, timing))
        elif found[0] != text:
            db.execute("INSERT OR IGNORE INTO correction VALUES (?,?,?,?,?)",
                       (basis, stamp.isoformat(), receipt, evidence["raw_sha256"], text))


def covered(db, basis, start, end):
    return db.execute("SELECT count(*) FROM item WHERE basis=? AND source_at>=? AND source_at<?",
                      (basis, start.isoformat(), end.isoformat())).fetchone()[0]


def save_metadata(db, now):
    rules = current_metadata()
    if not rules.get("as_of") or utc(rules["as_of"]) > now:
        return {"state": "METADATA_MISSING", "record": None}
    db.execute("INSERT OR IGNORE INTO metadata VALUES (?,?,?)", (rules["as_of"], now.isoformat(), encode(rules)))
    return {"state": "CURRENT" if now - utc(rules["as_of"]) <= pd.Timedelta(hours=24) else "STALE",
            "record": rules}


def observe(*, launcher="operator", transport=None, now=None, study=None, after_horizon=None):
    protocol = study if study is not None else active()
    if not protocol:
        raise ValueError("Register the future-data study first")
    if not control()["enabled"]:
        return {"state": "COLLECTION_OFF", "protocol": protocol["id"]}
    body = protocol["body"]
    policy = body["acquisition"]
    now = now if now is not None else utc_now()
    start, horizon = utc(body["start"]), utc(body["end_exclusive"])
    due = target_boundary(now, protocol)
    step = pd.Timedelta(minutes=policy["step_minutes"])
    grace = pd.Timedelta(seconds=policy["close_grace_seconds"])
    park = after_horizon if after_horizon is not None else horizon + pd.Timedelta(days=3650)
    if due <= start:
        schedule(protocol, start.ceil(step) + grace if start.ceil(step) > start else start + step + grace)
        return {"state": "WAITING_FOR_FIRST_CLOSED_WINDOW", "protocol": protocol["id"], "start": body["start"]}
    root = directory(protocol)
    with writer_lock(root / ".capture.lock"), closing(db_open(protocol)) as db:
        last = db.execute("SELECT max(boundary) FROM cycle WHERE state IN ('COMPLETE','PARTIAL_FINAL')").fetchone()[0]
        if last and utc(last) >= horizon:
            schedule(protocol, park)
            return {"state": "PRIMARY_COLLECTION_ENDED", **counters(db)}
        if last and utc(last) >= due:
            schedule(protocol, min(due + step, horizon) + grace)
            return {"state": "WAITING_FOR_NEXT_WINDOW", **counters(db)}
        existing = db.execute("SELECT boundary,lo,attempt,state,body FROM cycle WHERE state IN "
                              "('RETRY','ACTIVE','ACCESS_RESTRICTED') ORDER BY boundary LIMIT 1").fetchone()
        if existing:
            boundary, left, attempt, phase, document = existing
            if phase == "ACCESS_RESTRICTED":
                return {"state": phase, **counters(db)}
            due, left = utc(boundary), utc(left)
            old = json.loads(document)
            if old.get("retry_after") and now < utc(old["retry_after"]):
                return {"state": "RETRY_AFTER", **old}
        else:
            cursor = utc(last) if last else start
            left = max(cursor, due - pd.Timedelta(minutes=policy["max_catchup_minutes"]))
            attempt, old = 0, {"skipped_prior_gap": [cursor.isoformat(), left.isoformat()] if left > cursor else None}
        boundary = due.isoformat()
        if attempt >= policy["max_attempts_per_boundary"]:
            db.execute("UPDATE cycle SET state='PARTIAL_FINAL' WHERE boundary=?", (boundary,))
            db.commit()
            schedule(protocol, min(due + step, horizon) + grace if due < horizon else park)
            return {"state": "PARTIAL_FINAL", "reason": "Frozen boundary attempts consumed", **counters(db)}
        saved_meta = save_metadata(db, now)
        expected = int((due - left).total_seconds() // 60)
        funding_due = (not db.execute("SELECT 1 FROM request WHERE basis='funding' AND receipt IS NOT NULL LIMIT 1").fetchone()
                       or due.minute % policy["funding_poll_minutes"] == 0 or due == horizon)
        funding_ok = old.get("funding_query_success", False)
        attempt += 1
        report = {"protocol": protocol["id"], "launcher": launcher, "left": left.isoformat(),
                  "end_exclusive": boundary, "attempt": attempt, "metadata": saved_meta["state"],
                  "skipped_prior_gap": old.get("skipped_prior_gap"), "funding_query_success": funding_ok, "receipts": []}
        db.execute("INSERT OR REPLACE INTO cycle VALUES (?,?,?,?,?)", (boundary, left.isoformat(), attempt, "ACTIVE", encode(report)))
        db.commit()
        pending = [(basis, kline(basis, "XBTUSDTM", "FUTURES", market, "1min", left, due))
                   for basis, market in (("trade", "TRADE"), ("mark", "MARK_PRICE"))
                   if covered(db, basis, left, due) < expected]
        if funding_due and not funding_ok:
            pending.append(("funding", funding("funding", max(start, due - pd.Timedelta(hours=policy["funding_overlap_hours"])), due)))
        stop = None
        used = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
        with httpx.Client(timeout=policy["timeout_seconds"], follow_redirects=False, transport=transport) as client:
            for basis, spec in pending[:policy["max_requests_per_cycle"]]:
                if (counters(db)["requests"] >= policy["max_requests"]
                        or used + 2 * policy["max_response_bytes"] >= policy["max_retained_bytes"]
                        or shutil.disk_usage(root).free < policy["min_free_disk_bytes"]):
                    stop = "RESOURCE_LIMIT"
                    break
                time.sleep(policy["min_spacing_seconds"])
                request_id = db.execute("INSERT INTO request(boundary,basis,spec,reserved) VALUES (?,?,?,?)",
                                        (boundary, basis, encode(spec), utc_now().isoformat())).lastrowid
                db.commit()
                evidence = send(client, spec, policy["max_response_bytes"], root / "raw")
                if digest := evidence.get("raw_sha256"):
                    used += (root / "raw" / f"{digest}.json").stat().st_size + 4096
                try:
                    if evidence["status"] == "RECEIVED":
                        payload = load_payload(root, evidence)
                        frame = (settled_rates(payload, spec) if basis == "funding" else
                                 candles(payload, spec, utc(evidence["received_at"]),
                                         body["instrument"]["contract"]["base_per_contract"]))
                        store_frame(db, basis, frame, evidence, policy)
                        if basis == "funding":
                            funding_ok = True
                        evidence["rows"] = len(frame)
                except (KeyError, ValueError) as exc:
                    evidence.update(status="INVALID_RESPONSE", error=str(exc))
                db.execute("UPDATE request SET receipt=? WHERE id=?", (encode(evidence), request_id))
                db.commit()
                report["receipts"].append({"basis": basis, "request": request_id, **evidence})
                if evidence["status"] in ("RATE_LIMITED", "ACCESS_RESTRICTED"):
                    stop = evidence["status"]
                    if evidence.get("resume_after"):
                        report["retry_after"] = evidence["resume_after"]
                    break
        report["funding_query_success"] = funding_ok
        report["expected_price_minutes"] = expected
        report["received"] = {basis: covered(db, basis, left, due) for basis in ("trade", "mark")}
        report["coverage_complete"] = all(count == expected for count in report["received"].values())
        if stop == "ACCESS_RESTRICTED":
            phase = stop
            wake = horizon + pd.Timedelta(days=3650)
        elif report["coverage_complete"] and (funding_ok or not funding_due):
            phase, wake = "COMPLETE", min(due + step, horizon) + grace if due < horizon else park
        elif attempt >= policy["max_attempts_per_boundary"] or stop == "RESOURCE_LIMIT":
            phase, wake = "PARTIAL_FINAL", max(now + pd.Timedelta(seconds=60), min(due + step, horizon) + grace)
        else:
            phase = "RETRY"
            wake = max(now + pd.Timedelta(seconds=60 * attempt), utc(report.get("retry_after", now)))
        report.update(state=phase, stop_reason=stop, recorded_at=utc_now().isoformat())
        db.execute("UPDATE cycle SET state=?,body=? WHERE boundary=?", (phase, encode(report), boundary))
        db.commit()
        # SQLite retains every request receipt even when a cycle retries.
        schedule(protocol, wake)
        report.update(counters(db))
        write_json(root / "last-cycle.json", report)
        return report


def status():
    if not (protocol := active()):
        return {"state": "UNREGISTERED"}
    root = directory(protocol)
    body = protocol["body"]
    result = {"protocol": protocol["id"], "enabled": control()["enabled"],
              "start": body["start"], "end_exclusive": body["end_exclusive"],
              "technical_review_due": body["technical_review_due"], "future_evaluations": 0,
              "warmup": "Day needs post-registration causal warm-up. Swing requires 201 days, beyond the 90-day primary horizon.",
              "state": "REGISTERED_WAITING_FOR_CYCLE"}
    if (root / "observations.sqlite3").exists():
        with closing(db_open(protocol, readonly=True)) as db:
            result.update(counters(db))
            latest = db.execute("SELECT state,body FROM cycle ORDER BY boundary DESC LIMIT 1").fetchone()
            managed = db.execute("SELECT body FROM cycle WHERE state='COMPLETE' AND "
                                 "json_extract(body, '$.launcher') LIKE 'scheduled:%' ORDER BY boundary DESC LIMIT 1").fetchone()
        if latest:
            result["last_cycle"] = {**json.loads(latest[1]), "state": latest[0]}
            result["state"] = latest[0]
        if managed:
            result["last_managed_cycle"] = json.loads(managed[0])
    return result
