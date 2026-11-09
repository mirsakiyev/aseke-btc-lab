from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import subprocess
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from lab.config import ROOT as PROJECT
from lab.phase10.worker import process_state
from lab.phase9.data import settled_rates
from lab.phase9.public import funding, load_payload, send
from lab.phase13 import observation, protocol
from lab.phase13.registry import checked, freeze
from lab.phase14.context import FOUR, ROOT, pointer, save
from lab.storage import read_json, utc, utc_now, writer_lock


def scheduler_status():
    ps = Path(os.environ["WINDIR"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    result = subprocess.run([str(ps), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                             str(PROJECT / "phase10.ps1"), "status"], check=True, capture_output=True, timeout=30)
    return json.loads(result.stdout)


def managed_evidence(now=None, scheduler=None):
    now = now if now is not None else utc_now()
    worker, live_and_locked = process_state()
    original = protocol.active()
    enabled = protocol.control()["enabled"]
    scheduler = scheduler if scheduler is not None else scheduler_status()
    native = scheduler["scheduler"]
    launch = worker.get("launch_id")
    label = f"scheduled:{launch}"
    heartbeat = (now - utc(worker["heartbeat"])).total_seconds() if worker.get("heartbeat") else None
    same_task = (native["registration"] == "validated" and native["task_state"] == "Running" and native["session"] == 0
                 and scheduler["identity"]["launch_id"] == launch and worker.get("launcher") == "scheduled")
    owner_receipt = observation.ROOT.parent / "phase10/managed-verification.json"
    direct = native["task_worker_identity_verified"]
    if owner_receipt.exists():
        proof = read_json(owner_receipt)
        direct = direct or (proof["identity"]["launch_id"] == launch and proof["scheduler"]["task_worker_identity_verified"])
    current = []
    amendment = pointer("amendment")
    tail = tail_protocol(amendment, original) if amendment and original else None
    for source in (original, tail):
        if not source:
            continue
        database = observation.directory(source) / "observations.sqlite3"
        if not database.exists():
            continue
        with closing(observation.db_open(source, readonly=True)) as db:
            for row in db.execute("SELECT body FROM cycle WHERE state='COMPLETE' AND json_extract(body,'$.launcher')=? "
                                  "ORDER BY boundary DESC LIMIT 2", (label,)):
                cycle = json.loads(row[0])
                receipts = [r for r in cycle.get("receipts", []) if r["basis"] in {"trade", "mark"} and r["status"] == "RECEIVED"]
                if {r["basis"] for r in receipts} != {"trade", "mark"} or not cycle.get("coverage_complete"):
                    continue
                if any(not utc(cycle["end_exclusive"]) <= utc(r["received_at"]) <= now for r in receipts):
                    continue
                for r in receipts:
                    checked(database.parent / "raw" / (r["raw_sha256"] + ".json"), r["raw_sha256"])
                current.append(cycle)
    current = sorted(current, key=lambda cycle: utc(cycle["end_exclusive"]), reverse=True)[:2]
    advanced = (len(current) == 2 and utc(current[1]["end_exclusive"]) < utc(current[0]["end_exclusive"])
                and (now - utc(current[0]["end_exclusive"])).total_seconds() <= 1800)
    ready = bool(enabled and same_task and live_and_locked and heartbeat is not None and -5 <= heartbeat <= 90 and advanced)
    next_due = read_json(observation.PULSE).get("wake") if observation.PULSE.exists() else None
    return {"state": "TASK_MANAGED_PHASE13_READY" if ready else "WAITING_FOR_TASK_MANAGED_PHASE13_EVIDENCE",
            "pid": worker.get("pid"), "launch_id": launch, "heartbeat_age_seconds": heartbeat,
            "registered_task_running_in_session_zero": same_task, "process_identity_and_writer_lock": live_and_locked,
            "direct_windows_account_owner_verified": bool(direct), "owner_query_result": native["owner_query"],
            "ancestry_authenticated_capture": bool(current), "enabled": enabled, "checkpoint_advanced": advanced,
            "cycles": current, "next_due_utc": next_due}


def register():
    from lab.phase13.diagnostics import gates_off
    from lab.phase14.context import admit
    from lab.phase14.features import freeze_readiness

    with writer_lock(ROOT / ".amendment.lock"):
        if existing := pointer("amendment"):
            return existing
        seed = admit()
        features = freeze_readiness(seed)
        collection = managed_evidence()
        if features["body"]["state"] != "BOTH_FEATURES_READY" or collection["state"] != "TASK_MANAGED_PHASE13_READY":
            return {"state": "REGISTRATION_WAITING_FOR_READINESS", "features": features["body"], "collection": collection}
        if not gates_off():
            raise ValueError("Continuous training must remain off for the amended study")
        original, registry = protocol.active(), freeze()
        if (seed["body"]["original_protocol"] != original["id"] or seed["body"]["registry"] != registry["id"]
                or original["body"]["registry"] != registry["id"]):
            raise ValueError("Seed, protocol and candidate registry do not agree")
        now = utc_now()
        start = now.floor(FOUR) + FOUR
        end, review = start + pd.Timedelta(days=90), start + pd.Timedelta(days=7)
        toronto = ZoneInfo("America/Toronto")
        original_body = original["body"]
        rules = {**original_body["rules"], "feature_history": "Versioned FEATURE_CONTEXT_ONLY KuCoin seed. Bridge is observed trade only.",
                 "swing_readiness_warning": "The admitted pre-start seed supplies 201 contiguous closed daily bars. Gaps still block readiness.",
                 "metadata": "Dated captured contract and tier snapshots. Receipt freshness and regime changes are audited.",
                 "funding_receipt_grace_seconds": 7200}
        clocks = ["start", "end_exclusive", "technical_review_due"]
        return save("amendment", {"registration_version": "phase14-feature-context-amendment-v1",
                    "type": "PREREGISTERED_FUTURE_DATA_OFFLINE_REPLAY", "original_protocol": original["id"],
                    "original_dates_utc": {key: original_body[key] for key in clocks},
                    "original_dates_toronto": {key: utc(original_body[key]).tz_convert(toronto).isoformat() for key in clocks},
                    "registry": registry["id"], "seed": seed["id"], "readiness": features["id"],
                    "registered_at": now.isoformat(), "start": start.isoformat(), "end_exclusive": end.isoformat(),
                    "technical_review_due": review.isoformat(), "display_timezone": str(toronto),
                    "dates_toronto": dict(zip(clocks, (t.tz_convert(toronto).isoformat() for t in (start, end, review)))),
                    "reason": "Original horizon was shorter than the frozen Swing context. Amendment is readiness-based.",
                    "start_rule": "First strict future UTC 4h boundary after admission, readiness verification and registration",
                    "candidates": original_body["candidates"], "instrument": original_body["instrument"], "rules": rules,
                    "acquisition": original_body["acquisition"], "collection_at_registration": collection,
                    "collection_continuation": "Reuse original receipts through its exclusive end. Use a separately bounded tail under the same worker.",
                    "approval": "No validated model", "publication": "RESEARCH_ONLY", "execution": "MANUAL_OUTSIDE_LAB",
                    "historical_estimator_fits": 0, "warmup_outcomes": 0})


def tail_protocol(amendment, original):
    body = amendment["body"]
    first, end = utc(original["body"]["end_exclusive"]), utc(body["end_exclusive"])
    if end <= first:
        return None
    steps = int((end - first).ceil("15min") / pd.Timedelta(minutes=15)) + 1
    return {"id": amendment["id"], "pulse_id": original["id"], "storage_root": ROOT,
            "body": {**body, "start": max(first, utc(body["start"])).isoformat(),
                     "acquisition": {**body["acquisition"], "max_requests": min(steps * 9, 22500)}}}


def original_complete(original):
    if not (observation.directory(original) / "observations.sqlite3").exists():
        return False
    with closing(observation.db_open(original, readonly=True)) as db:
        last = db.execute("SELECT max(boundary) FROM cycle WHERE state IN ('COMPLETE','PARTIAL_FINAL')").fetchone()[0]
    return last is not None and utc(last) >= utc(original["body"]["end_exclusive"])


def funding_closeout(amendment, tail, now):
    from lab.phase14.outcomes import audit

    body = tail["body"]
    end = utc(body["end_exclusive"])
    due = end + pd.Timedelta(seconds=body["rules"]["funding_receipt_grace_seconds"])
    park = end + pd.Timedelta(days=3650)
    if now < due:
        observation.schedule(tail, due)
        return {"state": "WAITING_FOR_FINAL_FUNDING_RECEIPTS", "next_due": due.isoformat()}
    audit_result = audit(amendment, now)["funding"]
    unresolved = {utc(row["scheduled_at"]) for row in audit_result["slots"] if row["rate"] is None}
    policy = body["acquisition"]
    left = max(utc(amendment["body"]["start"]), end-pd.Timedelta(hours=policy["funding_overlap_hours"]))
    if not unresolved.intersection(pd.date_range(left, end, freq="1min", inclusive="left")) or now >= end+pd.Timedelta(days=1):
        observation.schedule(tail, park)
        return {"state": "FUNDING_CLOSEOUT_FINISHED", "funding": audit_result}
    root = observation.directory(tail)
    with writer_lock(root / ".capture.lock"), closing(observation.db_open(tail)) as db:
        boundary = end.isoformat()+"/funding-closeout"
        prior = [json.loads(row[0]) if row[0] else {} for row in db.execute(
            "SELECT receipt FROM request WHERE boundary=? ORDER BY id", (boundary,))]
        used = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
        if (len(prior) >= 2 or (prior and prior[-1].get("status") in {"RECEIVED", "ACCESS_RESTRICTED"})
                or observation.counters(db)["requests"] >= policy["max_requests"]
                or used + policy["max_response_bytes"] >= policy["max_retained_bytes"]
                or shutil.disk_usage(root).free < policy["min_free_disk_bytes"]):
            observation.schedule(tail, park)
            return {"state": "FUNDING_CLOSEOUT_FINISHED", "funding": audit_result}
        spec = funding("amended-funding-closeout", left, end)
        request = db.execute("INSERT INTO request(boundary,basis,spec,reserved) VALUES (?,?,?,?)",
                             (boundary, "funding", observation.encode(spec), now.isoformat())).lastrowid
        db.commit()
        with httpx.Client(timeout=policy["timeout_seconds"], follow_redirects=False) as client:
            evidence = send(client, spec, policy["max_response_bytes"], root / "raw")
        if evidence["status"] == "RECEIVED":
            try:
                frame = settled_rates(load_payload(root, evidence), spec)
                pending = frame.loc[frame.settlement_at.dt.round("min").isin(unresolved)]
                observation.store_frame(db, "funding", pending, evidence, policy)
                evidence["rows"] = len(pending)
            except (KeyError, ValueError) as exc:
                evidence.update(status="INVALID_RESPONSE", error=str(exc))
        db.execute("UPDATE request SET receipt=? WHERE id=?", (observation.encode(evidence), request))
        db.commit()
        retry = evidence["status"] not in {"RECEIVED", "ACCESS_RESTRICTED"} and not prior
        wake = max(now+pd.Timedelta(minutes=1), utc(evidence.get("resume_after", now))) if retry else park
        observation.schedule(tail, wake)
        return {"state": "FUNDING_CLOSEOUT_RECORDED", "receipt": evidence}


def managed_observe(launcher):
    amendment = pointer("amendment")
    if not amendment:
        return observation.observe(launcher=launcher)
    original = protocol.active()
    if amendment["body"]["original_protocol"] != original["id"]:
        raise ValueError("Amended collection cannot attach to an unrelated Phase 13 protocol")
    tail = tail_protocol(amendment, original)
    if not tail:
        return observation.observe(launcher=launcher)
    if original_complete(original):
        now = utc_now()
        if original_complete(tail) and protocol.control()["enabled"]:
            return funding_closeout(amendment, tail, now)
        end = utc(tail["body"]["end_exclusive"])
        closeout = end + pd.Timedelta(seconds=tail["body"]["rules"]["funding_receipt_grace_seconds"])
        return observation.observe(launcher=launcher, study=tail, after_horizon=closeout)
    first = utc(tail["body"]["start"])
    step = pd.Timedelta(minutes=tail["body"]["acquisition"]["step_minutes"])
    wake = first.floor(step) + step + pd.Timedelta(seconds=tail["body"]["acquisition"]["close_grace_seconds"])
    return observation.observe(launcher=launcher, after_horizon=wake)
