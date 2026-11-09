from hashlib import sha256
from pathlib import Path
import sys

import pandas as pd
import psutil

from lab.config import ROOT, fingerprint
from lab.phase2 import store
from lab.phase2.collector import GRACE, status
from lab.phase6.design import RECEIPT_HORIZON
from lab.storage import utc, utc_now, writer_lock


ACCOUNT_FIELDS = ("cash", "position", "pending_entry", "unresolved", "entry_fills", "closed_trades")


def locked():
    try:
        with writer_lock(store.FORWARD / ".worker.lock"):
            return False
    except RuntimeError:
        return True


def checkpoint(watermark=None):
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        experiment = store.get(db, "experiment")
        latest = db.execute("SELECT coalesce(max(id), 0) FROM events").fetchone()[0]
        through = latest if watermark is None else watermark
        digest = sha256()
        for row in db.execute("SELECT * FROM events WHERE id<=? ORDER BY id", (through,)):
            digest.update(store.dump(tuple(row)).encode("utf-8"))
        new_events = db.execute("""
            SELECT kind, json_extract(body, '$.observation_quality') AS quality, count(*) AS count
            FROM events WHERE id>? GROUP BY kind, quality ORDER BY kind, quality
        """, (through,)).fetchall()
        return {
            "at": utc_now(), "experiment": experiment, "event_watermark": latest, "hashed_through": through,
            "journal_prefix_sha256": digest.hexdigest(), "accounts": {
                key: {field: store.get_account(db, key)[field] for field in ACCOUNT_FIELDS}
                for key in experiment["profile_hashes"]
            }, "feeds": store.feed_status(db), "new_event_kinds_and_observation_quality": [dict(row) for row in new_events],
        }


def process_identity(worker):
    if not worker["process_alive"]:
        return None
    try:
        p = psutil.Process(worker["pid"])
        if p.create_time() != worker["process_created"]:
            return None
        parent = p.parent()
        cwd, command, exe = Path(p.cwd()), p.cmdline(), Path(p.exe())
        launcher = parent.exe() if parent else None
        expected = ROOT / ".venv" / "Scripts" / "python.exe"
        arguments = command[3:] if command[1:3] == ["-X", "utf8"] else command[1:]
        mode_matches = (("--managed" in arguments) == (worker.get("launcher") == "scheduled")
                        and ("--shadow" in arguments) == worker["shadow_requested"])
        token_matches = ("--token" not in arguments or
                         arguments[arguments.index("--token") + 1:][:1] == [worker["token"]])
        identity_matches = (cwd == ROOT and arguments[:3] == ["-m", "lab.phase2", "collect"] and mode_matches
                            and token_matches
                            and (exe == expected or (launcher and Path(launcher) == expected
                                                     and exe == Path(sys._base_executable))))
        return {"pid": p.pid, "owner": p.username(), "created_epoch": p.create_time(), "cwd": str(cwd),
                "executable": str(exe), "command": command, "parent_pid": parent.pid if parent else None,
                "parent_executable": launcher, "expected_launch_identity": bool(identity_matches)}
    except (OSError, psutil.Error):
        return None


def health():
    now = utc_now()
    worker = status()
    identity = process_identity(worker)
    worker_lock = locked()
    heartbeat = worker.get("heartbeat") or {}
    heartbeat_age = (now - utc(heartbeat["received_at"])).total_seconds() if heartbeat.get("received_at") else None
    running = worker["process_alive"]
    paused = worker["intentionally_paused"]
    if paused:
        process_state = "stopping_intentionally" if running else "intentionally_stopped"
    elif running:
        process_state = "status_stale" if worker["runner_stale"] else worker["state"]
    elif worker.get("storage_limited"):
        process_state = "storage_budget_stop"
    elif worker_lock:
        process_state = "lock_held_without_matching_status"
    elif worker["state"] in ("running", "starting", "degraded", "failed"):
        process_state = "process_failed"
    else:
        process_state = worker["state"]
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        experiment = store.get(db, "experiment")
        feeds = store.feed_status(db)
        receipt_counts = db.execute("""
            SELECT seconds, count(*) AS batches FROM responses
            WHERE received_at>=? AND problem IS NULL AND accepted>0 GROUP BY seconds
        """, (utc(worker["started_at"]).isoformat() if worker.get("started_at") else now.isoformat(),)).fetchall()
        batches = {str(row["seconds"]): row["batches"] for row in receipt_counts}
        accounts = {row["profile"]: {field: store.get_account(db, row["profile"]).get(field) for field in ACCOUNT_FIELDS}
                    for row in db.execute("SELECT profile FROM accounts")}
    for seconds, feed in feeds.items():
        step = pd.Timedelta(seconds=int(seconds))
        due = (now - RECEIPT_HORIZON).floor(step)
        observed, scanned = feed["last_observed_close"], feed["scanned_through"]
        stalled = scanned is None or scanned < due
        feed.update(latest_close_with_elapsed_horizon=due, successful_batches_since_worker_start=batches.get(seconds, 0),
                    next_collection_due_utc=(now - GRACE).floor(step) + step + GRACE,
                    progress=("inactive" if not running or paused else "scan_stalled" if stalled else
                              "latest_due_candle_missing" if observed is None or observed < due else "current"))
    fields = ("version", "observation_start_utc", "profile_hashes", "model_versions")
    return {
        "at": now, "process_state": process_state, "worker": worker, "process": identity, "worker_lock_held": worker_lock,
        "uptime_seconds": (now - utc(worker["started_at"])).total_seconds() if running else None,
        "heartbeat_state": ("inactive" if not running else "healthy" if heartbeat.get("healthy")
                            and heartbeat_age is not None and heartbeat_age < 15 else "stale_or_missing"),
        "heartbeat_age_seconds": heartbeat_age, "feeds": feeds, "accounts": accounts,
        "experiment_id": fingerprint({key: experiment[key] for key in fields})[:24] if experiment else None,
        "experiment": experiment, "launcher_executable": str(ROOT / ".venv" / "Scripts" / "python.exe"),
        "python_version": sys.version.split()[0], "database": str(store.DATABASE),
        "forward_bytes": store.budget(), "forward_budget_bytes": store.STORE_LIMIT,
    }
