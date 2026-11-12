"""Credential-free Kraken producer supervised by the existing Windows watchdog."""

from contextlib import closing
from pathlib import Path
import os
import shutil
import sqlite3
import subprocess
import sys
from time import sleep

import pandas as pd
import psutil

from lab.config import ROOT as PROJECT
from lab.kraken_paper import catalog
from lab.learning.control import process_identity
from lab.storage import read_json, utc_now, write_json, writer_lock


MIB = 1024**2
THREADS = {key: "1" for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS")}


def _resources(root, *, current=False):
    limits = catalog.LIMITS
    if psutil.virtual_memory().available < limits["min_available_mib"] * MIB:
        return "Available RAM below Kraken paper reserve"
    if shutil.disk_usage(root).free < limits["min_free_disk_mib"] * MIB:
        return "Free disk below Kraken paper reserve"
    if current and psutil.Process().memory_info().rss >= limits["worker_rss_mib"] * MIB:
        return "Kraken producer exceeded its RSS limit"
    return None


def status(*, root=None):
    root = catalog.ROOT if root is None else Path(root)
    heartbeat = root / "producer-heartbeat.json"
    process = root / "producer-process.json"
    pulse = read_json(heartbeat) if heartbeat.is_file() else {}
    record = read_json(process) if process.is_file() else {}
    live = process_identity(record.get("pid"), record.get("created"))
    return {"state": ("RUNNING" if live else "STOPPED") if record else "NOT_STARTED",
            "pid": record.get("pid") if live else None, "heartbeat": pulse}


def _authorized(root, at):
    with closing(catalog.db_open(readonly=True, root=root)) as db:
        catalog.approved_authorization(db, at=at)
        return max(grant.end for grant in catalog.grants(db))


def ensure(*, root=None):
    """Start at most one supervised producer after consent and code pins validate."""
    from lab.kraken_paper import retirement

    root = catalog.ROOT if root is None else Path(root)
    if not (root / "paper.sqlite3").is_file():
        return {"state": "NOT_REGISTERED"}
    if retirement.history(root=root):
        return {"state": retirement.STATE}
    with writer_lock(root / ".producer-dispatch.lock"):
        if retirement.history(root=root):
            return {"state": retirement.STATE}
        now = utc_now()
        if now >= _authorized(root, now):
            return {"state": "SOURCE_WINDOW_ENDED"}
        prior = status(root=root)
        if prior["pid"]:
            return prior
        if reason := _resources(root):
            return {"state": "BLOCKED_RESOURCES", "reason": reason}
        child = subprocess.Popen(
            [sys.executable, "-B", "-X", "utf8", "-m", "lab.kraken_paper", "_produce", "--root", str(root.resolve())],
            cwd=PROJECT, env={**os.environ, **THREADS}, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS,
        )
        try:
            created = psutil.Process(child.pid).create_time()
        except psutil.NoSuchProcess:
            return {"state": "WAITING_PRODUCER", "reason": "Producer exited during launch"}
        write_json(root / "producer-process.json", {"pid": child.pid, "created": created, "started_at": now.isoformat()})
        return {"state": "STARTED", "pid": child.pid}


def run(*, root=None):
    """Run inside the watchdog's unattended task identity with a single writer lock."""
    from lab.kraken_paper import source

    root = catalog.ROOT if root is None else Path(root)
    if not (root / "paper.sqlite3").is_file():
        raise PermissionError("Kraken producer requires approved registrations")
    heartbeat = root / "producer-heartbeat.json"
    with writer_lock(root / ".producer-process.lock"):
        while True:
            now = utc_now()
            outcome = {}
            try:
                if now >= _authorized(root, now):
                    write_json(heartbeat, {"state": "SOURCE_WINDOW_ENDED", "at": now.isoformat()})
                    return
                if reason := _resources(root, current=True):
                    outcome = {"state": "BLOCKED_RESOURCES", "reason": reason}
                    if "RSS" in reason:
                        write_json(heartbeat, {**outcome, "at": now.isoformat()})
                        return
                else:
                    outcome = {"state": "RUNNING", "collection": source.collect_registered(root=root)}
            except PermissionError as exc:
                write_json(heartbeat, {"state": "BLOCKED_PERMISSION", "reason": str(exc), "at": now.isoformat()})
                return
            except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                outcome = {"state": "WAITING_SOURCE", "reason": str(exc)}
            write_json(heartbeat, {**outcome, "at": utc_now().isoformat()})
            finished = utc_now()
            next_minute = finished.floor("min") + pd.Timedelta(minutes=1, seconds=8)
            sleep(max(.1, (next_minute - finished).total_seconds()))
