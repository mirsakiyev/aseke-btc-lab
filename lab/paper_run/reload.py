"""Verified handoff from a flat predecessor runner to its amended source code."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

import pandas as pd
import psutil

from lab.storage import read_json, utc, utc_now, writer_lock


ARGS = ["-X", "utf8", "-m", "lab.paper_run", "run"]
START_TOLERANCE = pd.Timedelta(seconds=5)
HEARTBEAT_AGE = pd.Timedelta(seconds=45)


def _blocked(reason, **details):
    return {"state": "RUNNER_RELOAD_BLOCKED", "reason": reason, **details}


def _checked_process(process, project, executable_paths, user):
    return (process.username().casefold() == user.casefold()
            and Path(process.exe()).resolve() in executable_paths
            and Path(process.cwd()).resolve() == project
            and process.cmdline()[1:] == ARGS)


def _flat(root):
    path = (root / "paper.sqlite3").resolve()
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        db.execute("BEGIN")
        states = [json.loads(raw) for raw, in db.execute("SELECT state FROM book")]
        if len(states) != 7:
            raise ValueError("Unexpected number of current books")
        return all(state["position"] is None and state["pending"] is None for state in states)


def if_needed(root, generation_id, amendment_id, *, project, clock=utc_now, lookup=psutil.Process, expected_build=None):
    """Restart only the verified old runner, while financing state is fenced and flat."""
    root, project = Path(root).resolve(), Path(project).resolve()
    cycle_path = root / "last-cycle.json"
    if not amendment_id or not cycle_path.is_file():
        return None
    try:
        heartbeat = read_json(cycle_path)
        if heartbeat.get("source_amendment_id") == amendment_id and (
                expected_build is None or heartbeat.get("runner_build_id") == expected_build):
            return None
        with writer_lock(root / ".writer.lock"):
            heartbeat = read_json(cycle_path)
            if heartbeat.get("source_amendment_id") == amendment_id and (
                    expected_build is None or heartbeat.get("runner_build_id") == expected_build):
                return None
            if heartbeat["generation"] != generation_id or heartbeat["state"] != "RUNNING":
                return _blocked("CURRENT_RUNNER_GENERATION_UNVERIFIED")
            now = clock()
            observed = utc(heartbeat["at"])
            # A failing cycle can leave a locked, identifiable process without a fresh heartbeat.
            maximum_age = pd.Timedelta(minutes=15) if expected_build else HEARTBEAT_AGE
            if not pd.Timedelta(0) <= now - observed <= maximum_age:
                return _blocked("CURRENT_RUNNER_HEARTBEAT_STALE")
            if not _flat(root):
                return _blocked("CURRENT_BOOK_HAS_EXPOSURE_OR_PENDING_IDEA")
            launch = read_json(root / "last-launch.json")
            launched = utc(launch["at"])
            if any(type(value) is not int or value <= 0 for value in (heartbeat["pid"], launch["pid"])):
                return _blocked("CURRENT_RUNNER_PID_INVALID")
            process = lookup(heartbeat["pid"])
            launcher = process if process.pid == launch["pid"] else process.parent()
            if launcher is None or launcher.pid != launch["pid"]:
                return _blocked("CURRENT_RUNNER_ANCESTRY_MISMATCH", pid=process.pid)
            first, second = (pd.Timestamp(item.create_time(), unit="s", tz="UTC") for item in (launcher, process))
            if not (launched - START_TOLERANCE <= first <= second <= launched + START_TOLERANCE
                    and second <= observed):
                return _blocked("CURRENT_RUNNER_CREATE_TIME_MISMATCH", pid=process.pid)
            paths = {(project / ".venv/Scripts/python.exe").resolve(), Path(sys._base_executable).resolve()}
            user = lookup().username()
            if not all(_checked_process(item, project, paths, user) for item in (process, launcher)):
                return _blocked("CURRENT_RUNNER_OWNERSHIP_MISMATCH", pid=process.pid)
            process.terminate()
            process.wait(timeout=5)
            return {"state": "RUNNER_RELOADED", "retired_pid": process.pid, "source_amendment_id": amendment_id}
    except psutil.AccessDenied:
        return _blocked("CURRENT_RUNNER_WINDOWS_ACCESS_DENIED")
    except psutil.NoSuchProcess:
        return _blocked("CURRENT_RUNNER_IDENTITY_DISAPPEARED")
    except psutil.TimeoutExpired:
        return _blocked("CURRENT_RUNNER_EXIT_TIMEOUT")
    except RuntimeError:
        return _blocked("CURRENT_RUNNER_WRITER_BUSY")
    except (KeyError, OSError, TypeError, ValueError, OverflowError, sqlite3.Error):
        return _blocked("CURRENT_RUNNER_RELOAD_EVIDENCE_UNAVAILABLE")
