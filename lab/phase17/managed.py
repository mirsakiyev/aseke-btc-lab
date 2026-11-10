from contextlib import suppress
import subprocess
import sys

import pandas as pd
import psutil

from lab.config import ROOT as PROJECT
from lab.phase13 import observation
from lab.phase17.collection import GRACE, HOUR, MINUTE, QUARTER, capture_spot, coverage, existing_sources, futures_stores, spot_store
from lab.phase17.registry import ROOT
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


def optional(path):
    return read_json(path) if path.exists() else None


def dispatch(record, now):
    profiles = record["body"]["profiles"]
    closed = (now-pd.Timedelta(hours=2)).floor("1D")
    due = {name: min(closed, utc(p["end_exclusive"]).floor("1D")) for name, p in profiles.items()
           if closed >= utc(p["context_lower_bound"])}
    if not due:
        return
    from lab.learning.control import admission, process_identity

    with writer_lock(ROOT / ".dispatch.lock"):
        pending = optional(ROOT / "label-worker.json")
        if pending and process_identity(pending["pid"], pending["created"]):
            return
        if pending and pending["profile"] in due:
            old_end = utc(pending["cutoff"])
            work = ROOT / "checks" / pending["profile"] / old_end.strftime("%Y%m%dT%H%M%S")
            clock = optional(work / "deadline.json")
            if not (work / "result.json").exists() and clock and now < utc(clock["deadline"]):
                due = {pending["profile"]: old_end, **{k:v for k,v in due.items() if k != pending["profile"]}}
        for name, end in due.items():
            last = optional(ROOT / "readiness" / f"{name}.json")
            if last and utc(last["cutoff"]) >= end:
                continue
            if pending and pending["profile"] == name and now < utc(pending["at"]) + pd.Timedelta(hours=1):
                continue
            if reason := admission(record["body"]["budget"]):
                write_json(ROOT / "label-admission.json", {"at": now.isoformat(), "reason": reason})
                return
            child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase17", "_supervise", name, end.isoformat()],
                                     cwd=PROJECT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
            with suppress(psutil.NoSuchProcess):
                process = psutil.Process(child.pid)
                write_json(ROOT / "label-worker.json", {"pid": child.pid, "created": process.create_time(),
                           "profile": name, "cutoff": end.isoformat(), "at": now.isoformat()})
            return


def tick(record, launcher, source_cycle, now=None, transport=None):
    now = utc_now() if now is None else now
    active = utc(record["body"]["activation"])
    if now < active:
        return
    futures_end = (now-GRACE).floor(QUARTER)
    # Include the current closed window even if it was partial; never count
    # pre-activation source minutes as Phase 17 managed commissioning evidence.
    left = max(active, futures_end-QUARTER)
    futures = coverage([*existing_sources(record), *futures_stores(record)], left, max(left, futures_end), MINUTE, now)
    spot = None
    if source_cycle.get("stop_reason") not in {"RATE_LIMITED", "ACCESS_RESTRICTED"} and source_cycle.get("state") not in {
            "COLLECTION_OFF", "RETRY_AFTER", "ACCESS_RESTRICTED", "FUNDING_CLOSEOUT_RECORDED"}:
        spot = capture_spot(record, now, transport)
    receipt = (spot or {}).get("receipt", {}).get("received_at")
    as_of = max(now, utc(receipt)) if receipt else now
    end = (now-GRACE).floor(HOUR)
    start = max(utc(record["body"]["profiles"]["position"]["start"]), end-HOUR)
    spot_coverage = coverage([spot_store(record)], start, max(start, end), HOUR, as_of, mark_required=False)
    evidence = {"registration": record["id"], "at": as_of.isoformat(), "launcher": launcher, "source_protocol": source_cycle.get("protocol"),
                "source_cycle": {k: source_cycle[k] for k in ("state", "left", "end_exclusive", "recorded_at", "received", "receipts")
                                 if k in source_cycle},
                "futures": futures, "spot": spot, "spot_coverage": spot_coverage, "discovery_gets": 0,
                "next_check": (optional(observation.PULSE) or {}).get("wake")}
    write_json(ROOT / "last-managed.json", evidence)
    if launcher.startswith("scheduled:") and spot and spot.get("state") == "RECEIVED" and spot["receipt"].get("rows"):
        with writer_lock(ROOT / ".first-spot.lock"):
            path = ROOT / "first-spot-managed.json"
            if not path.exists():
                write_json(path, evidence)
    if (launcher.startswith("scheduled:") and source_cycle.get("state") == "COMPLETE" and source_cycle.get("left")
            and utc(source_cycle["left"]) <= left < futures_end == utc(source_cycle["end_exclusive"])
            and futures["paired"] and futures["paired"] == futures["expected_per_price_basis"]):
        with writer_lock(ROOT / ".first-managed.lock"):
            path = ROOT / "first-complete-managed.json"
            if not path.exists():
                write_json(path, evidence)
    dispatch(record, now)
    if (ROOT / "pilot" / "registration.json").exists():
        from lab.phase17.pilot_managed import tick as pilot_tick
        pilot_tick(now)
    return evidence
