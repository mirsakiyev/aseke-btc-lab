from contextlib import suppress
import subprocess
import sys

import pandas as pd
import psutil

from lab.config import ROOT as PROJECT
from lab.phase17.pilot_registry import PILOT, current
from lab.storage import read_json, utc, write_json, writer_lock


def tick(now):
    pilot = current()
    if not pilot or now < utc(pilot["body"]["activation"]):
        return
    from lab.learning.control import admission, process_identity
    from lab.phase16.control import settings
    from lab.phase17.pilot_context import pin

    eligible = {name:cfg for name,cfg in pilot["body"]["profiles"].items() if utc(cfg["label_start"]) <= now}
    for name in eligible:
        if not (PILOT / "pins" / f"{name}.json").exists():
            pin(pilot, name, now)
    closed = (now-pd.Timedelta(hours=2)).floor("1D")
    with writer_lock(PILOT / ".dispatch.lock"):
        path = PILOT / "worker.json"
        prior = read_json(path) if path.exists() else None
        if prior and process_identity(prior["pid"],prior["created"]):
            return
        for name,cfg in eligible.items():
            end = min(closed, utc(cfg["source_end_exclusive"]).floor("1D"))
            if end <= utc(cfg["label_start"]) or not (PILOT / "pins" / f"{name}.json").exists():
                continue
            ready = PILOT / "profiles" / name / "readiness.json"
            last = read_json(ready) if ready.exists() else None
            ready_to_fit = bool(last and last["state"] == "FIT_ELIGIBLE_TRAINING_OFF" and settings()["recurring"])
            if last and (last["state"] in {"CANDIDATE_FROZEN", "FIT_DEADLINE_ELAPSED"} or utc(last["checked_cutoff"]) >= end and not ready_to_fit):
                continue
            # A completed readiness-only check did not spend a fitting clock.
            # A later enable can run that same sample with its first fit budget.
            kind = "pilot-fit" if ready_to_fit else "pilot-check"
            if ready_to_fit:
                end = utc(last["checked_cutoff"])
            if prior and prior["profile"] == name and utc(prior["at"])+pd.Timedelta(minutes=30) > now:
                continue
            if reason := admission(pilot["body"]["budget"]):
                write_json(PILOT / "last-guard.json", {"at":now.isoformat(),"reason":reason})
                return
            child = subprocess.Popen([sys.executable,"-X","utf8","-m","lab.phase17","_supervise",name,end.isoformat(),"--kind",kind],
                cwd=PROJECT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
            with suppress(psutil.NoSuchProcess):
                proc = psutil.Process(child.pid)
                write_json(path,{"pid":child.pid,"created":proc.create_time(),"at":now.isoformat(),"profile":name,"cutoff":end.isoformat()})
            return
