from contextlib import suppress
import subprocess
import sys
import time

import psutil

from lab.config import ROOT as PROJECT
from lab.learning.control import admission, cancel_tree
from lab.learning.design import MIB, STATE as SHARED_STATE
from lab.phase11.archive import size_under
from lab.phase11.campaign import gates_off, start_clock
from lab.phase17.registry import ROOT, registered
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


def work_path(profile, cutoff, kind):
    base = ROOT / "checks" if kind == "original" else ROOT / "pilot" / kind
    return base / profile / utc(cutoff).strftime("%Y%m%dT%H%M%S")


def expired_fit(profile):
    root = ROOT / "pilot" / "profiles" / profile
    path = root / "fit" / "deadline.json"
    if path.exists() and not (root / "candidate.json").exists() and utc_now() >= utc(read_json(path)["deadline"]):
        ready = root / "readiness.json"
        row = read_json(ready) if ready.exists() else {}
        write_json(ready, {**row, "state":"FIT_DEADLINE_ELAPSED", "reason":"Original pilot fit budget elapsed. The intent and attempts are retained."})
        return True
    return False


def supervise(profile, cutoff, *, kind="original"):
    record = registered()
    budget = record["body"]["budget"]
    end = utc(cutoff)
    work = work_path(profile, end, kind)
    path = work / "result.json"
    with writer_lock(SHARED_STATE / ".supervisor.lock"), writer_lock(ROOT / ".labels.lock"):
        if kind in {"pilot-check", "pilot-fit"} and expired_fit(profile):
            return {"state":"FIT_DEADLINE_ELAPSED"}
        if path.exists():
            saved = read_json(path)
            from lab.phase16.control import settings
            if not (kind == "pilot-fit" and saved["state"] == "FIT_ELIGIBLE_TRAINING_OFF" and settings()["recurring"]):
                return {**saved, "reused": True}
        reason = admission(budget) if gates_off() else "Legacy training gates must remain off"
        if reason:
            write_json(ROOT / "label-admission.json", {"at": utc_now().isoformat(), "reason": reason})
            return {"state": "WAITING_FOR_RESOURCE_ADMISSION", "reason": reason}
        clock = start_clock(work, budget["wall_seconds"])
        peak, reason = 0, None
        if utc_now() >= utc(clock["deadline"]):
            return {"state": "DEADLINE_ELAPSED", **clock}
        with (work / "worker.log").open("ab") as output:
            child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase17", "_label", profile, end.isoformat(), "--kind", kind],
                                     cwd=PROJECT, stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
            process = psutil.Process(child.pid)
            try:
                while child.poll() is None:
                    with suppress(psutil.NoSuchProcess):
                        peak = max(peak, sum(proc.memory_info().rss for proc in (process, *process.children(recursive=True))))
                    size = sum(size_under(ROOT / name) for name in ("partitions", "labels", "metadata", "sources", "checks", "pilot"))
                    fitting = work / "fitting.json"
                    fit_clock = None
                    if fitting.exists():
                        from pathlib import Path
                        from lab.phase16.control import settings
                        fit_clock = read_json(Path(read_json(fitting)["fit_root"]) / "deadline.json")
                        if not settings()["recurring"]:
                            reason = "Pilot training paused or disabled"
                            break
                    reason = ("Native label RSS guard" if peak > budget["child_rss_mib"]*MIB else
                              "Native qualification output guard" if size >= budget["output_cap_mib"]*MIB else
                              "Original native qualification deadline elapsed" if utc_now() >= min(utc(clock["deadline"]), utc((fit_clock or clock)["deadline"])) else
                              "A legacy training gate opened" if not gates_off() else admission(budget))
                    if reason:
                        break
                    time.sleep(2)
            finally:
                if child.poll() is None:
                    cancel_tree(process)
        result = {**clock, "cutoff": end.isoformat(), "profile": profile, "exit_code": child.poll(),
                  "peak_rss_bytes": peak, "stop_reason": reason, "at": utc_now().isoformat()}
        write_json(work / "resources.json", result)
        if path.exists():
            return {**read_json(path), "resources": result}
        return {**result, "state": "INTERRUPTED"}


def work(profile, cutoff, *, kind="original"):
    from threadpoolctl import threadpool_limits

    from lab.phase17.admission import run_labels

    end = utc(cutoff)
    root = work_path(profile, end, kind)
    path = root / "result.json"
    with writer_lock(SHARED_STATE / ".fit.lock"), threadpool_limits(limits=2):
        if kind == "original":
            result = run_labels(profile, end.isoformat())
        else:
            from lab.phase17 import pilot
            registration = pilot.current()
            if kind in {"pilot-check", "pilot-fit"}:
                result = pilot.prepare(registration, profile, end.isoformat(), root)
                write_json(pilot.profile_dir(profile) / "readiness.json", {"checked_cutoff":end.isoformat(), **result})
            else:
                result = pilot.evaluate(registration, profile, root)
        write_json(path, result)
