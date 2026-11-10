from contextlib import suppress
import subprocess
import sys
import time

import psutil
import pandas as pd

from lab.config import ROOT as PROJECT
from lab.learning.control import admission, cancel_tree, process_identity
from lab.learning.design import MIB, STATE as SHARED_STATE
from lab.phase11.archive import size_under
from lab.phase11.campaign import gates_off, start_clock
from lab.phase13.registry import sealed
from lab.phase16.plan import ROOT, folder, freeze_initial
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


def optional(path):
    return read_json(path) if path.exists() else None


def settings():
    return optional(ROOT/"controls.json") or {"recurring": False, "action": "off"}


def change(action):
    if action not in {"enable", "pause", "resume", "stop"}:
        raise ValueError("Unknown Phase 16 training control")
    with writer_lock(ROOT/".control.lock"):
        current = settings()
        recurring = action == "enable" or action == "resume" and current.get("resume_recurring", False)
        updated = {"recurring": recurring, "action": action, "at": utc_now().isoformat(),
                   "resume_recurring": (current["recurring"] or current.get("resume_recurring", False)) if action == "pause" else False}
        write_json(ROOT/"controls.json", updated)
        return updated


def active():
    row = optional(ROOT/"active.json")
    return row if row and process_identity(row["pid"], row["created"]) else None


def queued():
    waiting = []
    now = utc_now()
    for path in (ROOT/"queue").glob("*.json"):
        record = read_json(path)
        root = folder(record["id"])
        clock = optional(root/"deadline.json")
        if (root/"result.json").exists() or utc(record["not_before"]) > now or (clock and utc(clock["deadline"]) <= now):
            continue
        waiting.append(record)
    return sorted(waiting, key=lambda row: (row["at"], row["id"]))


def managed_tick():
    if not settings()["recurring"]:
        return
    with writer_lock(ROOT/".dispatch.lock"):
        dispatch = optional(ROOT/"dispatch.json")
        if not settings()["recurring"] or active() or (dispatch and process_identity(dispatch["pid"], dispatch["created"])):
            return
        if not (jobs := queued()):
            pending = [read_json(path) for path in (ROOT/"queue").glob("*.json")]
            upcoming = [row["not_before"] for row in pending if utc(row["not_before"]) > utc_now()
                        and not (folder(row["id"])/"result.json").exists()]
            write_json(ROOT/"last-managed-check.json", {"at": utc_now().isoformat(),
                "state": "WAITING_FOR_INTERVAL" if upcoming else "NO_NEW_INPUTS", "next_eligible_check": min(upcoming, default=None)})
            return
        child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase16", "_supervise", jobs[0]["id"]], cwd=PROJECT,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
        with suppress(psutil.NoSuchProcess):
            proc = psutil.Process(child.pid)
            write_json(ROOT/"dispatch.json", {"campaign": jobs[0]["id"], "pid": proc.pid, "created": proc.create_time()})
            write_json(ROOT/"last-managed-check.json", {"at": utc_now().isoformat(), "state": "QUEUED", "campaign": jobs[0]["id"]})


def supervise(identifier, *, recurring=False):
    from lab.phase16.evaluate import counts

    root = folder(identifier)
    manifest = sealed(root/"manifest.json", identifier)
    result_path = root/"result.json"
    if result := optional(result_path):
        return {**result, "reused": True}
    policy = manifest["body"]["policy"]
    budget = policy["budget"]
    with writer_lock(SHARED_STATE/".supervisor.lock"), writer_lock(ROOT/".run.lock"):
        if result := optional(result_path):
            return {**result, "reused": True}
        if recurring and not settings()["recurring"]:
            return {"state": "TRAINING_DISABLED", "campaign": identifier}
        if settings()["action"] == "pause":
            return {"state": "PAUSED", "campaign": identifier}
        body = manifest["body"]
        if body["kind"] == "INCREMENTAL_ALLOCATION":
            profile, = body["profiles"].values()
            due = utc(body["previous"]["completed_at"])+pd.Timedelta(days=profile["interval_days"])
            if utc_now() < due:
                return {"state": "WAITING_FOR_INTERVAL", "campaign": identifier, "next_eligible_check": due.isoformat()}
        reason = admission(budget) if gates_off() else "Legacy training gates must remain off"
        if reason:
            return {"state": "WAITING_FOR_ADMISSION", "campaign": identifier, "reason": reason}
        clock = start_clock(root, budget["wall_seconds"])
        deadline = utc(clock["deadline"])
        if utc_now() >= deadline:
            return {"state": "DEADLINE_ELAPSED", "campaign": identifier, **clock, **counts(root)}
        write_json(ROOT/"checkpoint.json", {"id": identifier, "at": utc_now().isoformat()})
        peak, stop = 0, None
        with (root/"worker.log").open("ab") as output:
            child = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase16", "_work", identifier],
                                     cwd=PROJECT, stdout=output, stderr=output, stdin=subprocess.DEVNULL,
                                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
            process = psutil.Process(child.pid)
            write_json(ROOT/"active.json", {"campaign": identifier, "pid": process.pid, "created": process.create_time(),
                                            "recurring": recurring, "at": utc_now().isoformat()})
            try:
                while child.poll() is None:
                    with suppress(psutil.NoSuchProcess):
                        peak = max(peak, sum(p.memory_info().rss for p in [process, *process.children(recursive=True)]))
                    commands = settings()
                    changed = commands.get("at") and utc(commands["at"]) >= utc(clock["started_at"])
                    stopped = changed and commands["action"] in {"pause", "stop"}
                    stop = (f"Operator requested {commands['action']}" if stopped else
                            "Recurring controls disabled" if recurring and not commands["recurring"] else
                            "Child RSS ceiling" if peak > budget["child_rss_mib"]*MIB else
                            "Campaign output ceiling" if size_under(root) >= budget["output_cap_mib"]*MIB else
                            "Original two-hour deadline elapsed" if utc_now() >= deadline else
                            "A legacy training gate opened" if not gates_off() else admission(budget))
                    if stop:
                        break
                    time.sleep(2)
            finally:
                if child.poll() is None:
                    cancel_tree(process)
                (ROOT/"active.json").unlink(missing_ok=True)
        result = optional(result_path)
        evidence = {**clock, "at": utc_now().isoformat(), "peak_rss_bytes": peak, "output_bytes": size_under(root),
                    "stop_reason": stop, "exit_code": child.poll(), **counts(root)}
        write_json(root/"resources.json", evidence)
        return {**result, "resources": evidence} if result else {"state": "INTERRUPTED", "campaign": identifier, **evidence}


def run_once(identifier=None):
    if settings()["action"] == "stop":
        change("resume")
    return supervise(identifier or freeze_initial()["id"])


def resume():
    checkpoint = optional(ROOT/"checkpoint.json")
    updated = change("resume")
    if checkpoint and not (folder(checkpoint["id"])/"result.json").exists():
        return supervise(checkpoint["id"])
    return updated
