from contextlib import suppress
import subprocess
import sys
import time

import psutil

from lab.config import ROOT
from lab.learning import state
from lab.learning.design import MIB, OUTPUT, PROFILE_IDS, STATE, config, freeze, inventory, read_manifest
from lab.phase7.operation import health
from lab.storage import put_bytes, read_json, utc, utc_now, writer_lock


ACTIVE = {"TRAINING", "EVALUATING", "PAUSING"}
FLAGS = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP


def space_used():
    total = 0
    for directory in (OUTPUT, STATE):
        for path in directory.rglob("*"):
            with suppress(FileNotFoundError):
                if path.is_file():
                    total += path.stat().st_size
    return total


def admission(budget):
    available = inventory()
    if available["ram_available_mib"] < budget["min_available_ram_mib"]:
        return "Available RAM below the frozen collector reserve"
    if available["disk_free_mib"] < budget["min_free_disk_mib"] or space_used() >= budget["output_cap_mib"] * MIB:
        return "Learning disk or workstation free-space guard"
    try:
        collector = health()
    except (OSError, ValueError, RuntimeError):
        return "Collector health unavailable"
    if (collector["process_state"] != "running" or collector["heartbeat_state"] != "healthy"
            or not collector["worker_lock_held"] or any(feed["progress"] != "current" for feed in collector["feeds"].values())):
        return "Collector heartbeat, lock, or candle progress is degraded"
    return None


def change(action, *, profile=None, enabled=None, manifest=None, blocked=None):
    with state.connect(write=True) as db:
        current = state.current(db)
        phase, mode = current["state"], current["mode"]
        if action == "profile":
            if profile not in PROFILE_IDS or type(enabled) is not bool:
                raise ValueError("Select a research profile and a boolean enable flag")
            if phase in ACTIVE | {"QUEUED"}:
                raise ValueError("Profile flags are pinned while a campaign is active or queued")
            return state.save(db, current, profiles={**current["profiles"], profile: enabled})
        if action in ("pause", "stop"):
            if action == "pause" and mode in ("off", "paused"):
                return current
            requested_mode = "paused" if action == "pause" else "off"
            next_phase = "PAUSING" if phase in ACTIVE else "PAUSED" if action == "pause" else "OFF"
            return state.save(db, current, mode=requested_mode, state=next_phase, reason=f"Operator requested {action}",
                              resume_mode=mode if mode in ("continuous", "once") else current["resume_mode"])
        if action == "run-once":
            if mode != "off" or phase in ACTIVE | {"QUEUED"}:
                raise ValueError("A campaign or learner session is already active. Stop it before run-once.")
            if blocked:
                return state.save(db, current, state="WAITING_FOR_DATA", reason=blocked)
            current = state.save(db, current, mode="once", resume_mode="once", reason="Single campaign requested")
            if current["job"]:
                return state.save(db, current, state="QUEUED", reason="Resume the checkpointed campaign")
            current = state.enqueue(db, current, manifest)
            if current["job"] is None:
                return state.save(db, current, mode="off", state="OFF")
            return current
        if action in ("start", "resume"):
            if phase == "PAUSING":
                raise ValueError("Wait for PAUSING to complete")
            if mode == "once" and phase in ACTIVE | {"QUEUED"}:
                raise ValueError("A run-once campaign is active")
            mode = current["resume_mode"] if action == "resume" and current["job"] else "continuous"
            phase = phase if phase in ACTIVE | {"QUEUED"} else ("QUEUED" if current["job"] else "WAITING_FOR_DATA")
            return state.save(db, current, mode=mode, state=phase, reason="Learning enabled", resume_mode=mode)
        raise ValueError(f"Unknown control action {action}")


def process_identity(pid, created):
    if not pid or not created:
        return None
    try:
        process = psutil.Process(pid)
        return process if process.create_time() == created and process.is_running() else None
    except psutil.Error:
        return None


def cancel_tree(process):
    try:
        children = process.children(recursive=True)
    except psutil.NoSuchProcess:
        return
    for child in [*reversed(children), process]:
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs([*children, process], timeout=4)
    if alive:
        raise RuntimeError("Learning child shutdown is still pending")


def output_ledger(job):
    folder = OUTPUT / job["manifest"] / job["id"]
    with state.connect() as db:
        evidence = state.events(db, job["id"])
    put_bytes(folder / "all-trials.jsonl", ("\n".join(state.encode(row) for row in evidence) + "\n").encode("utf-8"))


def finish(job, reason=None, failed=False, abandoned=False):
    with state.connect(write=True) as db:
        current = state.current(db)
        complete = db.execute("SELECT 1 FROM events WHERE job=? AND kind='JOB_COMPLETE'", (job["id"],)).fetchone()
        if not complete:
            registered = {trial["id"]: trial for trial in read_manifest(job["manifest"], check_code=False)["design"]["trials"]}
            results = state.trials(db, job["id"])
            for key in job["trial_ids"]:
                if results.get(key, {}).get("status") not in state.TERMINAL_TRIALS:
                    state.event(db, job["id"], "TRIAL_RESULT", {"status": "INCOMPLETE", "reason": reason,
                                                              "trial": registered[key]}, key)
            state.event(db, job["id"], "JOB_INTERRUPTED", {"reason": reason, "deadline_exceeded": abandoned})
        mode = current["mode"]
        active_job = None if complete or abandoned else job["id"]
        if mode == "paused":
            next_mode, next_phase = "paused", "PAUSED"
        elif mode == "off":
            next_mode, next_phase = "off", "OFF"
        elif failed:
            next_mode, next_phase = "off", "FAILED"
        elif mode == "once":
            next_mode, next_phase = "off", "OFF"
        else:
            next_mode, next_phase = "continuous", "WAITING_FOR_DATA"
        state.save(db, current, mode=next_mode, state=next_phase, job=active_job, active_trial=None,
                   child_pid=None, child_created=None, reason=reason or "Campaign complete")
    output_ledger(job)


def supervise_job(job, budget):
    folder = OUTPUT / job["manifest"] / job["id"]
    folder.mkdir(parents=True, exist_ok=True)
    with state.connect(write=True) as db:
        current = state.current(db)
        if current["mode"] not in ("once", "continuous"):
            return
        process = process_identity(current.get("child_pid"), current.get("child_created"))
        child = None
        if utc_now() < utc(job["deadline"]):
            if process is None:
                log = folder / "training.log"
                with log.open("ab") as output:
                    child = subprocess.Popen([sys.executable, "-m", "lab.learning", "_child", job["id"]], cwd=ROOT,
                                             stdin=subprocess.DEVNULL, stdout=output, stderr=output, close_fds=True,
                                             creationflags=FLAGS | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
                process = psutil.Process(child.pid)
                current = state.save(db, current, state="EVALUATING", child_pid=child.pid,
                                     child_created=process.create_time(), reason="Independent learning process started")
    if process is None:
        finish(job, "Campaign wall-clock deadline elapsed", failed=True, abandoned=True)
        return
    cancelled_since = None
    reason, failed, expired = None, False, False
    checked = 0.
    while True:
        if child is not None and child.poll() is not None:
            break
        with state.connect() as db:
            live = state.current(db)
        root = process_identity(live.get("child_pid"), live.get("child_created"))
        if root is None:
            break
        expired = utc_now() >= utc(job["deadline"])
        if expired:
            reason, failed = "Campaign wall-clock deadline elapsed", True
        elif live["mode"] in ("off", "paused"):
            reason = f"Operator requested {live['mode']}"
        elif time.monotonic() - checked >= 5:
            checked = time.monotonic()
            try:
                memory = root.memory_info().rss + sum(p.memory_info().rss for p in root.children(recursive=True))
            except psutil.NoSuchProcess:
                memory = 0
            violation = "Learning worker memory cap exceeded" if memory > budget["child_rss_mib"] * MIB else admission(budget)
            if violation:
                reason, failed = violation, True
        if reason:
            with state.connect(write=True) as db:
                state.save(db, state.current(db), state="PAUSING", reason=reason)
            if cancelled_since is None:
                cancelled_since = time.monotonic()
            if failed or time.monotonic() - cancelled_since >= 4:
                cancel_tree(root)
                break
        time.sleep(.5)
    with state.connect() as db:
        complete = db.execute("SELECT 1 FROM events WHERE job=? AND kind='JOB_COMPLETE'", (job["id"],)).fetchone()
        mode = state.current(db)["mode"]
    if not complete and reason is None:
        reason = "Learning child exited before campaign completion"
        failed = mode not in ("off", "paused")
    finish(job, reason, failed, expired)


def serve(*, single=False):
    try:
        with writer_lock(STATE / ".supervisor.lock"):
            while True:
                with state.connect(write=True) as db:
                    current = state.current(db)
                    mode = current["mode"]
                    job = state.job(db, current["job"]) if current["job"] else None
                if mode in ("off", "paused"):
                    if current["state"] == "PAUSING":
                        if process := process_identity(current.get("child_pid"), current.get("child_created")):
                            cancel_tree(process)
                        if job:
                            finish(job, "Operator requested pause or stop")
                    return
                if job:
                    budget = read_manifest(job["manifest"])["design"]["settings"]["budget"]
                    violation = admission(budget)
                    if violation:
                        with state.connect(write=True) as db:
                            current = state.current(db)
                            state.save(db, current, state="WAITING_FOR_DATA", mode="off" if single else current["mode"],
                                       reason=violation)
                        if single:
                            return
                        time.sleep(10)
                        continue
                    supervise_job(job, budget)
                    if single:
                        return
                else:
                    settings = config()
                    checked = current["last_check"]
                    due = not checked or (utc_now() - utc(checked)).total_seconds() >= settings["budget"]["daily_check_seconds"]
                    if due:
                        violation = admission(settings["budget"])
                        manifest = freeze() if violation is None else None
                        with state.connect(write=True) as db:
                            current = state.current(db)
                            if current["mode"] == "continuous" and current["job"] is None:
                                if violation:
                                    state.save(db, current, state="WAITING_FOR_DATA", reason=violation)
                                else:
                                    state.enqueue(db, current, manifest)
                    time.sleep(2)
    except RuntimeError as exc:
        if not str(exc).startswith("Another writer owns"):
            raise


def command(action, *, profile=None, enabled=None, wait=True):
    manifest = freeze() if action == "run-once" else None
    blocked = admission(manifest["design"]["settings"]["budget"]) if manifest else None
    result = change(action, profile=profile, enabled=enabled, manifest=manifest, blocked=blocked)
    if ((action in ("run-once", "start", "resume") and result["mode"] in ("once", "continuous"))
            or result["state"] == "PAUSING"):
        if wait and action == "run-once":
            serve(single=True)
        else:
            subprocess.Popen([sys.executable, "-m", "lab.learning", "_serve"], cwd=ROOT, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True, creationflags=FLAGS)
    return state.status()


def latest_report():
    current = state.status()
    job = current["latest_job"]
    if not job:
        return None
    folder = OUTPUT / job["manifest"] / job["id"]
    path = folder / "report.json"
    return {"directory": str(folder), "report": read_json(path) if path.exists() else None}
