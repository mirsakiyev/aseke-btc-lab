import json
import os
import shutil
import subprocess
import sys
import threading
import time
from contextlib import contextmanager, suppress
from importlib.metadata import version as package_version

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import ORIGIN, ROOT as PROJECT
from lab.learning.design import STATE as SHARED_STATE
from lab.storage import utc_now, writer_lock
from lab.strategy_lab.datasets import bounds, data_id, load_market, timestamp
from lab.strategy_lab.evaluator import code_hash, compile_spec, tested
from lab.strategy_lab.registry import ROOT, canonical, connect, digest, event, get, now
from lab.strategy_lab.replay import ASSUMPTIONS, BASE_CASES, FUTURES_CASES, run_case, stages


MIB = 1024**2
BUDGET = {"threads": 1, "wall_seconds": 900, "cpu_seconds": 600, "rss_mib": 640, "min_ram_mib": 1250,
          "min_disk_mib": 2048, "checkpoint_mib": 2, "sandbox_mib": 512, "attempts": 3, "queue": 12}
ACTIVE = ("QUEUED", "RUNNING", "CANCEL_REQUESTED", "INTERRUPTED")
ELIGIBLE = ("UNIT_TESTED", "HISTORICAL_RESEARCH", "INCONCLUSIVE", "SHORTLISTED")


def log(db, key, message):
    db.execute("INSERT INTO job_event(job_hash,at,message) VALUES(?,?,?)", (key, now(), message))


def status(db, key):
    row = db.execute("SELECT * FROM job WHERE hash=?", (key,)).fetchone()
    if row is None:
        raise ValueError("Unknown job")
    request = json.loads(row["request"])
    if digest(request["identity"]) != key:
        raise ValueError("Job request checksum mismatch")
    return {**dict(row), "request": request, "checkpoints": [
        item[0] for item in db.execute("SELECT stage FROM checkpoint WHERE job_hash=? ORDER BY rowid", (key,))
    ]}


def runtime():
    return {"python": sys.version.split()[0], **{name: package_version(name) for name in
                                                ("numpy", "pandas", "pyarrow", "threadpoolctl", "scikit-learn")}}


def request_job(db, version, dataset, development, validation, approved_by):
    if not isinstance(approved_by, str) or not approved_by.strip():
        raise ValueError("Record explicit operator approval for this historical job")
    record, body = get(db, version), data_id(db, dataset)
    spec, code = record["spec"], code_hash()
    if record["state"] not in ELIGIBLE or not tested(record, code):
        raise ValueError("A reviewed strategy and a unit test for the current evaluator code are required")
    _, _, _, risk, cost = compile_spec(spec)
    if any(spec[field] != body[field] for field in ("profile", "market", "instrument", "venue")):
        raise ValueError("Strategy and dataset product capabilities do not match")
    first, end, _ = bounds(body)
    dev_start, dev_end = map(timestamp, development)
    val_start, val_end = map(timestamp, validation)
    step = pd.Timedelta(spec["timeframe"])
    if not first <= dev_start < dev_end <= val_start < val_end <= end or any(
        (stamp - ORIGIN) % step for stamp in (dev_start, dev_end, val_start, val_end)
    ):
        raise ValueError("Use aligned, disjoint chronological development and validation intervals")
    baseline_id = f"phase15/{spec['profile']}/fixed"
    baseline = db.execute("SELECT hash FROM strategy_version WHERE strategy_id=? AND version=1", (baseline_id,)).fetchone()
    if baseline is None:
        raise ValueError("Seed the existing registered rules-only baselines first")
    baseline_spec = baseline_for(db, baseline["hash"], spec)
    compile_spec(baseline_spec)
    intervals = {"development": [dev_start.isoformat(), dev_end.isoformat()],
                 "validation": [val_start.isoformat(), val_end.isoformat()]}
    identity = {"version": version, "dataset": dataset, "intervals": intervals, "baseline": baseline["hash"],
                "baseline_recipe": digest(baseline_spec),
                "code": code, "runtime": runtime(), "budget": BUDGET, "assumptions": ASSUMPTIONS}
    key = digest(identity)
    if db.execute("SELECT 1 FROM job WHERE hash=?", (key,)).fetchone():
        return status(db, key)
    placeholders = ",".join("?" for _ in ACTIVE)
    if db.execute(f"SELECT COUNT(*) FROM job WHERE state IN ({placeholders})", ACTIVE).fetchone()[0] >= BUDGET["queue"]:
        raise ValueError("Active queue is at capacity")
    payload = {"identity": identity, "version": version, "dataset": dataset, "profile": spec["profile"],
               "family": spec["family_id"], "approval": {"operator": approved_by.strip(), "at": now()},
               "risk": vars(risk), "cost": vars(cost)}
    db.execute("INSERT INTO job(hash,created_at,request,state) VALUES(?,?,?,'QUEUED')", (key, now(), canonical(payload)))
    log(db, key, "Historical replay explicitly requested")
    return status(db, key)


def baseline_for(db, baseline, spec):
    reference = get(db, baseline)["spec"]
    return {**reference, "cost": spec["cost"], "risk": spec["risk"],
            "session": spec["session"], "execution": spec["execution"]}


def update(db, key, state, reason):
    db.execute("UPDATE job SET state=? WHERE hash=?", (state, key))
    log(db, key, reason)


def cancel(db, key):
    job = status(db, key)
    if job["state"] in ("QUEUED", "INTERRUPTED"):
        update(db, key, "CANCELLED", "Cancelled by operator")
    elif job["state"] == "RUNNING":
        update(db, key, "CANCEL_REQUESTED", "Operator requested cancellation")
    return status(db, key)


def recover(db):
    # Call only while holding the dispatcher lock, which every supervisor owns.
    for row in db.execute("SELECT hash,state FROM job WHERE state IN ('RUNNING','CANCEL_REQUESTED')").fetchall():
        update(db, row["hash"], "CANCELLED" if row["state"] == "CANCEL_REQUESTED" else "INTERRUPTED",
               "Recovered orphaned bookkeeping; durable cases retained")


def resume(db, key):
    job = status(db, key)
    if job["state"] != "INTERRUPTED" or job["attempts"] >= BUDGET["attempts"] or (
        job["deadline"] and utc_now() >= timestamp(job["deadline"])
    ):
        raise ValueError("Only an interrupted job within its original time and attempt budget can resume")
    update(db, key, "QUEUED", "Requeued from durable checkpoints")
    return status(db, key)


def checkpoint(db, key, stage, data):
    if status(db, key)["state"] != "RUNNING":
        raise ValueError("Job is no longer running")
    payload = json.dumps(data, default=str, sort_keys=True, allow_nan=False)
    if len(payload.encode("utf-8")) > BUDGET["checkpoint_mib"] * MIB:
        raise ValueError("Checkpoint exceeds the output budget")
    db.execute("INSERT INTO checkpoint VALUES(?,?,?)", (key, stage, payload))
    log(db, key, f"Saved {stage}")


def records(db, key):
    return {row["stage"]: json.loads(row["body"]) for row in
            db.execute("SELECT stage,body FROM checkpoint WHERE job_hash=? ORDER BY rowid", (key,))}


def resource_issue(root, process=None):
    if psutil.virtual_memory().available < BUDGET["min_ram_mib"] * MIB:
        return "Host RAM reserve"
    if shutil.disk_usage(root).free < BUDGET["min_disk_mib"] * MIB:
        return "Host disk reserve"
    with suppress(FileNotFoundError):
        used = sum(item.stat().st_size for item in root.rglob("*") if item.is_file())
        if used >= BUDGET["sandbox_mib"] * MIB:
            return "Sandbox storage ceiling"
    if process:
        if process.memory_info().rss > BUDGET["rss_mib"] * MIB:
            return "Child RSS ceiling"
        cpu = process.cpu_times()
        if cpu.user + cpu.system > BUDGET["cpu_seconds"]:
            return "Child CPU ceiling"
    return None


@contextmanager
def supervised(parent):
    done = threading.Event()

    def guard():
        while not done.wait(.5):
            if not parent.is_running():
                os._exit(3)

    observer = threading.Thread(target=guard, daemon=True)
    observer.start()
    try:
        yield
    finally:
        done.set()
        observer.join()


def work(root, key, parent):
    parent_pid, parent_created = parent.split("/")
    parent_process = psutil.Process(int(parent_pid))
    if parent_process.create_time() != float(parent_created) or not parent_process.is_running():
        raise ValueError("Supervisor identity changed")
    with (supervised(parent_process), writer_lock(SHARED_STATE / ".supervisor.lock"),
          writer_lock(SHARED_STATE / ".fit.lock"), threadpool_limits(limits=1)):
        with connect(root, write=True) as db:
            job = status(db, key)
            if job["state"] != "QUEUED" or job["attempts"] >= BUDGET["attempts"]:
                return
            request = job["request"]["identity"]
            if request["code"] != code_hash() or request["runtime"] != runtime():
                raise ValueError("Evaluator code or runtime changed. Test and approve a new pinned request")
            candidate = get(db, request["version"])
            if candidate["state"] not in ELIGIBLE or not tested(candidate, request["code"]):
                raise ValueError("Strategy is no longer eligible")
            if job["deadline"] and utc_now() >= timestamp(job["deadline"]):
                update(db, key, "FAILED", "Original deadline elapsed")
                return
            deadline = job["deadline"] or (utc_now() + pd.Timedelta(seconds=BUDGET["wall_seconds"])).isoformat()
            db.execute("UPDATE job SET deadline=?,attempts=attempts+1 WHERE hash=?", (deadline, key))
            update(db, key, "RUNNING", "Acquired exclusive research locks")
            spec = candidate["spec"]
            baseline = baseline_for(db, request["baseline"], spec)
            if digest(baseline) != request["baseline_recipe"]:
                raise ValueError("Rules-only baseline changed")
            body = data_id(db, request["dataset"])
        market, contract, coverage = load_market(root, body)
        cache = {}
        for stage in stages(spec["market"] == "FUTURES"):
            with connect(root) as db:
                job = status(db, key)
            if job["state"] != "RUNNING" or not parent_process.is_running():
                return
            if stage in job["checkpoints"]:
                continue
            result = run_case(market, contract, baseline if stage.endswith("/baseline") else spec,
                              stage, request["intervals"], request["dataset"], cache)
            with connect(root, write=True) as db:
                if status(db, key)["state"] != "RUNNING":
                    return
                checkpoint(db, key, stage, result)
        with connect(root, write=True) as db:
            if status(db, key)["state"] == "RUNNING":
                cases = records(db, key)
                finding = verdict(cases, coverage, body)
                checkpoint(db, key, "verdict", finding)
                strategy = get(db, request["version"])
                complete = cases["validation/base"]["metrics"]["accounting_complete"]
                event(db, strategy["hash"], strategy["state"], "Historical sandbox evidence recorded",
                      flags={"evidence_quality": "CONSUMED_DEVELOPMENT", "reproducibility": "PINNED_INPUTS",
                             "data_completeness": "COMPLETE_BASE_PATH" if complete else "INCOMPLETE_BASE_PATH"},
                      reference={"sandbox_job": key, "conclusion": finding["status"]})
                update(db, key, "COMPLETE", "Historical report assembled")


def verdict(cases, coverage, body):
    base = cases["validation/base"]
    peer = cases["validation/baseline"]
    stress = [case for case in BASE_CASES + FUTURES_CASES if case not in ("base", "missing_data")
              and f"validation/{case}" in cases]
    uncertainty = base["metrics"]["uncertainty"].get("mean_net_r_95_interval")
    supported = (base["status"] == peer["status"] == cases["development/base"]["status"] == "DESCRIPTIVE"
                 and all(cases[f"validation/{case}"]["status"] == "DESCRIPTIVE" for case in stress)
                 and uncertainty is not None and uncertainty[0] > 0
                 and base["metrics"]["return"] > max(0, peer["metrics"]["return"])
                 and base["metrics"]["drawdown_observed_marks"] >= peer["metrics"]["drawdown_observed_marks"]
                 and all(cases[f"validation/{case}"]["metrics"]["mean_net_r"] > 0 for case in stress)
                 and body["provenance"]["origin"] != "CONTROLLED_FIXTURE")
    return {"status": "EXPLORATORY_SCREEN" if supported else "INCONCLUSIVE", "data": coverage,
            "sample_floor": 25, "evidence": "HISTORICAL_DEVELOPMENT_ONLY", "selection_adjusted": False,
            "reason": "Sample, uncertainty, economics, baseline and stress gates cleared" if supported else
                      "Sample, uncertainty, data, stress or baseline requirements did not all clear"}


def run_next(root=ROOT):
    if not (root / "registry.sqlite3").exists():
        return {"state": "EMPTY"}
    with writer_lock(root / ".dispatch.lock"):
        with connect(root, write=True) as db:
            recover(db)
            row = db.execute("SELECT hash FROM job WHERE state='QUEUED' ORDER BY created_at LIMIT 1").fetchone()
        if row is None:
            return {"state": "EMPTY"}
        key = row["hash"]
        if reason := resource_issue(root):
            return {"state": "WAITING_FOR_RESOURCES", "reason": reason, "job": key}
        flags = subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS
        env = {**os.environ, **dict.fromkeys(("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS",
                                             "NUMBA_NUM_THREADS", "ARROW_NUM_THREADS"), "1")}
        parent = psutil.Process()
        command = [sys.executable, "-X", "utf8", "-m", "lab.strategy_lab", "_work", key,
                   f"{parent.pid}/{parent.create_time()}"]
        with (root / "worker.log").open("wb") as output:
            child = subprocess.Popen(command, cwd=PROJECT, stdin=subprocess.DEVNULL, stdout=output,
                                     stderr=output, creationflags=flags, env=env)
            reason = None
            started = time.monotonic()
            try:
                process = psutil.Process(child.pid)
                while child.poll() is None:
                    with connect(root) as db:
                        job = status(db, key)
                    with suppress(psutil.NoSuchProcess):
                        reason = ("Operator requested cancellation" if job["state"] in ("CANCEL_REQUESTED", "CANCELLED")
                                  else "Wall deadline" if time.monotonic() - started > BUDGET["wall_seconds"] or
                                  (job["deadline"] and utc_now() >= timestamp(job["deadline"]))
                                  else resource_issue(root, process))
                    if reason:
                        break
                    time.sleep(.25)
            except psutil.NoSuchProcess:
                pass
            finally:
                if child.poll() is None:
                    child.kill()
                exit_code = child.wait(timeout=10)
        with connect(root, write=True) as db:
            job = status(db, key)
            if job["state"] == "CANCEL_REQUESTED":
                update(db, key, "CANCELLED", "Cancellation acknowledged. Completed cases retained")
            elif job["state"] == "RUNNING":
                update(db, key, "FAILED" if reason == "Wall deadline" or job["attempts"] >= BUDGET["attempts"] else "INTERRUPTED",
                       reason or f"Worker exited with code {exit_code}. See worker.log")
            elif job["state"] == "QUEUED" and exit_code == 2:
                log(db, key, "Waiting for an existing research worker to release its lock")
                return {"state": "WAITING_FOR_RESEARCH_LOCK", "job": key}
            elif job["state"] == "QUEUED":
                update(db, key, "FAILED", reason or f"Worker exited before admission with code {exit_code}. See worker.log")
            return status(db, key)
