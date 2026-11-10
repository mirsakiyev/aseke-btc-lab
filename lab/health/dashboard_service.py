import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import subprocess
import sys
import threading
import time
from urllib.request import ProxyHandler, build_opener

import psutil

from lab.config import ROOT as PROJECT
from lab.storage import RUNS, read_json, utc_now, write_json, writer_lock


ROOT = RUNS / "health"
PORT = 8501
PYTHON = PROJECT / ".venv" / "Scripts" / "python.exe"


def expected(process):
    try:
        args = process.cmdline()
        return (Path(process.cwd()).resolve() == PROJECT.resolve()
                and Path(process.exe()).resolve() in {PYTHON.resolve(), Path(sys._base_executable).resolve()}
                and any(args[i:i+3] == ["-m", "streamlit", "run"] and i+3 < len(args)
                        and (PROJECT / args[i+3]).resolve() == (PROJECT / "dashboard.py").resolve()
                        for i in range(len(args))))
    except (PermissionError, psutil.AccessDenied):
        return None
    except (OSError, psutil.Error):
        return False


def inspect(*, probe=True):
    try:
        rows = [row for row in psutil.net_connections(kind="tcp") if row.status == psutil.CONN_LISTEN and row.laddr.port == PORT]
        if not rows:
            return {"state": "NO_LISTENER"}
        identities = {row.pid for row in rows}
        if len(identities) != 1 or any(row.laddr.ip != "127.0.0.1" for row in rows):
            return {"state": "PORT_CONFLICT", "pids": sorted(pid for pid in identities if pid)}
        pid = identities.pop()
        if pid is None:
            return {"state": "IDENTITY_UNKNOWN"}
        process = psutil.Process(pid)
        identity = {"pid": pid}
        try:
            identity.update(created=process.create_time(), rss_mib=round(process.memory_info().rss/1024**2, 1))
        except (PermissionError, psutil.AccessDenied):
            pass
        verified = expected(process)
        if verified is False:
            return {"state": "PORT_CONFLICT", **identity}
        if not probe:
            return {"state": "EXPECTED_LISTENER" if verified else "IDENTITY_UNKNOWN", **identity}
        try:
            with build_opener(ProxyHandler({})).open(f"http://127.0.0.1:{PORT}/_stcore/health", timeout=2) as response:
                ready = response.status == 200 and response.read(32).strip() == b"ok"
        except (OSError, ValueError):
            ready = False
        state = "HEALTHY" if verified else "HTTP_HEALTHY_IDENTITY_UNVERIFIED"
        return {"state": state if ready else "HTTP_UNREACHABLE", "identity_verified": bool(verified), **identity}
    except (OSError, psutil.Error):
        return {"state": "IDENTITY_UNKNOWN"}


def record(state, **values):
    process = psutil.Process()
    write_json(ROOT / "dashboard-supervisor.json", {"state": state, "at": utc_now().isoformat(),
               "supervisor_pid": process.pid, "supervisor_created": process.create_time(), **values})


def copy_output(stream, logger):
    with stream:
        for line in stream:
            logger.info(line.rstrip())


def serve():
    ROOT.mkdir(parents=True, exist_ok=True)
    with writer_lock(ROOT / ".dashboard.lock"):
        logger = logging.getLogger("aseke.dashboard")
        logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(ROOT / "dashboard.log", maxBytes=512*1024, backupCount=3, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)
        failures = 0
        child = None
        try:
            while failures < 4:
                listener = inspect(probe=False)
                if listener["state"] == "PORT_CONFLICT":
                    record(listener["state"], listener=listener, failures=failures)
                    logger.error("Port %s unavailable to the verified project instance", PORT)
                    return 1
                if listener["state"] in {"EXPECTED_LISTENER", "IDENTITY_UNKNOWN"}:
                    state = "OBSERVING_EXPECTED_LISTENER" if listener["state"] == "EXPECTED_LISTENER" else "WAITING_FOR_PORT_IDENTITY"
                    record(state, listener=listener, failures=failures)
                    time.sleep(10)
                    continue
                child = subprocess.Popen([str(PYTHON), "-X", "utf8", "-m", "streamlit", "run", "dashboard.py",
                           "--server.address", "127.0.0.1", "--server.port", str(PORT), "--server.headless", "true",
                           "--browser.gatherUsageStats", "false"], cwd=PROJECT, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
                           creationflags=subprocess.CREATE_NO_WINDOW)
                threading.Thread(target=copy_output, args=(child.stdout, logger), daemon=True).start()
                record("CHILD_STARTED", child=child.pid, failures=failures)
                started = time.monotonic()
                code = child.wait()
                failures = 1 if time.monotonic()-started >= 300 else failures+1
                record("RESTART_PENDING" if failures < 4 else "RETRY_BUDGET_EXHAUSTED", exit_code=code, failures=failures)
                logger.warning("Dashboard child exited with code %s, consecutive short runs %s", code, failures)
                if failures < 4:
                    time.sleep(5*failures)
            return 1
        finally:
            if child and child.poll() is None:
                child.terminate()
            logger.removeHandler(handler)
            handler.close()


def recover():
    status = inspect(probe=False)
    supervisor = read_json(ROOT / "dashboard-supervisor.json")
    if status["state"] != "EXPECTED_LISTENER" or supervisor.get("state") != "CHILD_STARTED":
        raise ValueError("Recovery needs a verified dashboard child started by this supervisor")
    process = psutil.Process(status["pid"])
    ancestry = {p.pid: p for p in process.parents()}
    owner = ancestry.get(supervisor["supervisor_pid"])
    if (not owner or owner.create_time() != supervisor.get("supervisor_created")
            or supervisor["child"] not in ancestry.keys() | {process.pid}
            or process.create_time() != status["created"] or not expected(process)):
        raise ValueError("Dashboard recovery identity changed")
    process.terminate()
    return {"state": "DASHBOARD_CHILD_STOPPED_FOR_RECOVERY", "pid": process.pid}


if __name__ == "__main__":
    raise SystemExit(serve())
