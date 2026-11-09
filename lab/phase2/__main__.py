import argparse
import os
import subprocess
import sys
import time
from uuid import uuid4

for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ[key] = "1"

from lab.config import ROOT
from lab.storage import utc_now, write_json, writer_lock
from lab.phase2 import store
from lab.phase2.collector import LOG_PATH, PAUSE, STOP, run_worker, status


def start(shadow: bool) -> dict:
    with writer_lock(store.FORWARD / ".control.lock"):
        current = status()
        if current["process_alive"]:
            return current
        PAUSE.unlink(missing_ok=True)
        token = uuid4().hex
        command = [sys.executable, "-m", "lab.phase2", "collect", "--token", token]
        if shadow:
            command.append("--shadow")
        child = subprocess.Popen(
            command, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
        )
        for _ in range(60):
            time.sleep(0.25)
            current = status()
            if current.get("token") == token and current["process_alive"] and current["state"] != "starting":
                return current
            if child.poll() is not None:
                raise RuntimeError(f"Collector exited {child.returncode}. See {LOG_PATH}")
        if current.get("token") == token and current["process_alive"]:
            return current
        raise RuntimeError(f"Collector did not publish its process identity. See {LOG_PATH}")


def stop() -> dict:
    with writer_lock(store.FORWARD / ".control.lock"):
        if not PAUSE.exists():
            write_json(PAUSE, {"requested_at": utc_now()})
        current = status()
        if not current["process_alive"]:
            return current
        write_json(STOP, {"token": current["token"], "requested_at": utc_now()})
        for _ in range(160):
            time.sleep(0.25)
            current = status()
            if not current["process_alive"]:
                return current
        raise RuntimeError("Stop requested. Worker is still finishing a bounded request. Check status again.")


def allow_scheduled_start() -> dict:
    with writer_lock(store.FORWARD / ".control.lock"):
        current = status()
        if current["process_alive"]:
            raise RuntimeError("The current worker must release its lock before a scheduled launch.")
        with writer_lock(store.FORWARD / ".worker.lock"):
            PAUSE.unlink(missing_ok=True)
        return {"state": "scheduled_start_allowed"}


def main():
    import json

    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Phase 2")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("audit")
    commands.add_parser("status")
    commands.add_parser("stop")
    commands.add_parser("allow-scheduled-start", help=argparse.SUPPRESS)
    begin = commands.add_parser("start")
    begin.add_argument("--shadow", action="store_true")
    foreground = commands.add_parser("collect")
    foreground.add_argument("--shadow", action="store_true")
    foreground.add_argument("--once", action="store_true")
    foreground.add_argument("--token")
    foreground.add_argument("--managed", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.command == "audit":
        from lab.phase2.audit import audit
        print(audit())
        return
    if args.command == "collect":
        if args.managed and (args.once or not args.shadow):
            parser.error("The scheduled task must run continuous shadow collection.")
        result = run_worker(args.shadow, once=args.once, token=args.token or uuid4().hex,
                            managed=args.managed, notify=lambda _: None)
    elif args.command == "start":
        result = start(args.shadow)
    elif args.command == "stop":
        result = stop()
    elif args.command == "allow-scheduled-start":
        result = allow_scheduled_start()
    else:
        result = status()
    print(json.dumps(result, indent=2, default=str, allow_nan=False))
    if args.command == "collect" and args.once and result.get("last_error"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
