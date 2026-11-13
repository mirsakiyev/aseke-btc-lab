"""Pause and hold the local write boundaries during a KuCoin task repair."""

from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import json
import msvcrt
import os
from pathlib import Path
import sqlite3
import sys
from time import monotonic, sleep
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
WORKER = "data/phase10/.worker.lock"
ACTIVITY = "data/phase10/.activity.lock"
CONTROL = "data/phase10/control.json"
CONTROL_LOCK = "data/phase10/.control.lock"
REPAIR = "data/phase10/observer-repair.json"
WRITE_LOCKS = (
    "runs/phase10/.observer-cycle.lock", "runs/phase15/.writer.lock",
    "runs/phase16/.dispatch.lock", "runs/phase17/.dispatch.lock", "runs/phase17/.first-spot.lock",
    "runs/phase17/.first-managed.lock", "runs/phase17/pilot/.dispatch.lock",
)


class Busy(RuntimeError):
    pass


@contextmanager
def _lease(path):
    with path.open("a+b") as handle:
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise Busy(f"Active writer at {path.name}") from exc
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def _read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _save(path, value):
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, allow_nan=False), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _state(root, action, reason=None):
    with _lease(root / CONTROL_LOCK):
        control = _read(root / CONTROL)
        if type(control["paused"]) is not bool or type(control["monitor"]) is not bool:
            raise ValueError("Invalid KuCoin monitoring control")
        runtime = _read(root / "data/phase10/runtime.json") if action != "status" else {}
        old = _read(root / REPAIR) if (root / REPAIR).is_file() else {}
        if action in {"pause", "block", "resume"}:
            pause = action != "resume"
            if action == "resume" and not control["monitor"]:
                raise ValueError("KuCoin monitoring was independently disabled")
            if control["paused"] != pause:
                control["paused"] = pause
                _save(root / CONTROL, control)
        if action != "status":
            labels = {"pause": "DRAINING", "block": "BLOCKED", "resume": "START_PENDING", "started": "START_REQUESTED"}
            launch = old.get("launch_id") if action == "started" else runtime.get("launch_id")
            _save(root / REPAIR, {"state": labels[action], "paused": control["paused"], "launch_id": launch,
                                 "reason": reason, "at": datetime.now(timezone.utc).isoformat()})
    try:
        with _lease(root / WORKER):
            owned = False
    except Busy:
        owned = True
    repair = _read(root / REPAIR) if (root / REPAIR).is_file() else None
    return {"paused": control["paused"], "monitor": control["monitor"], "worker_lock_held": owned, "repair": repair}


def _update(root, action, reason=None):
    deadline = monotonic() + 3
    while True:
        try:
            return _state(root, action, reason)
        except Busy:
            if monotonic() >= deadline:
                raise
            sleep(.1)


@contextmanager
def quiescence(root, allow_worker=False):
    """Keep admission and journal/capture locks held until the repair is done."""
    from lab.phase15.restart_preflight import verify

    with ExitStack() as held:
        held.enter_context(_lease(root / CONTROL_LOCK))
        if _read(root / CONTROL)["paused"] is not True:
            raise ValueError("KuCoin must be paused during repair")
        if allow_worker:
            runtime = _read(root / "data/phase10/runtime.json")
            if runtime.get("drain_guard_version") != 1 or not (root / ACTIVITY).is_file():
                raise ValueError("Live worker has no verified activity guard")
            held.enter_context(_lease(root / ACTIVITY))
        else:
            held.enter_context(_lease(root / WORKER))
        for relative in WRITE_LOCKS:
            held.enter_context(_lease(root / relative))
        for name in ("phase13", "phase14", "phase17"):
            for path in sorted((root / "runs" / name).rglob(".capture.lock")):
                held.enter_context(_lease(path))
        verify(root)
        yield


def main():
    action, *arguments = sys.argv[1:]
    if action == "hold" and arguments in ([], ["allow-worker"]):
        with quiescence(ROOT, bool(arguments)):
            print("QUIESCENT", flush=True)
            sys.stdin.readline()
    elif action == "status" and not arguments:
        print(json.dumps(_update(ROOT, action)))
    elif action in {"pause", "resume", "block", "started"} and len(arguments) <= 1:
        reason = sys.stdin.readline().strip() if arguments == ["--reason-stdin"] else (arguments[0] if arguments else None)
        print(json.dumps(_update(ROOT, action, reason)))
    else:
        raise ValueError("Invalid observer repair action")


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    try:
        main()
    except (OSError, RuntimeError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        print(f"KuCoin repair guard: {exc}" if isinstance(exc, Busy) else f"KuCoin repair guard failed: {type(exc).__name__}",
              file=sys.stderr)
        raise SystemExit(1) from None
