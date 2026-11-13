"""Bounded stage receipts for the unattended watchdog."""

import ctypes
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time


BELOW_NORMAL_PRIORITY_CLASS = 0x4000
NORMAL_PRIORITY_CLASS = 0x20


class StageLog:
    def __init__(self, directory):
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.events = []
        self.path = directory / f"scheduled-stage-{int(time.time() / 180) % 16:02d}.json"

    def record(self, stage, *, reason=None, decisions=None):
        event = {"stage": stage, "at": datetime.now(timezone.utc).isoformat(),
                 "wall_seconds": round(time.monotonic() - self.started, 3),
                 "cpu_seconds": round(time.process_time() - self.cpu_started, 3)}
        if reason is not None:
            event["reason"] = reason
        if decisions is not None:
            event["decisions"] = decisions
        self.events = (self.events + [event])[-32:]
        temporary = self.path.with_suffix(".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            try:
                temporary.write_text(json.dumps({"pid": os.getpid(), "parent_pid": os.getppid(),
                                                 "events": self.events}), encoding="utf-8")
                temporary.replace(self.path)
            finally:
                temporary.unlink(missing_ok=True)
        except OSError:
            pass


def scheduled_priority():
    """Give the watchdog equal CPU scheduling with ordinary Windows processes."""
    if os.name != "nt":
        return "NOT_WINDOWS"
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    kernel.GetPriorityClass.argtypes = [ctypes.c_void_p]
    kernel.SetPriorityClass.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    process = kernel.GetCurrentProcess()
    priority = kernel.GetPriorityClass(process)
    if not priority:
        return "PRIORITY_UNAVAILABLE"
    if priority != BELOW_NORMAL_PRIORITY_CLASS:
        return "UNCHANGED"
    if not kernel.SetPriorityClass(process, NORMAL_PRIORITY_CLASS):
        return "PRIORITY_CHANGE_FAILED"
    return "NORMAL"


def bootstrap():
    priority = scheduled_priority()
    trace = StageLog(Path(__file__).resolve().parents[2] / "runs" / "health")
    trace.record("python_started", reason=priority)
    return trace
