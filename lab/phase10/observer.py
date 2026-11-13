"""Own and audit the one-shot observer spawned by the scheduled KuCoin worker."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import subprocess
import sys
import time


DEADLINE_SECONDS = 90
RETRY_SECONDS = 45


class Observer:
    def __init__(self, project):
        self.project = project
        self.child = None
        self.started = 0.
        self.retry_at = 0.
        self.timed_out = False
        self.status = {"state": "WAITING"}

    def tick(self, launch, due):
        """Poll without waiting so the KuCoin collector and heartbeat can continue."""
        now = time.monotonic()
        if self.child is not None:
            elapsed = now - self.started
            code = self.child.poll()
            if code is None:
                if elapsed >= DEADLINE_SECONDS and not self.timed_out:
                    try:
                        self.child.terminate()
                    except OSError:
                        if self.child.poll() is None:
                            raise
                    self.timed_out = True
                    self.status.update(state="TIMEOUT_TERMINATING", elapsed_seconds=round(elapsed, 3))
                return
            self.status.update(state="TIMEOUT" if self.timed_out else "EXITED" if code == 0 else "FAILED",
                               exit_code=code, elapsed_seconds=round(elapsed, 3))
            if self.timed_out or code:
                self.retry_at = now + RETRY_SECONDS
            self.child = None
            return
        if now < self.retry_at or not due():
            return
        # CPython's Windows venv executable is a redirector. Starting the
        # underlying interpreter directly gives ownership of the actual writer.
        self.child = subprocess.Popen(
            [sys._base_executable, "-X", "utf8", "-m", "lab.phase13", "_observe", launch], cwd=self.project,
            env={**os.environ, "__PYVENV_LAUNCHER__": sys.executable},
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.NORMAL_PRIORITY_CLASS,
        )
        self.started = now
        self.timed_out = False
        self.status = {"state": "RUNNING", "pid": self.child.pid, "at": datetime.now(timezone.utc).isoformat()}

    def drain(self):
        """Poll an already-owned child during an operator pause without stopping it."""
        if self.child is None:
            return True
        code = self.child.poll()
        if code is None:
            self.status.update(state="DRAINING")
            return False
        self.status.update(state="EXITED" if code == 0 else "FAILED", exit_code=code)
        self.child = None
        return True


class ObservationAudit:
    """Record timing and outcome identifiers with a fixed local storage bound."""

    def __init__(self, output, launch):
        self.output = output
        self.launch = launch

    @contextmanager
    def open(self):
        self.output.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(self.output / "observer.jsonl", maxBytes=256 * 1024,
                                      backupCount=3, encoding="utf-8")
        self.logger = logging.Logger("lab.phase10.observer")
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        try:
            yield self
        finally:
            self.logger.removeHandler(handler)
            handler.close()

    def record(self, stage, state, **evidence):
        self.logger.info(json.dumps({"at": datetime.now(timezone.utc).isoformat(), "pid": os.getpid(),
                                     "launch": self.launch, "stage": stage, "state": state, **evidence},
                                    default=str, separators=(",", ":")))

    @contextmanager
    def stage(self, name):
        started = time.monotonic()
        self.record(name, "STARTED")
        try:
            yield
        except Exception as error:
            self.record(name, "FAILED", error=type(error).__name__, seconds=round(time.monotonic() - started, 3))
            raise
        else:
            self.record(name, "COMPLETED", seconds=round(time.monotonic() - started, 3))

    def phase15(self, result):
        for book, evaluation in result.get("opportunities", {}).items():
            gates = evaluation.get("gates", {})
            self.record("phase15_decision", evaluation["state"], book=book, evaluated_at=evaluation.get("as_of"),
                        completed_at=result.get("at"),
                        boundary=evaluation.get("source_cutoff"), receipt=evaluation.get("latest_candle_receipt"),
                        completed_candle=gates.get("completed candle"), feature_readiness=gates.get("feature readiness"),
                        decision=evaluation.get("decision"), category=evaluation.get("category"))
