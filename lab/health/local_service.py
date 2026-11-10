"""Temporary, detached watchdog for a session without task-registration access."""

import os
import subprocess
import time

import psutil

from lab.health.dashboard_service import PROJECT, PYTHON, ROOT
from lab.storage import utc_now, write_json, writer_lock


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    with writer_lock(ROOT / ".local-watch.lock"):
        write_json(ROOT / "local-watch.json", {"pid": os.getpid(), "created": psutil.Process().create_time()})
        while True:
            started = time.monotonic()
            try:
                subprocess.run([str(PYTHON), "-X", "utf8", "-m", "lab.health", "check-once", "--local"],
                               cwd=PROJECT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               creationflags=subprocess.CREATE_NO_WINDOW, check=True, timeout=100)
            except (OSError, subprocess.SubprocessError) as error:
                write_json(ROOT / "local-watch-error.json", {"at": utc_now().isoformat(), "reason": type(error).__name__})
            time.sleep(max(1, 180 - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
