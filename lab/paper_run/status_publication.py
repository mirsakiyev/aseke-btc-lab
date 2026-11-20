"""Owner-produced status snapshots are independent of financial progress."""

import logging
from threading import Condition, Thread

from lab import runtime_snapshot


LOG = logging.getLogger(__name__)


def publish(path, document):
    try:
        runtime_snapshot.publish(path, document)
    except (OSError, ValueError) as error:
        LOG.exception("Paper status publication failed for %s", path.name)
        return {"state": "FAILED", "reason": type(error).__name__}
    return {"state": "PUBLISHED"}


class Publisher:
    """Keep only the newest pending document of each kind in memory."""

    def __init__(self):
        self.condition = Condition()
        self.pending = {}
        self.previous = {}
        self.stopping = False
        self.thread = Thread(target=self._run, name="paper-status-publication", daemon=True)
        self.thread.start()

    def queue(self, path, document):
        with self.condition:
            self.pending[path] = document
            self.condition.notify()
            return {"state": "QUEUED", "previous_attempt": self.previous.get(path)}

    def _run(self):
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self.stopping or self.pending)
                if self.stopping:
                    return
                path = next(iter(self.pending))
                document = self.pending.pop(path)
            result = publish(path, document)
            with self.condition:
                self.previous[path] = result

    def stop(self):
        with self.condition:
            self.stopping = True
            self.pending.clear()
            self.condition.notify()
        # The status thread does not own a financial connection. A blocked OS
        # operation can finish before process exit; handoff fences that process.
