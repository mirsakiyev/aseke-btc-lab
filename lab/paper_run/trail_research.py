"""Read completed trail inputs without waiting on the financial owner."""

import logging
from threading import Condition, Thread
from time import monotonic_ns

from lab.paper_run import breakout


LOG = logging.getLogger(__name__)
MAX_TRACKED_BOOKS = 32


class Preparer:
    def __init__(self):
        self.condition = Condition()
        self.requested = {}
        self.pending = {}
        self.results = {}
        self.submitted = {}
        self.stopping = False
        self.thread = Thread(target=self._run, name="paper-trail-research", daemon=True)
        self.thread.start()

    def retain(self, books):
        with self.condition:
            for requests in (self.requested, self.pending, self.results, self.submitted):
                for stale in requests.keys() - books:
                    del requests[stale]

    def get(self, generation, book, lane, now, identity):
        book_id = book["book_id"]
        key = generation, identity
        with self.condition:
            if self.requested.get(book_id) == key:
                result = self.results.pop(book_id, None)
                if result is not None:
                    del self.requested[book_id]
                    del self.submitted[book_id]
                return result
            if book_id not in self.requested and len(self.requested) >= MAX_TRACKED_BOOKS:
                return None
            self.requested[book_id] = key
            self.submitted[book_id] = monotonic_ns()
            self.results.pop(book_id, None)
            # Only the reference-free position data used by trail_inputs.
            position = book["position"]
            state = {"position": {name: position.get(name) for name in ("entry_at", "last_trail")}}
            self.pending[book_id] = key, state, lane, now
            self.condition.notify()
            return None

    def _run(self):
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self.stopping or self.pending)
                if self.stopping:
                    return
                book_id = next(iter(self.pending))
                key, book, lane, now = self.pending.pop(book_id)
            try:
                result = breakout.trail_inputs(book, lane, now), None, now
            except Exception as error:
                LOG.exception("Paper trail evidence preparation failed for %s", book_id)
                result = (), type(error).__name__, now
            with self.condition:
                if not self.stopping and self.requested.get(book_id) == key:
                    self.results[book_id] = result
                    self.condition.notify_all()

    def status(self):
        with self.condition:
            unready = self.requested.keys() - self.results.keys()
            oldest = min((self.submitted[key] for key in unready), default=None)
            return {"unready": len(unready), "ready": len(self.results),
                    "oldest_unready_ns": monotonic_ns() - oldest if oldest is not None else None}

    def stop(self):
        with self.condition:
            self.stopping = True
            self.pending.clear()
            self.results.clear()
            self.requested.clear()
            self.submitted.clear()
            self.condition.notify()
