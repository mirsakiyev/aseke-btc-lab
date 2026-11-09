from collections.abc import Callable
from hashlib import sha256
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import sys
from threading import Event, Thread
import time

import httpx
import pandas as pd
import psutil
from websockets.exceptions import WebSocketException
from websockets.sync.client import connect as websocket

from lab.config import ROOT
from lab.data import fetch, normalize, requests
from lab.storage import DATA, read_json, utc, utc_now, write_json, writer_lock

from lab.phase2.checkpoint import holdout_end
from lab.phase2 import store

WS_URL = "wss://ws-feed.exchange.coinbase.com"
GRACE = pd.Timedelta("40s")
LOG_PATH = ROOT / "logs" / "research-collector.log"
WORKER = store.FORWARD / "worker.json"
STOP = store.FORWARD / "stop.json"
PAUSE = store.FORWARD / "paused.json"
LOGGER = logging.getLogger("btc_lab.collector")


def publish_status(record: dict) -> None:
    for attempt in range(3):
        try:
            write_json(WORKER, record)
            return
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(0.1)


def logger() -> None:
    if LOGGER.handlers:
        return
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(LOG_PATH, maxBytes=1_048_576, backupCount=4, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


class Heartbeat:
    """Only the latest public heartbeat is held. It is never converted into price or volume."""

    def __init__(self, stop: Event):
        self.stop = stop
        self.last: dict = {}
        self.error: str | None = None

    def listen(self):
        pause = 1
        while not self.stop.is_set():
            try:
                with websocket(WS_URL, open_timeout=8, close_timeout=2, max_size=65536, max_queue=8) as ws:
                    ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["heartbeat"]}))
                    last_rx = time.monotonic()
                    while not self.stop.is_set():
                        try:
                            packet = json.loads(ws.recv(timeout=2))
                        except TimeoutError:
                            if time.monotonic() - last_rx > 12:
                                raise TimeoutError("No BTC-USD heartbeat for 12 seconds")
                            continue
                        if packet.get("type") == "error":
                            raise ValueError(packet.get("message", "WebSocket subscription error"))
                        if packet.get("type") == "heartbeat" and packet.get("product_id") == "BTC-USD":
                            received = utc_now()
                            self.last = {"event_at": utc(packet["time"]).isoformat(), "received_at": received.isoformat(),
                                         "sequence": packet["sequence"], "last_trade_id": packet["last_trade_id"]}
                            self.error, pause, last_rx = None, 1, time.monotonic()
            except (OSError, TimeoutError, ValueError, KeyError, WebSocketException) as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                LOGGER.warning("Public stream reconnect in %ss (%s)", pause, self.error)
                self.stop.wait(pause)
                pause = min(60, pause * 2)

    def status(self) -> dict:
        latest = self.last
        healthy = bool(latest and not self.error
                       and utc_now() - utc(latest["received_at"]) < pd.Timedelta("15s")
                       and abs(utc(latest["received_at"]) - utc(latest["event_at"])) < pd.Timedelta("15s"))
        return {**latest, "healthy": healthy, "error": self.error}


def collect(db, client: httpx.Client, boundary: pd.Timestamp, now: pd.Timestamp,
            root: Path = DATA, limit: int = 4) -> list[dict]:
    """Fetch only newly closed intervals, with a two-bar reconciliation overlap."""
    outcomes = []
    with writer_lock(root / ".writer.lock"):
        for seconds in (300, 3600):
            step = pd.Timedelta(seconds=seconds)
            target = (now - GRACE).floor(step)
            cursor = store.checkpoint(db, seconds, boundary)
            if cursor >= target:
                continue
            start = max(boundary, cursor - 2 * step)
            for index, (left, right) in enumerate(requests(start, target, seconds)):
                if index >= limit:
                    break
                response = fetch(client, left, right, seconds)
                receipt = utc_now()
                problem, frame = None, None
                try:
                    frame = normalize(response.json(), left, right, seconds, receipt,
                                      sha256(response.content).hexdigest())
                except ValueError as exc:
                    problem = str(exc)
                result = store.commit_response(db, seconds, left, right, receipt, str(response.request.url),
                                               response.content, frame, problem)
                expected = int((right - left) // step)
                outcomes.append({"seconds": seconds, "start": left, "end": right,
                                 "missing": expected - result["accepted"] if problem is None else None, **result})
                if problem:
                    break
    return outcomes


def status() -> dict:
    worker = read_json(WORKER) if WORKER.exists() else {"state": "not_started"}
    alive = False
    if "pid" in worker:
        try:
            process = psutil.Process(worker["pid"])
            alive = process.create_time() == worker["process_created"] and process.is_running()
        except (psutil.Error, KeyError):
            pass
    age = (utc_now() - utc(worker["updated_at"])).total_seconds() if worker.get("updated_at") else None
    return {**worker, "process_alive": alive, "status_age_seconds": age,
            "runner_stale": bool(alive and age is not None and age > 120),
            "intentionally_paused": PAUSE.exists(), "log": str(LOG_PATH)}


def run_worker(enable_shadow: bool, *, once: bool = False, token: str = "foreground", managed: bool = False,
               notify: Callable = print) -> dict:
    from lab.phase2.shadow import Shadow

    identity = {"pid": os.getpid(), "process_created": psutil.Process().create_time(),
                "token": token, "started_at": utc_now(), "shadow_requested": enable_shadow,
                "mode": "one_cycle" if once else "continuous",
                "launcher": "scheduled" if managed else "manual"}
    stop = Event()
    last_error = None
    last_batches = []
    state = {}
    if PAUSE.exists():
        return {"state": "intentionally_paused"}
    with writer_lock(store.FORWARD / ".worker.lock"):
        if PAUSE.exists():
            return {"state": "intentionally_paused"}
        boundary = holdout_end()
        logger()
        LOGGER.info("Worker starting pid=%d shadow=%s", os.getpid(), enable_shadow)
        state = {**identity, "state": "starting", "updated_at": utc_now()}
        publish_status(state)
        stream = Heartbeat(stop)
        thread = Thread(target=stream.listen, name="coinbase-heartbeat", daemon=True)
        thread.start()
        try:
            with store.connect() as db, httpx.Client(timeout=10) as client:
                shadow = Shadow(boundary) if enable_shadow else None
                next_request = 0.0
                retry_pause = 5
                if once:
                    for _ in range(50):
                        if stream.status()["healthy"]:
                            break
                        stop.wait(0.2)
                while True:
                    now = utc_now()
                    if PAUSE.exists() or (STOP.exists() and read_json(STOP).get("token") == token):
                        state["stop_reason"] = "operator_stop"
                        break
                    if store.budget() >= store.STORE_LIMIT:
                        state["storage_limited"] = True
                        last_error = "Forward storage budget reached"
                        state.update(last_error=last_error, stop_reason="storage_budget")
                        LOGGER.error(last_error)
                        break
                    if time.monotonic() >= next_request:
                        try:
                            observed = collect(db, client, boundary, now)
                            if observed:
                                last_batches = observed
                                LOGGER.info("Published %d REST batches: %s", len(observed), observed)
                            last_error = next((b["error"] for b in observed if "error" in b), None)
                            retry_pause = min(retry_pause * 2, 120) if last_error else 5
                        except (httpx.HTTPError, ValueError, RuntimeError) as exc:
                            last_error = f"{type(exc).__name__}: {exc}"
                            LOGGER.warning("Collection temporarily blocked (%s)", last_error)
                            retry_pause = min(retry_pause * 2, 120)
                        next_request = time.monotonic() + retry_pause
                    public = stream.status()
                    if shadow is not None:
                        try:
                            shadow.tick(db, public)
                        except (ValueError, OSError) as exc:
                            shadow.problem = f"{type(exc).__name__}: {exc}"
                            LOGGER.warning("Shadow temporarily blocked (%s)", shadow.problem)
                    state = {
                        **identity, "state": "running" if not last_error else "degraded", "updated_at": utc_now(),
                        "heartbeat": public, "feeds": store.feed_status(db), "last_batches": last_batches,
                        "last_error": last_error, "bytes_used": store.budget(), "budget_bytes": store.STORE_LIMIT,
                        "shadow": shadow.status(db) if shadow else {"state": "disabled"},
                    }
                    publish_status(state)
                    if once:
                        break
                    stop.wait(2)
        except KeyboardInterrupt:
            state["stop_reason"] = "keyboard_interrupt"
            LOGGER.info("Keyboard interrupt")
        except Exception as exc:
            LOGGER.exception("Worker failed")
            state.update(state="failed", last_error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            stop.set()
            thread.join(timeout=4)
            if state["state"] != "failed":
                state["state"] = "cycle_complete" if once else "stopped"
            state["updated_at"] = utc_now()
            publish_status(state)
            LOGGER.info("Worker %s", state["state"])
    notify(f"Collector {state['state']}. Log {LOG_PATH}")
    return state
