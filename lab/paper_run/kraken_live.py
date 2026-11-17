"""One bounded public Kraken BTC/USD trade stream, shared by the EMA books."""

from collections import Counter, deque
from contextlib import closing
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json
from math import ceil, isfinite
from statistics import median
import re
from threading import Event, Lock, Thread
import time
from urllib.parse import urlsplit
from uuid import uuid4

import pandas as pd
import psutil
from websockets.exceptions import InvalidStatus, WebSocketException
from websockets.sync.client import connect

from lab.ema_paper import kraken_reference
from lab.kraken_paper import source as kraken_source
from lab.paper_run import ema, execution
from lab.storage import utc


URL = "wss://ws.kraken.com/v2"
KIND = "WEBSOCKET_TRADE"
SUBSCRIBE = {"method": "subscribe", "params": {"channel": "trade", "symbol": [ema.SYMBOL], "snapshot": False}, "req_id": 1}
AGE_NS = 2_000_000_000
CLOCK_TOLERANCE_NS = 100_000_000
PERMISSION_INTERVAL_NS = 60_000_000_000
OPEN_TIMEOUT = 5.
ACK_TIMEOUT = 5.
IDLE_TIMEOUT = 10.
QUEUE_LIMIT = 256
MAX_MESSAGE_BYTES = 32 * 1024
WITNESS_LIMIT = 128
WITNESS_SCHEMA = "KRAKEN_TRANSPORT_WITNESS_V1"


def _safe_detail(value):
    text = " ".join(re.sub(r"[\x00-\x1f\x7f]", " ", str(value)).split())[:1024]

    def redact_url(match):
        try:
            parts = urlsplit(match[0])
        except ValueError:
            return "[redacted URL]"
        return f"{parts.scheme}://{parts.hostname or '[redacted]'}{parts.path}"

    text = re.sub(r"(?:https?|wss?)://[^\s]+", redact_url, text)
    text = re.sub(r"(?i)authorization\s*[:=]\s*(?:Bearer\s+)?\S+", "authorization=[redacted]", text)
    text = re.sub(r"(?i)(authorization|api[_-]?key|token|secret|password)\s*[:=]\s*\S+", r"\1=[redacted]", text)
    return text[:240]


def _exception_details(error):
    if error is None:
        return None
    result = {"type": type(error).__name__, "message": _safe_detail(error)}
    for name in ("rcvd", "sent"):
        frame = getattr(error, name, None)
        if frame is not None:
            code = getattr(frame, "code", None)
            result[name] = {"code": int(code) if isinstance(code, int) else None,
                            "reason": _safe_detail(getattr(frame, "reason", ""))}
    cause = error.__cause__
    if cause is not None:
        result["cause"] = {"type": type(cause).__name__, "message": _safe_detail(cause)}
    return result


def permission(at):
    """Verify the retained permission evidence and current BTC/USD instrument receipt."""
    root = kraken_reference.ROOT
    identity = kraken_reference.scope_identity(root=root, as_of=at)
    db = kraken_source._read_connection(root / "source")
    if db is None:
        raise PermissionError("Kraken BTC/USD source receipt store unavailable")
    with closing(db):
        instrument = kraken_source._instrument_result(kraken_source._latest_instrument(db, identity.id, at), at)
        if instrument["state"] != "READY":
            raise PermissionError(f"Kraken BTC/USD instrument evidence {instrument['state']}")
        saved = db.execute("SELECT body FROM raw WHERE sha=?", (instrument["raw_sha256"],)).fetchone()
        if saved is None or sha256(saved[0]).hexdigest() != instrument["raw_sha256"]:
            raise PermissionError("Kraken BTC/USD instrument evidence checksum failed")


def _time(value):
    if not isinstance(value, str):
        raise ValueError("An exchange timestamp with offset is required")
    at = pd.Timestamp(value)
    if pd.isna(at) or at.tzinfo is None:
        raise ValueError("An exchange timestamp with offset is required")
    return utc(at)


def _local_step(previous, current):
    old_at, old_mono = previous
    at, mono = current
    return mono < old_mono or abs((at - old_at).value - (mono - old_mono)) > CLOCK_TOLERANCE_NS


def ready_reason(observation, at, mono):
    """Recheck an already decoded stream point at its real or sampled decision clock."""
    if not observation:
        return "NO_LIVE_TRADE"
    source, receipt = (_time(observation[key]) for key in ("source_at", "received_at"))
    if source > receipt or receipt > at:
        return "FUTURE_EXCHANGE_OR_RECEIPT_CLOCK"
    if _local_step((receipt, observation["received_mono_ns"]), (at, mono)):
        return "LOCAL_CLOCK_STEP"
    if max((at - source).value, (receipt - source).value + (mono - observation["received_mono_ns"])) > AGE_NS:
        return "STALE_AT_CONSUME"
    return None


class _ChannelRejected(ValueError):
    pass


class KrakenTradeFeed:
    """One owner thread, one socket, and a nonblocking chronological consumer queue."""

    def __init__(self, *, connector=connect, read_clock=execution.clock, authorize=permission, queue_size=QUEUE_LIMIT):
        if type(queue_size) is not int or not 1 <= queue_size <= QUEUE_LIMIT:
            raise ValueError("Kraken live queue must have a finite approved bound")
        self._connect, self._clock, self._authorize = connector, read_clock, authorize
        self._limit = queue_size
        self._queue = deque()
        self._lock = Lock()
        self._stop, self._reconnect = Event(), Event()
        self._thread = None
        self._state = "NOT_STARTED"
        self._reason = None
        self._details = {}
        self._counts = Counter()
        self._high_water = 0
        self._last_id = 0
        self._last_source = None
        self._last_point = None
        self._feed_instance = uuid4().hex
        self._witness_sequence = 0
        self._witnesses = deque()
        self._witness_overflow = None
        self._awaiting_recovery = False

    def start(self):
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("The Kraken trade feed has been closed")
            if self._thread is None:
                self._thread = Thread(target=self._run, name="Kraken BTC/USD paper reference", daemon=True)
                self._thread.start()
        return self

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=OPEN_TIMEOUT + 2.)

    def request_reconnect(self):
        """Ask the owner to discard buffered data and resubscribe using normal backoff."""
        self._reconnect.set()

    def drain(self):
        with self._lock:
            points = list(self._queue)
            self._queue.clear()
        return points

    def pending_witnesses(self):
        """Copy incident evidence. A failed consumer transaction leaves it pending."""
        with self._lock:
            overflow = [self._witness_overflow] if self._witness_overflow else []
            return deepcopy([*overflow, *self._witnesses])

    def acknowledge_witnesses(self, identifiers):
        """Acknowledge exact witness IDs only after the consumer commits them."""
        identifiers = set(identifiers)
        with self._lock:
            self._witnesses = deque(item for item in self._witnesses if item["id"] not in identifiers)
            if self._witness_overflow and self._witness_overflow["id"] in identifiers:
                self._witness_overflow = None

    def status(self):
        with self._lock:
            return {"state": self._state, "reason": self._reason, "queue_size": len(self._queue),
                    "queue_high_water": self._high_water, "queue_limit": self._limit,
                    "counts": dict(self._counts), "pending_witness_count": len(self._witnesses),
                    "witness_overflow": deepcopy(self._witness_overflow), **self._details}

    def _witness_locked(self, kind, at, mono, **details):
        self._witness_sequence += 1
        witness = {"id": f"{self._feed_instance}:{self._witness_sequence}", "schema": WITNESS_SCHEMA,
                   "kind": kind, "at": at.isoformat(), "monotonic_ns": mono,
                   "feed_instance_id": self._feed_instance, "endpoint": URL,
                   "source": ema.SOURCE, "symbol": ema.SYMBOL, "counts": dict(self._counts),
                   "subscription_epoch": self._details.get("subscription_epoch"), **deepcopy(details)}
        if len(self._witnesses) >= WITNESS_LIMIT:
            lost = self._witnesses.popleft()
            previous = self._witness_overflow or {}
            self._counts["witness_overflow"] += 1
            self._witness_overflow = {
                **witness, "id": f"{self._feed_instance}:overflow:{self._counts['witness_overflow']}",
                "kind": "WITNESS_OVERFLOW", "first_lost_at": previous.get("first_lost_at", lost["at"]),
                "first_lost_id": previous.get("first_lost_id", lost["id"]), "last_lost_id": lost["id"],
                "dropped_count": previous.get("dropped_count", 0) + 1,
                "historical_coverage": "UNRESOLVED"}
        self._witnesses.append(witness)

    def _count(self, name, quantity=1):
        with self._lock:
            self._counts[name] += quantity

    def _state_changed(self, state, reason=None):
        with self._lock:
            self._state, self._reason = state, reason

    def _disconnected(self, reason, *, error=None):
        at, mono = self._clock()
        with self._lock:
            previous_state = self._state
            if self._state in {"STREAMING", "SUBSCRIBING"}:
                self._counts["disconnects"] += 1
            discarded = len(self._queue)
            first_discarded = deepcopy(self._queue[0]) if self._queue else None
            last_discarded = deepcopy(self._queue[-1]) if self._queue else None
            self._counts["discarded_on_disconnect"] += discarded
            self._queue.clear()
            self._state = "BACKOFF"
            self._reason = reason
            self._details["disconnected_at"] = at.isoformat()
            self._awaiting_recovery = True
            self._witness_locked("DISCONNECTED", at, mono, reason=reason, prior_state=previous_state,
                                 exception=_exception_details(error), last_accepted_reference=self._last_point,
                                 discarded_count=discarded, first_discarded_reference=first_discarded,
                                 last_discarded_reference=last_discarded,
                                 subscription={key: value for key, value in self._details.items()
                                               if key.startswith("subscription_")})

    def _message(self, raw, received, session, generation):
        if not isinstance(raw, bytes) or len(raw) > MAX_MESSAGE_BYTES:
            self._count("INVALID_MESSAGE")
            return session
        try:
            payload = json.loads(raw, parse_float=Decimal)
        except (UnicodeError, ValueError):
            self._count("INVALID_MESSAGE")
            return session
        if not isinstance(payload, dict):
            self._count("INVALID_MESSAGE")
            return session
        channel = payload.get("channel")
        if payload.get("method") == "subscribe":
            if session.get("ack"):
                self._count("UNEXPECTED_ACKNOWLEDGEMENT")
                return session
            result = payload.get("result", {})
            if (payload.get("req_id") != SUBSCRIBE["req_id"] or payload.get("success") is not True
                    or not isinstance(result, dict) or result.get("channel") != "trade"
                    or result.get("symbol") != ema.SYMBOL or result.get("snapshot") is not False):
                raise _ChannelRejected("KRAKEN_TRADE_SUBSCRIPTION_NOT_ACKNOWLEDGED")
            requested, sent_mono = session["request"]
            local, recv_mono = received
            incoming, outgoing = (_time(payload[key]) for key in ("time_in", "time_out"))
            if incoming > outgoing or _local_step(session["request"], received):
                raise ValueError("INCONSISTENT_SUBSCRIPTION_CLOCKS")
            interval = ((outgoing - local).total_seconds(), (incoming - requested).total_seconds())
            if interval[0] - interval[1] > 0.001:
                raise ValueError("KRAKEN_ACK_CLOCK_BOUNDS_INCONSISTENT")
            ack = {"subscription_requested_at": requested.isoformat(), "subscription_ack_at": local.isoformat(),
                   "subscription_request_mono_ns": sent_mono, "subscription_ack_mono_ns": recv_mono,
                   "subscription_exchange_in_at": incoming.isoformat(), "subscription_exchange_out_at": outgoing.isoformat(),
                   "subscription_round_trip_seconds": (recv_mono - sent_mono) / 1e9,
                   "subscription_clock_offset_bounds_seconds": list(interval), "subscription_epoch": generation}
            with self._lock:
                self._state, self._reason = "STREAMING", None
                self._details.update(ack)
                self._counts["subscriptions_acknowledged"] += 1
                self._witness_locked("SUBSCRIPTION_ACKNOWLEDGED", local, recv_mono,
                                     subscription=ack, reference_quality="NO_PRICE_OBSERVATION")
            return {**session, "ack": ack, "gate": incoming}
        if channel in {"heartbeat", "status"}:
            self._count(channel)
            return session
        if channel != "trade":
            self._count("UNEXPECTED_MESSAGE")
            return session
        trades = payload.get("data")
        if not isinstance(trades, list) or not trades:
            self._count("INVALID_TRADE_BATCH")
            return session
        self._count("trade_messages")
        self._count("trade_rows", len(trades))
        if payload.get("type") != "update":
            self._count("HISTORY_OR_SNAPSHOT", len(trades))
            return session
        if not session.get("ack"):
            self._count("TRADE_BEFORE_ACKNOWLEDGEMENT", len(trades))
            return session
        digest = sha256(raw).hexdigest()
        for trade in trades:
            self._trade(trade, received, session, digest)
        return session

    def _trade(self, trade, received, session, digest):
        at, mono = received
        try:
            if (trade["symbol"] != ema.SYMBOL or type(trade["trade_id"]) is not int or trade["trade_id"] <= 0
                    or not (type(trade["price"]) is int or isinstance(trade["price"], Decimal))):
                raise ValueError("Unexpected Kraken trade identity or price type")
            price = Decimal(trade["price"])
            if not price.is_finite() or not isfinite(float(price)) or price <= 0 or float(price) <= 0:
                raise ValueError("Invalid Kraken trade price")
            source = _time(trade["timestamp"])
        except (KeyError, TypeError, ValueError, OverflowError):
            self._count("INVALID_TRADE")
            return
        identity = trade["trade_id"]
        if source > at:
            self._count("FUTURE_EXCHANGE_CLOCK")
            return
        if source < session["gate"]:
            self._count("HISTORY_BEFORE_SUBSCRIPTION")
            return
        if (at - source).value > AGE_NS:
            self._count("STALE_AT_RECEIPT")
            return
        if identity <= self._last_id or self._last_source is not None and source < self._last_source:
            self._count("DUPLICATE_OR_REORDERED_TRADE")
            return
        point = {**session["ack"], "status": "RECEIVED", "observation_kind": KIND, "source": ema.SOURCE, "symbol": ema.SYMBOL,
                 "source_at": source.isoformat(), "exchange_timestamp_text": trade["timestamp"],
                 "received_at": at.isoformat(), "received_mono_ns": mono, "event_id": str(identity),
                 "price": float(price), "price_text": str(price), "endpoint": URL, "raw_sha256": digest}
        self._last_id, self._last_source = identity, source
        with self._lock:
            if len(self._queue) == self._limit:
                dropped = self._queue.popleft()
                self._counts["queue_dropped"] += 1
                self._details["last_queue_drop_at"] = at.isoformat()
                self._witness_locked("QUEUE_DROPPED", at, mono, reason="KRAKEN_CONSUMER_QUEUE_OVERFLOW",
                                     last_accepted_reference=self._last_point, dropped_reference=dropped,
                                     dropped_count=1)
            self._queue.append(point)
            self._high_water = max(self._high_water, len(self._queue))
            self._counts["accepted_at_receipt"] += 1
            self._details["last_trade_received_at"] = at.isoformat()
            self._last_point = point
            if self._awaiting_recovery:
                self._witness_locked("RECOVERY_REFERENCE", at, mono, observation=point,
                                     reference_quality="ACCEPTED_AT_RECEIPT_REQUIRES_CONSUMER_VALIDATION",
                                     historical_coverage="PENDING_REVIEW")
                self._awaiting_recovery = False

    def _session(self, generation):
        with self._connect(URL, open_timeout=OPEN_TIMEOUT, close_timeout=1., ping_interval=20., ping_timeout=10.,
                           max_size=MAX_MESSAGE_BYTES, max_queue=8, compression=None) as socket:
            request = self._clock()
            socket.send(json.dumps(SUBSCRIBE, separators=(",", ":")))
            self._state_changed("SUBSCRIBING")
            session = {"request": request}
            with self._lock:
                self._witness_locked("SUBSCRIPTION_REQUESTED", *request,
                                     subscription_epoch=generation, requested_at=request[0].isoformat())
            previous = request
            anchor = None
            checked = request[1]
            while not (self._stop.is_set() or self._reconnect.is_set()):
                try:
                    raw = socket.recv(timeout=1., decode=False)
                except TimeoutError:
                    now = self._clock()
                    if now[1] - previous[1] >= IDLE_TIMEOUT * 1e9:
                        raise TimeoutError("KRAKEN_WEBSOCKET_IDLE")
                else:
                    now = self._clock()
                    if _local_step(previous, now) or (anchor is not None and _local_step(anchor, now)):
                        raise ValueError("LOCAL_CLOCK_STEP")
                    previous = now
                    session = self._message(raw, now, session, generation)
                    if anchor is None and session.get("ack"):
                        anchor = now
                if not session.get("ack") and now[1] - request[1] >= ACK_TIMEOUT * 1e9:
                    raise TimeoutError("KRAKEN_SUBSCRIPTION_ACK_TIMEOUT")
                if now[1] - checked >= PERMISSION_INTERVAL_NS:
                    self._authorize(now[0])
                    checked = now[1]
        return "REQUESTED_RECONNECT" if self._reconnect.is_set() else "FEED_CLOSED"

    def _run(self):
        delay = 5.
        while not self._stop.is_set():
            error = None
            try:
                self._authorize(self._clock()[0])
                self._state_changed("CONNECTING")
                self._count("connection_attempts")
                generation = self.status()["counts"]["connection_attempts"]
                self._reconnect.clear()
                why = self._session(generation)
            except PermissionError as exc:
                self._disconnected("KRAKEN_PERMISSION_OR_INSTRUMENT_UNAVAILABLE", error=exc)
                self._state_changed("BLOCKED", "KRAKEN_PERMISSION_OR_INSTRUMENT_UNAVAILABLE")
                if self._stop.wait(60.):
                    break
                continue
            except _ChannelRejected as exc:
                self._disconnected("KRAKEN_TRADE_SUBSCRIPTION_REJECTED", error=exc)
                self._state_changed("BLOCKED", "KRAKEN_TRADE_SUBSCRIPTION_REJECTED")
                break
            except InvalidStatus as exc:
                error = exc
                code = exc.response.status_code
                why = f"KRAKEN_WEBSOCKET_HTTP_{code}"
                if code in (401, 403, 451):
                    self._disconnected(why, error=error)
                    self._state_changed("BLOCKED", why)
                    break
                if code == 429:
                    delay = 60.
            except (OSError, WebSocketException, ValueError, KeyError, TypeError) as exc:
                error = exc
                why = str(exc) if isinstance(exc, ValueError | TimeoutError) and str(exc).isupper() else type(exc).__name__
            live = self.status().get("state") == "STREAMING"
            self._disconnected(why, error=error)
            if self._stop.wait(5. if live else delay):
                break
            delay = 5. if live else min(2 * delay, 60.)
        if self.status()["state"] != "BLOCKED":
            self._state_changed("STOPPED")


def _distribution(values):
    if not values:
        return None
    values = sorted(values)
    return {"count": len(values), "median": median(values), "p95": values[ceil(.95 * len(values)) - 1], "max": values[-1]}


def measure(seconds=120., *, period=.1, reconnect=False, feed=None):
    """Measure a single bounded data-only subscription at a simulated dispatch cadence."""
    if not 1 <= seconds <= 300 or not .05 <= period <= .5:
        raise ValueError("Kraken observation exercise must be bounded")
    feed = KrakenTradeFeed() if feed is None else feed
    process = psutil.Process()
    started_wall, started = execution.clock()
    last_tick = started
    cpu_start = sum(process.cpu_times()[:2])
    rss_start = rss_peak = process.memory_info().rss
    event_ages, ready_ages, handling = [], [], []
    without_fresh = 0
    rejected = Counter()
    newest = None
    forced = False
    last_status = {}
    feed.start()
    try:
        while True:
            wall, tick = execution.clock()
            elapsed = (tick - started) / 1e9
            drained = feed.drain()
            # Sample after the drain. The owner thread may have received a frame
            # after the first clock read but before the consumer acquired the queue.
            wall, tick = execution.clock()
            for point in drained:
                event_ages.append((_time(point["received_at"]) - _time(point["source_at"])).total_seconds())
                handling.append((tick - point["received_mono_ns"]) / 1e9)
                if reason := ready_reason(point, wall, tick):
                    rejected[reason] += 1
                else:
                    ready_ages.append((wall - _time(point["source_at"])).total_seconds())
                newest = point
            last_status = feed.status()
            if last_status["state"] != "STREAMING" or not newest or (
                    newest["subscription_epoch"] != last_status["subscription_epoch"]) or ready_reason(newest, wall, tick):
                without_fresh += tick - last_tick
            last_tick = tick
            rss_peak = max(rss_peak, process.memory_info().rss)
            if elapsed >= seconds:
                break
            if reconnect and not forced and elapsed >= seconds / 2 and last_status["state"] == "STREAMING":
                feed.request_reconnect()
                forced = True
            time.sleep(min(period, seconds - elapsed))
    finally:
        feed.close()
    duration = (tick - started) / 1e9
    return {"source": ema.SOURCE, "symbol": ema.SYMBOL, "endpoint": URL, "started_at": started_wall.isoformat(),
            "ended_at": wall.isoformat(), "duration_seconds": duration, "sample_period_seconds": period,
            "event_to_receipt_seconds": _distribution(event_ages), "age_at_decision_readiness_seconds": _distribution(ready_ages),
            "receipt_to_consumer_seconds": _distribution(handling), "consumer_rejections": dict(rejected),
            "unavailable_seconds_estimate": without_fresh / 1e9, "forced_reconnect_requested": forced,
            "rss_start_bytes": rss_start, "rss_peak_bytes": rss_peak,
            "mean_process_cpu_percent_of_one_core": 100. * (sum(process.cpu_times()[:2]) - cpu_start) / duration,
            "feed": last_status}
