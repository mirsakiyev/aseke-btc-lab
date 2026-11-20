from datetime import timedelta
from contextlib import ExitStack, contextmanager
from hashlib import sha256
import json
import logging
import math
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import psutil

from lab import runtime_snapshot
from lab.config import ROOT, fingerprint
from lab.phase10.observer import Observer
from lab.phase9.data import candles, inspect_batch, market_quote, mark_quote, metadata
from lab.phase9.ideas import FINAL, append_event, compute, events, fresh, observation_state, publish
from lab.phase9.public import INTERVALS, kline, latest, load_payload, plan_requests, send
from lab.phase9.reference import (APPROVAL, MODE, NAMES, controls as phase9_controls, freeze_reference,
                                  parent_design, read_frozen_reference, reference)
from lab.storage import DATA, RUNS, read_json, utc, utc_now, write_json, writer_lock


STATE, OUTPUT = DATA / "phase10", RUNS / "phase10"
CONTROL = STATE / "control.json"
RUNTIME = STATE / "runtime.json"
LOCK = STATE / ".worker.lock"
POLL_SECONDS = 15
TIMELY_SECONDS = 120
DELAYED_SECONDS = 300
MAX_REQUESTS_PER_CYCLE = 20
MAX_RAW_BYTES = 96 * 1024**2
WAITING = "WAITING_FOR_QUALIFIED_FUTURES_BUNDLE"
LOG = logging.getLogger("lab.phase10")
BACKOFF_SCHEMA = "PHASE10_BACKOFF_V1"
RUNTIME_REPLACE_DELAYS = (.05, .1)
LOADED_WORKER_SOURCE_SHA256 = sha256(Path(__file__).read_bytes()).hexdigest()
LOADED_SNAPSHOT_SOURCE_SHA256 = sha256(Path(runtime_snapshot.__file__).read_bytes()).hexdigest()
SOURCE_REQUEST_SCHEMA = "PHASE10_SOURCE_REQUEST_V1"
INFLIGHT_PERMISSION_SECONDS = 5


class SourceOutcomePending(RuntimeError):
    """A short source-result publication still owns its admission fence."""


@contextmanager
def source_outcome_lock(*, wait_seconds=.15):
    """Serialize only result publication and tiny permission reads, never HTTP."""
    if not 0 <= wait_seconds <= .2:
        raise ValueError("Source outcome fence wait must be bounded to 200ms")
    started = time.monotonic()
    deadline = started + wait_seconds
    with ExitStack() as stack:
        while True:
            try:
                stack.enter_context(writer_lock(STATE / ".source-outcome.lock"))
                break
            except RuntimeError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise SourceOutcomePending("Phase 10 source outcome publication is pending") from exc
                time.sleep(min(.01, remaining))
        yield max(0., time.monotonic() - started)


def live_request(row, now):
    """A young, exact-build, OS-owned request is ordinary concurrent HTTP.

    Five seconds is a conservative permission window, not a promise of total
    httpx wall time. Older intents remain unknown until their result commits.
    The caller must already hold source_outcome_lock while reading metadata.
    """
    try:
        intent = row["active_request"]
        if (not isinstance(intent, dict) or intent.get("schema") != SOURCE_REQUEST_SCHEMA
                or not isinstance(intent.get("key"), str) or not intent["key"]
                or row.get("worker_stage") != "COLLECTING" or row.get("stopped_at")
                or row.get("worker_source_sha256") != LOADED_WORKER_SOURCE_SHA256
                or type(row.get("pid")) is not int or row["pid"] <= 0
                or not isinstance(row.get("launch_id"), str) or not row["launch_id"]
                or type(row.get("created")) not in (int, float) or not math.isfinite(row["created"])):
            return False
        if any(intent.get(key) != row[key] for key in ("pid", "created", "launch_id")):
            return False
        mono_start, mono_finish = request_monotonic_window(intent)
        if not mono_start <= time.monotonic() <= mono_finish:
            return False
        started, finish = pd.Timestamp(intent["started_at"]), pd.Timestamp(intent["permission_until"])
        if (pd.isna(started) or pd.isna(finish) or started.tzinfo is None or finish.tzinfo is None
                or finish != started + pd.Timedelta(seconds=INFLIGHT_PERMISSION_SECONDS)
                or not started <= now <= finish):
            return False
        return psutil.Process(row["pid"]).create_time() == row["created"] and worker_lock_held()
    except (KeyError, TypeError, ValueError, OverflowError, OSError, psutil.Error):
        return False


def request_monotonic_window(intent):
    """The cross-process Windows monotonic window cannot revive after a clock step."""
    start, finish = intent["started_monotonic"], intent["permission_monotonic_until"]
    if (type(start) not in (int, float) or type(finish) not in (int, float)
            or not math.isfinite(start) or not math.isfinite(finish) or start < 0
            or finish != start + INFLIGHT_PERMISSION_SECONDS):
        raise ValueError("Invalid source-request monotonic permission window")
    return start, finish


class RuntimePersistenceError(OSError):
    """Publishing worker metadata failed; no market request is implied."""

    def __init__(self, original):
        super().__init__(f"Phase 10 runtime persistence failed at {RUNTIME}: {original}")
        self.original = original


class ResearchPersistenceError(OSError):
    """An explicitly local research file operation failed outside HTTP."""

    def __init__(self, original, operation):
        super().__init__(f"Phase 10 local research storage failed ({operation}): {original}")
        self.original, self.operation = original, operation


def research_io(operation, *args, **kwargs):
    """Classify only OSError from an explicitly local operation, never send()."""
    try:
        return operation(*args, **kwargs)
    except OSError as exc:
        raise ResearchPersistenceError(exc, operation.__name__) from exc


def set_backoff(row, cause, seconds, *, scope, at=None, retry_at=None, **details):
    """Keep research retries separate from durable source-wide restrictions."""
    now = utc_now() if at is None else utc(at)
    until = now + pd.Timedelta(seconds=seconds) if retry_at is None else utc(retry_at)
    source = row.get("source_backoff")
    # A local failure must never shorten or erase an existing source breaker.
    if isinstance(source, dict) and source.get("retry_at"):
        until = max(until, utc(source["retry_at"]))
    evidence = {"schema": BACKOFF_SCHEMA, "origin": "lab.phase10.worker", "scope": scope,
                "cause": cause, "at": now.isoformat(), "retry_at": until.isoformat(), **details}
    row.update(retry_at=until.isoformat(), backoff=evidence)
    if scope == "SHARED_KUCOIN_SOURCE":
        row["source_backoff"] = dict(evidence)


def record_cycle_failure(row, exc, *, at=None):
    if isinstance(exc, CorruptRuntime):
        # Invalid persisted provenance needs review, not a new default timeout.
        raise exc
    now = utc_now() if at is None else utc(at)
    local = isinstance(exc, (RuntimePersistenceError, ResearchPersistenceError))
    original = exc.original if local else exc
    details = {"error_type": type(original).__name__, "errno": getattr(original, "errno", None),
               "winerror": getattr(original, "winerror", None)}
    row["last_error"] = {"at": now.isoformat(), "reason": str(exc), **details}
    cause = ("LOCAL_RUNTIME_PERSISTENCE" if isinstance(exc, RuntimePersistenceError) else
             "LOCAL_RESEARCH_STORAGE" if local else "UNCLASSIFIED_WORKER_ERROR")
    set_backoff(row, cause, 30,
                scope="RESEARCH_WORKER" if local else "SHARED_KUCOIN_SOURCE", at=now, **details)


def optional(path, fallback):
    return read_json(path) if path.exists() else fallback


class CorruptRuntime(ValueError):
    """The persisted worker identity needs an explicit, ownership-checked repair."""


def require_completed_request(row):
    """Do not erase a prior durable request whose outcome never committed."""
    if row.get("active_request") is not None:
        raise RuntimeError(
            f"Unresolved Phase 10 source-request outcome at {RUNTIME}. Preserve the runtime and request evidence; "
            "verify worker/observer ownership and source restrictions before an explicit repair."
        )


def validate_backoff(row, *, at=None):
    """Never acquire fresh evidence by overwriting an inconsistent breaker."""
    now = utc_now() if at is None else utc(at)

    def explicit_stamp(value):
        stamp = pd.Timestamp(value)
        if pd.isna(stamp) or stamp.tzinfo is None:
            raise ValueError("Backoff timestamp must be explicit")
        return stamp.tz_convert("UTC")

    try:
        windows = {}
        for name in ("backoff", "source_backoff"):
            value = row.get(name)
            if value is None:
                continue
            if (not isinstance(value, dict) or value.get("schema") != BACKOFF_SCHEMA
                    or value.get("origin") != "lab.phase10.worker"
                    or value.get("scope") not in ("RESEARCH_WORKER", "SHARED_KUCOIN_SOURCE")
                    or not isinstance(value.get("cause"), str) or not value["cause"]):
                raise ValueError("Unknown backoff provenance")
            start, finish = explicit_stamp(value["at"]), explicit_stamp(value["retry_at"])
            if start > now or start >= finish or name == "source_backoff" and value["scope"] != "SHARED_KUCOIN_SOURCE":
                raise ValueError("Inconsistent backoff provenance")
            windows[name] = finish
        retry = explicit_stamp(row["retry_at"]) if row.get("retry_at") is not None else None
        if "backoff" in windows and retry != windows["backoff"]:
            raise ValueError("Worker retry checkpoint disagrees with scope")
        source = windows.get("source_backoff")
        if source is not None and now < source and (retry is None or retry < source):
            raise ValueError("Worker retry would bypass an undischarged source restriction")
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise CorruptRuntime(
            f"Inconsistent Phase 10 runtime backoff at {RUNTIME}. Preserve it and verify ownership and source restrictions before repair."
        ) from exc


def read_runtime():
    try:
        row = runtime_snapshot.read(RUNTIME, lambda: optional(RUNTIME, {}))
    except ValueError as exc:
        raise CorruptRuntime(f"Invalid Phase 10 runtime JSON at {RUNTIME}. Preserve it and verify the worker and observer locks before repair.") from exc
    if not isinstance(row, dict):
        raise CorruptRuntime(f"Phase 10 runtime at {RUNTIME} must contain a JSON object.")
    return row


def write_runtime(row):
    runtime_snapshot.publish(RUNTIME, row, cleanup=False)


def write_legacy_runtime(row):
    """Flush once, then retry only bounded Windows replace contention.

    This is a fenced compatibility export for an older stopped worker.
    WinError5 can also mean a permanent ACL denial. Three total attempts and
    150ms total delay do not reinterpret it as success or change permissions.
    The old file is never truncated, deleted or replaced with a default.
    """
    body = json.dumps(row, indent=2, default=str, allow_nan=False).encode("utf-8")
    RUNTIME.parent.mkdir(parents=True, exist_ok=True)
    temporary = RUNTIME.with_name(f".{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(len(RUNTIME_REPLACE_DELAYS) + 1):
            try:
                temporary.replace(RUNTIME)
                break
            except OSError as exc:
                if getattr(exc, "winerror", None) not in (5, 32, 33) or attempt == len(RUNTIME_REPLACE_DELAYS):
                    raise
                time.sleep(RUNTIME_REPLACE_DELAYS[attempt])
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            # Never mask the primary replace error or truncate its old target.
            LOG.exception("Phase 10 temporary runtime cleanup failed: %s", temporary)


def worker_lock_held():
    try:
        with writer_lock(LOCK):
            return False
    except RuntimeError:
        return True


def controls():
    old = phase9_controls()
    default = {"paused": True, "monitor": old["monitor"], "ideas": old["ideas"],
               "training": dict.fromkeys(NAMES, False), "timezone": "America/Toronto"}
    return optional(CONTROL, default)


def set_control(key, value, profile=None):
    if key == "timezone":
        ZoneInfo(value)
    elif type(value) is not bool or key not in ("paused", "monitor", "ideas", "training"):
        raise ValueError("Unknown control or value")
    if key == "training" and profile == "position" and value:
        raise ValueError("Position uses the saved Phase 9 Spot campaign. Phase 10 queues futures only.")
    with writer_lock(STATE / ".control.lock"):
        flags = controls()
        if key in ("ideas", "training"):
            if profile not in NAMES:
                raise ValueError("Select a profile")
            flags[key][profile] = value
        else:
            flags[key] = value
        write_json(CONTROL, flags)
    return flags


def process_state():
    row = read_runtime()
    if not row.get("pid"):
        return row, False
    try:
        proc = psutil.Process(row["pid"])
        live = proc.create_time() == row["created"]
    except (OSError, psutil.Error):
        live = False
    return row, live and worker_lock_held()


def cadence(config, parent):
    profiles = parent["design"]["settings"]["profiles"]
    return {name: pd.Timedelta(profiles[item["parent"]]["decision"]) for name, item in config["profiles"].items()}


def boundary(now, step, grace):
    return (now - pd.Timedelta(seconds=grace)).floor(step)


def timing(now, close):
    delay = (now - close).total_seconds()
    return "TIMELY" if delay <= TIMELY_SECONDS else "DELAYED" if delay <= DELAYED_SECONDS else "CATCH_UP"


def decision_key(name, close, strategy):
    return fingerprint([name, close.isoformat(), strategy])


def cache_read(name):
    path = STATE / "candles" / f"{name}.parquet"
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def cache_save(name, frame):
    path = STATE / "candles" / f"{name}.parquet"
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(temporary, index=False)
        temporary.replace(path)
    except OSError as exc:
        raise ResearchPersistenceError(exc, f"cache_save:{name}") from exc
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            LOG.exception("Phase 10 temporary research cache cleanup failed: %s", temporary)


def missing_requests(name, mode, frame, close, budget):
    step = pd.Timedelta(INTERVALS[mode["warmup_interval"]])
    left = close - mode["warmup_bars"] * step
    needed = pd.date_range(left, close, freq=step, inclusive="left")
    have = pd.DatetimeIndex(frame.open_at) if not frame.empty else pd.DatetimeIndex([], tz="UTC")
    missing = needed.difference(have)
    # Fetch the newest gaps first, then warmup gaps, without pulling already cached bars.
    spans = []
    for at in missing:
        if spans and spans[-1][1] == at:
            spans[-1] = (spans[-1][0], at + step)
        else:
            spans.append((at, at + step))
    max_bars = budget["spot_page_bars" if mode["trade_type"] == "SPOT" else "futures_page_bars"]
    width = step * max_bars
    specs = []
    for start, end in reversed(spans):
        while end > start:
            page = max(start, end - width)
            specs.append(kline(name, mode["symbol"], mode["trade_type"], "TRADE", mode["warmup_interval"], page, end))
            end = page
    return specs


def merged_cache(name, old, new, spec, received, sha, close, mode):
    if new.empty:
        return old
    new = new.assign(received_at=received, raw_sha256=sha, receipt_class=spec)
    # First receipt wins. Later evidence remains in the raw log.
    frame = pd.concat([old, new], ignore_index=True).drop_duplicates("open_at", keep="first").sort_values("open_at")
    step = pd.Timedelta(INTERVALS[mode["warmup_interval"]])
    frame = frame.loc[frame.open_at >= close - (mode["warmup_bars"] + 8) * step].reset_index(drop=True)
    cache_save(name, frame)
    return frame


class Worker:
    def __init__(self, runtime, config, parent):
        self.rt, self.cfg, self.parent = runtime, config, parent
        self.phase13_observer = Observer(ROOT)
        self.policy = config["ideas"]
        self.budget = config["acquisition"]
        self.steps = cadence(config, parent)
        self.ref_id = freeze_reference(config)
        self.raw = OUTPUT / "raw"
        self.used = sum(item.stat().st_size for item in self.raw.glob("*") if item.is_file())
        self.frames = {name: cache_read(name) for name in NAMES}
        self.rules = metadata()
        path = STATE / "metadata.json"
        prior_rules = runtime_snapshot.read(path, lambda: optional(path, {}))
        if prior_rules.get("as_of") and utc(prior_rules["as_of"]) > utc(self.rules.get("as_of") or "2000-01-01"):
            self.rules = prior_rules

    def save(self):
        candidate = {**self.rt, "heartbeat": utc_now().isoformat(), "raw_bytes": self.used}
        try:
            write_runtime(candidate)
        except OSError as exc:
            raise RuntimePersistenceError(exc) from exc
        # A failed write must not advance even the in-memory success heartbeat.
        self.rt.update(heartbeat=candidate["heartbeat"], raw_bytes=candidate["raw_bytes"])
        if getattr(self, "_known_source_result", False) and self.rt.get("active_request") is None:
            self.release_source_outcome()

    def hold_source_outcome(self):
        if getattr(self, "_source_outcome_guard", None) is None:
            try:
                _, permission_end = request_monotonic_window(self.rt["active_request"])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                # Such an intent is never eligible for live_request admission.
                raise SourceOutcomePending("Unverified source-request permission window") from exc
            # Paper reads wait at most 150ms. A producer may wait longer after
            # HTTP, so a slow permission reader cannot leave a young admissible
            # intent with a completed but unpublished rate-limit/access result.
            # Once this loop fails, monotonic live permission is already over,
            # even if Windows wall time steps backwards during the wait.
            acquire_end = permission_end + .01
            while True:
                remaining = max(0., acquire_end - time.monotonic())
                guard = source_outcome_lock(wait_seconds=min(.15, remaining))
                try:
                    guard.__enter__()
                except (SourceOutcomePending, OSError) as exc:
                    remaining = acquire_end - time.monotonic()
                    if remaining <= 0:
                        raise SourceOutcomePending(
                            "Source result could not acquire its fence; monotonic request permission expired"
                        ) from exc
                    time.sleep(min(.01, remaining))
                else:
                    self._source_outcome_guard = guard
                    break

    def release_source_outcome(self):
        guard = getattr(self, "_source_outcome_guard", None)
        if guard is not None:
            self._source_outcome_guard = None
            self._known_source_result = False
            guard.__exit__(None, None, None)

    def save_or_defer(self):
        """Retain the single owner during failed idle metadata publication."""
        try:
            self.save()
        except RuntimePersistenceError as exc:
            record_cycle_failure(self.rt, exc)
            LOG.exception("Phase 10 runtime remains unpersisted; research retry deferred")
            return False
        return True

    def seed(self, now):
        folder = research_io(latest, "current")
        if not folder or all(not frame.empty for frame in self.frames.values()):
            return
        plan = research_io(read_json, folder / "plan.json")["design"]
        if plan["scope"] != "PROSPECTIVE_FEATURE_WARMUP_ONLY":
            return
        pinned = research_io(read_frozen_reference, plan["reference_id"])["reference"]["profiles"]
        history, frames, _ = research_io(inspect_batch, folder, self.rules)
        for row in history:
            name = row["key"].split("-", 1)[0]
            if name not in NAMES or row["key"] not in frames or not self.frames[name].empty:
                continue
            mode = self.cfg["profiles"][name]
            fields = ("venue", "symbol", "trade_type", "quote", "warmup_interval")
            if any(pinned[name][field] != mode[field] for field in fields):
                continue
            # The first page is only one of several for Day and Swing. Add all pages below.
            for item in history:
                key = item["key"]
                if key.startswith(name + "-") and key in frames:
                    cutoff = boundary(now, self.steps[name], self.policy["completed_candle_grace_seconds"])
                    self.frames[name] = merged_cache(name, self.frames[name], frames[key], "BOOTSTRAP_PHASE9",
                                                     utc(item["received_at"]), item["raw_sha256"], cutoff, mode)

    def fetch(self, client, spec):
        require_completed_request(self.rt)
        validate_backoff(self.rt)
        if (self.remaining <= 0 or controls()["paused"] or not controls()["monitor"]
                or (self.rt.get("retry_at") and utc_now() < utc(self.rt["retry_at"]))):
            return None
        if self.used + self.budget["maximum_response_bytes"] > MAX_RAW_BYTES:
            self.rt["last_error"] = {"at": utc_now().isoformat(), "reason": "KuCoin raw evidence disk ceiling reached"}
            set_backoff(self.rt, "RESEARCH_RAW_CAPACITY", 3600, scope="RESEARCH_WORKER")
            return None
        time.sleep(self.budget["minimum_spacing_seconds"])
        with writer_lock(STATE / ".control.lock"):
            if controls()["paused"]:
                return None
            self.remaining -= 1
            started = utc_now()
            started_monotonic = time.monotonic()
            self.rt["active_request"] = {
                "schema": SOURCE_REQUEST_SCHEMA, "key": spec["key"], "started_at": started.isoformat(),
                "permission_until": (started + pd.Timedelta(seconds=INFLIGHT_PERMISSION_SECONDS)).isoformat(),
                "started_monotonic": started_monotonic,
                "permission_monotonic_until": started_monotonic + INFLIGHT_PERMISSION_SECONDS,
                **{key: self.rt.get(key) for key in ("pid", "created", "launch_id")}}
            try:
                self.save()
            except BaseException:
                self.rt["active_request"] = None
                raise
        # An exception inside send may follow dispatch or a response but precede
        # durable evidence. Retain its intent until source-correct review rather
        # than pretending a local exception proves the venue outcome.
        try:
            evidence = send(client, spec, self.budget["maximum_response_bytes"], self.raw)
        except BaseException:
            self.hold_source_outcome()
            raise
        self.hold_source_outcome()
        status = evidence["status"]
        if status not in {"RECEIVED", "RATE_LIMITED", "ACCESS_RESTRICTED", "RESPONSE_LIMIT",
                          "HTTP_ERROR", "API_ERROR", "INVALID_RESPONSE", "TRANSPORT_ERROR"}:
            raise ValueError("Unknown Phase 10 source-request outcome")
        row = {"request": spec, **evidence}
        self.rt["requests"] = self.rt.get("requests", 0) + 1
        self.rt["last_request_result"] = {"key": spec["key"],
                                          **{key: evidence.get(key) for key in
                                             ("status", "http_status", "received_at", "raw_sha256")}}
        if status != "RECEIVED":
            failures = self.rt.get("consecutive_errors", 0) + 1
            self.rt["consecutive_errors"] = failures
            self.rt["last_error"] = row
            wait = 3600 if evidence["status"] == "ACCESS_RESTRICTED" else min(900, 15 * 2**min(failures, 6))
            earliest = utc_now() + pd.Timedelta(seconds=wait)
            set_backoff(self.rt, evidence["status"], wait, scope="SHARED_KUCOIN_SOURCE",
                        retry_at=max(earliest, utc(row.get("resume_after", earliest))), request_status=evidence["status"])
        else:
            self.rt.update(retry_at=None, backoff=None, source_backoff=None, consecutive_errors=0)
        self.rt["active_request"] = None
        self._known_source_result = True
        # Commit the completed result before another request or shutdown can
        # erase its intent. If publication fails, the old intent stays visible
        # to consumers and blocks a new owner's automatic restart.
        self.save()
        if evidence.get("raw_sha256"):
            path = self.raw / f'{evidence["raw_sha256"]}.json'
            if evidence["raw_sha256"] not in self.seen:
                size = research_io(path.stat).st_size
                self.seen.add(evidence["raw_sha256"])
                self.used += size
        return row if status == "RECEIVED" else None

    def payload(self, evidence):
        return research_io(load_payload, OUTPUT, evidence)

    def refresh_metadata(self, client, now):
        if self.rules.get("as_of") and now - utc(self.rules["as_of"]) < pd.Timedelta("23h"):
            return
        # The Phase 9 parser accepts the same versioned attempt format.
        from lab.storage import immutable_bytes
        folder = OUTPUT / "metadata" / now.strftime("%Y%m%dT%H%M%S")
        for number, spec in enumerate(plan_requests("metadata", self.cfg, now, self.parent["design"]["holdout"])):
            row = self.fetch(client, spec)
            if row is None:
                return
            raw_path = self.raw / f'{row["raw_sha256"]}.json'
            research_io(immutable_bytes, folder / "raw", research_io(raw_path.read_bytes), "json")
            research_io(write_json, folder / "attempts" / f"{number:02}.json", row)
        rules = research_io(metadata, folder)
        if rules["status"] == "OBSERVED":
            research_io(runtime_snapshot.publish, STATE / "metadata.json", rules)
            self.rules = rules

    def prices(self, client, name, close):
        mode = self.cfg["profiles"][name]
        frame = self.frames[name]
        fetched = []
        for spec in missing_requests(name, mode, frame, close, self.budget):
            row = self.fetch(client, spec)
            if row is None:
                break
            received = utc(row["received_at"])
            multiplier = self.rules.get("contract", {}).get("multiplier")
            data = candles(self.payload(row), spec, received, float(multiplier) if multiplier else None)
            if data.empty:
                continue
            receipt = "LIVE_INCREMENT" if len(frame) and data.close_at.max() == close else "CATCH_UP"
            frame = merged_cache(name, frame, data, receipt, received, row["raw_sha256"], close, mode)
            fetched.append({**row, "rows": len(data), "first_open": data.open_at.min().isoformat(),
                            "last_close": data.close_at.max().isoformat(), "receipt_class": receipt})
        self.frames[name] = frame
        if fetched:
            source = fetched[-1]
            cycle = {"profile": name, "at": utc_now().isoformat(), "due_boundary": close.isoformat(),
                     "last_close": frame.close_at.max().isoformat(), "source": fetched,
                     "launch_id": self.rt["launch_id"], "launcher": self.rt["launcher"]}
            research_io(write_json, OUTPUT / "cycles" / f'{fingerprint(cycle)}.json', cycle)
            self.rt["profiles"][name].update(checkpoint=cycle["last_close"], last_success=cycle["at"],
                                             last_receipt=source["received_at"], last_cycle=fingerprint(cycle))
            self.save()

    def quotes(self, client, requested):
        quotes = {}
        for spec in plan_requests("quotes", self.cfg, utc_now(), self.parent["design"]["holdout"]):
            key = spec["key"]
            if key not in requested:
                continue
            row = self.fetch(client, spec)
            if row is None:
                break
            value, received = self.payload(row), utc(row["received_at"])
            parsed = (mark_quote(value, received) if key == "perp-mark" else
                      market_quote(value, spec["params"]["symbol"], spec["params"]["tradeType"], received))
            quotes[key] = {**parsed, "raw_sha256": row["raw_sha256"]}
        return quotes

    def existing_decision(self, name, close):
        key = decision_key(name, close, self.policy["version"])
        return OUTPUT / "decisions" / name / f"{key}.json"

    def mirror(self, row, path):
        idea = research_io(publish, row["idea"], row["reference_id"]) if row["idea"] is not None else None
        if idea and idea["status"] == "CONDITIONAL":
            self.rt.setdefault("active", {})[idea["idea_id"]] = idea
        name = row["profile"]
        item = self.rt["profiles"][name]
        if last := item.get("boundary"):
            missed = (utc(row["boundary"]) - utc(last)) // self.steps[name] - 1
            if missed > 0:
                item["older_boundaries_missed"] = item.get("older_boundaries_missed", 0) + missed
        item.update(boundary=row["boundary"], decision=row["status"],
            timing=row["timing"], waiting_reason=row["reason"], last_decision=str(path),
            last_idea=idea["idea_id"] if idea else None)

    def recover_publications(self):
        for name in NAMES:
            item = self.rt["profiles"][name]
            last = utc(item.get("boundary", "2000-01-01"))
            paths = research_io(list, (OUTPUT / "decisions" / name).glob("*.json"))
            pending = [(research_io(read_json, path), path) for path in paths
                       if research_io(path.stat).st_mtime >= last.timestamp()]
            for row, path in sorted(pending, key=lambda item: item[0]["boundary"]):
                if utc(row["boundary"]) > last:
                    self.mirror(row, path)

    def record(self, name, close, quotes, flags):
        path = self.existing_decision(name, close)
        now = utc_now()
        if research_io(path.exists):
            row = research_io(read_json, path)
        else:
            arrival = timing(now, close)
            frame = self.frames[name]
            mode = self.cfg["profiles"][name]
            step = pd.Timedelta(INTERVALS[mode["warmup_interval"]])
            left = close - mode["warmup_bars"] * step
            if not frame.empty:
                frame = frame.loc[frame.open_at.ge(left) & frame.close_at.le(close)]
            spot = name == "position"
            quote = quotes.get("spot-quote" if spot else "perp-quote")
            mark = None if spot else quotes.get("perp-mark")
            ready_at = None if frame.empty else frame.received_at.max().isoformat()
            if arrival == "CATCH_UP" or not flags["ideas"][name]:
                row = {"status": "CATCH_UP_ONLY" if flags["ideas"][name] else "IDEAS_OFF", "idea": None,
                       "timing": "CATCH_UP" if flags["ideas"][name] else "UNKNOWN",
                       "reason": "Decision boundary passed its live window" if flags["ideas"][name] else "Profile ideas disabled"}
            else:
                idea = compute(name, frame, quote, mark, self.rules, self.cfg, self.parent, now,
                               evidence={"phase10_boundary": close.isoformat(), "latest_input_receipt": ready_at,
                                         "candle_raw_sha256": sorted(set(frame.raw_sha256)) if len(frame) else []})
                if idea["source_cutoff"] != close.isoformat():
                    return
                if idea["status"] == "DATA_NOT_READY" and arrival == "TIMELY":
                    self.rt["profiles"][name]["waiting_reason"] = idea["reason"]
                    return
                row = {"status": idea["status"], "reason": idea["reason"], "idea": idea,
                       "timing": "UNKNOWN" if idea["status"] == "DATA_NOT_READY" else arrival}
            row.update(boundary=close.isoformat(), recorded_at=now.isoformat(), profile=name,
                       strategy_version=self.policy["version"], source_receipt=ready_at,
                       model_readiness="MODEL_UNAVAILABLE_RULES_PINNED", launch_id=self.rt["launch_id"],
                       launcher=self.rt["launcher"], reference_id=self.ref_id)
            research_io(write_json, path, row)
        self.mirror(row, path)
        self.save()

    def cycle(self, client):
        now = utc_now()
        flags = controls()
        if not flags["monitor"]:
            for entry in self.rt["profiles"].values():
                entry["waiting_reason"] = "MONITORING_OFF"
            self.manage_active({}, now)
            return
        if self.rt.get("retry_at") and now < utc(self.rt["retry_at"]):
            # Back off the research cycle itself, not just each HTTP request.
            self.manage_active({}, now)
            return
        self.remaining = MAX_REQUESTS_PER_CYCLE
        grace = self.policy["completed_candle_grace_seconds"]
        closes = {name: boundary(now, self.steps[name], grace) for name in NAMES}
        todo = []
        for name in ("day", "swing", "position"):
            close = closes[name]
            item = self.rt["profiles"][name]
            item["next_due"] = (close + self.steps[name] + pd.Timedelta(seconds=grace)).isoformat()
            last = item.get("boundary")
            if not last or utc(last) < close:
                todo.append(name)
        for name in todo:
            self.prices(client, name, closes[name])
        active = self.rt.get("active", {})
        wanted = {name for name in todo if flags["ideas"][name] and timing(utc_now(), closes[name]) != "CATCH_UP"}
        wanted |= {idea["profile"] for idea in active.values()}
        keys = {"spot-quote"} if "position" in wanted else set()
        if wanted & {"swing", "day"}:
            keys |= {"perp-quote", "perp-mark"}
        quotes = self.quotes(client, keys)
        for name in todo:
            self.record(name, closes[name], quotes, flags)
        self.manage_active(quotes, utc_now())
        self.refresh_metadata(client, utc_now())

    def admit_research(self):
        from lab.phase10.research import should_run
        flags = controls()
        if flags["paused"] or not any(flags["training"].values()):
            return
        if getattr(self, "supervisor", None) and self.supervisor.poll() is None:
            return
        if job := should_run():
            self.supervisor = subprocess.Popen(
                [sys.executable, "-X", "utf8", "-m", "lab.phase10", "_supervise", job], cwd=ROOT,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS,
            )

    def observe_registered_research(self):
        from lab.phase13.observation import scheduled_due

        observer = self.phase13_observer
        previous = observer.status["state"]
        with writer_lock(STATE / ".control.lock"):
            if controls()["paused"]:
                return
            observer.tick(self.rt["launch_id"], scheduled_due)
        self.rt["phase13_observer"] = observer.status
        if observer.status["state"] != previous:
            LOG.info("Phase 13 observer %s", observer.status)

    def manage_active(self, quotes, now):
        for key, idea in list(self.rt.get("active", {}).items()):
            market = idea["market"] == "FUTURES"
            quote = quotes.get("perp-quote" if market else "spot-quote")
            mark = quotes.get("perp-mark") if market else None
            if research_io(observe_idea, idea, quote, mark, now, self.policy):
                del self.rt["active"][key]


def observe_idea(idea, quote, mark, now, policy):
    history = events(idea["idea_id"])
    by_type = {row["type"]: row for row in history}
    limit, skew = policy["maximum_quote_age_seconds"], policy["maximum_quote_clock_skew_seconds"]
    if not by_type.keys() & FINAL:
        kind = observation_state(idea, quote, mark, now, limit, skew, "ENTRY_CONDITION_OBSERVED" in by_type)
        if kind and kind not in by_type:
            by_type[kind] = append_event(idea, kind, {"quote": quote, "mark": mark, "source": "PHASE10_PUBLIC_QUOTES"}, now)
    if "SIMULATED_OUTCOME" in by_type:
        return True
    fill = by_type.get("SIMULATED_FILL")
    long = idea["direction"] == "LONG"
    direction = 1 if long else -1
    if not fill and "ENTRY_CONDITION_OBSERVED" in by_type:
        observation = by_type["ENTRY_CONDITION_OBSERVED"]
        entry_quote = observation["evidence"]["quote"]
        # Only the worker's own timestamped receipt supports the quote-fill proxy.
        if observation["evidence"].get("source") == "PHASE10_PUBLIC_QUOTES":
            fill = append_event(idea, "SIMULATED_FILL", {"observed_at": observation["recorded_at"],
                                "price": entry_quote["ask" if long else "bid"], "source": "HYPOTHETICAL_QUOTE_PROXY",
                                "quantity_base": idea["paper"]["base_quantity"], "market_event": observation["id"]}, now)
    if not fill:
        return bool(by_type.keys() & FINAL)
    entry = fill["evidence"]
    opened = utc(entry["observed_at"])
    deadline = opened + pd.Timedelta(minutes=idea["time_exit"]["maximum_hold_minutes"])
    if (flat := idea["time_exit"].get("session_flat_minute_utc")) is not None:
        deadline = min(deadline, opened.normalize() + pd.Timedelta(minutes=flat))
    valid = fresh(quote, now, limit, skew) and min(utc(quote["at"]), utc(quote["received_at"])) > opened
    if idea["stop"]["basis"] == "MARK":
        valid = valid and fresh(mark, now, limit, skew) and min(utc(mark["at"]), utc(mark["received_at"])) > opened
    if valid:
        trigger = mark["mark"] if mark else quote["last"]
        reason = ("STOP" if direction * (trigger - idea["stop"]["price"]) <= 0 else
                  "TARGET" if direction * (trigger - idea["take_profit"][0]["price"]) >= 0 else
                  "TIME" if now >= deadline else None)
        if reason:
            price, size = quote["bid" if long else "ask"], entry["quantity_base"]
            gross = direction * (price - entry["price"]) * size
            fees = (price + entry["price"]) * size * idea["cost"]["taker"]
            futures = idea["market"] == "FUTURES"
            append_event(idea, "SIMULATED_OUTCOME", {"reason": reason, "exit_price": price, "gross": gross,
                         "fee_reference": fees, "net": None if futures else gross-fees,
                         "funding": "UNOBSERVED" if futures else "NOT_APPLICABLE", "fill_event": fill["id"],
                         "quote": quote, "mark": mark, "source": "HYPOTHETICAL_QUOTE_PROXY"}, now)
            return True
    return False


def status():
    from lab.learning.state import status as learner_status
    from lab.phase10.research import work_state
    runtime_error = None
    try:
        rt, running = process_state()
    except CorruptRuntime as exc:
        runtime_error = {"reason": str(exc), "writer_lock_held": worker_lock_held()}
        rt, running = {}, False
    flags = controls()
    stamp = utc_now()
    heartbeat = rt.get("heartbeat")
    runtime = "CORRUPT_RUNTIME" if runtime_error else (
        "RUNNING" if running and heartbeat and stamp - utc(heartbeat) < pd.Timedelta("2min") else "STALE" if running else "STOPPED")
    legacy = learner_status()
    probe = optional(OUTPUT / "binance-probe" / "result.json", {})
    cfg, parent = reference(), parent_design()
    view = {name: dict(rt.get("profiles", {}).get(name, {})) for name in NAMES}
    for name, step in cadence(cfg, parent).items():
        grace = cfg["ideas"]["completed_candle_grace_seconds"]
        due = boundary(stamp, step, grace) + step + pd.Timedelta(seconds=grace)
        result = view[name].get("decision")
        reason = view[name].get("waiting_reason")
        if not flags["monitor"]:
            reason = "MONITORING_OFF"
        elif not flags["ideas"][name]:
            reason = "IDEAS_OFF"
        elif runtime_error:
            reason = "CORRUPT_RUNTIME"
        elif runtime == "STOPPED":
            reason = "WORKER_STOPPED"
        elif reason == "MONITORING_OFF":
            reason = "Waiting for next boundary"
        view[name].update(next_due_utc=due.isoformat(), next_due_local=due.tz_convert(flags["timezone"]).isoformat(),
                          waiting_reason=reason,
                          input_readiness="READY_AT_LAST_DECISION" if result in ("NO_TRADE", "CONDITIONAL") else
                          "DATA_NOT_READY" if result == "DATA_NOT_READY" else "WAITING_FOR_LIVE_INPUTS",
                          model_readiness="MODEL_UNAVAILABLE_RULES_PINNED",
                          training_readiness=WAITING if name != "position" else "PHASE9_SPOT_RESULT_PINNED")
    counts = dict.fromkeys(("TIMELY", "DELAYED", "CATCH_UP", "UNKNOWN"), 0)
    for path in (OUTPUT / "decisions").glob("*/*.json"):
        value = read_json(path)["timing"]
        counts[value] += 1
    return {"execution_mode": MODE, "validation": APPROVAL, "runtime": runtime, "controls": flags,
            "identity": {key: rt.get(key) for key in ("pid", "created", "launcher", "launch_id", "started_at", "heartbeat")},
            "last_error": runtime_error or rt.get("last_error"), "retry_at": rt.get("retry_at"),
            "backoff": rt.get("backoff"), "source_backoff": rt.get("source_backoff"), "raw_bytes": rt.get("raw_bytes", 0),
            "active_request": rt.get("active_request"), "last_request_result": rt.get("last_request_result"),
            "loaded_worker_source_sha256": rt.get("worker_source_sha256"),
            "loaded_snapshot_source_sha256": rt.get("snapshot_source_sha256"),
            "disk_worker_source_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
            "raw_ceiling_bytes": MAX_RAW_BYTES, "profiles": view, "decision_counts": counts,
            "research": {**work_state(), "reason": probe.get("reason", "Finite source probe pending"),
                         "legacy_mode": legacy["mode"], "phase9_manual_gate": phase9_controls()["training"]}}


def run(managed=False):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.INFO)
    handler = RotatingFileHandler(OUTPUT / "worker.log", maxBytes=1024**2, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(handler)
    worker = None
    try:
        with writer_lock(LOCK):
            activity = STATE / ".activity.lock"
            with writer_lock(activity):
                if controls()["paused"]:
                    return
                now = utc_now()
                try:
                    rt = read_runtime()
                except CorruptRuntime as exc:
                    LOG.error("KuCoin worker start blocked: %s", exc)
                    raise
                require_completed_request(rt)
                validate_backoff(rt, at=now)
                previous = rt.get("launch_id")
                rt.update(pid=os.getpid(), created=psutil.Process().create_time(), launcher="scheduled" if managed else "local",
                          launch_id=uuid4().hex, started_at=now.isoformat(), previous_launch=previous, stopped_at=None,
                          drain_guard_version=1, active_request=None, worker_stage="STARTING", phase13_observer=None,
                          worker_source_sha256=LOADED_WORKER_SOURCE_SHA256,
                          snapshot_source_sha256=LOADED_SNAPSHOT_SOURCE_SHA256)
                rt.setdefault("profiles", {name: {} for name in NAMES})
                worker = Worker(rt, reference(), parent_design())
                worker.seen = {item.stem for item in worker.raw.glob("*.json")}
                worker.recover_publications()
                worker.save()
            try:
                with httpx.Client(timeout=5, follow_redirects=False) as client:
                    while True:
                        with writer_lock(activity):
                            if controls()["paused"]:
                                break
                            try:
                                rt["worker_stage"] = "COLLECTING"
                                if not (rt.get("retry_at") and utc_now() < utc(rt["retry_at"])):
                                    worker.seed(utc_now())
                                    worker.cycle(client)
                                    worker.admit_research()
                            except (ValueError, KeyError, OSError, RuntimeError) as exc:
                                record_cycle_failure(rt, exc)
                                LOG.exception("KuCoin cycle failed")
                            try:
                                if not controls()["paused"]:
                                    worker.observe_registered_research()
                            except (ValueError, KeyError, OSError, RuntimeError):
                                LOG.exception("Phase 13 observation launch failed")
                            rt["worker_stage"] = "IDLE"
                            worker.save_or_defer()
                        # A failed deletion of an older held snapshot cannot
                        # retain the completed source-publication fence.
                        runtime_snapshot.trim(RUNTIME, limit=128)
                        for _ in range(POLL_SECONDS):
                            if controls()["paused"]:
                                break
                            time.sleep(1)
            finally:
                if not worker.phase13_observer.drain():
                    with writer_lock(activity):
                        rt["worker_stage"] = "DRAINING_OBSERVER"
                        rt["phase13_observer"] = worker.phase13_observer.status
                        worker.save()
                    while not worker.phase13_observer.drain():
                        time.sleep(1)
                with writer_lock(activity):
                    rt["worker_stage"] = "STOPPED"
                    rt["phase13_observer"] = worker.phase13_observer.status
                    rt["stopped_at"] = utc_now().isoformat()
                    worker.save()
                LOG.info("KuCoin worker stopped")
    finally:
        # The worker lease has already exited. An unresolved durable intent is
        # no longer allowed to claim a live managed owner after fence release.
        if worker is not None:
            worker.release_source_outcome()
        handler.close()
        LOG.removeHandler(handler)


def start_local():
    if process_state()[1]:
        return status()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    set_control("paused", False)
    subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.phase10", "worker"], cwd=ROOT,
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                     creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP)
    for _ in range(30):
        if process_state()[1]:
            return status()
        time.sleep(.3)
    raise RuntimeError("KuCoin worker did not acquire its lock")


def _drain(seconds):
    read_runtime()
    set_control("paused", True)
    deadline = time.monotonic() + seconds
    while True:
        rt = read_runtime()
        worker_owned = True
        observer_owned = None
        try:
            with writer_lock(LOCK):
                worker_owned = False
                with writer_lock(OUTPUT / ".observer-cycle.lock"):
                    return {"state": "STOPPED", "writer_lock_held": False, "observer_lock_held": False, "paused": True}
        except RuntimeError:
            if not worker_owned:
                observer_owned = True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                owner = "Worker" if worker_owned else "Observer"
                return {"state": "BLOCKED", "writer_lock_held": worker_owned, "observer_lock_held": observer_owned,
                        "paused": True, "reason": f"{owner} lock still held at the graceful shutdown deadline",
                        **{field: rt.get(field) for field in ("pid", "launch_id", "heartbeat", "active_request",
                                                            "phase13_observer", "worker_stage")}}
            time.sleep(min(.5, remaining))


def stop_local(*, drain_seconds=None):
    result = _drain(12.5 if drain_seconds is None else drain_seconds)
    if drain_seconds is not None:
        return result
    if result["state"] == "BLOCKED":
        raise RuntimeError("Worker is still draining its current request")
    return status()
