"""Sole writer and capability dispatch for registered paper strategy books."""

from contextlib import ExitStack, closing, contextmanager
from copy import deepcopy
from functools import lru_cache
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import time

import httpx
import pandas as pd

from lab import runtime_snapshot
from lab.config import ROOT as PROJECT, fingerprint
from lab.learning.simulator import Position
from lab.phase10 import worker as monitoring
from lab.phase13 import protocol
from lab.phase13.registry import sealed
from lab.phase15 import catalog, paper
from lab.paper_run import archive, breakout, ema, execution, gate, kraken_live, source_amendment, sources
from lab.paper_run import reload as runner_reload
from lab.paper_run import protection_incidents as incidents, protection_reporting
from lab.paper_run import capabilities, membership, operations, delivery, recovery, consumer_spool, funding
from lab.paper_run import status_publication, trail_research
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


VERSION = "fresh-reference-paper-v2"
POLL_SECONDS = 5
REQUEST_LIMIT = 6000
SQL = """
CREATE TABLE IF NOT EXISTS book (id TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS event (key TEXT PRIMARY KEY, book TEXT NOT NULL, kind TEXT NOT NULL,
    idea TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS book_events ON event(book,kind,recorded_at);
CREATE INDEX IF NOT EXISTS protection_position_events ON event(book,idea)
    WHERE kind IN ('PROTECTION_INCIDENT_OPENED','PROTECTION_INCIDENT_EVIDENCE_ADDED','PROTECTION_INCIDENT_RECOVERED');
CREATE INDEX IF NOT EXISTS protection_witness_ids ON event(idea) WHERE kind='PROTECTION_TRANSPORT_WITNESS';
CREATE TABLE IF NOT EXISTS source_state (id TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS observation (source TEXT NOT NULL, at TEXT NOT NULL, event_id TEXT NOT NULL, body TEXT NOT NULL,
    PRIMARY KEY(source, event_id));
CREATE INDEX IF NOT EXISTS observation_at ON observation(source,at);
"""
ERRORS = (OSError, ValueError, KeyError, TypeError, StopIteration, ArithmeticError, RuntimeError, sqlite3.Error)
LOG = logging.getLogger(__name__)
_status_publisher = None
RUNNER_BUILD = fingerprint([file_hash(Path(__file__).with_name(name)) for name in (
    "runtime.py", "gate.py", "ema.py", "execution.py", "kraken_live.py", "source_amendment.py",
    "protection_incidents.py", "protection_commands.py", "protection_evidence.py", "protection_reporting.py",
    "operator_close.py", "capabilities.py", "membership.py", "operations.py", "delivery.py", "recovery.py",
    "consumer_spool.py", "reload.py", "operational_status.py", "status_publication.py", "trail_research.py", "breakout.py", "funding.py",
    "funding_sources.py", "funding_commands.py", "clock_quality.py", "gate.py")]
    + [file_hash(Path(runtime_snapshot.__file__))])


@contextmanager
def status_publications():
    global _status_publisher
    if _status_publisher is not None:
        raise RuntimeError("Paper status publisher already exists")
    _status_publisher = status_publication.Publisher()
    try:
        yield
    finally:
        _status_publisher.stop()
        _status_publisher = None


def publish_status(name, document):
    path = gate.ROOT / name
    if _status_publisher is not None:
        return _status_publisher.queue(path, document)
    return status_publication.publish(path, document)


def read_status_snapshot(name, fallback):
    path = gate.ROOT / name
    return runtime_snapshot.read(path, lambda: monitoring.optional(path, fallback))


def current():
    pointer = gate.ROOT / "current.json"
    if not pointer.exists():
        return None
    identifier = read_json(pointer)["id"]
    if not re.fullmatch(r"[a-f0-9]{64}", identifier):
        raise ValueError("Invalid current generation identifier")
    record = sealed(gate.ROOT / "registrations" / f"{identifier}.json", identifier)
    retirement = gate.retired()
    if not retirement or retirement["id"] != record["body"]["archive_id"] or record["body"]["protocol"] != VERSION:
        raise ValueError("Current registration requires its verified legacy retirement")
    return record


def connect(*, readonly=False, timeout=3):
    path = gate.ROOT / "paper.sqlite3"
    if readonly:
        return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=timeout)
    gate.ROOT.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=timeout)
    db.executescript(SQL)
    ema.init(db)
    operations.init(db)
    return db


def _load_book(db, lane):
    if not db.execute("SELECT 1 FROM book WHERE id=?", (lane["book_id"],)).fetchone():
        raise ValueError("REGISTERED_BOOK_STATE_MISSING: " + lane["book_id"])
    book = paper.load(db, lane)
    if book.get("book_id") != lane["book_id"]:
        raise ValueError("REGISTERED_BOOK_STATE_IDENTITY_MISMATCH: " + lane["book_id"])
    incidents.restore_admission(db, book)
    return book


def _lanes(record, amendment=None, *, db=None):
    return membership.lanes(record, amendment, db=db)


def _signal_adapter(lane):
    return capabilities.adapter(lane, "signal")


def _entry_adapter(lane):
    name = capabilities.adapter(lane, "entry")
    if name == "ema_reference_v1":
        return ema.entry_terms
    if name == "breakout_reference_v1":
        return breakout.entry_terms
    raise capabilities.UnsupportedCapability("UNSUPPORTED_ENTRY_ADAPTER", name)


def _ws_lane(lane):
    return capabilities.source_identity(lane)["transport"] == execution.WS_TRADE


def _source_health(db, lane, at, stream=None):
    scope = capabilities.source_scope_id(lane)
    reason = _source_permission(lane["source"], at, entry_only=False, wait_seconds=0)
    stored = _lane_source_state(db, lane)
    if stored.get("restricted"):
        reason = stored.get("reason") or "SOURCE_ACCESS_RESTRICTED"
    if stored.get("retry_at") and at < utc(stored["retry_at"]):
        reason = stored.get("reason") or "SOURCE_BACKOFF"
    if _ws_lane(lane):
        try:
            kraken_live.permission(at)
        except (OSError, ValueError, PermissionError, sqlite3.Error):
            reason = "SOURCE_PERMISSION_UNAVAILABLE"
        try:
            state = stream or read_status_snapshot("ema-live-status.json", {})
        except (OSError, ValueError):
            state = {"state": "UNAVAILABLE", "reason": "SOURCE_TRANSPORT_HEARTBEAT_UNAVAILABLE"}
        if state.get("state") != "STREAMING":
            reason = state.get("reason") or "SOURCE_TRANSPORT_" + str(state.get("state", "UNAVAILABLE"))
        elif state.get("at") and not pd.Timedelta(0) <= at - utc(state["at"]) < pd.Timedelta(seconds=45):
            reason = "SOURCE_TRANSPORT_HEARTBEAT_STALE"
        record = current()
        if record:
            try:
                if consumer_spool.pending(gate.ROOT, record["id"], scope):
                    reason = "SOURCE_CONSUMER_LOSS_PENDING"
            except (OSError, ValueError, RuntimeError):
                reason = "CONSUMER_LOSS_EVIDENCE_UNAVAILABLE"
    elif stored.get("reason"):
        reason = reason or stored["reason"]
    return {"source_scope_id": scope, "available": reason is None, "reason_code": reason,
            "identity": capabilities.source_identity(lane),
            "evidence_semantics": "CONTINUOUS_TRADE_STREAM" if _ws_lane(lane) else "SAMPLED_REFERENCE_ON_DEMAND",
            "last_observation_at": (stored.get("last_observation") or {}).get("source_at")}


def _capture_operations(db, record, lanes, books, at, *, stream=None):
    clock_ok = synchronized(at)
    for slot, book in books.items():
        lane = lanes[slot]
        health = _source_health(db, lane, at, stream)
        operations.source_transition(db, record, lane, at, available=health["available"],
                                     reason=health["reason_code"])
        operations.capture(db, record, lane, book, at, source_health=health, clock_ok=clock_ok,
                           entries_enabled=_entries_enabled(lane))


def _entries_enabled(lane):
    return not (gate.ROOT / "entries-paused").exists() and _profile_entries_enabled(lane)


def _profile_entries_enabled(lane):
    if _signal_adapter(lane) == "breakout_v1":
        flags = monitoring.controls()
        return bool(flags["monitor"] and not flags["paused"] and flags["ideas"].get(lane["profile"]))
    return True


def definitions(archive_record, qualifications, now):
    frozen = catalog.registered()
    parent = protocol.active()
    if not frozen or not parent or parent["id"] != frozen["body"]["source_protocol"]:
        raise ValueError("Expected the original Phase 15 source registration")
    original = frozen["body"]
    start = now.floor(catalog.QUARTER) + catalog.QUARTER
    retired = archive_record["body"]["slots"]
    lanes = {**breakout.lanes(frozen), **ema.lanes(now=now)}
    if set(lanes) != set(retired) or len(lanes) != 7:
        raise ValueError("Exactly seven mapped predecessor slots are required")
    result = {}
    for slot, lane in lanes.items():
        if lane["legacy_book_id"] != retired[slot]["book_id"] or lane["legacy_registration"] != retired[slot]["registration_id"]:
            raise ValueError(f"Retired book mapping differs for {slot}")
        finish = min(utc(original["follow_end_exclusive"]), utc(parent["body"]["end_exclusive"]))
        if lane["family"] == "EMA":
            finish = utc(lane["feature_scope_end"])
            hold_minutes = lane["spec"]["session"]["max_hold_minutes"]
        else:
            hold_minutes = lane["profile_rules"]["max_hold_minutes"]
        follow = pd.Timedelta(minutes=hold_minutes) + pd.Timedelta(hours=2)
        end = min(start + pd.Timedelta(days=30), finish - follow)
        if end <= start:
            raise ValueError(f"Reviewed source coverage is too short for {slot}")
        identity = fingerprint([VERSION, archive_record["id"], slot, start.isoformat()])
        result[slot] = {**lane, "book_id": identity, "risk": {**lane["risk"], "starting_equity": 10000.},
                        "publication_end_exclusive": end.isoformat(), "follow_end_exclusive": (end + follow).isoformat(),
                        "qualification": qualifications.get(lane["source"], {"state": "BLOCKED", "reason": "SOURCE_NOT_QUALIFIED"})}
    return {"protocol": VERSION, "archive_id": archive_record["id"], "archive": archive_record["body"]["path"],
            "cutoff": archive_record["body"]["cutoff"], "registered_at": now.isoformat(), "start": start.isoformat(),
            "approval": "No validated model", "execution_mode": "MANUAL_OUTSIDE_LAB", "paper_only": True,
            "lanes": result, "config": original["config"], "holdout_end_exclusive": original["holdout_end_exclusive"],
            "source_protocol": original["source_protocol"], "phase14_amendment": original["phase14_amendment"],
            "max_live_delay_seconds": original["max_live_delay_seconds"], "request_spacing_seconds": 15,
            "freshness_seconds": 2, "protection_gap_seconds": execution.PROTECTION_GAP.total_seconds(),
            "ema_seed_ledger": str(Path(archive_record["body"]["path"]) / "runs/ema_paper/paper.sqlite3")}


def initialize(archive_record, qualifications, *, now=None, prepared=None, before_publish=None):
    now = utc_now() if now is None else now
    with writer_lock(gate.ROOT / ".writer.lock"):
        if gate.reset_pending():
            raise ValueError("Resume the pending generation reset before initializing")
        return _initialize_locked(archive_record, qualifications, now, prepared, before_publish)


def _initialize_locked(archive_record, qualifications, now, prepared, before_publish, *, refresh_start=True):
    retired = gate.retired()
    if retired != archive_record:
        raise ValueError("Retire and verify legacy writers before opening successors")
    if prior := current():
        return prior
    intent = gate.ROOT / "initializing.json"
    body = sealed(intent)["body"] if intent.exists() else prepared or definitions(archive_record, qualifications, now)
    if body["archive_id"] != retired["id"] or not refresh_start and body != prepared:
        raise ValueError("Successor initialization requires the verified cutoff and prepared generation")
    if refresh_start and utc(body["start"]) <= now:
        with closing(connect()) as db:
            if not db.execute("SELECT 1 FROM book LIMIT 1").fetchone():
                body = definitions(archive_record, qualifications, now)
    record = {"id": fingerprint(body), "body": body}
    write_json(intent, record)
    snapshot = gate.ROOT / "registrations" / f"{record['id']}.json"
    write_json(snapshot, record)
    with closing(connect()) as db, db:
        existing = {row[0] for row in db.execute("SELECT id FROM book")}
        expected = {lane["book_id"] for lane in body["lanes"].values()}
        if existing and existing != expected:
            raise ValueError("Interrupted initialization belongs to another immutable generation")
        if existing:
            opening = {row[0] for row in db.execute("SELECT book FROM event WHERE kind='OPENING_BALANCE' AND "
                "json_extract(body,'$.generation')=?", (record["id"],))}
            if opening != existing:
                raise ValueError("Interrupted initialization is missing its opening balance journal")
        for slot, lane in body["lanes"].items():
            if lane["book_id"] in existing:
                continue
            book = paper.initial(lane)
            paper.event(db, book, "OPENING_BALANCE", "", now, now,
                        {"generation": record["id"], "slot": slot, "book_id": lane["book_id"],
                         "retired_book_id": lane.get("retired_book_id", lane["legacy_book_id"]),
                         "opening_balance": lane["risk"]["starting_equity"],
                         "ledger_unit": lane.get("ledger_unit", "USDT"), "basis": "NEW_HYPOTHETICAL_ENDOWMENT"})
            paper.save(db, book)
    if before_publish is not None:
        before_publish(record)
    write_json(gate.ROOT / "current.json", {"id": record["id"]})
    return record


@lru_cache(maxsize=2)
def _windows_clock(five_minute_bucket):
    try:
        status = subprocess.run(["w32tm", "/query", "/status", "/verbose"], capture_output=True, text=True,
                                timeout=3, check=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)).stdout
        offset = float(re.search(r"Phase Offset:\s+(-?[0-9.]+)s", status).group(1))
        age = float(re.search(r"Time since Last Good Sync Time:\s+([0-9.]+)s", status).group(1))
        return ("Leap Indicator: 0" in status and "State Machine: 2 (Sync)" in status
                and abs(offset) <= .25 and age <= 18000)
    except (OSError, AttributeError, ValueError, subprocess.SubprocessError):
        return False


def synchronized(now):
    return _windows_clock(now.value // 300_000_000_000)


def _backoff_window(value):
    """Read the worker's versioned scope without guessing from exception text."""
    if (not isinstance(value, dict) or value.get("schema") != "PHASE10_BACKOFF_V1"
            or value.get("origin") != "lab.phase10.worker"
            or value.get("scope") not in ("RESEARCH_WORKER", "SHARED_KUCOIN_SOURCE")
            or not isinstance(value.get("cause"), str) or not value["cause"]):
        raise ValueError("Invalid worker backoff scope")
    start, finish = (pd.Timestamp(value[key]) for key in ("at", "retry_at"))
    if (pd.isna(start) or pd.isna(finish) or start.tzinfo is None or finish.tzinfo is None
            or start >= finish):
        raise ValueError("Invalid worker backoff interval")
    return start.tz_convert("UTC"), finish.tz_convert("UTC")


def _worker_backoff_permission(state, now, *, entry_only):
    """Keep real/unknown source restrictions; isolate proven local research faults.

    A legacy retry remains conservative. Only the new worker's exact, internally
    consistent local classification can allow an independent protection request.
    Entry admission is never relaxed by this exception. A separate source barrier
    survives a later local failure, so one cause cannot overwrite the other.
    """
    if (not isinstance(state, dict) or not state
            or not any(name in state for name in ("active_request", "retry_at"))):
        raise ValueError("An explicit worker request/retry checkpoint is required")
    if state.get("active_request") is not None:
        intent = state["active_request"]
        if not isinstance(intent, dict) or not isinstance(intent.get("key"), str) or not intent["key"]:
            raise ValueError("Invalid outstanding source request")
        requested = pd.Timestamp(intent["started_at"])
        if pd.isna(requested) or requested.tzinfo is None or requested > now:
            raise ValueError("Invalid outstanding source request timestamp")
        # Only this reviewed worker's bounded, verified live HTTP request can
        # overlap baseline independent market observations. An abandoned/legacy
        # intent never becomes permission. Completed-but-unpublished outcomes
        # are excluded separately by the source-outcome OS fence.
        if not monitoring.live_request(state, now):
            return "SOURCE_REQUEST_OUTCOME_UNRESOLVED"
    windows = {}
    for name in ("backoff", "source_backoff"):
        if state.get(name) is not None:
            start, finish = _backoff_window(state[name])
            if start > now or name == "source_backoff" and state[name]["scope"] != "SHARED_KUCOIN_SOURCE":
                raise ValueError("Inconsistent worker backoff provenance")
            windows[name] = (start, finish)
    if "source_backoff" in windows and now < windows["source_backoff"][1]:
        return "SHARED_SOURCE_BACKOFF"
    retry = state.get("retry_at")
    if retry is None:
        if "backoff" in windows and now < windows["backoff"][1]:
            raise ValueError("Worker retry checkpoint is missing")
        return None
    finish = pd.Timestamp(retry)
    if pd.isna(finish) or finish.tzinfo is None:
        raise ValueError("Worker retry timestamp must be explicit")
    finish = finish.tz_convert("UTC")
    if "backoff" in windows and windows["backoff"][1] != finish:
        raise ValueError("Worker retry checkpoint disagrees with its scope")
    if now >= finish:
        return None
    backoff = state.get("backoff") or {}
    research_only = ("backoff" in windows and backoff["scope"] == "RESEARCH_WORKER"
                     and backoff["cause"] in ("LOCAL_RUNTIME_PERSISTENCE", "LOCAL_RESEARCH_STORAGE", "RESEARCH_RAW_CAPACITY"))
    return None if research_only and not entry_only else "SHARED_SOURCE_BACKOFF"


def _source_permission(source, now, *, entry_only, wait_seconds=.15):
    if source == ema.SOURCE:
        return None
    if source not in sources.KUCOIN:
        return "UNSUPPORTED_SOURCE_PERMISSION"
    try:
        flags = monitoring.controls()
        if not protocol.control()["enabled"] or not flags["monitor"] or (entry_only and flags["paused"]):
            return "KUCOIN_MONITOR_OR_PROTECTION_PERMISSION_CLOSED"
        # Serialize only the tiny admission read with HTTP-result publication,
        # never the network call. A failed publication retains the worker's
        # OS fence until the known outcome is durable. Ordinary live HTTP does
        # not hold this fence or alias the 15-second protection sampling loop.
        with monitoring.source_outcome_lock(wait_seconds=wait_seconds) as waited:
            state = monitoring.read_runtime()
            admission_at = now + pd.Timedelta(seconds=waited)
            return _worker_backoff_permission(state, admission_at, entry_only=entry_only)
    except monitoring.SourceOutcomePending:
        return "SOURCE_OUTCOME_PUBLICATION_PENDING"
    except (OSError, ValueError, KeyError, TypeError, OverflowError):
        # Corrupt/inaccessible ownership metadata is not evidence of permission.
        # Return a KuCoin-local rejection instead of aborting other source work.
        return "SHARED_SOURCE_STATE_UNAVAILABLE"


def _observe(source, previous=None):
    if source in sources.KUCOIN:
        return sources.observe(source)
    if source != ema.SOURCE:
        raise capabilities.UnsupportedCapability("UNSUPPORTED_REFERENCE_SOURCE", str(source))
    try:
        return ema.observe(previous_request_at=previous)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 429:
            return {"status": "RATE_LIMITED", "retry_at": (utc_now() + pd.Timedelta(minutes=1)).isoformat()}
        if exc.response.status_code in (401, 403, 451):
            return {"status": "ACCESS_RESTRICTED"}
        return {"status": f"HTTP_{exc.response.status_code}"}
    except PermissionError:
        return {"status": "KRAKEN_PERMISSION_OR_QUALIFICATION_UNAVAILABLE"}
    except (ValueError, RuntimeError):
        return {"status": "KRAKEN_SOURCE_SPACING_OR_EVIDENCE_REJECTED"}
    except httpx.TransportError:
        return {"status": "TRANSPORT_ERROR"}


def probe():
    """Bounded, read-only qualification of the actual adapters and workstation clock."""
    result = {}
    for source, symbol in ((sources.MARK, "XBTUSDTM"), (sources.SPOT, "BTC-USDT"), (ema.SOURCE, ema.SYMBOL)):
        measurements, previous, last_event, last_stamp = [], None, None, None
        for attempt in range(3):
            now = utc_now()
            problem = _source_permission(source, now, entry_only=True) or (
                None if synchronized(now) else "WORKSTATION_CLOCK_SYNC_UNVERIFIED")
            obs = {"status": problem} if problem else _observe(source, previous)
            checked_at, mono = execution.clock()
            failure = execution.valid_snapshot(obs, {"source": source, "symbol": symbol}, checked_at, mono)
            if not failure and (last_event == obs["event_id"] or (last_stamp is not None and utc(obs["source_at"]) <= last_stamp)):
                failure = "DUPLICATE_OR_REORDERED_REFERENCE"
            if not failure:
                last_event, last_stamp = obs["event_id"], utc(obs["source_at"])
            previous = obs.get("requested_at", now.isoformat())
            measurements.append({"checked_at": checked_at.isoformat(), "failure": failure,
                "exchange_at": obs.get("source_at"), "receipt_at": obs.get("received_at"),
                "age_seconds": (checked_at - utc(obs["source_at"])).total_seconds() if obs.get("source_at") else None})
            if problem or obs.get("status") in ("ACCESS_RESTRICTED", "RATE_LIMITED"):
                break
            if attempt < 2:
                time.sleep(1.25)
        accepted = sum(row["failure"] is None for row in measurements)
        result[source] = {"state": "QUALIFIED" if accepted >= 2 else "BLOCKED", "accepted": accepted,
                          "reason": None if accepted >= 2 else next((row["failure"] for row in reversed(measurements) if row["failure"]),
                                                                    "INSUFFICIENT_DISTINCT_REFERENCES"),
                          "measurements": measurements}
    return result


def _source_state(db, source):
    row = db.execute("SELECT state FROM source_state WHERE id=?", (source,)).fetchone()
    return json.loads(row[0]) if row else {}


def _lane_source_state(db, lane):
    scoped = _source_state(db, "SCOPE:" + capabilities.source_scope_id(lane))
    if scoped:
        return scoped
    legacy = _source_state(db, lane["source"])
    point = legacy.get("last_observation") or {}
    if point and (point.get("symbol") != lane["symbol"] or
                  point.get("observation_kind", "HTTP_SNAPSHOT") != lane.get("observation_kind", "HTTP_SNAPSHOT")):
        return {}
    return legacy


def _save_source(db, source, state):
    db.execute("INSERT OR REPLACE INTO source_state VALUES (?,?)", (source, catalog.encode(state)))


def _save_lane_source(db, lane, state):
    scope = capabilities.source_scope_id(lane)
    legacy = _source_state(db, lane["source"])
    if legacy.get("alias_for_scope") not in (None, scope):
        raise capabilities.UnsupportedCapability("AMBIGUOUS_LEGACY_SOURCE_ALIAS", lane["source"])
    value = {**state, "source_scope_id": scope, "source_identity": capabilities.source_identity(lane)}
    _save_source(db, "SCOPE:" + scope, value)
    _save_source(db, lane["source"], {**value, "alias_for_scope": scope})


def _replayed(obs, previous):
    if not previous:
        return False
    at, prior = utc(obs["source_at"]), utc(previous["source_at"])
    if obs.get("observation_kind") == execution.WS_TRADE and previous.get("observation_kind") == execution.WS_TRADE:
        return int(obs["event_id"]) <= int(previous["event_id"]) or at < prior
    return obs["event_id"] == previous["event_id"] or at <= prior


def _applicable_protection_reference(position, obs):
    if not position or position.get("exit_observed"):
        return False
    source = utc(obs["source_at"])
    if source <= utc(position["entry_at"]):
        return False
    previous = utc(position["mark_source_at"])
    if obs.get("observation_kind") == execution.WS_TRADE:
        previous_id = str(position.get("mark_event_id", ""))
        return source >= previous and (not previous_id.isdigit() or int(obs["event_id"]) > int(previous_id))
    return source > previous


def _sample_permission(source, now, *, open_positions):
    permission = _source_permission(source, now, entry_only=not open_positions)
    try:
        has_space = shutil.disk_usage(gate.ROOT).free >= 2 * 1024**3
    except OSError:
        has_space = False
    return permission, has_space, synchronized(now)


def _reserve_sample(db, lane, now, preflight):
    state = _lane_source_state(db, lane)
    if state.get("last_request") and now < utc(state["last_request"]) + sources.INTERVAL:
        return None, None
    # Only one request per source across books and across restarts.
    permission, has_space, clock_ok = preflight
    if state.get("restricted") or (state.get("retry_at") and now < utc(state["retry_at"])):
        permission = state.get("reason", "SOURCE_BACKOFF")
    count = state.get("requests", 0) if state.get("utc_day") == str(now.date()) else 0
    if count >= REQUEST_LIMIT or not has_space:
        permission = "SOURCE_REQUEST_OR_DISK_BUDGET"
    if not clock_ok:
        permission = "WORKSTATION_CLOCK_SYNC_UNVERIFIED"
    if permission:
        state["reason"] = permission
        _save_lane_source(db, lane, state)
        return None, {"status": permission}
    previous = state.get("last_request")
    state.update(last_request=now.isoformat(), requests=count + 1, utc_day=str(now.date()))
    _save_lane_source(db, lane, state)
    db.commit()
    return (state["last_request"], previous), None


def _receive_sample(db, lane, request, obs, *, clock_ok=True, permission=None):
    state = _lane_source_state(db, lane)
    if state.get("last_request") != request[0] or not obs:
        return {"status": "REFERENCE_ACQUISITION_INVALIDATED"}
    source = lane["source"]
    at, mono = execution.clock()
    reason = execution.valid_snapshot(obs, lane, at, mono) or permission
    if not clock_ok:
        reason = "WORKSTATION_CLOCK_SYNC_UNVERIFIED"
    if not reason:
        previous_point = state.get("last_observation")
        if _replayed(obs, previous_point):
            reason = "DUPLICATE_OR_REORDERED_REFERENCE"
        elif not db.execute("INSERT OR IGNORE INTO observation VALUES (?,?,?,?)",
                            (source, obs["source_at"], obs["event_id"], catalog.encode(obs))).rowcount:
            reason = "DUPLICATE_OR_REORDERED_REFERENCE"
        else:
            state["last_observation"] = obs
    state.update(reason=reason, retry_at=obs.get("retry_at"), restricted=obs.get("status") == "ACCESS_RESTRICTED")
    _save_lane_source(db, lane, state)
    return {**obs, "status": reason} if reason else obs


def sample(db, source, lane, now, *, open_positions):
    """Synchronous adapter used by isolated callers and source qualification tests."""
    last = _lane_source_state(db, lane).get("last_request")
    if last and now < utc(last) + sources.INTERVAL:
        return None
    request, blocked = _reserve_sample(db, lane, now, _sample_permission(source, now, open_positions=open_positions))
    return _receive_sample(db, lane, request, _observe(source, request[1])) if request else blocked


def _can_enter(lane, body, now):
    if (lane["qualification"]["state"] != "QUALIFIED" or not utc(body["start"]) <= now < utc(lane["publication_end_exclusive"])
            or (gate.ROOT / "entries-paused").exists()):
        return False
    if lane.get("source_effective_at") and now < utc(lane["source_effective_at"]):
        return False
    return _entries_enabled(lane) and synchronized(now)


def _settle(db, book, lane, body, now, metadata, observation=None):
    if lane["source"] == sources.MARK:
        return funding.settle(db, book, lane, body, now, metadata, observation, generation=current()["id"])
    record = book["position"]
    plan = record["plan"]
    entry = utc(record["entry_at"])
    exit_point = record.get("exit_observed")
    horizon = utc(exit_point["exit_at"]) if exit_point else now
    if plan.get("model_financing_rate"):
        interval = pd.Timedelta(minutes=plan["model_financing_interval_minutes"])
        slots = set(pd.date_range(entry.floor(interval) + interval, horizon, freq=interval))
        basis = ema.FINANCING_BASIS
    else:
        slots = set()
        basis = None
    receipts = record.setdefault("funding_references", {})
    if observation and not execution.valid_snapshot(observation, lane, *execution.clock()):
        value_at = utc(observation["source_at"])
        for slot in slots:
            if slot <= value_at <= slot + execution.PROTECTION_GAP:
                receipts.setdefault(slot.isoformat(), {key: observation[key] for key in ("price", "source_at", "received_at")})
    done = set(record.get("settled_funding", []))
    due = sorted(slot for slot in slots if slot.isoformat() not in done)
    if due:
        pos = Position(**record["position"])
        for slot in due:
            key = slot.isoformat()
            reference = receipts.get(key)
            rate = pos.direction * plan["model_financing_rate"]
            if not reference:
                continue
            execution.funding(db, book, record, pos, slot, reference["received_at"], rate,
                              reference["price"], now, basis)
            done.add(key)
        record["settled_funding"] = sorted(done)
    record["unresolved_costs"] = "WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION" if len(set(s.isoformat() for s in slots) - done) else None
    if exit_point and not record["unresolved_costs"]:
        execution.close(db, book, lane, now)
    return bool(record["unresolved_costs"])


def evaluate(db, record, books, candidates, now, metadata):
    body = record["body"]
    eligible = {}
    for slot, lane in candidates.items():
        book = books[slot]
        if not _source_health(db, lane, now)["available"] or not synchronized(now):
            continue
        adapter = _signal_adapter(lane)
        grace = int(body["config"]["ideas"]["completed_candle_grace_seconds"]) if adapter == "breakout_v1" else 0
        boundary = (now - pd.Timedelta(seconds=grace)).floor(lane["timeframe"])
        source_start = utc(lane.get("source_effective_at", body["start"]))
        resume = (book.get("prospective_admission") or {}).get("effective_at")
        first = max(utc(body["start"]), source_start, utc(resume) if resume else source_start)
        if boundary < first or book["position"] or book["pending"] or not incidents.admission_allowed(book, now):
            continue
        prior = book.get("last_evaluation") or {}
        if prior.get("boundary") == boundary.isoformat() and (
                prior["state"] == "EVALUATED" or now - utc(prior["at"]) < pd.Timedelta(seconds=15)):
            continue
        seconds = (body["max_live_delay_seconds"] if adapter == "breakout_v1"
                   else lane["source_policy"]["publication_max_seconds"])
        deadline = boundary + pd.Timedelta(seconds=seconds)
        if now > deadline:
            continue
        eligible[slot] = (lane, boundary, deadline)
    breakouts = {slot: lane for slot, (lane, _, _) in eligible.items() if _signal_adapter(lane) == "breakout_v1"}
    try:
        rows = breakout.confirmation_rows(record, breakouts, books, now) if breakouts else {}
    except ERRORS as exc:
        LOG.exception("Registered Breakout signal evidence failed")
        rows = {}
        for slot, lane in breakouts.items():
            operations.record_fault(db, record, lane, books[slot], now, "SIGNAL_EVIDENCE_UNAVAILABLE",
                                    historical=False, details={"exception_type": type(exc).__name__, "adapter": "breakout_v1"})
    for slot, (lane, boundary, deadline) in eligible.items():
        book = books[slot]
        row = rows.get(slot)
        if _signal_adapter(lane) == "breakout_v1":
            if row is None:
                operations.record_fault(db, record, lane, book, now, "SIGNAL_EVIDENCE_UNAVAILABLE",
                                        historical=False, details={"adapter": "breakout_v1", "boundary": boundary.isoformat()})
                book["last_evaluation"] = {"at": now.isoformat(), "boundary": boundary.isoformat(),
                                           "state": "WAITING", "reason": "Registered Breakout adapter returned no evidence"}
                paper.save(db, book)
                continue
            operations.clear_fault(book, "SIGNAL_EVIDENCE_UNAVAILABLE")
            plan = breakout.plan(row, lane, metadata) if row.get("plan") else None
            waiting = row["category"] == "DATA_UNAVAILABLE" or (row.get("plan") and not plan)
            assessment = {"state": "WAITING" if waiting else "EVALUATED",
                          "reason": "Dated funding schedule unavailable" if row.get("plan") and not plan else row["reason"],
                          "analysis": {**row, "plan": plan}}
        else:
            try:
                assessment_book = {**book, "problem": None} if (
                    book.get("problem") and incidents.admission_allowed(book, now)) else book
                assessment = ema.confirmation(lane, assessment_book, body, now, db=db)
                if assessment.get("state") == "EVALUATED":
                    operations.clear_fault(book, "SIGNAL_EVALUATION_FAILED")
                    operations.clear_fault(book, "SIGNAL_QUALIFICATION_UNAVAILABLE")
                elif assessment.get("state") == "BLOCKED":
                    operations.record_fault(db, record, lane, book, now, "SIGNAL_QUALIFICATION_UNAVAILABLE",
                                            historical=False, details={"reason": assessment.get("reason")})
            except ERRORS as exc:
                LOG.exception("EMA evaluation failed for %s", slot)
                operations.record_fault(db, record, lane, book, now, "SIGNAL_EVALUATION_FAILED",
                                        historical=False, details={"exception_type": type(exc).__name__})
                assessment = {"state": "WAITING", "reason": f"EMA chart evaluation failed ({type(exc).__name__})"}
        actual = utc_now()
        if _signal_adapter(lane) == "ema_chart_v1":
            if actual > deadline:
                continue
            previous = book.get("last_evaluation") or {}
            if previous.get("state") == "EVALUATED":
                book["last_completed_evaluation"] = previous
        evaluation = {"at": actual.isoformat(), "boundary": boundary.isoformat(),
                      "state": assessment["state"], "reason": assessment.get("reason"),
                      "signal": assessment.get("signal")}
        book["last_evaluation"] = evaluation
        if _signal_adapter(lane) == "ema_chart_v1" and evaluation["state"] == "EVALUATED":
            book["last_completed_evaluation"] = evaluation
        analysis = assessment.get("analysis")
        if analysis and analysis.get("plan") and _can_enter(lane, body, actual):
            plan = analysis["plan"]
            if _ws_lane(lane):
                connection = source_amendment.active(record)
                plan.update(source_amendment_id=connection["id"] if connection else None,
                            observation_kind=execution.WS_TRADE,
                            source_scope_id=capabilities.source_scope_id(lane))
            if actual < utc(plan["eligible_from"]) < min(utc(plan["entry_expiry"]), utc(lane["publication_end_exclusive"])):
                if book.get("problem") and incidents.admission_allowed(book, actual):
                    prospective = {**book, "problem": None}
                    paper.publish(db, prospective, analysis, actual)
                    book.update(pending=prospective["pending"], cursor=prospective["cursor"])
                else:
                    paper.publish(db, book, analysis, actual)
        paper.save(db, book)


def _books_for_cycle(db, record, lanes):
    books = {}
    for slot, lane in lanes.items():
        try:
            capabilities.contract(lane, generation=record["id"], slot=slot)
            books[slot] = _load_book(db, lane)
        except ERRORS as exc:
            operations.record_fault(db, record, lane, {"book_id": lane["book_id"]}, utc_now(),
                                    "REGISTERED_BOOK_INSPECTION_FAILED", historical=False,
                                    details={"exception_type": type(exc).__name__, "exposure": "UNKNOWN"})
            LOG.exception("Registered book inspection failed for %s", slot)
    return books


def _reserve_reference(db, record, lanes, books, obs, at, *, rejection=None):
    if not db.in_transaction:
        db.execute("BEGIN IMMEDIATE")
    for slot, book in books.items():
        if not book.get("position") and not book.get("pending"):
            continue
        db.execute("SAVEPOINT reference_reservation")
        try:
            delivery.reserve(db, lanes[slot], book, obs, at=at, rejection=rejection)
            operations.clear_fault(book, "REFERENCE_DELIVERY_CAPACITY_FAILED")
            paper.save(db, book)
        except ERRORS as exc:
            db.execute("ROLLBACK TO reference_reservation")
            operations.record_fault(db, record, lanes[slot], book, at, "REFERENCE_DELIVERY_CAPACITY_FAILED",
                                    details={"exception_type": type(exc).__name__,
                                             "event_id": obs.get("event_id"), "source_at": obs.get("source_at")})
            paper.save(db, book)
        finally:
            db.execute("RELEASE reference_reservation")


def _drain_references(db, record, lanes, books, metadata=None, *, failed=None, slots=None):
    processed = 0
    failed = set() if failed is None else failed
    slots = books if slots is None else slots
    for _ in range(delivery.MAX_PENDING):
        progressed = False
        for slot in slots:
            if slot in failed:
                continue
            if processed >= 128:
                break
            lane = lanes[slot]
            scope = capabilities.source_scope_id(lane)
            queued = delivery.pending(db, lane["book_id"], scope)
            if not queued:
                if delivery.losses(db, lane["book_id"], scope):
                    db.execute("BEGIN IMMEDIATE")
                    book = _load_book(db, lane)
                    _delivery_losses(db, record, lane, book, utc_now())
                    paper.save(db, book)
                    db.commit()
                    books[slot] = book
                continue
            book = _load_book(db, lane)
            try:
                _apply_live_point(db, book, lane, record["body"], queued[0]["observation"],
                                  work=queued[0], metadata=metadata)
            except ERRORS as exc:
                failed.add(slot)
                db.rollback()
                book = _load_book(db, lane)
                operations.record_fault(db, record, lane, book, utc_now(), "BOOK_DELIVERY_TRANSACTION_FAILED",
                                        historical=False, details={"exception_type": type(exc).__name__,
                                                                   "pending_work_id": queued[0]["id"]})
                paper.save(db, book)
                db.commit()
                LOG.exception("Reference work retained for %s", slot)
            books[slot] = book
            processed += 1
            progressed = True
        if not progressed or processed >= 128:
            break


def _reassess_book(db, record, lane, book, at):
    point = _lane_source_state(db, lane).get("last_observation")
    health = _source_health(db, lane, at)
    fresh = not execution.valid_snapshot(point, lane, *execution.clock())
    records = incidents.summaries(db, book, record["id"])
    # A reviewed historical disposition may need bounded advisory acquisition
    # while the global entry pause is still held. Profile/source bans remain
    # effective and recovery._decision still requires reconciled flatness.
    recovery_probe = bool(records and all(item.get("outcome") for item in records) and _profile_entries_enabled(lane))
    if (not records and not book.get("problem") and not book.get("permanent_block")
            and not (book.get("position") or {}).get("unresolved_costs")
            and health["available"] and synchronized(at) and _entries_enabled(lane)):
        # An on-demand reference is acquired after a signal. Its normal absence
        # while a clean account is flat is not an outage or a recovery job.
        return recovery.reassess(db, record, lane, book, records, at,
                                 health={"qualified": True}, controls={"paused": False})
    return recovery.reassess(db, record, lane, book, records, at,
                             health={"qualified": health["available"] and synchronized(at) and fresh,
                                     "reason": health["reason_code"] or (None if fresh else "FRESH_REFERENCE_REQUIRED")},
                             controls={"paused": not (_entries_enabled(lane) or recovery_probe),
                                       "independent_risk_block": bool(book.get("permanent_block"))})


def _delivery_target(book):
    return ("POSITION", book["position"]["id"]) if book.get("position") else (
        ("ENTRY", book["pending"]["id"]) if book.get("pending") else None)


@contextmanager
def _cycle_writer(record, amendment, timings):
    with writer_lock(gate.ROOT / ".writer.lock"):
        started = time.monotonic_ns()
        try:
            current_record = current()
            if (not current_record or current_record["id"] != record["id"] or
                    source_amendment.active(current_record) != amendment):
                raise RuntimeError("Paper cycle authority changed between writer leases")
            # This sets SQLite's busy-handler limit only. A filesystem call
            # may return later, and the measured lease remains authoritative.
            with closing(connect(timeout=.2)) as db:
                yield db
        finally:
            timings.append(time.monotonic_ns() - started)


@contextmanager
def _trail_preparer():
    preparer = trail_research.Preparer()
    try:
        yield preparer
    finally:
        preparer.stop()


def _advance_trails(db, record, lanes, books, preparer):
    tracked = {slot: book for slot, book in books.items()
               if book.get("position") and capabilities.adapter(lanes[slot], "trail")}
    preparer.retain({book["book_id"] for book in tracked.values()})
    for slot, book in tracked.items():
        lane, now = lanes[slot], utc_now()
        identity = breakout.trail_identity(book, lane, now)
        if identity is None:
            continue
        result = preparer.get(record["id"], book, lane, now, identity)
        if result is None:
            continue
        proposals, error, captured_at = result
        if captured_at > now:
            continue  # A wall-clock rollback cannot make future trail data eligible.
        if not error:
            candidate = deepcopy(book)
            db.execute("SAVEPOINT trail_update")
            try:
                breakout.apply_trail(db, candidate, lane, now, identity, proposals)
                operations.clear_fault(candidate, "TRAIL_CALCULATION_FAILED")
                paper.save(db, candidate)
            except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                db.execute("ROLLBACK TO trail_update")
                db.execute("RELEASE trail_update")
                error = type(exc).__name__
            else:
                db.execute("RELEASE trail_update")
                book.update(candidate)
        if error:
            book["position"]["trail_check"] = identity[1]
            operations.record_fault(db, record, lane, book, now, "TRAIL_CALCULATION_FAILED",
                                    details={"exception_type": error})
            paper.save(db, book)


def cycle(*, trail_researcher=None):
    record = current()
    if not record:
        return {"state": "UNREGISTERED"}
    body, now = record["body"], utc_now()
    amendment = source_amendment.active(record)
    timings = []
    try:
        metadata = protocol.current_metadata()
    except ERRORS:
        metadata = {}
    with _cycle_writer(record, amendment, timings) as db:
        lanes = membership.validate_accounts(db, record, amendment)
        books = _books_for_cycle(db, record, lanes)
        if trail_researcher is not None:
            _advance_trails(db, record, lanes, books, trail_researcher)
        candidates = {slot: lanes[slot] for slot in books if _can_enter(lanes[slot], body, now)}
        for slot, book in books.items():
            execution.pending_ready(db, book, now)
            if slot not in candidates and book["pending"]:
                execution.finish(db, book, now, "NO_FILL", "ENTRIES_OFF_OR_STUDY_WINDOW_CLOSED")
            _reassess_book(db, record, lanes[slot], book, now)
            paper.save(db, book)
        db.commit()
        groups = {}
        for slot in books:
            lane = lanes[slot]
            groups.setdefault(capabilities.source_scope_id(lane), {})[slot] = lane
    for scope, matching in sorted(groups.items(), key=lambda item: next(iter(item[1].values()))["source"]):
        reference_lane = next(iter(matching.values()))
        if _ws_lane(reference_lane):
            continue
        source = reference_lane["source"]
        opened = any(book["position"] and not book["position"].get("exit_observed")
                     for slot, book in books.items() if slot in matching)
        at = utc_now()
        preflight = _sample_permission(source, at, open_positions=opened)
        with _cycle_writer(record, amendment, timings) as db:
            # Re-read all targets when a lease was released. A new snapshot
            # must never write an earlier in-memory version of a changed book.
            relevant = _books_for_cycle(db, record, matching)
            targets = {slot: _delivery_target(book) for slot, book in relevant.items()}
            actual_open = any(book["position"] and not book["position"].get("exit_observed") for book in relevant.values())
            pending = [slot for slot, book in relevant.items() if slot in candidates and execution.pending_ready(db, book, utc_now())]
            acquisition = [slot for slot, book in relevant.items() if recovery.acquisition_due(db, book["book_id"], utc_now())
                           and _profile_entries_enabled(matching[slot])]
            request, obs = None, None
            if actual_open or pending or acquisition:
                if actual_open != opened:
                    obs = {"status": "REFERENCE_ACQUISITION_TARGET_CHANGED"}
                else:
                    request, obs = _reserve_sample(db, reference_lane, at, preflight)
            db.commit()
        # The acquisition and operating-system/venue latency own no financial
        # connection or lease. The returned result has no authority by itself.
        if request:
            try:
                obs = _observe(source, request[1])
            except ERRORS as exc:
                obs = {"status": f"PRICE_EVIDENCE_UNAVAILABLE_{type(exc).__name__}"}
        received, _ = execution.clock()
        clock_ok = synchronized(received) if request else None
        receive_permission = (_source_permission(source, received, entry_only=not opened, wait_seconds=0)
                              if request else None)
        with _cycle_writer(record, amendment, timings) as db:
            relevant = _books_for_cycle(db, record, matching)
            if request:
                # An old request, changed generation, terminal target or stale
                # reference cannot produce a newly applied financial effect.
                obs = _receive_sample(db, reference_lane, request, obs,
                                      clock_ok=clock_ok, permission=receive_permission)
            at, mono = execution.clock()
            valid = bool(obs) and not execution.valid_snapshot(obs, reference_lane, at, mono)
            if valid:
                unchanged = {slot: book for slot, book in relevant.items()
                             if targets.get(slot) == _delivery_target(book)}
                _reserve_reference(db, record, matching, unchanged, obs, at)
            for slot in acquisition:
                if obs is not None and slot in relevant:
                    recovery.record_acquisition(db, relevant[slot]["book_id"], at, success=valid,
                                                reason=None if valid else obs.get("status"))
            shared_health = _source_health(db, reference_lane, at)
            operations.source_transition(db, record, reference_lane, at, available=shared_health["available"],
                                         reason=shared_health["reason_code"])
            db.commit()
            _drain_references(db, record, matching, relevant, metadata)
            for slot, book in list(relevant.items()):
                if (book.get("position") and (not valid or book["position"].get("exit_observed"))
                        and not delivery.pending(db, book["book_id"], scope)):
                    try:
                        _apply_live_point(db, book, matching[slot], body, obs if not valid else None, metadata=metadata)
                    except ERRORS as exc:
                        db.rollback()
                        book = _load_book(db, matching[slot])
                        operations.record_fault(db, record, matching[slot], book, utc_now(), "BOOK_ACCOUNTING_OR_PROTECTION_FAILED",
                                                historical=False, details={"exception_type": type(exc).__name__})
                        paper.save(db, book)
                        db.commit()
                books[slot] = book
    with _cycle_writer(record, amendment, timings) as db:
        books = _books_for_cycle(db, record, lanes)
        for matching in groups.values() if trail_researcher is None else ():
            if _ws_lane(next(iter(matching.values()))):
                continue
            relevant = {slot: books[slot] for slot in matching if slot in books}
            for slot, book in relevant.items():
                lane = lanes[slot]
                if book.get("position") and capabilities.adapter(lane, "trail"):
                    try:
                        breakout.trail(db, book, lane, utc_now())
                        operations.clear_fault(book, "TRAIL_CALCULATION_FAILED")
                    except ERRORS as exc:
                        operations.record_fault(db, record, lane, book, utc_now(), "TRAIL_CALCULATION_FAILED",
                                                details={"exception_type": type(exc).__name__})
                    paper.save(db, book)
        _capture_operations(db, record, lanes, books, utc_now())
        for slot, book in books.items():
            _reassess_book(db, record, lanes[slot], book, utc_now())
        evaluate(db, record, books, candidates, utc_now(), metadata)
        _capture_operations(db, record, lanes, books, utc_now())
        policy = incidents.active(db)
        db.commit()
    longest = max(timings)
    if longest > 1_000_000_000:
        LOG.warning("Paper cycle writer lease held %d ns, exceeding the one-second budget", longest)
    result = {"state": "RUNNING" if longest <= 1_000_000_000 else "DEGRADED",
              "at": utc_now().isoformat(), "generation": record["id"], "pid": os.getpid(),
              "writer_lease": {"max_measured_ns": longest, "over_one_second": longest > 1_000_000_000},
              "runner_build_id": RUNNER_BUILD, "protection_policy": policy["policy"] if policy else None,
              "source_amendment_id": amendment["id"] if amendment else None,
              "ema_live_needed": bool(amendment) and any((book["position"] or book["pending"])
                                                        and _ws_lane(lanes[slot]) for slot, book in books.items())}
    if trail_researcher is not None:
        result["trail_research"] = trail_researcher.status()
    return {**result, "status_publication": publish_status("last-cycle.json", result)}


def _delivery_losses(db, record, lane, book, at):
    scope = capabilities.source_scope_id(lane)
    for loss in delivery.losses(db, book["book_id"], scope):
        if loss["requires_review"]:
            original = book.get("position")
            position = original if (original or {}).get("id") == loss["position_id"] else loss.get("frozen_position")
            if not position:
                raise ValueError("Stored delivery loss requires exposure evidence review")
            shadow = {**book, "position": position}
            episode = operations.record_fault(db, record, lane, shadow, at, "UNCOMMITTED_PROTECTION_REFERENCE",
                                              details={"delivery_loss_id": loss["id"], "loss": loss})
            book.pop("prospective_admission", None)
            if original is position:
                book["position"] = shadow["position"]
            else:
                book["problem"] = book.get("problem") or "UNRESOLVED_PROTECTION_INTERVAL"
            row = db.execute("SELECT key,body FROM event WHERE book=? AND idea=? AND kind LIKE 'PROTECTION_INCIDENT_%' "
                             "ORDER BY rowid DESC LIMIT 1", (book["book_id"], loss["position_id"])).fetchone()
            if row is None:
                # Before explicit policy activation retain a legacy gap cause.
                paper.event(db, shadow, "PROTECTION_GAP", loss["position_id"], at, at,
                            {"reason": "UNCOMMITTED_PROTECTION_REFERENCE", "delivery_loss_id": loss["id"],
                             "loss": loss, "historical_coverage": "UNRESOLVED"})
                row = db.execute("SELECT key,body FROM event WHERE book=? AND idea=? ORDER BY rowid DESC LIMIT 1",
                                 (book["book_id"], loss["position_id"])).fetchone()
            delivery.acknowledge_loss(db, book["book_id"], scope, loss["id"], at, {"event_id": row[0]})
        else:
            if book.get("pending"):
                execution.pending_ready(db, book, at)
            delivery.acknowledge_loss(db, book["book_id"], scope, loss["id"], at,
                                      {"reason": "NO_HISTORICAL_EXPOSURE; rejected point is not a late fill; normal fresh retry and expiry remain"})


def _apply_live_point(db, book, lane, body, obs, *, work=None, metadata=None):
    """Stage a book transaction and check freshness again immediately before committing it."""
    db.execute("BEGIN IMMEDIATE")
    try:
        latest = db.execute("SELECT coalesce(max(rowid),0) FROM event").fetchone()[0]
        now = utc_now()
        acknowledgement = "APPLIED"
        actual = book.get("position") if work and work["target_kind"] == "POSITION" else book.get("pending")
        expected = work["position_id"] or work["pending_id"] if work else None
        if work and (actual or {}).get("id") != expected:
            delivery.acknowledge(db, book["book_id"], work["source_scope_id"], work["event_id"], now, "TARGET_CHANGED")
            _delivery_losses(db, {"id": body.get("generation", current()["id"])}, lane, book, now)
            paper.save(db, book)
            db.commit()
            return
        invalid = (work.get("ingestion_rejection") or execution.valid_snapshot(obs, lane, *execution.clock())) if work else None
        protection_delivery = work and work["protection_required"] and not invalid and _applicable_protection_reference(actual, obs)
        if invalid:
            acknowledgement = {"state": "STALE" if "STALE" in invalid else "REJECTED", "reason": invalid}
            obs = {"status": "DELIVERY_" + invalid}
        if book["position"]:
            unresolved = _settle(db, book, lane, body, now, metadata or {}, obs)
            if book["position"]:
                shared = operations.source_state(db, lane)
                context = {"lane": lane, "source_scope_id": capabilities.source_scope_id(lane),
                           "correlation_id": shared.get("correlation_id")}
                if obs:
                    execution.protect(db, book, lane, obs, unresolved_costs=unresolved, source_context=context)
                    if book.get("position") and book["position"].get("mark_event_id") == obs.get("event_id"):
                        for fault in ("KRAKEN_STREAM_DISCONTINUITY", "UNCOMMITTED_PROTECTION_REFERENCE",
                                      "REFERENCE_DELIVERY_CAPACITY_FAILED"):
                            operations.clear_fault(book, fault)
                else:
                    execution.missing_protection(db, book, now, "WAITING_FOR_QUALIFYING_REFERENCE", **context)
                if not unresolved:
                    operations.clear_fault(book, "BOOK_ACCOUNTING_OR_PROTECTION_FAILED")
        elif book["pending"] and obs and _can_enter(lane, body, now):
            connection = source_amendment.active(current())
            entry_permission = _source_permission(lane["source"], now, entry_only=True, wait_seconds=0)
            if entry_permission:
                # A shared protection sample is not a waiver for a different
                # pending consumer's independent entry-admission restrictions.
                execution.reject(db, book, now, entry_permission)
            elif _ws_lane(lane) and book["pending"]["plan"].get("source_amendment_id") != (connection or {}).get("id"):
                execution.finish(db, book, now, "NO_FILL", "INTENTION_PRECEDES_APPROVED_TRADE_STREAM")
            else:
                def terms(b, l, p, price, at):
                    return _entry_adapter(l)(b, l, p, metadata or {}, body["config"], price, at)
                execution.entry(db, book, lane, obs, terms)
        operations.clear_fault(book, "BOOK_DELIVERY_TRANSACTION_FAILED")
        paper.save(db, book)
        transition = db.execute("SELECT kind FROM event WHERE rowid>? AND book=? AND kind IN ('HYPOTHETICAL_FILL','EXIT_OBSERVED')",
                                (latest, book["book_id"])).fetchone()
        at, mono = execution.clock()
        reason = execution.valid_snapshot(obs, lane, at, mono) if transition or protection_delivery else None
        if transition and transition[0] == "HYPOTHETICAL_FILL" and at >= utc(book["position"]["plan"]["entry_expiry"]):
            reason = "ENTRY_WINDOW_ELAPSED"
        if reason:
            db.rollback()
            db.execute("BEGIN IMMEDIATE")
            restored = _load_book(db, lane)
            book.clear()
            book.update(restored)
            if book["pending"] and execution.pending_ready(db, book, at):
                execution.reject(db, book, at, f"LEDGER_COMMIT_{reason}")
            elif book["position"]:
                execution.missing_protection(db, book, at, f"LEDGER_COMMIT_{reason}")
            paper.save(db, book)
            acknowledgement = {"state": "REJECTED", "reason": "LEDGER_COMMIT_" + reason}
        operations.note_entry_events(db, {"id": current()["id"]}, lane, book, latest, utc_now())
        if work:
            delivery.acknowledge(db, book["book_id"], work["source_scope_id"], work["event_id"],
                                 utc_now(), acknowledgement)
            _delivery_losses(db, {"id": current()["id"]}, lane, book, utc_now())
            paper.save(db, book)
        db.commit()
    except BaseException:
        db.rollback()
        raise


def _journal_stream_witnesses(db, record, lanes, books, scope, source_lane, now, policy, witnesses):
    selected = [witness for witness in witnesses if policy or witness["kind"] == consumer_spool.KIND]
    if not selected:
        return []
    journal = {"book_id": "SOURCE:" + scope}
    db.execute("BEGIN IMMEDIATE")
    try:
        for witness in selected:
            present = db.execute("SELECT 1 FROM event WHERE book=? AND kind='PROTECTION_TRANSPORT_WITNESS' "
                                 "AND idea=?", (journal["book_id"], witness["id"])).fetchone()
            if present:
                continue
            incidents.append(db, journal, "PROTECTION_TRANSPORT_WITNESS", witness["id"], now,
                             {"generation": record["id"], "source_scope_id": scope,
                              "source_identity": capabilities.source_identity(source_lane),
                              "registered_consumers": [lane["book_id"] for lane in lanes.values()],
                              "witness": witness}, identity=[scope, witness["id"]])
            if witness["kind"] not in ("DISCONNECTED", "QUEUE_DROPPED", "WITNESS_OVERFLOW", consumer_spool.KIND):
                continue
            shared = operations.source_transition(db, record, source_lane, now, available=False,
                                                 reason=witness.get("reason") or witness["kind"],
                                                 correlation_id=witness["id"], evidence=witness)
            for slot, book in books.items():
                lane = lanes[slot]
                if book.get("position") and utc(witness.get("last_detected_at", witness["at"])) >= utc(book["position"]["entry_at"]):
                    if policy:
                        incidents.open_incident(db, book, now, witness.get("reason", witness["kind"]),
                                                diagnostics={**witness, "at": witness.get("last_detected_at", witness["at"])},
                                                witness_id=witness["id"], lane=lane, source_scope_id=scope,
                                                correlation_id=shared["correlation_id"])
                    else:
                        book["position"]["unresolved_protection"] = True
                        book.pop("prospective_admission", None)
                        paper.event(db, book, "PROTECTION_GAP", book["position"]["id"], now, now,
                                    {"reason": witness["kind"], "source_scope_id": scope,
                                     "correlation_id": shared["correlation_id"],
                                     "last_reference": book["position"].get("mark_received_at")})
                    paper.save(db, book)
                operations.capture(db, record, lane, book, now,
                                   source_health={"available": False, "reason_code": shared["reason_code"],
                                                  "source_scope_id": scope}, clock_ok=synchronized(now),
                                   entries_enabled=_entries_enabled(lane))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    local = [witness["id"] for witness in selected if witness["kind"] == consumer_spool.KIND]
    if local:
        # Clear the exact durable spool revision before assessing the new reference,
        # including when the journal succeeded on an earlier attempt.
        consumer_spool.mark_committed(gate.ROOT, record["id"], local, at=utc_now())
    return [witness["id"] for witness in selected]


def live_prices(record, amendment, points, diagnostics, *, detect_gap=False, witnesses=()):
    """Deliver one authorized stream to every registered dependent capability.

    Ingestion and bounded per-book work are reserved durably together. A book's
    acknowledgment commits with its own financial transaction; another book's
    failure cannot consume that work or prevent independent protection.
    """
    with writer_lock(gate.ROOT / ".writer.lock"), ExitStack() as stack:
        if gate.reset_pending():
            raise ValueError("Cannot process references while a paper generation reset is pending")
        active = current()
        if not active or active["id"] != record["id"] or (amendment and amendment["body"]["generation"] != active["id"]):
            raise ValueError("References must be delivered to the current paper generation")
        record = active
        body = record["body"]
        db = stack.enter_context(closing(connect(timeout=.2)))
        all_lanes = _lanes(record, amendment, db=db)
        lanes = {}
        for slot, lane in all_lanes.items():
            try:
                capabilities.contract(lane, generation=record["id"], slot=slot)
                if _ws_lane(lane):
                    lanes[slot] = lane
            except ERRORS as exc:
                operations.record_fault(db, record, lane, {"book_id": lane["book_id"]}, utc_now(),
                                        "REGISTERED_BOOK_INSPECTION_FAILED", historical=False,
                                        details={"exception_type": type(exc).__name__, "exposure": "UNKNOWN"})
        if not lanes:
            db.commit()
            return []
        scopes = {capabilities.source_scope_id(lane) for lane in lanes.values()}
        if len(scopes) != 1:
            raise capabilities.UnsupportedCapability("UNREVIEWED_STREAM_MULTIPLEXING", repr(sorted(scopes)))
        source_lane = next(iter(lanes.values()))
        scope = capabilities.source_scope_id(source_lane)
        books = {}
        for slot, lane in lanes.items():
            try:
                books[slot] = _load_book(db, lane)
            except ERRORS as exc:
                operations.record_fault(db, record, lane, {"book_id": lane["book_id"]}, utc_now(),
                                        "REGISTERED_BOOK_INSPECTION_FAILED", historical=False,
                                        details={"exception_type": type(exc).__name__, "exposure": "UNKNOWN"})
        db.commit()
        now = utc_now()
        policy = incidents.active(db)
        recorded_witnesses = _journal_stream_witnesses(db, record, lanes, books, scope, source_lane, now, policy, witnesses)
        if detect_gap and not any(w["kind"] in ("DISCONNECTED", "QUEUE_DROPPED", "WITNESS_OVERFLOW", "CONSUMER_QUEUE_OVERFLOW") for w in witnesses):
            for slot, book in books.items():
                if book.get("position"):
                    operations.record_fault(db, record, lanes[slot], book, now, "KRAKEN_STREAM_DISCONTINUITY",
                                            details=diagnostics)
                    paper.save(db, book)
            db.commit()
        failed_books = set()
        # A prior failed application can leave FIFO work waiting. Give that
        # work its retry before a later trade can displace it from the queue.
        _drain_references(db, record, lanes, books, failed=failed_books)
        for obs in points:
            at, mono = execution.clock()
            reason = execution.valid_snapshot(obs, source_lane, at, mono)
            if not synchronized(at):
                reason = "WORKSTATION_CLOCK_SYNC_UNVERIFIED"
            health = _source_health(db, source_lane, at, diagnostics or None)
            if not health["available"]:
                reason = health["reason_code"]
            state = _lane_source_state(db, source_lane)
            expired_delivery = reason == "STALE_REFERENCE" and not execution.valid_snapshot(
                obs, source_lane, utc(obs["received_at"]), obs["received_mono_ns"])
            if (not reason or expired_delivery) and _replayed(obs, state.get("last_observation")):
                reason = "DUPLICATE_OR_REORDERED_REFERENCE"
                expired_delivery = False
            targets = tuple(slot for slot, book in books.items() if book.get("position") or book.get("pending"))
            if expired_delivery:
                targets = tuple(slot for slot in targets
                                if _applicable_protection_reference(books[slot].get("position"), obs))
            if reason:
                for slot, book in books.items():
                    if expired_delivery and slot in targets:
                        continue
                    if book.get("position") or book.get("pending"):
                        try:
                            _apply_live_point(db, book, lanes[slot], body, {"status": reason})
                        except ERRORS:
                            db.rollback()
                            books[slot] = _load_book(db, lanes[slot])
                            LOG.exception("Rejected reference processing failed for %s", slot)
                if not expired_delivery or not targets:
                    continue
            db.execute("BEGIN IMMEDIATE")
            try:
                _reserve_reference(db, record, lanes, {slot: books[slot] for slot in targets}, obs, at,
                                   rejection=reason if expired_delivery else None)
                if not expired_delivery:
                    state["last_observation"] = obs
                    _save_lane_source(db, source_lane, state)
                    operations.source_transition(db, record, source_lane, at, available=True)
                db.commit()
            except BaseException:
                db.rollback()
                raise
            # Service FIFO work while it can still pass the commit-time check.
            # An expired trade that passed at receipt goes through the same FIFO
            # to preserve the loss, without advancing the accepted source mark.
            if targets:
                _drain_references(db, record, lanes, books, failed=failed_books, slots=targets)
        if not points:
            for slot, book in books.items():
                if book.get("position"):
                    try:
                        _apply_live_point(db, book, lanes[slot], body,
                                          {"status": diagnostics.get("reason") or "MISSING_REFERENCE"})
                    except ERRORS as exc:
                        db.rollback()
                        book = _load_book(db, lanes[slot])
                        operations.record_fault(db, record, lanes[slot], book, utc_now(),
                                                "BOOK_ACCOUNTING_OR_PROTECTION_FAILED", historical=False,
                                                details={"exception_type": type(exc).__name__})
                        paper.save(db, book)
                        db.commit()
                        books[slot] = book
        _capture_operations(db, record, lanes, books, utc_now(), stream=diagnostics or None)
        db.commit()
        return recorded_witnesses


def status():
    from lab.paper_run.operational_status import read_status

    return read_status()


def run():
    with writer_lock(gate.ROOT / ".runner.lock"), status_publications(), _trail_preparer() as trail_reader:
        if gate.reset_pending():
            raise ValueError("Complete the pending generation reset before starting the writer")
        record = current()
        if not record:
            raise ValueError("Initialize the current generation before starting the writer")
        if gate.stop_requested(record["id"]):
            raise ValueError("The current generation has an administrative stop request")
        feed, amendment, result, last_usable = None, None, {}, None
        retry_points = []
        next_cycle, next_gap_check, prior_gaps = 0., 0., (0, 0)
        try:
            while not gate.stop_requested(record["id"]):
                now = time.monotonic()
                if feed:
                    points, diagnostics = retry_points + feed.drain(), feed.status()
                    counts = diagnostics["counts"]
                    gaps = counts.get("disconnects", 0), counts.get("queue_dropped", 0)
                    has_gap = gaps != prior_gaps
                    registered = _lanes(record, amendment)
                    reference_lanes = [lane for lane in registered.values() if _ws_lane(lane)]
                    if not reference_lanes:
                        raise capabilities.UnsupportedCapability("STREAM_WITHOUT_REGISTERED_CONSUMER", "Kraken trade feed")
                    lane = reference_lanes[0]
                    if points and not execution.valid_snapshot(points[-1], lane, *execution.clock()):
                        last_usable = points[-1]
                    witnesses = (feed.pending_witnesses() if result.get("protection_policy") else []) + consumer_spool.pending(gate.ROOT, record["id"])
                    if witnesses or points or has_gap or now >= next_gap_check:
                        next_gap_check = now + POLL_SECONDS
                        try:
                            accepted = live_prices(record, amendment, points, diagnostics, detect_gap=has_gap,
                                                   witnesses=witnesses)
                            feed.acknowledge_witnesses(accepted)
                            prior_gaps = gaps
                            retry_points = []
                        except (RuntimeError, *ERRORS):
                            if len(points) > kraken_live.QUEUE_LIMIT:
                                consumer_spool.record_loss(gate.ROOT, record["id"], capabilities.source_scope_id(lane),
                                                           points[:-kraken_live.QUEUE_LIMIT], at=utc_now())
                            retry_points = points[-kraken_live.QUEUE_LIMIT:]
                            LOG.exception("Kraken paper reference processing failed")
                now = time.monotonic()
                if now >= next_cycle:
                    # Service already received protection work before periodic strategy evaluation.
                    next_cycle = now + POLL_SECONDS
                    try:
                        amendment = source_amendment.active(record)
                        if amendment and feed is None:
                            feed = kraken_live.KrakenTradeFeed().start()
                        result = cycle(trail_researcher=trail_reader)
                        if feed:
                            diagnostics = feed.status()
                            publish_status("ema-live-status.json",
                                       {**diagnostics, "at": utc_now().isoformat(), "amendment_id": amendment["id"],
                                        "last_usable_observation": last_usable})
                    except (RuntimeError, *ERRORS):
                        LOG.exception("Current paper cycle failed")
                time.sleep(.1 if feed else min(1., max(0., next_cycle - time.monotonic())))
        finally:
            if feed:
                feed.close()


def ensure_runner():
    if gate.reset_pending():
        return {"state": "GENERATION_RESET_PENDING"}
    record = current()
    if not record:
        return {"state": "AWAITING_CURRENT_REGISTRATION"}
    if gate.stop_requested(record["id"]):
        return {"state": "RUNNER_STOP_REQUESTED", "generation": record["id"]}
    try:
        with writer_lock(gate.ROOT / ".runner.lock"):
            pass
    except RuntimeError:
        amendment = source_amendment.active(record)
        # Source amendments require a controlled handoff. A code edit alone
        # must not interrupt a running owner or its position monitoring.
        outcome = runner_reload.if_needed(gate.ROOT, record["id"], amendment["id"], project=PROJECT) if amendment else None
        if not outcome or outcome["state"] != "RUNNER_RELOADED":
            return outcome or {"state": "RUNNER_LOCKED"}
    with writer_lock(gate.ROOT / ".launch.lock"):
        if gate.reset_pending():
            return {"state": "GENERATION_RESET_PENDING"}
        if (current() or {}).get("id") != record["id"]:
            return {"state": "GENERATION_CHANGED"}
        if gate.stop_requested(record["id"]):
            return {"state": "RUNNER_STOP_REQUESTED", "generation": record["id"]}
        launch = monitoring.optional(gate.ROOT / "last-launch.json", {})
        now = utc_now()
        if launch and now - utc(launch["at"]) < pd.Timedelta(seconds=30):
            return {"state": "LAUNCH_IN_PROGRESS", **launch}
        args = [sys.executable, "-X", "utf8", "-m", "lab.paper_run", "run"]
        with (gate.ROOT / "runner.log").open("ab") as log:
            proc = subprocess.Popen(args, cwd=PROJECT,
                stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        body = {"at": now.isoformat(), "pid": proc.pid}
        write_json(gate.ROOT / "last-launch.json", body)
        return {"state": "RUNNER_STARTED", **body}
