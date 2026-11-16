"""Sole writer and current-generation status for the seven paper strategy slots."""

from contextlib import closing
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

from lab.config import ROOT as PROJECT, fingerprint
from lab.learning.simulator import Position
from lab.phase10 import worker as monitoring
from lab.phase13 import protocol
from lab.phase13.future import expected_funding
from lab.phase13.registry import sealed
from lab.phase15 import catalog, paper
from lab.paper_run import archive, breakout, ema, execution, gate, kraken_live, source_amendment, sources
from lab.paper_run import reload as runner_reload
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


VERSION = "fresh-reference-paper-v2"
POLL_SECONDS = 5
REQUEST_LIMIT = 6000
SQL = """
CREATE TABLE IF NOT EXISTS book (id TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS event (key TEXT PRIMARY KEY, book TEXT NOT NULL, kind TEXT NOT NULL,
    idea TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS book_events ON event(book,kind,recorded_at);
CREATE TABLE IF NOT EXISTS source_state (id TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS observation (source TEXT NOT NULL, at TEXT NOT NULL, event_id TEXT NOT NULL, body TEXT NOT NULL,
    PRIMARY KEY(source, event_id));
CREATE INDEX IF NOT EXISTS observation_at ON observation(source,at);
"""
ERRORS = (OSError, ValueError, KeyError, TypeError, StopIteration, sqlite3.Error)
LOG = logging.getLogger(__name__)
RUNNER_BUILD = fingerprint([file_hash(Path(__file__).with_name(name)) for name in (
    "runtime.py", "ema.py", "execution.py", "kraken_live.py", "source_amendment.py")])


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


def connect(*, readonly=False):
    path = gate.ROOT / "paper.sqlite3"
    if readonly:
        return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
    gate.ROOT.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=3)
    db.executescript(SQL)
    ema.init(db)
    return db


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
    retired = gate.retired()
    if retired != archive_record:
        raise ValueError("Retire and verify legacy writers before opening successors")
    now = utc_now() if now is None else now
    with writer_lock(gate.ROOT / ".writer.lock"):
        if prior := current():
            return prior
        intent = gate.ROOT / "initializing.json"
        body = sealed(intent)["body"] if intent.exists() else prepared or definitions(archive_record, qualifications, now)
        if body["archive_id"] != retired["id"]:
            raise ValueError("Successor initialization requires the verified cutoff")
        with closing(connect()) as db:
            existing = {row[0] for row in db.execute("SELECT id FROM book")}
        if not existing and utc(body["start"]) <= now:
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
                             "retired_book_id": lane["legacy_book_id"], "opening_balance": 10000.,
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


def _source_permission(source, now, *, entry_only):
    if source == ema.SOURCE:
        return None
    flags = monitoring.controls()
    if not protocol.control()["enabled"] or not flags["monitor"] or (entry_only and flags["paused"]):
        return "KUCOIN_MONITOR_OR_PROTECTION_PERMISSION_CLOSED"
    state = monitoring.optional(monitoring.RUNTIME, {})
    if state.get("retry_at") and now < utc(state["retry_at"]):
        return "SHARED_SOURCE_BACKOFF"
    return None


def _observe(source, previous=None):
    if source in sources.KUCOIN:
        return sources.observe(source)
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


def _save_source(db, source, state):
    db.execute("INSERT OR REPLACE INTO source_state VALUES (?,?)", (source, catalog.encode(state)))


def _replayed(obs, previous):
    if not previous:
        return False
    at, prior = utc(obs["source_at"]), utc(previous["source_at"])
    if obs.get("observation_kind") == execution.WS_TRADE and previous.get("observation_kind") == execution.WS_TRADE:
        return int(obs["event_id"]) <= int(previous["event_id"]) or at < prior
    return obs["event_id"] == previous["event_id"] or at <= prior


def sample(db, source, lane, now, *, open_positions):
    state = _source_state(db, source)
    if state.get("last_request") and now < utc(state["last_request"]) + sources.INTERVAL:
        return None
    # Only one request per source across books and across restarts.
    permission = _source_permission(source, now, entry_only=not open_positions)
    if state.get("restricted") or (state.get("retry_at") and now < utc(state["retry_at"])):
        permission = state.get("reason", "SOURCE_BACKOFF")
    count = state.get("requests", 0) if state.get("utc_day") == str(now.date()) else 0
    if count >= REQUEST_LIMIT or shutil.disk_usage(gate.ROOT).free < 2 * 1024**3:
        permission = "SOURCE_REQUEST_OR_DISK_BUDGET"
    if not synchronized(now):
        permission = "WORKSTATION_CLOCK_SYNC_UNVERIFIED"
    if permission:
        return {"status": permission}
    previous = state.get("last_request")
    state.update(last_request=now.isoformat(), requests=count + 1, utc_day=str(now.date()))
    _save_source(db, source, state)
    db.commit()
    obs = _observe(source, previous)
    at, mono = execution.clock()
    reason = execution.valid_snapshot(obs, lane, at, mono)
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
    _save_source(db, source, state)
    return {**obs, "status": reason} if reason else obs


def _can_enter(lane, body, now):
    if (lane["qualification"]["state"] != "QUALIFIED" or not utc(body["start"]) <= now < utc(lane["publication_end_exclusive"])
            or (gate.ROOT / "entries-paused").exists()):
        return False
    if lane.get("source_effective_at") and now < utc(lane["source_effective_at"]):
        return False
    if lane["family"] == "EMA":
        return True
    flags = monitoring.controls()
    return bool(flags["monitor"] and not flags["paused"] and flags["ideas"].get(lane["profile"]))


def _settle(db, book, lane, body, now, metadata, observation=None):
    record = book["position"]
    plan = record["plan"]
    entry = utc(record["entry_at"])
    exit_point = record.get("exit_observed")
    horizon = utc(exit_point["exit_at"]) if exit_point else now
    if lane["source"] == sources.MARK:
        current_schedule = metadata.get("contract", {})
        expected = expected_funding({"contract": plan["funding_schedule"]}, entry + pd.Timedelta(nanoseconds=1),
                                    horizon + pd.Timedelta(nanoseconds=1))
        latest = expected_funding({"contract": current_schedule}, entry + pd.Timedelta(nanoseconds=1),
                                  horizon + pd.Timedelta(nanoseconds=1))
        if expected is None or latest is None:
            record["unresolved_costs"] = "FUNDING_SCHEDULE_UNAVAILABLE"
            return True
        slots = set(expected) | set(latest)
        basis = "KUCOIN_SETTLED_RATE_FORWARD_MARK_VALUATION"
    elif plan.get("model_financing_rate"):
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
        actual = breakout.settled(body, due[0], due[-1], now) if lane["source"] == sources.MARK else {}
        pos = Position(**record["position"])
        for slot in due:
            key = slot.isoformat()
            reference = receipts.get(key)
            observed = actual.get(slot, {})
            rate = observed.get("rate") if lane["source"] == sources.MARK else pos.direction * plan["model_financing_rate"]
            if observed.get("actual_at") and not entry < utc(observed["actual_at"]) <= horizon:
                done.add(key)
                continue
            if rate is None or not reference:
                continue
            execution.funding(db, book, record, pos, slot, observed.get("received_at", reference["received_at"]), rate,
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
        grace = int(body["config"]["ideas"]["completed_candle_grace_seconds"]) if lane["family"] == "BREAKOUT" else 0
        boundary = (now - pd.Timedelta(seconds=grace)).floor(lane["timeframe"])
        source_start = utc(lane.get("source_effective_at", body["start"]))
        if boundary < max(utc(body["start"]), source_start) or any((book["position"], book["pending"], book["problem"])):
            continue
        prior = book.get("last_evaluation") or {}
        if prior.get("boundary") == boundary.isoformat() and (
                prior["state"] == "EVALUATED" or now - utc(prior["at"]) < pd.Timedelta(seconds=15)):
            continue
        if lane["family"] == "BREAKOUT" and now > boundary + pd.Timedelta(seconds=body["max_live_delay_seconds"]):
            continue
        eligible[slot] = (lane, boundary)
    breakouts = {slot: lane for slot, (lane, _) in eligible.items() if lane["family"] == "BREAKOUT"}
    rows = breakout.confirmation_rows(record, breakouts, books, now) if breakouts else {}
    for slot, (lane, boundary) in eligible.items():
        book = books[slot]
        row = rows.get(slot)
        if row is not None:
            plan = breakout.plan(row, lane, metadata) if row.get("plan") else None
            waiting = row["category"] == "DATA_UNAVAILABLE" or (row.get("plan") and not plan)
            assessment = {"state": "WAITING" if waiting else "EVALUATED",
                          "reason": "Dated funding schedule unavailable" if row.get("plan") and not plan else row["reason"],
                          "analysis": {**row, "plan": plan}}
        else:
            try:
                assessment = ema.confirmation(lane, book, body, now, db=db)
            except ERRORS as exc:
                LOG.exception("EMA evaluation failed for %s", slot)
                assessment = {"state": "WAITING", "reason": f"EMA chart evaluation failed ({type(exc).__name__})"}
        actual = utc_now()
        book["last_evaluation"] = {"at": actual.isoformat(), "boundary": boundary.isoformat(),
                                   "state": assessment["state"], "reason": assessment.get("reason"),
                                   "signal": assessment.get("signal")}
        analysis = assessment.get("analysis")
        if analysis and analysis.get("plan") and _can_enter(lane, body, actual):
            plan = analysis["plan"]
            if lane.get("source_amendment_id"):
                plan.update(source_amendment_id=lane["source_amendment_id"], observation_kind=lane["observation_kind"])
            if actual < utc(plan["eligible_from"]) < min(utc(plan["entry_expiry"]), utc(lane["publication_end_exclusive"])):
                paper.publish(db, book, analysis, actual)
        paper.save(db, book)


def cycle():
    record = current()
    if not record:
        return {"state": "UNREGISTERED"}
    body, now = record["body"], utc_now()
    amendment = source_amendment.active(record)
    lanes = source_amendment.lanes(record, amendment)
    with writer_lock(gate.ROOT / ".writer.lock"), closing(connect()) as db:
        books = {slot: paper.load(db, lane) for slot, lane in lanes.items()}
        try:
            metadata = protocol.current_metadata()
        except ERRORS:
            metadata = {}
        candidates = {slot: lane for slot, lane in lanes.items() if _can_enter(lane, body, now)}
        for slot, book in books.items():
            if slot not in candidates and book["pending"]:
                execution.finish(db, book, now, "NO_FILL", "ENTRIES_OFF_OR_STUDY_WINDOW_CLOSED")
            execution.pending_ready(db, book, now)
            paper.save(db, book)
        db.commit()

        for source in sorted({lane["source"] for lane in lanes.values()}):
            if source == ema.SOURCE and amendment:
                continue
            matching = {slot: lane for slot, lane in lanes.items() if lane["source"] == source}
            opened = [slot for slot in matching if books[slot]["position"]]
            pending = [slot for slot in matching if slot in candidates and execution.pending_ready(db, books[slot], utc_now())]
            if not opened and not pending:
                continue
            for slot in opened:
                book, lane = books[slot], lanes[slot]
                if lane["family"] == "BREAKOUT":
                    try:
                        breakout.trail(db, book, lane, utc_now())
                    except ERRORS:
                        book["position"]["unresolved_protection"] = True
                    paper.save(db, book)
            db.commit()
            observe = any(not books[slot]["position"].get("exit_observed") for slot in opened) or bool(pending)
            try:
                obs = sample(db, source, next(iter(matching.values())), utc_now(), open_positions=bool(opened)) if observe else None
            except ERRORS as exc:
                db.rollback()
                obs = {"status": f"PRICE_EVIDENCE_UNAVAILABLE_{type(exc).__name__}"}
            # Protection and cost reconciliation run regardless of entry controls.
            for slot in opened:
                book, lane = books[slot], lanes[slot]
                try:
                    unresolved = _settle(db, book, lane, body, utc_now(), metadata, obs)
                except ERRORS as exc:
                    book["position"]["unresolved_costs"] = f"FUNDING_EVIDENCE_UNAVAILABLE_{type(exc).__name__}"
                    unresolved = True
                if book["position"]:
                    if obs:
                        execution.protect(db, book, lane, obs, unresolved_costs=unresolved)
                    else:
                        execution.missing_protection(db, book, utc_now(), "WAITING_FOR_QUALIFYING_REFERENCE")
                paper.save(db, book)
            db.commit()
            for slot in pending:
                book, lane = books[slot], lanes[slot]
                if obs and _can_enter(lane, body, utc_now()):
                    calculate = ema.entry_terms if lane["family"] == "EMA" else breakout.entry_terms
                    def terms(b, l, p, price, at):
                        return calculate(b, l, p, metadata, body["config"], price, at)
                    execution.entry(db, book, lane, obs, terms)
                paper.save(db, book)
                db.commit()
        evaluate(db, record, books, candidates, utc_now(), metadata)
        db.commit()
    result = {"state": "RUNNING", "at": utc_now().isoformat(), "generation": record["id"], "pid": os.getpid(),
              "runner_build_id": RUNNER_BUILD,
              "source_amendment_id": amendment["id"] if amendment else None,
              "ema_live_needed": bool(amendment) and any(books[slot]["position"] or books[slot]["pending"]
                                                        for slot in source_amendment.SLOTS)}
    write_json(gate.ROOT / "last-cycle.json", result)
    return result


def _apply_live_point(db, book, lane, body, obs):
    """Stage a book transaction and check freshness again immediately before committing it."""
    db.execute("BEGIN IMMEDIATE")
    try:
        latest = db.execute("SELECT coalesce(max(rowid),0) FROM event").fetchone()[0]
        now = utc_now()
        if book["position"]:
            unresolved = _settle(db, book, lane, body, now, {}, obs)
            if book["position"]:
                execution.protect(db, book, lane, obs, unresolved_costs=unresolved)
        elif book["pending"] and obs and _can_enter(lane, body, now):
            if book["pending"]["plan"].get("source_amendment_id") != lane["source_amendment_id"]:
                execution.finish(db, book, now, "NO_FILL", "INTENTION_PRECEDES_APPROVED_TRADE_STREAM")
            else:
                def terms(b, l, p, price, at):
                    return ema.entry_terms(b, l, p, {}, body["config"], price, at)
                execution.entry(db, book, lane, obs, terms)
        paper.save(db, book)
        transition = db.execute("SELECT kind FROM event WHERE rowid>? AND book=? AND kind IN ('HYPOTHETICAL_FILL','EXIT_OBSERVED')",
                                (latest, book["book_id"])).fetchone()
        at, mono = execution.clock()
        reason = execution.valid_snapshot(obs, lane, at, mono) if transition else None
        if transition and transition[0] == "HYPOTHETICAL_FILL" and at >= utc(book["position"]["plan"]["entry_expiry"]):
            reason = "ENTRY_WINDOW_ELAPSED"
        if reason:
            db.rollback()
            restored = paper.load(db, lane)
            book.clear()
            book.update(restored)
            if book["pending"] and execution.pending_ready(db, book, at):
                execution.reject(db, book, at, f"LEDGER_COMMIT_{reason}")
            elif book["position"]:
                execution.missing_protection(db, book, at, f"LEDGER_COMMIT_{reason}")
            paper.save(db, book)
        db.commit()
    except BaseException:
        db.rollback()
        raise


def live_prices(record, amendment, points, diagnostics, *, detect_gap=False):
    """One price consumer for three EMA slots. The subscription never writes to SQLite."""
    body = record["body"]
    lanes = source_amendment.lanes(record, amendment)
    ema_lanes = {slot: lanes[slot] for slot in source_amendment.SLOTS}
    with writer_lock(gate.ROOT / ".writer.lock"), closing(connect()) as db:
        books = {slot: paper.load(db, lane) for slot, lane in ema_lanes.items()}
        state = _source_state(db, ema.SOURCE)
        now = utc_now()
        if detect_gap:
            for book in books.values():
                if record_pos := book["position"]:
                    record_pos["unresolved_protection"] = True
                    paper.event(db, book, "PROTECTION_GAP", record_pos["id"], now, now,
                                {"reason": "KRAKEN_STREAM_DISCONTINUITY", "last_reference": record_pos["mark_received_at"]})
                    paper.save(db, book)
            db.commit()
        if not synchronized(now):
            points = []
            diagnostics = {**diagnostics, "reason": "WORKSTATION_CLOCK_SYNC_UNVERIFIED"}
        for obs in points or [None]:
            if not any(book["position"] or book["pending"] for book in books.values()):
                break
            at, mono = execution.clock()
            reason = execution.valid_snapshot(obs, ema_lanes["ema/day"], at, mono)
            if not reason and _replayed(obs, state.get("last_observation")):
                reason = "DUPLICATE_OR_REORDERED_REFERENCE"
            if reason:
                rejected = {"status": (diagnostics.get("reason") or reason) if not obs else reason}
                for slot, book in books.items():
                    if book["position"]:
                        _apply_live_point(db, book, ema_lanes[slot], body, rejected)
                    elif book["pending"] and obs:
                        _apply_live_point(db, book, ema_lanes[slot], body, rejected)
                continue
            # Advance a durable high-water mark before the price is used. Only economic
            # events retain individual points, keeping the stream journal bounded.
            state["last_observation"] = obs
            _save_source(db, ema.SOURCE, state)
            db.commit()
            for slot, book in books.items():
                if book["position"] or book["pending"]:
                    _apply_live_point(db, book, ema_lanes[slot], body, obs)


def status():
    record = current()
    if not record:
        return {"state": "LEGACY_RETIRED_AWAITING_RESET" if gate.retired() else "UNREGISTERED", "books": {}}
    body, now = record["body"], utc_now()
    view = {}
    amendment = source_amendment.active(record)
    lanes = source_amendment.lanes(record, amendment)
    stream = monitoring.optional(gate.ROOT / "ema-live-status.json", {})
    last_cycle = monitoring.optional(gate.ROOT / "last-cycle.json", None)
    live = bool(last_cycle and last_cycle["generation"] == record["id"] and
                pd.Timedelta(0) <= now - utc(last_cycle["at"]) < pd.Timedelta(seconds=45))
    with closing(connect(readonly=True)) as db:
        db.execute("BEGIN")
        for slot, lane in lanes.items():
            info = paper.book_report(db, lane)
            qualification = lane["qualification"]
            source = _source_state(db, lane["source"])
            reason = qualification.get("reason") or source.get("reason") or info["problem"]
            position = info["position"] or {}
            incomplete = position.get("unresolved_costs") or position.get("unresolved_protection") or info["problem"]
            state = "BLOCKED" if qualification["state"] != "QUALIFIED" or incomplete or source.get("restricted") else (
                "ACTIVE" if live and _can_enter(lane, body, now) else "WAITING")
            if not live and not reason:
                reason = "Waiting for a current-runner heartbeat"
            if lane.get("source_amendment_id"):
                if not incomplete and (not last_cycle or last_cycle.get("source_amendment_id") != amendment["id"]):
                    state, reason = "WAITING", "Awaiting managed runner adoption of the source amendment"
                elif not incomplete and stream.get("state") != "STREAMING":
                    state = "BLOCKED" if stream.get("state") == "BLOCKED" else "WAITING"
                    reason = stream.get("reason") or "Awaiting the Kraken trade subscription"
                if stream.get("last_usable_observation"):
                    source["last_observation"] = stream["last_usable_observation"]
            if incomplete:
                reason = reason or position.get("unresolved_costs") or "UNRESOLVED_PROTECTION_INTERVAL"
                info.update(accounting_complete=False, closed_net=None, expectancy=None, expectancy_r=None,
                            top_three_share_of_positive_gains=None)
            next_at = max(utc(body["start"]), now.floor(lane["timeframe"]) + pd.Timedelta(lane["timeframe"]))
            view[slot] = {**info, "run_id": lane["book_id"], "label": lane["label"], "price_basis": lane["price_basis"],
                          "entry_model": lane["entry_model"], "exit_model": lane["exit_model"],
                          "freshness_seconds": body["freshness_seconds"], "state": state, "reason": reason,
                          "next_evaluation": next_at.isoformat(), "last_observation": source.get("last_observation"),
                          "retired_book_id": lane["legacy_book_id"], "ledger_unit": lane.get("ledger_unit", "USDT"),
                          "publication_end_exclusive": lane["publication_end_exclusive"], "qualification": qualification,
                          "last_evaluation": info.get("last_evaluation"), "source_effective_at": lane.get("source_effective_at"),
                          "source_amendment_id": lane.get("source_amendment_id")}
    return {"state": "CURRENT", "generation_id": record["id"], "cutoff": body["cutoff"], "start": body["start"],
            "archive": body["archive"], "books": view, "last_cycle": last_cycle, "ema_live_feed": stream}


def run():
    record = current()
    if not record:
        raise ValueError("Initialize the current generation before starting the writer")
    with writer_lock(gate.ROOT / ".runner.lock"):
        feed, amendment, result, last_usable = None, None, {}, None
        next_cycle, next_gap_check, prior_gaps = 0., 0., (0, 0)
        try:
            while True:
                now = time.monotonic()
                if now >= next_cycle:
                    next_cycle = now + POLL_SECONDS
                    try:
                        amendment = source_amendment.active(record)
                        if amendment and feed is None:
                            feed = kraken_live.KrakenTradeFeed().start()
                        result = cycle()
                        if feed:
                            diagnostics = feed.status()
                            write_json(gate.ROOT / "ema-live-status.json",
                                       {**diagnostics, "at": utc_now().isoformat(), "amendment_id": amendment["id"],
                                        "last_usable_observation": last_usable})
                    except (RuntimeError, *ERRORS):
                        LOG.exception("Current paper cycle failed")
                if feed:
                    points, diagnostics = feed.drain(), feed.status()
                    counts = diagnostics["counts"]
                    gaps = counts.get("disconnects", 0), counts.get("queue_dropped", 0)
                    has_gap = gaps != prior_gaps
                    prior_gaps = gaps
                    lane = {"source": ema.SOURCE, "symbol": ema.SYMBOL, "observation_kind": execution.WS_TRADE}
                    if points and not execution.valid_snapshot(points[-1], lane, *execution.clock()):
                        last_usable = points[-1]
                    if result.get("ema_live_needed") and (points or has_gap or now >= next_gap_check):
                        next_gap_check = now + POLL_SECONDS
                        try:
                            live_prices(record, amendment, points, diagnostics, detect_gap=has_gap)
                        except (RuntimeError, *ERRORS):
                            LOG.exception("Kraken paper reference processing failed")
                time.sleep(.1 if feed else min(1., max(0., next_cycle - time.monotonic())))
        finally:
            if feed:
                feed.close()


def ensure_runner():
    record = current()
    if not record:
        return {"state": "AWAITING_CURRENT_REGISTRATION"}
    try:
        with writer_lock(gate.ROOT / ".runner.lock"):
            pass
    except RuntimeError:
        amendment = source_amendment.active(record)
        outcome = runner_reload.if_needed(gate.ROOT, record["id"], amendment["id"], project=PROJECT,
                                          expected_build=RUNNER_BUILD) if amendment else None
        if not outcome or outcome["state"] != "RUNNER_RELOADED":
            return outcome or {"state": "RUNNER_LOCKED"}
    with writer_lock(gate.ROOT / ".launch.lock"):
        launch = monitoring.optional(gate.ROOT / "last-launch.json", {})
        now = utc_now()
        if launch and now - utc(launch["at"]) < pd.Timedelta(seconds=30):
            return {"state": "LAUNCH_IN_PROGRESS", **launch}
        with (gate.ROOT / "runner.log").open("ab") as log:
            proc = subprocess.Popen([sys.executable, "-X", "utf8", "-m", "lab.paper_run", "run"], cwd=PROJECT,
                stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        body = {"at": now.isoformat(), "pid": proc.pid}
        write_json(gate.ROOT / "last-launch.json", body)
        return {"state": "RUNNER_STARTED", **body}
