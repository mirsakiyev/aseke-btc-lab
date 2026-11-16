"""Standalone lifecycle for the prospective XBTUSDTM mark-reference paper books."""

import argparse
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
import json
import logging
from math import isfinite
import sqlite3
import time

import pandas as pd

from lab.config import fingerprint
from lab.learning.simulator import Contract, Position, liquidation_price, triggers
from lab.phase9.ideas import current_rules, fresh, timedelta_age
from lab.phase10 import worker as monitoring
from lab.phase13 import protocol
from lab.phase13.registry import sealed
from lab.paper_run.gate import legacy_entries_closed
from lab.phase15 import catalog, paper, realtime, worker
from lab.phase15.feeds import futures_source, futures_window
from lab.storage import RUNS, read_json, utc, utc_now, write_json, writer_lock


ROOT = RUNS / "phase15-realtime-mark"
LANES = ("day/fixed", "swing/fixed", "swing/trail")
POLL_SECONDS = 15
SOURCE_ERRORS = (KeyError, OSError, StopIteration, TypeError, ValueError, sqlite3.Error)
LOG = logging.getLogger(__name__)


def registered():
    path = ROOT / "registration.json"
    if not path.exists():
        return None
    identifier = read_json(path)["id"]
    if len(identifier) != 64 or any(char not in "0123456789abcdef" for char in identifier):
        raise ValueError("Invalid mark-reference registration ID")
    record = sealed(ROOT / "registrations" / f"{identifier}.json", identifier)
    if (record["body"]["execution_protocol"] != realtime.VERSION or
            record["body"]["mark_source_policy"] != realtime.SOURCE_POLICY):
        raise ValueError("Unexpected mark-reference execution version")
    return record


def register(*, clock=utc_now):
    with writer_lock(ROOT / ".writer.lock"):
        if legacy_entries_closed(root=ROOT.parent / "paper-run"):
            raise ValueError("The seven-slot paper generation owns current strategy entries")
        if existing := registered():
            return existing
        frozen = catalog.registered()
        source = protocol.active()
        if not frozen or not source or source["id"] != frozen["body"]["source_protocol"]:
            raise ValueError("The existing Phase 15 registration and its reviewed futures source are required")
        original = frozen["body"]
        futures_source(original)
        now = clock()
        start = now.floor(catalog.QUARTER) + catalog.QUARTER
        if start < utc(original["start"]):
            raise ValueError("The new books must open within the existing Phase 15 source window")
        lanes = {}
        for key in LANES:
            lane = original["lanes"][key]
            if (lane["mode"]["trade_type"], lane["mode"]["symbol"]) != ("FUTURES", "XBTUSDTM"):
                raise ValueError("The mark-reference protocol supports KuCoin XBTUSDTM futures only")
            lanes[key] = {**lane, "execution_protocol": realtime.VERSION,
                          "book_id": fingerprint([realtime.VERSION, frozen["id"], key, start.isoformat()])}
        latest_follow = min(utc(original["follow_end_exclusive"]), utc(source["body"]["end_exclusive"]))
        follow_span = pd.Timedelta(minutes=max(lane["profile_rules"]["max_hold_minutes"] for lane in lanes.values()))
        follow_span += pd.Timedelta(days=1)
        issue_end = min(start + pd.Timedelta(days=30), latest_follow - follow_span)
        if issue_end <= start:
            raise ValueError("Existing approved source coverage is too short for the new entry and follow-through windows")
        body = {key: original[key] for key in ("source_protocol", "phase14_amendment", "config", "holdout_end_exclusive",
                                                "max_live_delay_seconds", "max_batch_minutes", "gap_grace_seconds",
                                                "funding_grace_seconds")}
        body.update(execution_protocol=realtime.VERSION, execution_mode=realtime.MODE, frozen_registration=frozen["id"],
                    registered_at=now.isoformat(), start=start.isoformat(), publication_end_exclusive=issue_end.isoformat(),
                    follow_end_exclusive=(issue_end+follow_span).isoformat(), lanes=lanes,
                    mark_source_policy=realtime.SOURCE_POLICY, approval="No validated model")
        record = {"id": fingerprint(body), "body": body}
        write_json(ROOT / "registrations" / f"{record['id']}.json", record)
        write_json(ROOT / "registration.json", {"id": record["id"]})
        return record


def entries_enabled(record):
    path = ROOT / "entries.json"
    if not path.exists():
        return False
    control = read_json(path)
    return control["registration"] == record["id"] and control["enabled"] is True


def set_entries(enabled, *, clock=utc_now):
    if type(enabled) is not bool:
        raise ValueError("Entry control requires a boolean")
    with writer_lock(ROOT / ".writer.lock"):
        if enabled and legacy_entries_closed(root=ROOT.parent / "paper-run"):
            raise ValueError("The seven-slot paper generation owns current strategy entries")
        record = registered()
        if not record:
            raise ValueError("Register the independent mark-reference books first")
        if enabled and clock() >= utc(record["body"]["publication_end_exclusive"]):
            raise ValueError("The sealed mark-reference entry window has ended")
        control = {"enabled": enabled, "registration": record["id"], "changed_at": clock().isoformat()}
        write_json(ROOT / "entries.json", control)
        return control


def connect(*, readonly=False):
    path = ROOT / "paper.sqlite3"
    if readonly:
        return sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True)
    db = sqlite3.connect(path)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS book (id TEXT PRIMARY KEY, state TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS event (key TEXT PRIMARY KEY, book TEXT NOT NULL, kind TEXT NOT NULL,
        idea TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL, body TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS book_events ON event(book,kind,recorded_at);
    """)
    realtime.init_observations(db)
    db.commit()
    return db


def protect_entry_minute(db, book, body, at, now, metadata):
    """Keep the entry minute auditable without treating pre-entry extremes as exits."""
    record = book["position"]
    plan = record["plan"]
    if metadata.get("as_of") and timedelta_age(metadata["as_of"], now, body["config"]["ideas"]["maximum_metadata_age_hours"]):
        if asdict(current_rules("FUTURES", metadata, body["config"])) != plan["contract"]:
            paper.problem(db, book, "Observed public instrument rules changed while exposure was possible", at, now, permanent=True)
            return False
    bars, funding, flags, scheduled = futures_window(body, at, at+catalog.STEP, now, at)
    if not scheduled:
        paper.problem(db, book, "Dated public funding schedule is unavailable", at, now)
        return False
    if at in flags:
        reason, permanent = flags[at]
        paper.problem(db, book, reason, at, now, permanent=permanent)
        return False
    trade, mark = (bars[basis].get(at) for basis in ("trade", "mark"))
    if not all(paper.complete_bar(bar, at) and utc(bar["received_at"]) <= now for bar in (trade, mark)):
        paper.problem(db, book, "Waiting for complete paired entry-minute evidence", at, now)
        return False
    settled = funding.get(at, ())
    if any(not at <= utc(row["settlement_at"]) < at+catalog.STEP or
           not utc(row["settlement_at"]) <= utc(row["received_at"]) <= now or not isfinite(row["rate"])
           for row in settled):
        paper.problem(db, book, "Funding timestamp, availability, or rate is invalid", at, now, permanent=True)
        return False
    if book["problem"]:
        paper.event(db, book, "EVIDENCE_RECOVERED", record["id"], at, now, {"previous": book["problem"]})
        book["problem"] = None
    pos = Position(**record["position"])
    contract = Contract.load(plan["contract"])
    fee = plan["cost"]["taker"]
    side = pos.direction
    liq_before = liquidation_price(pos.entry, pos.reserved/pos.remaining, side, contract, fee)
    for row in settled:
        if utc(row["settlement_at"]) > utc(record["entry_at"]):
            paper.apply_funding(db, book, record, pos, {**row, "valuation_basis": "ENTRY_MARK_REFERENCE"}, pos.entry, now)
    liq_after = liquidation_price(pos.entry, pos.reserved/pos.remaining, side, contract, fee)
    conservative_liq = max(liq_before, liq_after) if side > 0 else min(liq_before, liq_after)
    hits = triggers(tuple(mark[key] for key in paper.PRICE), side, record["stop"], record["target"], conservative_liq, False)
    risks = [kind for kind, _ in hits]
    if pos.remaining*mark["high"] > contract.tier_notional:
        risks.append("maintenance_tier")
    if utc(record["deadline"]) < at+catalog.STEP:
        risks.append("deadline")
    received = max(utc(bar["received_at"]) for bar in (trade, mark)).isoformat()
    assessment = {"idea_id": record["id"], "entry_at": record["entry_at"], "possible_events": risks, "source_received_at": received,
                  "entry_interval_end": (at+catalog.STEP).isoformat(), "trade_sha256": trade["raw_sha256"],
                  "mark_sha256": mark["raw_sha256"]}
    if risks:
        book["entry_interval_unresolved"] = {**assessment, "reason": "Intraminute evidence cannot date crossings relative to the entry"}
    paper.event(db, book, "ENTRY_INTERVAL_UNCERTAIN" if risks else "ENTRY_INTERVAL_CLEAR", record["id"], at+catalog.STEP,
                now, assessment)
    record.update(position=asdict(pos), last_mark=mark["close"], mark_source_at=mark["close_at"], mark_received_at=mark["received_at"])
    paper.mark_account(book, pos, mark["close"])
    book.update(cursor=(at+catalog.STEP).isoformat(), last_source_close=(at+catalog.STEP).isoformat(), last_source_received_at=received)
    return True


def protect(db, book, lane, body, now, metadata):
    if not book["position"] or book["permanent_block"]:
        return
    at = utc(book["cursor"])
    follow_end = utc(body["follow_end_exclusive"])
    end = min((now-pd.Timedelta(seconds=45)).floor(catalog.QUARTER), follow_end)
    if at >= end:
        if end == follow_end:
            paper.problem(db, book, "The fixed follow-through horizon ended with exposure unresolved", end, now, permanent=True)
        return
    working = deepcopy(book)
    db.execute("SAVEPOINT protect_book")
    try:
        partial = at < end and at == utc(working["position"]["entry_at"]).floor(catalog.STEP)
        if not partial or protect_entry_minute(db, working, body, at, now, metadata):
            worker.advance(db, working, lane, body, end, now, metadata)
    except SOURCE_ERRORS as exc:
        db.execute("ROLLBACK TO protect_book")
        paper.problem(db, book, f"Native protection evidence unavailable ({type(exc).__name__})", at, now)
    else:
        book.update(working)
    finally:
        db.execute("RELEASE protect_book")


def allowed_profiles(record, now):
    body = record["body"]
    if (legacy_entries_closed(root=ROOT.parent / "paper-run") or not entries_enabled(record)
            or not utc(body["start"]) <= now < utc(body["publication_end_exclusive"])):
        return set()
    flags, monitor = catalog.controls(), worker.monitoring_controls()
    if (monitor.get("paused") or not (flags["analysis"] and flags["publication"] and flags["paper"]
                                      and monitor["monitor"] and protocol.control()["enabled"])):
        return set()
    source = protocol.active()
    if not source or source["id"] != body["source_protocol"]:
        return set()
    futures_source(body)
    return {lane["profile"] for lane in body["lanes"].values() if monitor["ideas"][lane["profile"]]}


def confirmations(db, record, books, permitted, clock):
    body = record["body"]
    now = clock()
    grace = pd.Timedelta(seconds=body["config"]["ideas"]["completed_candle_grace_seconds"])
    maximum_delay = pd.Timedelta(seconds=body["max_live_delay_seconds"])
    selected = {}
    for key, lane in body["lanes"].items():
        book = books[key]
        if (lane["profile"] not in permitted or book["pending"] or book["position"] or book["problem"]
                or book.get("entry_interval_unresolved")):
            continue
        cutoff = (now-grace).floor(lane["profile_rules"]["decision"])
        if utc(body["start"]) <= cutoff <= now <= cutoff+maximum_delay:
            idea_id = fingerprint([book["book_id"], cutoff.isoformat()])
            if not db.execute("SELECT 1 FROM event WHERE book=? AND idea=? AND kind='PUBLISHED'", (book["book_id"], idea_id)).fetchone():
                selected[key] = lane
    if not selected:
        return {}
    analyses = worker.opportunity_rows({**record, "body": {**body, "lanes": selected}}, books, now)
    confirmed = {}
    policy = body["config"]["ideas"]
    permitted = allowed_profiles(record, clock())
    for key, analysis in analyses.items():
        lane = selected[key]
        plan = analysis.get("plan")
        if lane["profile"] not in permitted or not plan:
            continue
        at = clock()
        if not (utc(analysis["source_cutoff"]) <= at < utc(plan["eligible_from"]) and
                at <= utc(analysis["source_cutoff"]) + maximum_delay):
            continue
        if not all(fresh(plan[name], at, policy["maximum_quote_age_seconds"], policy["maximum_quote_clock_skew_seconds"])
                   for name in ("quote_receipt", "mark_receipt")):
            continue
        if idea := realtime.confirm(db, books[key], lane, analysis, at):
            confirmed[key] = idea["id"]
            paper.save(db, books[key])
    return confirmed


def cycle(*, clock=utc_now, transport=None):
    record = registered()
    if not record:
        return {"state": "UNREGISTERED", "mode": realtime.MODE}
    body = record["body"]
    lanes = body["lanes"]
    with writer_lock(ROOT / ".writer.lock"), closing(connect()) as db:
        now = clock()
        try:
            metadata = protocol.current_metadata()
        except SOURCE_ERRORS:
            metadata = {}
        books = {key: paper.load(db, lane) for key, lane in lanes.items()}
        db.execute("BEGIN IMMEDIATE")
        for key, book in books.items():
            if book["position"]:
                protect(db, book, lanes[key], body, now, metadata)
                paper.save(db, book)
            elif book["pending"]:
                realtime.expire_pending(db, book, now)
                paper.save(db, book)
        db.commit()

        diagnostics = {}
        try:
            permitted = allowed_profiles(record, clock())
        except SOURCE_ERRORS as exc:
            permitted = set()
            diagnostics["permissions"] = type(exc).__name__
        waiting = []
        for key, book in books.items():
            if book["pending"] and lanes[key]["profile"] not in permitted:
                realtime.expire_pending(db, book, clock(), reason="MARK_REFERENCE_ENTRIES_DISABLED_OR_PERMISSION_CLOSED")
                paper.save(db, book)
            elif realtime.eligible(book, clock()) and realtime.request_capacity(db, book["pending"]["id"]):
                waiting.append(key)
        db.commit()

        outcomes = {}
        if waiting:
            try:
                idea_ids = [books[key]["pending"]["id"] for key in waiting]
                shared_retry = monitoring.optional(monitoring.RUNTIME, {}).get("retry_at")
                if shared_retry and clock() < utc(shared_retry):
                    observation = {"status": "SHARED_SOURCE_BACKOFF"}
                else:
                    observation = realtime.observe(db, ROOT, idea_ids=idea_ids, clock=clock, transport=transport)
            except SOURCE_ERRORS as exc:
                db.rollback()
                diagnostics["mark_source"] = type(exc).__name__
                observation = {"status": "ENTRY_SOURCE_IO_ERROR"}
            for key in waiting:
                book = books[key]
                result = realtime.consider(db, book, lanes[key], metadata, body["config"], observation, clock=clock)
                paper.save(db, book)
                db.commit()
                if result:
                    outcomes[key] = result

        try:
            confirmed = confirmations(db, record, books, permitted, clock) if permitted else {}
            db.commit()
        except SOURCE_ERRORS as exc:
            db.rollback()
            diagnostics["confirmations"] = type(exc).__name__
            confirmed = {}
        return {"state": "RUNNING", "mode": realtime.MODE, "registration": record["id"],
                "entries_enabled": bool(permitted), "permitted_profiles": sorted(permitted),
                "confirmed": confirmed, "entry_outcomes": outcomes, "diagnostics": diagnostics}


def book_status(db, lane):
    book = paper.book_report(db, lane)
    book["entry_evidence_reasons"] = dict(db.execute(
        "SELECT json_extract(body,'$.reason'),count(*) FROM event WHERE book=? AND kind IN ('NO_FILL','ENTRY_EVIDENCE_REJECTED') "
        "GROUP BY 1", (lane["book_id"],)))
    if uncertain := book.get("entry_interval_unresolved"):
        book.update(accounting_complete=False, book_status="UNRESOLVED_ENTRY_INTERVAL", closed_net=None,
                    expectancy=None, expectancy_r=None, top_three_share_of_positive_gains=None)
        for closed in book["recent_closed"]:
            if closed["idea_id"] == uncertain["idea_id"]:
                closed["provisional"] = True
    return book


def status():
    record = registered()
    if not record:
        return {"state": "UNREGISTERED", "mode": realtime.MODE, "entries_enabled": False, "books": {}}
    lanes = record["body"]["lanes"]
    if (ROOT / "paper.sqlite3").exists():
        with closing(connect(readonly=True)) as db:
            db.execute("BEGIN")
            books = {key: book_status(db, lane) for key, lane in lanes.items()}
    else:
        books = {key: paper.initial(lane) for key, lane in lanes.items()}
    for book in books.values():
        book["execution_mode"] = realtime.MODE
    return {"state": "REGISTERED", "mode": realtime.MODE, "entries_enabled": entries_enabled(record),
            "registration": record, "books": books}


def run():
    if not registered():
        raise ValueError("Register the independent mark-reference books first")
    with writer_lock(ROOT / ".runner.lock"):
        previous_diagnostics = {}
        while True:
            started = time.monotonic()
            try:
                result = cycle()
                diagnostics = result.get("diagnostics", {})
                if diagnostics and diagnostics != previous_diagnostics:
                    LOG.warning("Mark-reference cycle diagnostics %s", diagnostics)
                previous_diagnostics = diagnostics
            except RuntimeError as exc:
                if not str(exc).startswith("Another writer owns "):
                    raise
            time.sleep(max(0, POLL_SECONDS-(time.monotonic()-started)))


def main():
    parser = argparse.ArgumentParser(prog="phase15-realtime-mark")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("register", "status", "report", "cycle", "run"):
        commands.add_parser(name)
    control = commands.add_parser("entries")
    control.add_argument("state", choices=("on", "off"))
    args = parser.parse_args()
    if args.command == "run":
        try:
            run()
        except KeyboardInterrupt:
            return
    else:
        action = {"register": register, "status": status, "report": status, "cycle": cycle,
                  "entries": lambda: set_entries(args.state == "on")}[args.command]
        print(json.dumps(action(), indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
