"""Durable operational evidence for prospective paper decisions."""

from contextlib import closing
import json
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.health.state import enqueue, get_meta, set_meta
from lab.storage import read_json, utc


SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_evaluation (
    book TEXT NOT NULL, boundary TEXT NOT NULL, outcome TEXT NOT NULL,
    checked_at TEXT NOT NULL, observed_at TEXT, receipt_at TEXT, receipt_id TEXT,
    PRIMARY KEY(book, boundary)
);
"""
PREFIX = "paper-evaluation:"


def _stamp(value):
    return pd.Timestamp(value, unit="ns", tz="UTC")


def _ema_receipts(source, stream, step, first, end, now, grace):
    rows = source.execute(
        "SELECT o.opened,MIN(r.available),MIN(r.id) FROM observation o JOIN receipt r ON r.id=o.receipt_id "
        "WHERE o.stream=? AND o.minutes=? AND o.opened>=? AND o.opened<? AND r.available<=? "
        "GROUP BY o.opened HAVING COUNT(*)=1 AND MIN(r.available)<=o.opened+?",
        (stream, int(step.total_seconds() // 60), (first - step).value, end.value, now.value, (step + grace).value),
    )
    return {_stamp(opened) + step: (_stamp(available), ident) for opened, available, ident in rows}


def _store(db, book, boundary, outcome, now, observed, receipt):
    available, ident = receipt if receipt else (None, None)
    db.execute("INSERT INTO paper_evaluation VALUES(?,?,?,?,?,?,?) ON CONFLICT(book,boundary) DO UPDATE SET "
               "outcome=excluded.outcome,checked_at=excluded.checked_at,observed_at=excluded.observed_at,"
               "receipt_at=excluded.receipt_at,receipt_id=excluded.receipt_id "
               "WHERE paper_evaluation.observed_at IS NULL AND excluded.observed_at IS NOT NULL",
               (book, boundary.isoformat(), outcome, now.isoformat(),
                observed.isoformat() if observed is not None else None,
                available.isoformat() if available is not None else None, ident))


def _report(db, book, latest, step):
    rows = db.execute(
        "SELECT boundary,outcome,receipt_at,receipt_id FROM paper_evaluation WHERE book=? "
        "ORDER BY boundary DESC LIMIT 2", (book,),
    ).fetchall()
    overdue = []
    for boundary, outcome, received, receipt in rows:
        if outcome != "MISSED_QUALIFIED_SOURCE":
            break
        if overdue and utc(overdue[-1]["boundary"]) - utc(boundary) != step:
            break
        overdue.append({"boundary": boundary, "receipt_at": received, "receipt_id": receipt})
    counts = dict(db.execute(
        "SELECT CASE WHEN observed_at IS NOT NULL THEN 'COMPLETED' ELSE outcome END,COUNT(*) "
        "FROM paper_evaluation WHERE book=? GROUP BY 1", (book,),
    ))
    if latest is None:
        latest = db.execute("SELECT MAX(boundary) FROM paper_evaluation WHERE book=? AND observed_at IS NOT NULL",
                            (book,)).fetchone()[0]
    return {"state": "STALE" if len(overdue) == 2 else "TRACKING",
            "last_completed": latest, "overdue": list(reversed(overdue)), "counts": counts}


def _completed_ema(saved):
    boundary = saved.get("last_evaluated_at")
    signal = saved.get("last_signal") or {}
    if boundary and signal.get("observed_at"):
        return utc(boundary), utc(signal["observed_at"]), signal["decision"]
    return None


def _record_ema(db, source, lane, saved, now, baseline=None):
    name = "ema/" + lane["profile"]
    fixed = saved.get("first_eligible_at")
    if not fixed or not saved.get("source_id"):
        return {"state": "WAITING_FOR_DATA", "overdue": []}
    first = utc(fixed)
    step = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
    receipt_grace = pd.Timedelta(seconds=lane["source"]["decision_receipt_seconds"])
    window = pd.Timedelta(seconds=lane["source"]["publication_max_seconds"])
    completed = {row[0]: row[1:] for row in (baseline, _completed_ema(saved)) if row is not None}
    actual = max(completed, default=None)
    end = min((now - window).floor(step), utc(saved["primary_end_exclusive"]) - step)
    prior = db.execute("SELECT MAX(boundary) FROM paper_evaluation WHERE book=?", (name,)).fetchone()[0]
    if prior is None:
        # Earlier readiness rows were overwritten by the old dispatcher. Begin
        # at its last surviving actual decision instead of guessing its history.
        starting_at = min(completed, default=None)
        first = max(first, starting_at) if starting_at is not None else first
    else:
        first = max(first, utc(prior))
    right = max(end, actual) if actual is not None else end
    evidence = _ema_receipts(source, saved["source_id"], step, first, right, now, receipt_grace) if right >= first else {}
    for boundary in pd.date_range(first, end, freq=step):
        if boundary in completed:
            continue
        receipt = evidence.get(boundary)
        _store(db, name, boundary, "MISSED_QUALIFIED_SOURCE" if receipt else "SOURCE_UNQUALIFIED", now, None, receipt)
    for boundary, (observed, decision) in completed.items():
        _store(db, name, boundary, decision, now, observed, evidence.get(boundary))
    return _report(db, name, actual.isoformat() if actual is not None else None, step)


def _record_breakout(db, root, record, managed, now):
    from lab.phase10.worker import decision_key

    report = {}
    body = record["body"]
    policy = body["config"]["ideas"]
    window = pd.Timedelta(seconds=body["max_live_delay_seconds"])
    for name, lane in body["lanes"].items():
        book = "breakout/" + name
        step = pd.Timedelta(lane["profile_rules"]["decision"])
        profile = lane["profile"]
        analysis = managed.get("opportunities", {}).get(name, {})
        last = analysis.get("source_cutoff")
        observed = utc(analysis.get("as_of", managed["at"])) if last else None
        evaluated = utc(last) if (last and analysis.get("gates", {}).get("feature readiness") == "PASSED"
                                  and analysis.get("latest_candle_receipt") and utc(last) <= observed) else None
        actual = evaluated if evaluated is not None and observed <= evaluated + window else None
        prior = db.execute("SELECT MAX(boundary) FROM paper_evaluation WHERE book=?", (book,)).fetchone()[0]
        # Each boundary is independently recorded by the source worker. A
        # catch-up-only record does not prove an eligible Phase 15 input.
        last_due = min((now - window).floor(step), utc(body["publication_end_exclusive"]).ceil(step) - step)
        first = max(utc(body["start"]).ceil(step), last_due - 2 * step,
                    utc(prior) if prior else last_due - 2 * step)
        if last and not prior:
            first = max(first, utc(last) + step)
        for boundary in pd.date_range(first, last_due, freq=step):
            if boundary == actual:
                continue
            ident = decision_key(profile, boundary, policy["version"])
            path = root / "phase10" / "decisions" / profile / (ident + ".json")
            candidate = read_json(path) if path.is_file() else {}
            idea = candidate.get("idea") or {}
            received = candidate.get("source_receipt")
            qualified = (received and idea.get("source_last_close")
                         and utc(idea["source_last_close"]) == boundary
                         and candidate.get("status") not in ("CATCH_UP_ONLY", "DATA_NOT_READY", "IDEAS_OFF")
                         and boundary <= utc(received) <= boundary + window)
            receipt = (utc(received), ident) if qualified else None
            _store(db, book, boundary, "MISSED_QUALIFIED_SOURCE" if receipt else "SOURCE_UNQUALIFIED", now, None, receipt)
        if actual is not None:
            receipt = utc(analysis["latest_candle_receipt"]), None
            _store(db, book, actual, analysis["decision"], now, observed, receipt)
        report[book] = _report(db, book, actual.isoformat() if actual is not None else None, step)
        if evaluated is not None:
            report[book]["last_managed_evaluation"] = {
                "boundary": evaluated.isoformat(), "evaluated_at": observed.isoformat(),
                "within_publication_window": actual is not None, "decision": analysis["decision"],
            }
    return report


def _alerts(db, summaries, now, private_route):
    for name, status in summaries.items():
        if status["state"] not in {"TRACKING", "STALE"}:
            continue
        route_id = private_route if name.startswith("ema/") else None
        if name.startswith("ema/") and not route_id:
            continue
        meta = PREFIX + name
        prior = json.loads(get_meta(db, meta) or "{}")
        if prior.get("route") != route_id:
            prior = {}
        boundaries = status["overdue"]
        if len(boundaries) == 2 and not prior:
            episode = fingerprint([name, boundaries[0]["boundary"], route_id])
            set_meta(db, meta, json.dumps({"id": episode, "since": boundaries[-1]["boundary"], "route": route_id}))
            outcome = "STALE"
        elif (prior and status["state"] == "TRACKING" and status["last_completed"]
              and utc(status["last_completed"]) > utc(prior["since"])):
            episode = prior["id"]
            set_meta(db, meta, "")
            outcome = "RECOVERED"
        else:
            continue
        prefix = "ema-private:" if route_id else ""
        family, _, lane = name.partition("/")
        label = ("EMA" if family == "ema" else "Breakout") + " " + lane.replace("/", " ").title()
        enqueue(db, prefix + PREFIX + episode + ":" + outcome, "health",
                f"ASEKE BTC LAB — PAPER OPERATIONS\n{label} evaluation · {outcome}", now.timestamp(),
                event_json=json.dumps({"event": outcome, "book": name, "overdue": boundaries,
                                       "last_completed": status["last_completed"], "private_route_id": route_id}))


def _ema_books(root):
    from lab.ema_paper import catalog, worker

    if not (root / "ema_paper" / "paper.sqlite3").is_file():
        return
    with closing(catalog.db_open(readonly=True, root=root / "ema_paper")) as ledger:
        ledger.execute("BEGIN")
        for original in catalog.registrations(ledger).values():
            lane = original["body"]
            key = lane["book_id"]
            yield lane, {**worker._record(ledger, "readiness", key), **worker._record(ledger, "eligibility", key)}


def baseline(root):
    path = root / "ema_paper" / "paper.sqlite3"
    if not path.is_file():
        return {}
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        rows = db.execute("SELECT registration.profile,readiness.body FROM readiness JOIN registration "
                          "ON readiness.book=json_extract(registration.body,'$.book_id')")
        return {profile: completed for profile, body in rows
                if (completed := _completed_ema(json.loads(body))) is not None}


def record(db, root, now, *, private_route=None, baseline=None, group_breakout=False):
    """Read committed component evidence, then write only to the watchdog DB."""
    from lab.kraken_paper.source import _read_connection
    from lab.phase15.catalog import registered

    db.execute(SCHEMA)
    summaries = {}
    source = _read_connection(root / "ema_kraken_reference" / "source")
    if source is not None:
        with closing(source):
            for lane, saved in _ema_books(root):
                profile = lane["profile"]
                summaries["ema/" + profile] = _record_ema(db, source, lane, saved, now, (baseline or {}).get(profile))
    managed_path = root / "phase15" / "last-managed.json"
    if managed_path.is_file() and (registration := registered()):
        managed = read_json(managed_path)
        if managed.get("registration") == registration["id"]:
            summaries.update(_record_breakout(db, root, registration, managed, now))
    alerts = {name: status for name, status in summaries.items()
              if not group_breakout or not name.startswith("breakout/")}
    _alerts(db, alerts, now, private_route)
    return summaries
