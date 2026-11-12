"""Read prospective Kraken journal entries into the existing Telegram outbox."""

from contextlib import closing
import json
from pathlib import Path

from lab.config import fingerprint
from lab.health.paper_events import clock, identity, price, receipt
from lab.health.state import enqueue, get_meta, set_meta
from lab.phase15.paper import load
from lab.storage import utc, writer_lock


CATEGORIES = {
    "PUBLISHED": "ideas", "HYPOTHETICAL_FILL": "paper_entry",
    "EXPIRED": "paper_no_entry", "INVALIDATED": "paper_no_entry", "CLOSED": "paper_exit",
}
SOURCE_STATES = {"WAITING_FOR_DATA", "BLOCKED_SOURCE", "BLOCKED_PERMISSION", "BLOCKED_EXECUTION_EVIDENCE"}
MAX_EVENTS_PER_BOOK = 128


def _heading(lane):
    return f"EMA Pullback {lane['profile'].title()} · Kraken Spot BTC/USD · USD"


def _plan_line(plan, entry):
    return f"Entry {entry} · Stop {price(plan.get('stop'))} · Target {price(plan.get('target'))} USD"


def _usd(value, *, signed=False):
    if value is None:
        return "unavailable"
    return f"{value:+,.4f} USD" if signed else f"{value:,.4f} USD"


def _handoff(body):
    times = [body.get(key, {}).get("observed_at") for key in ("trade", "trigger_bar")]
    return body.get("source_handoff_at") or max((at for at in times if at), key=utc, default=None)


def _render(lane, idea, event, body):
    _, book, identifier, source, recorded, kind, _, _ = event
    plan = idea["plan"]
    evidence = plan.get("evidence", {})
    if kind == "PUBLISHED":
        title = "PAPER IDEA PUBLISHED"
        entry = plan["entry"]
        prices = _plan_line(plan, f"{price(entry.get('minimum'))}–{price(entry.get('maximum'))}")
        details = (f"Signal decision {clock(idea['boundary'])} · Publication {clock(idea['published_at'])}\n"
                   f"Eligible open {clock(plan['eligible_from'])} · Expires {clock(plan['entry_expiry'])}")
        provider_at = evidence.get("source_received_at") or evidence.get("latest_provider_received_at")
        handoff_at = evidence.get("source_handoff_at") or evidence.get("latest_handoff_at")
        timing_label = "Publication time"
    elif kind == "HYPOTHETICAL_FILL":
        title = "HYPOTHETICAL PAPER ENTRY CONFIRMED"
        prices = _plan_line(plan, price(body.get("price")))
        details = f"Size {price(body.get('quantity'))} BTC · Entry fee {_usd(body.get('fee'))}"
        provider_at, handoff_at = receipt(body), _handoff(body)
        timing_label = "Hypothetical entry time"
    elif kind == "CLOSED":
        title = "HYPOTHETICAL PAPER TRADE CLOSED"
        prices = _plan_line(plan, price(body.get("entry_price")))
        details = (
            f"Exit {price(body.get('exit_price'))} USD · {body.get('exit_kind') or body.get('reason', 'unknown')}\n"
            f"Net paper P&L {_usd(body.get('net'), signed=True)} · Fees {_usd(body.get('fees'))}\n"
            f"Modeled adverse execution {_usd(body.get('adverse_cost_in_gross'))} · "
            f"Equity {_usd(body.get('equity_after'))}"
        )
        provider_at, handoff_at = receipt(body), _handoff(body)
        timing_label = "Hypothetical exit time"
    else:
        title = "PAPER IDEA EXPIRED" if kind == "EXPIRED" else "PAPER IDEA INVALIDATED"
        prices = _plan_line(plan, price(plan["entry"].get("reference")))
        details = f"Reason {body.get('reason', kind)}"
        provider_at, handoff_at = receipt(body), _handoff(body)
        timing_label = "Expiry time" if kind == "EXPIRED" else "Invalidation time"
    text = (
        f"ASEKE BTC LAB — {title}\n{_heading(lane)} · {idea['direction']}\n{prices}\n{details}\n"
        f"{timing_label} {clock(source)}\nProvider receipt {clock(provider_at)} · "
        f"Local handoff {clock(handoff_at)}\nLedger recorded {clock(recorded)}\n"
        f"{identity(book, identifier)}\nPaper only · MANUAL_OUTSIDE_LAB · No validated model"
    )
    return text, utc(provider_at).isoformat() if provider_at else None, utc(handoff_at).isoformat() if handoff_at else None


def _body(db, table, book):
    row = db.execute(f"SELECT body FROM {table} WHERE book=?", (book,)).fetchone()
    return json.loads(row[0]) if row else {}


def _eligible_event(paper, event, lane, start, end, now):
    _, book, identifier, at, recorded, kind, raw, _ = event
    at, recorded = utc(at), utc(recorded)
    if not start <= at <= recorded <= now:
        return None
    if kind == "PUBLISHED":
        published_at, idea = at, json.loads(raw)
    else:
        parent = paper.execute(
            "SELECT source_at,recorded_at,body FROM event WHERE book=? AND idea=? AND kind='PUBLISHED'",
            (book, identifier),
        ).fetchone()
        if parent is None or not start <= utc(parent[0]) <= utc(parent[1]) <= recorded:
            return None
        published_at, idea = utc(parent[0]), json.loads(parent[2])
    if (idea.get("id") != identifier or idea.get("direction") != "LONG"
            or not start <= utc(idea["boundary"]) < end
            or not utc(idea["boundary"]) <= published_at == utc(idea["published_at"]) <= at
            or not published_at < utc(idea["plan"]["eligible_from"]) < end
            or not utc(lane["registered_at"]) < start):
        return None
    if kind == "HYPOTHETICAL_FILL" and utc(idea["plan"]["eligible_from"]) != at:
        return None
    if kind == "CLOSED" and paper.execute(
        "SELECT 1 FROM event WHERE book=? AND idea=? AND kind='HYPOTHETICAL_FILL' AND source_at<=? "
        "AND recorded_at<=?", (book, identifier, at.isoformat(), recorded.isoformat()),
    ).fetchone() is None:
        return None
    return idea


def _queue_lifecycle(paper, health, registration, eligibility, now):
    lane = registration["body"]
    book = lane["book_id"]
    meta = "kraken-paper-lifecycle:" + registration["id"]
    kinds = tuple(CATEGORIES)
    latest = paper.execute("SELECT coalesce(max(rowid),0) FROM event WHERE book=? AND kind IN (?,?,?,?,?)",
                           (book, *kinds)).fetchone()[0]
    cursor = int(get_meta(health, meta, lane.get("notification_baseline_rowid", 0)))
    if cursor > latest:
        raise ValueError("Kraken notification cursor exceeds the committed paper journal")
    start, end = (utc(eligibility[key]) for key in ("first_eligible_at", "primary_end_exclusive"))
    if now < start:
        return {"cursor_rowid": cursor, "latest_rowid": latest}
    events = paper.execute(
        "SELECT rowid,book,idea,source_at,recorded_at,kind,body,key FROM event WHERE book=? AND rowid>? "
        "AND kind IN (?,?,?,?,?) ORDER BY rowid LIMIT ?",
        (book, cursor, *kinds, MAX_EVENTS_PER_BOOK),
    )
    for event in events:
        rowid, _, identifier, source, recorded, kind, raw, key = event
        if utc(recorded) > now:
            break
        idea = _eligible_event(paper, event, lane, start, end, now)
        if idea is not None:
            category = CATEGORIES[kind]
            text, received, handoff = _render(lane, idea, event, json.loads(raw))
            if kind == "HYPOTHETICAL_FILL":
                health.execute("UPDATE outbox SET status='SUPERSEDED' WHERE idea_id=? AND id LIKE 'kraken-paper:%' "
                               "AND category='paper_no_entry' AND status IN ('PENDING','RETRY')", (identifier,))
            context = {"venue": "Kraken Spot", "currency": "USD", "registration_id": registration["id"],
                       "book_id": book, "event": kind, "event_key": key, "source_at": source, "recorded_at": recorded,
                       "source_received_at": received, "source_handoff_at": handoff,
                       "decision_at": idea["boundary"], "ledger_rowid": rowid}
            enqueue(health, "kraken-paper:" + fingerprint([book, identifier, key]), category, text, now.timestamp(),
                    idea_id=identifier, transition=fingerprint(["kraken-paper", book, identifier, category]),
                    event_json=json.dumps(context))
        cursor = rowid
    set_meta(health, meta, cursor)
    return {"cursor_rowid": cursor, "latest_rowid": latest}


def _queue_source(health, paper, registration, eligibility, now, policy):
    lane = registration["body"]
    book = lane["book_id"]
    ready = _body(paper, "readiness", book)
    checked = ready.get("last_checked_at")
    start = utc(eligibility["first_eligible_at"])
    if not checked or not utc(lane["registered_at"]) < start <= utc(checked) <= now:
        return
    key = "kraken-paper-source:" + registration["id"]
    saved = json.loads(get_meta(health, key, "{}"))
    if saved.get("at") and utc(saved["at"]) >= utc(checked):
        return
    state = ready.get("state")
    if state in SOURCE_STATES:
        if "episode" not in saved:
            saved["episode"] = fingerprint([book, checked])
        saved.update(failures=saved.get("failures", 0) + 1, healthy=0)
        transition = "warning" if not saved.get("warned") and saved["failures"] >= policy["warning_checks"] else None
    elif state in {"ACTIVE", "FOLLOW_THROUGH"} and saved.get("episode"):
        saved.update(failures=0, healthy=saved.get("healthy", 0) + 1)
        transition = "recovery" if saved.get("warned") and saved["healthy"] >= policy["recovery_checks"] else None
        if transition is None and not saved.get("warned"):
            saved = {}
    else:
        transition = None
    if transition:
        prefix = "kraken-paper-source:" + saved["episode"]
        if transition == "recovery":
            health.execute("UPDATE outbox SET status='SUPERSEDED' WHERE id=? AND status IN ('PENDING','RETRY')",
                           (prefix + ":warning",))
        account = load(paper, lane)
        record = account.get("position") or account.get("pending") or {}
        plan = record.get("plan", {})
        opened = record.get("position", {})
        prices = _plan_line(plan, price(opened.get("entry", plan.get("entry", {}).get("reference"))))
        coverage = ready.get("coverage") or {}
        context = {"venue": "Kraken Spot", "currency": "USD", "registration_id": registration["id"],
                   "book_id": book, "event": "SOURCE_" + transition.upper(), "recorded_at": checked,
                   "source_at": coverage.get("latest_close_at"),
                   "source_received_at": coverage.get("latest_provider_received_at")}
        text = (
            f"ASEKE BTC LAB — KRAKEN DATA {transition.upper()}\n{_heading(lane)} · "
            f"{record.get('direction', 'No open or pending direction')}\n{prices}\n"
            f"{ready.get('reason') or state}\nLatest closed candle {clock(context['source_at'])} · "
            f"Provider receipt {clock(context['source_received_at'])} · Checked {clock(checked)}\n"
            f"Paper equity {_usd(account['equity'])} · Fees to date {_usd(account['fees'])}\n"
            f"Paper book {book}\nPaper only · MANUAL_OUTSIDE_LAB · No validated model"
        )
        enqueue(health, prefix + ":" + transition, "health", text, now.timestamp(), event_json=json.dumps(context))
        if transition == "warning":
            saved["warned"] = True
        else:
            saved = {}
    set_meta(health, key, json.dumps({**saved, "at": checked}))


def collect(health, now, policy, *, root=None, registrations=None, paper=None):
    """Queue eligible committed events. The existing watchdog handles all delivery."""
    from lab.kraken_paper import retirement

    if paper is None:
        from lab.kraken_paper import catalog

        root = catalog.ROOT if root is None else Path(root)
        if not (root / "paper.sqlite3").is_file():
            return {}
        with writer_lock(root / ".cycle.lock"), closing(catalog.db_open(readonly=True, root=root)) as reader:
            reader.execute("BEGIN")
            return collect(health, now, policy, root=root, registrations=catalog.registrations(reader), paper=reader)
    if retirement.audit(paper):
        return {}
    now = utc(now)
    result = {}
    health.execute("SAVEPOINT kraken_paper_lifecycle")
    try:
        for profile, registration in registrations.items():
            lane = registration["body"]
            fixed = _body(paper, "eligibility", lane["book_id"])
            if not fixed.get("first_eligible_at") or not lane.get("permissions", {}).get("telegram_lifecycle"):
                continue
            result[profile] = {"registration": registration["id"],
                               **_queue_lifecycle(paper, health, registration, fixed, now)}
            _queue_source(health, paper, registration, fixed, now, policy)
    except BaseException:
        health.execute("ROLLBACK TO kraken_paper_lifecycle")
        raise
    finally:
        health.execute("RELEASE kraken_paper_lifecycle")
    return result
