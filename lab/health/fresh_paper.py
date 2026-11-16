"""Generation-specific lifecycle summaries for committed fresh paper observations."""

from contextlib import closing
import json
from math import isfinite

from lab.config import fingerprint
from lab.ema_paper.private_notifications import route
from lab.health.state import enqueue, get_meta, set_meta
from lab.storage import utc


# The existing private sender verifies this prefix's saved route before sending.
PREFIX = "ema-private:fresh:"
EVENTS = {
    "PUBLISHED": ("ideas", "EXPERIMENTAL IDEA"),
    "HYPOTHETICAL_FILL": ("paper_entry", "PAPER ENTRY"),
    "NO_FILL": ("paper_no_entry", "NO PAPER ENTRY"),
    "EXPIRED": ("paper_no_entry", "IDEA EXPIRED"),
    "INVALIDATED": ("paper_no_entry", "IDEA INVALIDATED"),
    "CLOSED": ("paper_exit", "PAPER EXIT"),
}
LEGACY_PREFIXES = ("idea:", "paper:", "paper-update:", "ema-paper:", "ema-paper-update:", "ema-private:")
LEGACY_CATEGORIES = {category for category, _ in EVENTS.values()} | {"open_position"}
CURSOR_PREFIX = "fresh_paper_cursor_v1:"
MAX_EVENTS = 128


def _details(raw):
    try:
        result = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return result if isinstance(result, dict) else {}


def supersede_legacy(db, legacy_books, legacy_ideas, generation, now):
    """Supersede only the retired books' queued lifecycle rows under the watchdog lock."""
    books, ideas = set(legacy_books), set(legacy_ideas)
    rows = db.execute("SELECT id,category,idea_id,event_json FROM outbox WHERE status IN ('PENDING','RETRY') "
                      "ORDER BY rowid").fetchall()
    retired = []
    for identifier, category, idea, raw in rows:
        if category not in LEGACY_CATEGORIES or identifier.startswith(PREFIX) or not identifier.startswith(LEGACY_PREFIXES):
            continue
        book = _details(raw).get("book_id")
        if book in books or (book is None and (idea in ideas or (
                identifier.startswith("idea:") and identifier.removeprefix("idea:") in ideas))):
            retired.append(identifier)
    reason = f"Legacy paper generation administratively archived at {utc(now).isoformat()}; successor {generation}"
    db.executemany("UPDATE outbox SET status='SUPERSEDED', detail=? "
                   "WHERE id=? AND status IN ('PENDING','RETRY')", ((reason, identifier) for identifier in retired))
    return retired


def _text(slot, book, kind, occurred, recorded, body):
    label = EVENTS[kind][1]
    lines = [f"ASEKE BTC LAB — {label}", book["label"], str(book["price_basis"]),
             f"Paper event {utc(occurred).isoformat()}", f"Ledger recorded {utc(recorded).isoformat()}"]
    incomplete = bool(body.get("provisional") or body.get("accounting_complete") is False)
    if kind == "CLOSED" and incomplete:
        lines.append("Accounting remains unresolved.")
    if slot.startswith("ema/"):
        # The source review permits bounded private summaries. Raw Kraken
        # time/price observations and idea levels stay in the local evidence.
        if kind == "CLOSED" and not incomplete:
            for key, name in (("net", "Net realized P&L"), ("fees", "Modeled fees"),
                              ("funding", "Modeled financing"), ("equity_after", "Paper equity")):
                value = body.get(key)
                if type(value) in (int, float) and isfinite(value):
                    lines.append(f"{name} {value:,.2f} nominal USDT")
    else:
        reference = body.get("reference_price_text")
        if reference is None:
            reference = body.get("price") if kind == "HYPOTHETICAL_FILL" else body.get("exit_price")
        if reference is not None:
            lines.append(f"Paper reference {reference} USDT")
        if source := body.get("exchange_source_at"):
            lines.append(f"Exchange event {utc(source).isoformat()}")
        if receipt := body.get("source_received_at"):
            lines.append(f"Locally received {utc(receipt).isoformat()}")
    lines.extend((f"Run {book['run_id']}", "Manual outside the lab · No validated model · No real order"))
    return "\n".join(lines)


def collect(db, now, *, view=None, paper_db=None):
    """Queue only committed current-generation events on the existing private route."""
    if view is None or paper_db is None:
        from lab.paper_run import runtime

    view = runtime.status() if view is None else view
    generation = view.get("generation_id")
    if not generation:
        return {"state": "FRESH_GENERATION_NOT_READY"}
    saved = route(db)
    if paper_db is None:
        with closing(runtime.connect(readonly=True)) as source:
            source.execute("BEGIN")
            return collect(db, now, view=view, paper_db=source)
    now = utc(now)
    cutoff = utc(view["cutoff"])
    # A route enabled after this generation starts also starts forward-only.
    floor = max(cutoff, utc(saved["enabled_at"])) if saved else cutoff
    cursors = {}
    placeholders = ",".join("?" for kind in EVENTS)
    db.execute("SAVEPOINT fresh_paper_lifecycle")
    try:
        for slot, book in view["books"].items():
            run_id = book["run_id"]
            cursor_key = CURSOR_PREFIX + fingerprint([generation, slot, run_id])
            cursor = int(get_meta(db, cursor_key, 0))
            latest = paper_db.execute("SELECT coalesce(max(rowid),0) FROM event WHERE book=?", (book["book_id"],)).fetchone()[0]
            if cursor > latest:
                raise ValueError("Fresh paper notification cursor is beyond the registered journal")
            rows = paper_db.execute(
                f"SELECT rowid,key,kind,idea,source_at,recorded_at,body FROM event WHERE book=? AND rowid>? "
                f"AND kind IN ({placeholders}) ORDER BY rowid LIMIT ?",
                (book["book_id"], cursor, *EVENTS, MAX_EVENTS),
            )
            for rowid, key, kind, idea, occurred, recorded, raw in rows:
                if utc(recorded) > now:
                    break
                cursor = rowid
                if not saved or not floor <= utc(occurred) <= utc(recorded) or not idea:
                    continue
                body = _details(raw)
                if kind == "CLOSED" and book.get("accounting_complete") is False:
                    body = {**body, "accounting_complete": False}
                category = EVENTS[kind][0]
                context = {"generation_id": generation, "run_id": run_id, "book_id": book["book_id"],
                           "event": kind, "event_key": key, "ledger_rowid": rowid, "source_at": occurred,
                           "recorded_at": recorded, "exchange_source_at": body.get("exchange_source_at"),
                           "source_received_at": body.get("source_received_at"), "decision_at": body.get("paper_decision_at"),
                           "execution_committed_at": body.get("committed_at"),
                           "private_route_id": saved["route_id"]}
                identity = [generation, slot, run_id, idea]
                # One terminal no-entry summary per idea, even if the journal
                # records both evidence rejection and subsequent expiry.
                transition = "NO_ENTRY" if category == "paper_no_entry" else kind
                enqueue(db, PREFIX + fingerprint([*identity, key]), category,
                        _text(slot, book, kind, occurred, recorded, body), now.timestamp(),
                        idea_id=fingerprint(identity), transition=fingerprint([*identity, transition]),
                        event_json=json.dumps(context))
            set_meta(db, cursor_key, cursor)
            cursors[slot] = {"cursor_rowid": cursor, "latest_rowid": latest, "run_id": run_id}
    except BaseException:
        db.execute("ROLLBACK TO fresh_paper_lifecycle")
        raise
    finally:
        db.execute("RELEASE fresh_paper_lifecycle")
    return {"state": "PRIVATE_SUMMARIES_ENABLED" if saved else "LOCAL_ONLY_NO_PRIVATE_ROUTE",
            "generation_id": generation, "books": cursors}
