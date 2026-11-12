"""Queue committed EMA paper lifecycle events in the existing watchdog outbox."""

from contextlib import closing
import json

from lab.config import fingerprint
from lab.health.paper_events import (
    NO_REAL_ORDER, clock, close_text, identity, no_entry_text, price, receipt, size,
)
from lab.health.state import enqueue, get_meta, set_meta
from lab.phase15.paper import load
from lab.storage import utc


CATEGORIES = {
    "PUBLISHED": "ideas", "HYPOTHETICAL_FILL": "paper_entry", "EXPIRED": "paper_no_entry",
    "INVALIDATED": "paper_no_entry", "CLOSED": "paper_exit",
}
KINDS = tuple(CATEGORIES)
MAX_EVENTS_PER_BOOK = 128
UPDATE_PREFIX = "ema-paper-update:"


def _local_only(plan):
    evidence = plan.get("evidence", {})
    return any(record.get("synthetic") is True or record.get("source_mode") == "KRAKEN_SPOT_REFERENCE"
               or record.get("price_source") == "KRAKEN_BTC_USD_SPOT"
               or record.get("execution_model") in ("SYNTHETIC_BTC_LINEAR_PERPETUAL_V1", "SYNTHETIC_LEVERAGED_BTC")
               for record in (plan, evidence))


def _display_lane(lane):
    return {
        "profile": lane["profile"],
        "mode": {"symbol": lane["instrument"], "trade_type": lane["market"]},
    }


def _display_idea(idea):
    return {**idea, "exit_policy": "EMA Pullback"}


def _heading(lane, idea):
    return f"EMA Pullback {lane['profile'].title()} · {lane['instrument']} · {idea['direction']}"


def _idea_text(lane, idea, event):
    plan, entry = idea["plan"], idea["plan"]["entry"]
    signal = idea.get("boundary") or plan.get("decision_at")
    risk = plan.get("sizing", {})
    order = plan.get("order", {})
    leverage = order.get("leverage", lane.get("risk", {}).get("leverage"))
    return (
        f"ASEKE BTC LAB — EXPERIMENTAL IDEA\n{_heading(lane, idea)}\n"
        f"Confirmation {clock(signal)} · Published {clock(idea['published_at'])}\n"
        f"One eligible open at {clock(plan['eligible_from'])} within {entry['minimum']}–{entry['maximum']} USDT\n"
        f"Stop {price(plan['stop'])} · Frozen target {price(plan['target'])} USDT\n"
        f"Expires {clock(plan['entry_expiry'])} · Hold limit {plan['max_hold_minutes']} min\n"
        f"Planned risk {price(risk.get('planned_risk', order.get('planned_risk')))} USDT · "
        f"Nominal leverage {price(leverage)}×\n"
        f"Strategy {idea['strategy_version']}\n{identity(event[1], event[2])}\n"
        f"Experimental · No validated model\n{NO_REAL_ORDER}"
    )


def _entry_text(lane, idea, event, fill, *, closed):
    received = receipt(fill)
    delay = (utc(received or event[4]) - utc(event[3])).total_seconds()
    timing = " — DELAYED CONFIRMATION" if fill.get("delayed_confirmation") or delay >= 15 * 60 else ""
    state = "A later close is already recorded." if closed else "Hypothetical fill recorded."
    return (
        f"ASEKE BTC LAB — PAPER ENTRY CONFIRMED{timing}\n{_heading(lane, idea)}\n"
        f"Modeled entry {price(fill['price'])} USDT at {clock(event[3])}\n"
        f"Market evidence received {clock(received)} · Ledger recorded {clock(event[4])}\n"
        f"Size {size(fill['quantity'], fill.get('contracts'))}\n"
        f"Stop {price(fill['stop'])} · Frozen target {price(fill['target'])} USDT\n"
        f"Planned risk {fill['planned_risk']:.4f} USDT\n"
        f"{identity(event[1], event[2])}\n{state}\n{NO_REAL_ORDER}"
    ), received


def _eligible(row, registered_at, now):
    return registered_at <= utc(row[3]) <= utc(row[4]) <= now


def _events(paper, book, idea):
    rows = paper.execute(
        "SELECT rowid,book,idea,source_at,recorded_at,kind,body,key FROM event "
        "WHERE book=? AND idea=? ORDER BY rowid", (book, idea)
    )
    events = {}
    for row in rows:
        kind = "NO_ENTRY" if row[5] in {"EXPIRED", "INVALIDATED"} else row[5]
        events.setdefault(kind, row)
    return events


def _queue(db, registration, row, text, received, now, decision_at, source_mode):
    rowid, book, idea, occurred, recorded, kind, _, key = row
    category = CATEGORIES[kind]
    context = {
        "family": "ema_pullback_trend_continuation", "registration_id": registration["id"], "book_id": book,
        "event": kind, "event_key": key, "source_at": occurred, "recorded_at": recorded,
        "source_received_at": received, "decision_at": decision_at, "ledger_rowid": rowid,
    }
    if source_mode in {"KRAKEN_PROXY_SEEDED", "KUCOIN_NATIVE_READY"}:
        context["source_mode"] = source_mode
        headline, separator, remainder = text.partition("\n")
        text = f"{headline}\nFeature mode {source_mode}{separator}{remainder}"
    enqueue(db, "ema-paper:" + fingerprint([book, idea, key, category]), category, text, now.timestamp(),
            idea_id=idea, transition=fingerprint(["ema-paper", book, idea, category]), event_json=json.dumps(context))


def _queue_idea(paper, db, registration, book, identifier, now):
    lane = registration["body"]
    registered_at = utc(lane["registered_at"])
    events = _events(paper, book, identifier)
    published = events.get("PUBLISHED")
    if published is None or not _eligible(published, registered_at, now):
        return
    idea = json.loads(published[6])
    if (idea.get("id") != identifier or idea.get("direction") not in {"LONG", "SHORT"}
            or not registered_at <= utc(idea.get("boundary", published[3])) <= utc(published[3])):
        return
    plan = idea["plan"]
    if _local_only(plan):
        # The verified Kraken scope keeps the synthetic lifecycle local. The
        # caller still advances its cursor, so old events cannot replay later.
        return
    decision_at = idea.get("boundary", published[3])
    evidence = plan.get("evidence", {})
    source_receipt = plan.get("source_received_at") or evidence.get("source_received_at")
    source_mode = evidence.get("source_mode")
    _queue(db, registration, published, _idea_text(lane, idea, published), source_receipt, now, decision_at, source_mode)
    filled = events.get("HYPOTHETICAL_FILL")
    closed = events.get("CLOSED")
    if filled and _eligible(filled, registered_at, now) and utc(filled[3]) > utc(published[3]):
        db.execute("UPDATE outbox SET status='SUPERSEDED' WHERE idea_id=? AND id LIKE 'ema-paper:%' "
                   "AND category='paper_no_entry' AND status IN ('PENDING','RETRY')", (identifier,))
        fill = json.loads(filled[6])
        has_close = bool(closed and _eligible(closed, registered_at, now) and utc(closed[3]) >= utc(filled[3]))
        message, received = _entry_text(lane, idea, filled, fill, closed=has_close)
        _queue(db, registration, filled, message, received, now, decision_at, source_mode)
        if has_close:
            message, received = close_text(_display_lane(lane), _display_idea(idea), closed, json.loads(closed[6]), fill)
            _queue(db, registration, closed, message, received, now, decision_at, source_mode)
    elif not closed:
        terminal = events.get("NO_ENTRY")
        if terminal and _eligible(terminal, registered_at, now) and utc(terminal[3]) >= utc(published[3]):
            message, received = no_entry_text(_display_lane(lane), _display_idea(idea), terminal, json.loads(terminal[6]))
            _queue(db, registration, terminal, message, received, now, decision_at, source_mode)


def _updates(paper, db, registrations, now, policy):
    enabled = policy["categories"].get("open_position", False)
    active = set()
    if enabled:
        for registration in registrations.values():
            lane = registration["body"]
            account = load(paper, lane)
            position = account.get("position")
            if not position or utc(position["entry_at"]) <= utc(lane["registered_at"]):
                continue
            if _local_only(position) or _local_only(position.get("plan", {})):
                continue
            interval = policy["open_position_minutes"][lane["profile"]]
            if type(interval) is not int or interval <= 0:
                raise ValueError("Open-position reporting interval must be a positive integer")
            elapsed = (now - utc(position["entry_at"])).total_seconds()
            slot = int(elapsed // (interval * 60))
            if slot < 1:
                continue
            identifier = UPDATE_PREFIX + fingerprint([lane["book_id"], position["id"], interval, slot])
            active.add(identifier)
            marked, received = position.get("mark_source_at"), position.get("mark_received_at")
            fresh = bool(marked and received and not account["problem"] and utc(marked) <= utc(received) <= now
                         and (now - utc(marked)).total_seconds() <= 17 * 60)
            context = {"book_id": lane["book_id"], "registration_id": registration["id"], "event": "OPEN_UPDATE",
                       "source_at": marked, "source_received_at": received}
            basis = "Mark" if lane["market"] == "FUTURES" else "Trade"
            unrealized = account["equity"] - account["cash"]
            enqueue(db, identifier, "open_position",
                    f"ASEKE BTC LAB — OPEN PAPER POSITION UPDATE\n{_heading(lane, position)}\n"
                    f"Entry {price(position['position']['entry'])} USDT at {clock(position['entry_at'])}\n"
                    f"{basis} {price(position['last_mark'])} USDT at {clock(marked)} · "
                    f"{'Fresh at check' if fresh else 'Stale or unresolved at check'}\n"
                    f"Unrealized {unrealized:+.4f} USDT · Stop {price(position['stop'])} USDT\n"
                    f"Deadline {clock(position['deadline'])}\n{identity(lane['book_id'], position['id'])}\n{NO_REAL_ORDER}",
                    now.timestamp(), idea_id=position["id"], event_json=json.dumps(context))
    rows = db.execute("SELECT id FROM outbox WHERE category='open_position' AND id LIKE 'ema-paper-update:%' "
                      "AND status IN ('PENDING','RETRY')").fetchall()
    db.executemany("UPDATE outbox SET status='SUPERSEDED' WHERE id=?",
                   [(row[0],) for row in rows if row[0] not in active])


def collect(health_db, now, policy, registrations=None, paper_db=None):
    """Read committed events only. The health watchdog owns Telegram delivery."""
    if registrations is None:
        from lab.ema_paper.catalog import registrations as registered
        registrations = registered()
    if not registrations:
        return {}
    if paper_db is None:
        from lab.ema_paper.catalog import db_open
        with closing(db_open(readonly=True)) as paper:
            paper.execute("BEGIN")
            return collect(health_db, now, policy, registrations, paper)
    now = utc(now)
    result = {}
    health_db.execute("SAVEPOINT ema_lifecycle")
    try:
        for name, registration in registrations.items():
            lane = registration["body"]
            book = lane["book_id"]
            meta = "ema_paper_lifecycle:" + registration["id"]
            latest = paper_db.execute("SELECT coalesce(max(rowid),0) FROM event WHERE book=? "
                                      "AND kind IN (?,?,?,?,?)", (book, *KINDS)).fetchone()[0]
            cursor = int(get_meta(health_db, meta, lane.get("notification_baseline_rowid", 0)))
            if cursor > latest:
                raise ValueError("EMA notification cursor is beyond the committed journal")
            rows = paper_db.execute("SELECT rowid,idea,recorded_at FROM event WHERE book=? AND rowid>? "
                                    "AND kind IN (?,?,?,?,?) ORDER BY rowid LIMIT ?",
                                    (book, cursor, *KINDS, MAX_EVENTS_PER_BOOK)).fetchall()
            changed = set()
            for rowid, identifier, recorded in rows:
                if utc(recorded) > now:
                    break
                if identifier not in changed:
                    _queue_idea(paper_db, health_db, registration, book, identifier, now)
                    changed.add(identifier)
                cursor = rowid
            set_meta(health_db, meta, cursor)
            result[name] = {"registration": registration["id"], "cursor_rowid": cursor, "latest_rowid": latest}
        _updates(paper_db, health_db, registrations, now, policy)
    except BaseException:
        health_db.execute("ROLLBACK TO ema_lifecycle")
        raise
    finally:
        health_db.execute("RELEASE ema_lifecycle")
    return result
