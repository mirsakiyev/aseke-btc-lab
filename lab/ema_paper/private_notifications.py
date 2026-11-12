"""Private synthetic EMA summaries queued through the existing Telegram outbox."""

from contextlib import closing
import json
from math import isfinite
import re

from lab.config import fingerprint
from lab.ema_paper import catalog, synthetic_protocol
from lab.health.state import enqueue, get_meta, set_meta
from lab.kraken_paper.authorization import utc


PREFIX = "ema-private:"
ROUTE_META = "ema_private_telegram_route_v1"
TRADE_EVENTS = {"HYPOTHETICAL_FILL": "paper_entry", "CLOSED": "paper_exit"}
IDEA_EVENTS = {"PUBLISHED": "ideas", "EXPIRED": "paper_no_entry", "INVALIDATED": "paper_no_entry"}
EVENTS = {**TRADE_EVENTS, **IDEA_EVENTS}
EVENT_NAMES = {"PUBLISHED": "EXPERIMENTAL IDEA", "HYPOTHETICAL_FILL": "ENTRY", "EXPIRED": "IDEA EXPIRED",
               "INVALIDATED": "IDEA INVALIDATED", "CLOSED": "EXIT"}
PROFILES = {"day": "Day 15m", "swing": "Swing 4H", "position": "Position 1D"}
EXIT_NAMES = {"stop": "Stop-loss", "target": "Take-profit", "time_exit": "Hold limit", "liquidation": "Modeled liquidation"}
INCIDENTS = {"collector": "Kraken reference collection", "staleness": "Kraken reference continuity",
             "evaluation": "EMA scheduled evaluation", "watchdog": "EMA watchdog",
             "chart_recovery": "EMA chart recovery"}
_HASH = re.compile(r"[0-9a-f]{64}")


def _cursor(revision, *, ideas=False):
    stream = "idea_cursor" if ideas else "cursor"
    return f"ema_private_telegram_{stream}_v1:{revision}"


def _idea_start(revision):
    return "ema_private_telegram_idea_enabled_v1:" + revision


def _checked_books(registrations, revision):
    if registrations is None:
        registrations = catalog.registrations()
    synthetic = revision["body"]["books"]
    if set(registrations) != set(PROFILES) or set(synthetic) != set(PROFILES):
        raise ValueError("Private EMA alerts require the three original book revisions")
    for profile, original in registrations.items():
        effective = synthetic[profile]
        record, lane = effective["body"], original["body"]
        if (fingerprint(record) != effective["id"] or record["registration_id"] != original["id"]
                or record["book_id"] != lane["book_id"] or lane["profile"] != profile
                or record["source_mode"] != synthetic_protocol.SOURCE_MODE
                or not _HASH.fullmatch(lane["book_id"]) or not record["paper_only"] or record["live_orders"]):
            raise PermissionError("Private EMA alerts differ from the registered synthetic book")
        yield profile, lane, effective


def route(db):
    raw = get_meta(db, ROUTE_META)
    if not raw:
        return None
    saved = json.loads(raw)
    if not _HASH.fullmatch(saved["route_id"]):
        raise PermissionError("Private EMA Telegram route digest is invalid")
    utc(saved["enabled_at"])
    return saved


def enable(db, route_id, now, registrations=None, paper_db=None, *, revision=None):
    """Record a future-only baseline after the operator's private route is verified."""
    if not isinstance(route_id, str) or not _HASH.fullmatch(route_id):
        raise PermissionError("A verified private EMA Telegram route is required")
    now = utc(now)
    revision = synthetic_protocol.load(now=now) if revision is None else revision
    if paper_db is None:
        with closing(catalog.db_open(readonly=True)) as source:
            source.execute("BEGIN")
            return enable(db, route_id, now, registrations, source, revision=revision)
    books = tuple(_checked_books(registrations, revision))
    current = route(db)
    if current and current["route_id"] == route_id and current["revision_id"] == revision["id"]:
        return current
    saved = {"route_id": route_id, "enabled_at": now.isoformat(), "revision_id": revision["id"]}
    with db:
        if current:
            db.execute("UPDATE outbox SET status='SUPERSEDED',detail='Private EMA destination changed' "
                       "WHERE id LIKE 'ema-private:%' AND status IN ('PENDING','RETRY')")
        for _, lane, effective in books:
            last = paper_db.execute("SELECT coalesce(max(rowid),0) FROM event WHERE book=?", (lane["book_id"],)).fetchone()[0]
            set_meta(db, _cursor(effective["id"]), last)
            set_meta(db, _cursor(effective["id"], ideas=True), last)
            set_meta(db, _idea_start(effective["id"]), now.isoformat())
        for key in INCIDENTS:
            set_meta(db, "ema_private_telegram_incident_v1:" + key, "{}")
        set_meta(db, ROUTE_META, json.dumps(saved))
    return saved


def _number(body, key):
    value = body.get(key)
    if type(value) not in (int, float) or not -1e12 <= value <= 1e12 or not isfinite(value):
        return None
    return value


def _summary(profile, lane, revision, published, kind, occurred, recorded, body):
    plan = published.get("plan", {})
    evidence = plan.get("evidence", {})
    if (evidence.get("source_mode") != synthetic_protocol.SOURCE_MODE
            or evidence.get("protocol_amendment_id") != revision["id"]):
        return None
    direction = published.get("direction")
    leverage = revision["body"]["execution_model"]["leverage"]
    if direction not in ("LONG", "SHORT") or type(leverage) is not int or not 1 <= leverage <= 100:
        return None
    lines = [f"ASEKE BTC LAB — SIMULATED PAPER {EVENT_NAMES[kind]}",
             f"EMA Pullback {PROFILES[profile]} · {direction} · {leverage}×",
             "Kraken BTC/USD Spot reference · Synthetic execution · nominal USDT",
             f"Paper event time {utc(occurred):%Y-%m-%d %H:%M:%S UTC}",
             f"Ledger recorded {utc(recorded):%Y-%m-%d %H:%M:%S UTC}", f"Paper book {lane['book_id']}"]
    if kind in {"PUBLISHED", "HYPOTHETICAL_FILL"}:
        risk = _number(lane["risk"], "risk_fraction")
        if risk is not None and 0 < risk <= 1:
            lines.append(f"Stop-risk budget {risk:.2%} of paper equity")
    elif kind == "CLOSED":
        reason = body.get("exit_kind") or body.get("reason")
        outcome = EXIT_NAMES.get(reason, "Simulated close") if isinstance(reason, str) else "Simulated close"
        lines.append(f"Exit reason {outcome}")
        for key, label in (("gross_price_pnl", "Gross price P&L"), ("fees", "Modeled fees"),
                           ("funding", "Modeled financing"), ("net", "Net realized P&L"),
                           ("equity_after", "Updated paper equity")):
            if (value := _number(body, key)) is not None:
                lines.append(f"{label} {value:,.2f} nominal USDT")
        net, equity = _number(body, "net"), _number(body, "equity_before")
        if net is not None and equity is not None and equity > 0:
            lines.append(f"Net return {net / equity:+.2%} of pre-trade paper equity")
        duration = _number(body, "holding_minutes")
        if duration is not None and duration >= 0:
            lines.append(f"Modeled duration {duration:,.0f} min")
    else:
        lines.append("No hypothetical position was opened for this idea.")
    return "\n".join(lines + ["No real exchange order was placed."])


def collect(db, now, registrations=None, paper_db=None, *, revision=None):
    """Queue allowlisted summaries of newly committed events with independent cursors."""
    saved = route(db)
    if not saved:
        return {"state": "PRIVATE_ROUTE_NOT_ENABLED"}
    now, enabled = utc(now), utc(saved["enabled_at"])
    revision = synthetic_protocol.load(now=now) if revision is None else revision
    if saved["revision_id"] != revision["id"]:
        return {"state": "PRIVATE_ROUTE_REVIEW_REQUIRED"}
    if paper_db is None:
        with closing(catalog.db_open(readonly=True)) as source:
            source.execute("BEGIN")
            return collect(db, now, registrations, source, revision=revision)
    result = {"state": "PRIVATE_SUMMARIES_ENABLED", "books": {}}
    db.execute("SAVEPOINT ema_private_lifecycle")
    try:
        for profile, lane, effective in _checked_books(registrations, revision):
            cursor_key = _cursor(effective["id"])
            idea_cursor_key = _cursor(effective["id"], ideas=True)
            idea_start_key = _idea_start(effective["id"])
            cursor = int(get_meta(db, cursor_key))
            book = lane["book_id"]
            latest = paper_db.execute("SELECT coalesce(max(rowid),0) FROM event WHERE book=?", (book,)).fetchone()[0]
            # When an existing private route upgrades, baseline only the new
            # idea categories. Keep the established fill/close cursor intact.
            idea_checkpoint = get_meta(db, idea_cursor_key)
            idea_start = get_meta(db, idea_start_key)
            if idea_checkpoint is None or idea_start is None:
                idea_cursor, idea_enabled = latest, now
            else:
                idea_cursor, idea_enabled = int(idea_checkpoint), utc(idea_start)
            if max(cursor, idea_cursor) > latest:
                raise PermissionError("Private EMA notification cursor is beyond the committed ledger")
            rows = paper_db.execute("SELECT rowid,kind,idea,source_at,recorded_at,body,key FROM event "
                                    "WHERE book=? AND ((rowid>? AND kind IN (?,?)) OR "
                                    "(rowid>? AND kind IN (?,?,?))) ORDER BY rowid LIMIT 128",
                                    (book, cursor, *TRADE_EVENTS, idea_cursor, *IDEA_EVENTS)).fetchall()
            for rowid, kind, idea, occurred, recorded, raw, key in rows:
                if utc(recorded) > now:
                    break
                if kind in TRADE_EVENTS:
                    cursor = rowid
                    boundary = enabled
                else:
                    idea_cursor = rowid
                    boundary = idea_enabled
                if not boundary < utc(occurred) <= utc(recorded):
                    continue
                body = json.loads(raw)
                if kind == "PUBLISHED":
                    published = body
                else:
                    publication = paper_db.execute("SELECT body FROM event WHERE book=? AND idea=? AND kind='PUBLISHED' "
                                                   "AND rowid<? ORDER BY rowid LIMIT 1", (book, idea, rowid)).fetchone()
                    if publication is None:
                        continue
                    published = json.loads(publication[0])
                message = _summary(profile, lane, effective, published, kind, occurred, recorded, body)
                if message is None:
                    continue
                category = EVENTS[kind]
                context = {"book_id": book, "event": kind, "private_route_id": saved["route_id"], "ledger_rowid": rowid}
                # Keep existing fill/close transition keys. A terminal idea
                # can have only one private no-entry notification.
                transition = "NO_ENTRY" if category == "paper_no_entry" else kind
                enqueue(db, PREFIX + fingerprint([book, key]), category, message, now.timestamp(),
                        idea_id=fingerprint([PREFIX, book, idea]), transition=fingerprint([PREFIX, book, idea, transition]),
                        event_json=json.dumps(context))
            set_meta(db, cursor_key, cursor)
            set_meta(db, idea_cursor_key, idea_cursor)
            set_meta(db, idea_start_key, idea_enabled.isoformat())
            result["books"][profile] = cursor
    except BaseException:
        db.execute("ROLLBACK TO ema_private_lifecycle")
        raise
    finally:
        db.execute("RELEASE ema_private_lifecycle")
    return result


def observe(db, now, observations):
    """Debounce fixed operational summaries on the same guarded private route."""
    saved = route(db)
    if not saved:
        return
    now = utc(now)
    for key, healthy in observations.items():
        if key not in INCIDENTS or type(healthy) is not bool:
            raise ValueError("Unknown private EMA operational check")
        meta = "ema_private_telegram_incident_v1:" + key
        previous = json.loads(get_meta(db, meta, "{}"))
        if previous.get("checked_at") and utc(previous["checked_at"]) >= now:
            continue
        episode = previous.get("episode", 0)
        active = previous.get("active", False)
        count = previous.get("count", 0) + 1 if previous.get("healthy") is healthy else 1
        # A measured watchdog gap already represents a prolonged absence.
        failure_checks = 1 if key == "watchdog" else 3
        if (healthy and active and count >= 2) or (not healthy and not active and count >= failure_checks):
            if not healthy:
                episode += 1
            active = not healthy
            outcome = "RECOVERED" if healthy else "INTERRUPTED"
            identifier = PREFIX + fingerprint([saved["enabled_at"], key, episode, outcome])
            enqueue(db, identifier, "health", f"ASEKE BTC LAB — SIMULATED PAPER OPERATIONS\n{INCIDENTS[key]} · {outcome}",
                    now.timestamp(), event_json=json.dumps({"private_route_id": saved["route_id"], "event": outcome}))
        set_meta(db, meta, json.dumps({"active": active, "episode": episode, "healthy": healthy, "count": count,
                                      "checked_at": now.isoformat()}))
