"""Read committed Phase 15 events and queue observational Telegram messages."""

import json
from math import isclose
import time

from lab.config import fingerprint
from lab.health.state import enqueue, get_meta, local, set_meta
from lab.storage import utc


EVENTS = {
    "HYPOTHETICAL_FILL": "paper_entry",
    "EXPIRED": "paper_no_entry",
    "INVALIDATED": "paper_no_entry",
    "CLOSED": "paper_exit",
}
EXIT_NAMES = {"stop": "STOP", "target": "TARGET", "trailing_stop": "TRAILING STOP",
              "time_exit": "TIME", "session_flat": "SESSION FLAT", "liquidation": "LIQUIDATION"}
NO_REAL_ORDER = "No real exchange order was placed."


def clock(value):
    return local(utc(value).timestamp()) if value else "unavailable"


def receipt(body):
    if body.get("source_received_at"):
        return body["source_received_at"]
    stamps = [body.get(key, {}).get("received_at") for key in ("trade", "trigger_bar")]
    return max((stamp for stamp in stamps if stamp), key=utc, default=None)


def timing(event, source_receipt):
    at, recorded = event[3], event[4]
    label = {"HYPOTHETICAL_FILL":"Modeled entry time", "CLOSED":"Modeled close time",
             "EXPIRED":"Expired at", "INVALIDATED":"Invalidated at"}[event[5]]
    delay = (utc(recorded) - utc(at)).total_seconds()
    return (" — DELAYED" if delay >= 60 else "",
            f"{label} {clock(at)}\nMarket evidence received {clock(source_receipt)}\nLedger recorded {clock(recorded)}")


def heading(lane, idea):
    return f"{lane['profile'].title()} / {idea['exit_policy']} · {lane['mode']['symbol']} · {idea['direction']}"


def identity(book_id, idea_id):
    return f"Idea {idea_id}\nPaper book {book_id}"


def price(value):
    return str(value) if value is not None else "unavailable"


def size(quantity, contracts):
    return f"{quantity} BTC" if contracts is None else f"{contracts:g} contracts ({quantity} BTC)"


def entry_text(lane, idea, event, fill, observed, *, has_close):
    plan = idea["plan"]
    received = receipt(fill) or receipt(observed)
    delay, times = timing(event, received)
    risk = fill["planned_risk"]
    equity = fill.get("entry_equity") or plan.get("sizing", {}).get("equity")
    risk_share = f" ({risk/equity:.2%} of paper equity)" if equity else ""
    target = (f"ATR trailing ({plan['trailing_atr']:g} × ATR)" if plan.get("trailing_atr")
              else f"Fill-based target {price(fill.get('target'))}")
    entry_state = "HYPOTHETICAL ENTRY RECORDED · subsequent close in ledger" if has_close else (
        "HYPOTHETICAL POSITION OPEN when checked")
    contracts = fill.get("contracts")
    if contracts is None and lane["mode"].get("trade_type") == "FUTURES":
        contracts = fill["quantity"]/plan["contract"]["base_per_contract"]
    return (f"ASEKE BTC LAB — PAPER ENTRY CONFIRMED{delay}\n{heading(lane, idea)}\n"
            f"Modeled entry {price(fill['price'])} USDT\n{times}\n"
            f"Size {size(fill['quantity'], contracts)}\n"
            f"Stop {price(fill.get('stop', plan['stop']))} · {target}\n"
            f"Planned risk {risk:.4f} USDT{risk_share}\n"
            f"{identity(event[1], event[2])}\nStatus {entry_state}\n{NO_REAL_ORDER}"), received


def no_entry_text(lane, idea, event, body):
    received = receipt(body)
    _, times = timing(event, received)
    reason = {"ENTRY_OUTSIDE_FROZEN_RANGE": "Adverse-adjusted entry outside the frozen range",
              "INSUFFICIENT_EXECUTABLE_CAPACITY": "Insufficient executable capacity"}.get(
                  body.get("reason_code"), body["reason"])
    return (f"ASEKE BTC LAB — NO PAPER ENTRY\n{heading(lane, idea)}\n"
            f"Outcome {event[5]}\nReason {reason}\nPublished {clock(idea['published_at'])}\n{times}\n"
            f"{identity(event[1], event[2])}\nNo hypothetical position was opened.\n{NO_REAL_ORDER}"), received


def close_text(lane, idea, event, closed, fill):
    received = receipt(closed)
    delay, times = timing(event, received)
    kind = closed.get("exit_kind", closed["reason"])
    label = EXIT_NAMES.get(kind, kind.replace("_", " ").upper())
    attributed = closed["gross_price_pnl"] + closed["funding"] - closed["fees"] - closed["liquidation_cost"] + closed["unfunded_deficit"]
    before, after = closed.get("equity_before"), closed.get("equity_after")
    reconciled = (before is not None and after is not None and
                  isclose(attributed, closed["net"], rel_tol=0, abs_tol=1e-7) and
                  isclose(before + closed["net"], after, rel_tol=0, abs_tol=1e-7))
    equity_line = (f"Updated paper equity {after:,.4f} USDT" if reconciled
                   else "Accounting warning · closing equity could not be reconciled from saved evidence")
    return (f"ASEKE BTC LAB — PAPER TRADE CLOSED{delay}\n{heading(lane, idea)}\n"
            f"Entry {price(closed.get('entry_price', fill['price']))} · Exit {price(closed['exit_price'])} USDT\n"
            f"Entered {clock(closed['entry_at'])}\n{times}\n"
            f"Duration {closed['holding_minutes']:g} min · Exit {label} (ledger {closed['reason']})\n"
            f"Gross {closed['gross_price_pnl']:+.4f} · Fees {closed['fees']:.4f} USDT\n"
            f"Adverse execution {closed['adverse_cost_in_gross']:.4f} USDT, included in gross\n"
            f"Funding {closed['funding']:+.4f} · Liquidation cost {closed['liquidation_cost']:.4f} · "
            f"Unfunded deficit adjustment {closed['unfunded_deficit']:+.4f} USDT\n"
            f"Ledger net {closed['net']:+.8f} USDT · {closed['net_r']:+.6f} R\n{equity_line}\n"
            f"{identity(event[1], event[2])}\nStatus CLOSED — HYPOTHETICAL\n{NO_REAL_ORDER}"), received


def queue_event(db, event, text, source_receipt, now):
    rowid, book, idea, source, recorded, kind, _, key = event
    category = EVENTS[kind]
    context = {"book_id": book, "event_key": key, "event": kind, "source_at": source,
               "source_received_at": source_receipt, "recorded_at": recorded, "ledger_rowid": rowid}
    enqueue(db, "paper:" + fingerprint([book, idea, key, category]), category, text, now, idea_id=idea,
            transition=fingerprint([book, idea, category]), event_json=json.dumps(context))


def scan(paper, db, registration, lanes, books, now, cfg):
    latest = paper.execute("SELECT coalesce(max(rowid),0) FROM event").fetchone()[0]
    raw = get_meta(db, "paper_lifecycle")
    if raw is None:
        baseline = {"registration": registration, "baseline_rowid": latest, "cursor_rowid": latest,
                    "activated_at": now.isoformat(), "open_at_activation": [
                        {"book": name, "idea": info["position"]["id"], "entry_at": info["position"]["entry_at"],
                         "direction": info["position"]["direction"], "entry_price": info["position"]["position"]["entry"],
                         "quantity": info["position"]["position"]["remaining"]}
                        for name, info in books.items() if info["position"]]}
        set_meta(db, "paper_lifecycle", json.dumps(baseline))
        return baseline
    baseline = json.loads(raw)
    if baseline["registration"] != registration or baseline["cursor_rowid"] > latest:
        raise ValueError("Phase 15 notification cursor no longer matches the registered ledger")

    if latest > baseline["cursor_rowid"]:
        changed = paper.execute(
            "SELECT book,idea FROM event WHERE rowid>? AND kind IN "
            "('PUBLISHED','ENTRY_OBSERVED','HYPOTHETICAL_FILL','EXPIRED','INVALIDATED','CLOSED') "
            "GROUP BY book,idea ORDER BY min(rowid)", (baseline["cursor_rowid"],)).fetchall()
        for book, idea_id in changed:
            if book in lanes:
                queue_idea(paper, db, lanes[book], book, idea_id, baseline["baseline_rowid"])
        baseline["cursor_rowid"] = latest
        set_meta(db, "paper_lifecycle", json.dumps(baseline))
    open_updates(db, lanes, books, now, cfg)
    return baseline


def queue_idea(paper, db, lane, book, idea_id, floor):
    rows = paper.execute("SELECT rowid,book,idea,source_at,recorded_at,kind,body,key FROM event "
                         "WHERE book=? AND idea=? ORDER BY rowid", (book, idea_id))
    events = {}
    for row in rows:
        kind = "NO_ENTRY" if row[5] in {"EXPIRED", "INVALIDATED"} else row[5]
        events.setdefault(kind, row)
    if "PUBLISHED" not in events:
        return
    idea = json.loads(events["PUBLISHED"][6])
    if idea["id"] != idea_id or idea["direction"] not in {"LONG", "SHORT"}:
        raise ValueError("Saved lifecycle publication identity or direction is invalid")
    if fill := events.get("HYPOTHETICAL_FILL"):
        db.execute("UPDATE outbox SET status='SUPERSEDED' WHERE idea_id=? AND category='paper_no_entry' "
                   "AND status IN ('PENDING','RETRY')", (idea_id,))
        payload = json.loads(fill[6])
        if fill[0] > floor:
            observed = json.loads(events["ENTRY_OBSERVED"][6]) if "ENTRY_OBSERVED" in events else {}
            text, received = entry_text(lane, idea, fill, payload, observed, has_close="CLOSED" in events)
            queue_event(db, fill, text, received, time.time())
        closed = events.get("CLOSED")
        if closed and closed[0] > floor:
            text, received = close_text(lane, idea, closed, json.loads(closed[6]), payload)
            queue_event(db, closed, text, received, time.time())
    elif "CLOSED" not in events:
        terminal = events.get("NO_ENTRY")
        if terminal and terminal[0] > floor:
            text, received = no_entry_text(lane, idea, terminal, json.loads(terminal[6]))
            queue_event(db, terminal, text, received, time.time())


def open_updates(db, lanes, books, now, cfg):
    active = {info["position"]["id"] for info in books.values() if info["position"]}
    for row in db.execute("SELECT id,idea_id FROM outbox WHERE category='open_position' AND status IN ('PENDING','RETRY')"):
        if not cfg["categories"]["open_position"] or row["idea_id"] not in active:
            db.execute("UPDATE outbox SET status='SUPERSEDED' WHERE id=?", (row["id"],))
    if not cfg["categories"]["open_position"]:
        return
    for info in books.values():
        record = info["position"]
        if not record:
            continue
        lane = lanes[info["book_id"]]
        elapsed = (now - utc(record["entry_at"])).total_seconds()
        interval = cfg["open_position_minutes"][lane["profile"]]
        if type(interval) is not int or interval <= 0:
            raise ValueError("Open-position interval must be a positive integer")
        slot = int(elapsed // (interval * 60))
        if slot < 1:
            continue
        pos = record["position"]
        mark_at, received = record.get("mark_source_at"), record.get("mark_received_at")
        fresh = bool(mark_at and received and not info["problem"] and
                     utc(mark_at) <= utc(received) <= now and (now - utc(mark_at)).total_seconds() <= 17*60)
        basis = "Mark" if lane["mode"]["trade_type"] == "FUTURES" else "Trade reference"
        remaining = max(0, (utc(record["deadline"]) - now).total_seconds()/60)
        key = "paper-update:" + fingerprint([info["book_id"],record["id"],interval,slot])
        db.execute("UPDATE outbox SET status='SUPERSEDED' WHERE idea_id=? AND category='open_position' "
                   "AND id!=? AND status IN ('PENDING','RETRY')", (record["id"],key))
        enqueue(db, key, "open_position",
                f"ASEKE BTC LAB — OPEN PAPER POSITION UPDATE\n{heading(lane, record)}\n"
                f"Modeled entry {price(pos['entry'])} USDT at {clock(record['entry_at'])} · Size {pos['remaining']} BTC\n"
                f"{basis} {price(record['last_mark'])} USDT as of {clock(mark_at)} · "
                f"{'Fresh at check' if fresh else 'Stale or unverified at check'}\n"
                f"Unrealized {info['unrealized']:+.4f} USDT · Current stop {price(record['stop'])} · "
                f"{remaining:.0f} min to modeled deadline\n"
                f"{identity(info['book_id'], record['id'])}\n{NO_REAL_ORDER}", time.time(), idea_id=record["id"])
