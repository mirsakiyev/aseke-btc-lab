"""Forward-only XBTUSDTM mark reference entry for separate Phase 15 paper books."""

from dataclasses import asdict
import json
from math import isfinite
import re
import shutil
import time

import httpx
import pandas as pd

from lab.learning.simulator import Contract, Cost, Order, Position, Risk, fill_price, liquidation_price, quantize
from lab.phase9.data import mark_quote
from lab.phase9.ideas import current_rules
from lab.phase9.public import load_payload, request, send
from lab.phase15 import paper
from lab.phase15.catalog import STEP, encode
from lab.storage import utc, utc_now


VERSION = "phase15-realtime-mark-reference-v1"
MODE = "REALTIME_MARK_REFERENCE_PAPER"
SYMBOL = "XBTUSDTM"
SOURCE = "KUCOIN_FUTURES_CURRENT_MARK"
MAX_AGE = pd.Timedelta(seconds=2)
SOURCE_POLICY = {
    "max_age_seconds": MAX_AGE.total_seconds(),
    "max_request_seconds": MAX_AGE.total_seconds(),
    "io_timeout_seconds": 1.5,
    "min_request_spacing_seconds": 15,
    "requests_per_hour": 24,
    "total_requests": 5000,
    "max_response_bytes": 16 * 1024,
    "min_free_bytes": 2 * 1024**3,
    "attempts_per_idea": 2,
}
UNSENT = {"ACCESS_RESTRICTED", "BACKOFF", "SHARED_SOURCE_BACKOFF", "REQUEST_SPACING", "REQUEST_BUDGET", "DISK_LIMIT"}


def init_observations(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS mark_request (
        id INTEGER PRIMARY KEY, requested_at TEXT NOT NULL, observation TEXT
    );
    CREATE TABLE IF NOT EXISTS mark_request_idea (
        idea TEXT NOT NULL, request_id INTEGER NOT NULL, PRIMARY KEY(idea, request_id)
    );
    """)


def request_capacity(db, idea_id):
    """Count dispatched or interrupted reservations across runner restarts."""
    count = db.execute("SELECT count(*) FROM mark_request_idea WHERE idea=?", (idea_id,)).fetchone()[0]
    return count < SOURCE_POLICY["attempts_per_idea"]


def observe(db, root, *, idea_ids=(), clock=utc_now, transport=None):
    """One bounded GET to the existing allowlisted mark route, under the ledger writer lock."""
    idea_ids = tuple(dict.fromkeys(idea_ids))
    if any(not request_capacity(db, idea) for idea in idea_ids):
        return {"status": "REQUEST_BUDGET"}
    now = clock()
    previous = db.execute("SELECT requested_at, observation FROM mark_request ORDER BY id DESC LIMIT 1").fetchone()
    if previous:
        last = json.loads(previous[1]) if previous[1] else {}
        if last.get("status") == "ACCESS_RESTRICTED":
            return {"status": "ACCESS_RESTRICTED"}
        if last.get("resume_after") and now < utc(last["resume_after"]):
            return {"status": "BACKOFF"}
        if now < utc(previous[0]) + pd.Timedelta(seconds=SOURCE_POLICY["min_request_spacing_seconds"]):
            return {"status": "REQUEST_SPACING"}
    total, hourly = db.execute("SELECT count(*), count(CASE WHEN julianday(requested_at) >= julianday(?) THEN 1 END) "
                               "FROM mark_request", ((now - pd.Timedelta(hours=1)).isoformat(),)).fetchone()
    if total >= SOURCE_POLICY["total_requests"] or hourly >= SOURCE_POLICY["requests_per_hour"]:
        return {"status": "REQUEST_BUDGET"}
    if shutil.disk_usage(root).free < SOURCE_POLICY["min_free_bytes"]:
        return {"status": "DISK_LIMIT"}

    spec = request("phase15-realtime-mark", "mark_now")
    requested = clock()
    reservation = db.execute("INSERT INTO mark_request(requested_at) VALUES (?)", (requested.isoformat(),)).lastrowid
    db.executemany("INSERT INTO mark_request_idea VALUES (?,?)", ((idea, reservation) for idea in idea_ids))
    db.commit()
    begin = time.monotonic()
    with httpx.Client(timeout=SOURCE_POLICY["io_timeout_seconds"], follow_redirects=False, transport=transport) as client:
        result = send(client, spec, SOURCE_POLICY["max_response_bytes"], root / "raw")
    request_elapsed = time.monotonic() - begin
    observation = {**result, "request_id": reservation, "requested_at": requested.isoformat(),
                   "source": SOURCE, "symbol": SYMBOL, "endpoint": spec["url"], "request_elapsed_seconds": request_elapsed}
    if result["status"] == "RECEIVED":
        try:
            payload = load_payload(root, result)
            if payload["granularity"] != 1000 or isinstance(payload["value"], bool):
                raise ValueError("Unexpected mark granularity or price")
            parsed = mark_quote(payload, utc(result["received_at"]))
            observation.update(source_at=parsed["at"], mark=parsed["mark"])
        except (KeyError, TypeError, ValueError, OSError, OverflowError):
            observation["status"] = "INVALID_MARK_RESPONSE"
    db.execute("UPDATE mark_request SET observation=? WHERE id=?", (encode(observation), reservation))
    db.commit()
    return observation


def _require_lane(book, lane):
    if (lane.get("execution_protocol") != VERSION or lane["book_id"] != book["book_id"]
            or lane["mode"]["trade_type"] != "FUTURES" or lane["mode"]["symbol"] != SYMBOL):
        raise ValueError("An independent XBTUSDTM realtime paper book is required")


def confirm(db, book, lane, analysis, now):
    _require_lane(book, lane)
    if analysis["state"] != "ELIGIBLE_CONDITIONAL_IDEA" or not analysis.get("plan"):
        return None
    atr = float(analysis["structure"]["atr"])
    if not isfinite(atr) or atr <= 0:
        raise ValueError("Confirmed breakout is missing its ATR")
    original = analysis["plan"]
    plan = {**original, "atr": atr, "execution_mode": MODE, "execution_protocol": VERSION,
            "entry": {**original["entry"], "paper_basis": MODE},
            "cancel_if": ["Entry expiry", "Stop invalidation", "Mark outside entry range after trigger",
                          "Insufficient risk or collateral capacity", "Current instrument rules unavailable or changed"]}
    return paper.publish(db, book, {**analysis, "plan": plan}, now)


def eligible(book, now):
    idea = book["pending"]
    return (idea is not None and book["position"] is None and not book["problem"]
            and utc(idea["plan"]["eligible_from"]) <= now < utc(idea["plan"]["entry_expiry"])
            and idea.get("mark_attempts", 0) < SOURCE_POLICY["attempts_per_idea"])


def _finish(db, book, now, kind, reason, **details):
    paper.event(db, book, kind, book["pending"]["id"], now, now,
                {"execution_mode": MODE, "reason": reason, **details})
    book["pending"], book["cursor"] = None, None
    return kind


def expire_pending(db, book, now, reason=None):
    idea = book["pending"]
    if not idea:
        return None
    deadline = utc(idea["plan"]["entry_expiry"])
    if now >= deadline:
        return _finish(db, book, now, "EXPIRED", idea.get("last_mark_rejection", "NO_FRESH_MARK_BEFORE_EXPIRY"),
                       expired_at=deadline.isoformat())
    if reason:
        return _finish(db, book, now, "NO_FILL", reason)
    return None


def _stamp(value):
    if not isinstance(value, (str, pd.Timestamp)):
        raise ValueError("Snapshot clocks require explicit UTC offsets")
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Snapshot clocks require explicit UTC offsets")
    return utc(stamp)


def _snapshot(observation, idea, decision):
    if not observation:
        return "MISSING_MARK_SNAPSHOT", None
    if not isinstance(observation, dict):
        return "INVALID_MARK_SNAPSHOT", None
    if observation.get("status") != "RECEIVED":
        return observation.get("status") or "MISSING_MARK_SNAPSHOT", None
    try:
        if (observation["source"] != SOURCE or observation["symbol"] != SYMBOL
                or not re.fullmatch(r"[a-f0-9]{64}", observation["raw_sha256"])):
            return "WRONG_MARK_SOURCE", None
        source, receipt, requested = (_stamp(observation[key]) for key in ("source_at", "received_at", "requested_at"))
        mark = observation["mark"]
        elapsed = observation.get("request_elapsed_seconds", 0.)
        if isinstance(mark, bool) or not isfinite(mark) or mark <= 0 or not isfinite(elapsed) or elapsed < 0:
            return "INVALID_MARK_SNAPSHOT", None
    except (KeyError, TypeError, ValueError, OverflowError):
        return "INVALID_MARK_SNAPSHOT", None
    if source > receipt or receipt > decision or requested > decision:
        return "FUTURE_MARK_TIMESTAMP", None
    if requested > receipt:
        return "INCONSISTENT_MARK_CLOCKS", None
    earliest = utc(idea["plan"]["eligible_from"])
    if requested < earliest or source < earliest or source <= utc(idea["published_at"]):
        return "MARK_PRECEDES_ELIGIBILITY", None
    if decision - source > MAX_AGE or receipt - requested > MAX_AGE or elapsed > MAX_AGE.total_seconds():
        return "STALE_MARK_SNAPSHOT", None
    return None, (float(mark), source)


def _entry_terms(book, lane, plan, metadata, config, mark, now):
    """Reprice risk and capacity at the exact reference while respecting the original ceilings."""
    try:
        stamp = _stamp(metadata["as_of"])
        if not pd.Timedelta(0) <= now - stamp <= pd.Timedelta(hours=config["ideas"]["maximum_metadata_age_hours"]):
            return "STALE_INSTRUMENT_RULES", None
        contract = Contract.load(plan["contract"])
        if (contract != current_rules("FUTURES", metadata, config) or contract.family != "linear_perp"
                or metadata["contract"]["symbol"] != SYMBOL):
            return "INSTRUMENT_RULES_CHANGED", None
        maximum = float(metadata["contract"]["marketMaxOrderQty"])
        if not isfinite(maximum) or maximum < contract.min_contracts:
            return "INVALID_INSTRUMENT_LIMITS", None
    except (KeyError, StopIteration, TypeError, ValueError, OverflowError):
        return "MISSING_INSTRUMENT_RULES", None
    order = Order(**plan["order"])
    risk = Risk(**lane["risk"])
    cost = Cost(plan["cost"]["taker"], plan["cost"]["adverse_proxy"])
    side, stop, atr = plan["direction"], plan["stop"], plan["atr"]
    leverage, cash = order.leverage, book["cash"]
    if leverage != risk.leverage or not 1 <= leverage <= min(contract.max_leverage, risk.ordinary_cap) or cash <= 0:
        return "INSUFFICIENT_MARGIN_OR_LEVERAGE", None
    if stop <= 0 or stop != quantize(stop, contract.price_tick):
        return "INVALID_STOP_TICK", None
    funding_per_base = mark * lane["execution"]["funding_reserve_bps"] / 10000
    liq = liquidation_price(mark, mark / leverage - funding_per_base, side, contract, cost.fee)
    buffer = side * (stop - liq)
    if buffer < risk.liquidation_buffer_atr * atr:
        return "STOP_TO_LIQUIDATION_BUFFER", None
    stop_execution = fill_price(stop, -side, contract, cost)
    stop_loss_per_base = side * (mark - stop_execution) + cost.fee * (mark + stop_execution)
    loss_per_base = stop_loss_per_base + funding_per_base
    if stop_execution <= 0 or loss_per_base <= 0:
        return "INVALID_MODELED_STOP_RISK", None
    target = paper.fixed_target(mark, stop, plan["target_r"], side, contract.price_tick)
    if target is not None and (target <= 0 or side * (target - mark) <= 0):
        return "INVALID_TARGET_PRICE", None
    risk_budget = min(order.risk_budget, cash * risk.risk_fraction)
    notional_cap = min(order.quote_cap, cash * risk.notional_cap, contract.tier_notional)
    max_base = min(risk_budget / loss_per_base, notional_cap / mark,
                   cash / (mark / leverage + mark * cost.fee + funding_per_base))
    contracts = quantize(min(order.contracts, maximum, max_base / contract.base_per_contract), contract.quantity_step)
    if contracts < contract.min_contracts:
        return "INSUFFICIENT_PAPER_CAPACITY", None
    quantity = contracts * contract.base_per_contract
    notional = quantity * mark
    fee, margin = notional * cost.fee, notional / leverage
    return None, {
        "contracts": contracts, "quantity": quantity, "notional": notional, "fee": fee, "margin": margin,
        "reference_taker_fee_rate": cost.fee, "reference_fee_basis": plan["cost"].get("basis"),
        "exit_adverse_rate": cost.adverse, "leverage": leverage, "risk_budget": risk_budget,
        "notional_cap": notional_cap, "market_max_contracts": maximum, "instrument_rules_as_of": stamp.isoformat(),
        "planned_risk": quantity * loss_per_base,
        "stop_execution_proxy": stop_execution, "modeled_stop_loss": quantity * stop_loss_per_base,
        "funding_reserve": quantity * funding_per_base, "projected_liquidation": liq, "liquidation_buffer": buffer,
        "required_liquidation_buffer": risk.liquidation_buffer_atr * atr,
        "target": target,
    }


def _reject(db, book, now, reason, observation):
    idea = book["pending"]
    previous = idea.get("last_mark_rejection")
    evidence = observation if isinstance(observation, dict) else {}
    request_id = evidence.get("request_id")
    if request_id or evidence.get("status") not in UNSENT:
        idea["mark_attempts"] = idea.get("mark_attempts", 0) + 1
    idea["last_mark_rejection"] = reason
    if reason != previous or request_id:
        paper.event(db, book, "ENTRY_EVIDENCE_REJECTED", idea["id"], now, now,
                    {"execution_mode": MODE, "reason": reason, "mark_request_id": request_id})


def consider(db, book, lane, metadata, config, observation, *, clock=utc_now):
    _require_lane(book, lane)
    decision = clock()
    expired = expire_pending(db, book, decision)
    if expired or not eligible(book, decision):
        return expired
    idea = book["pending"]
    plan = idea["plan"]
    if plan.get("execution_protocol") != VERSION:
        raise ValueError("Entry idea belongs to a different execution protocol")
    reason, quote = _snapshot(observation, idea, decision)
    if reason:
        _reject(db, book, decision, reason, observation)
        return None

    mark, source = quote
    side, stop = plan["direction"], plan["stop"]
    if side not in (-1, 1):
        raise ValueError("Invalid breakout direction")
    if side * (mark - stop) <= 0:
        return _finish(db, book, decision, "INVALIDATED", "MARK_CROSSED_STOP", observation=observation)
    if side * (mark - plan["entry"]["trigger"]) < 0:
        _reject(db, book, decision, "MARK_HAS_NOT_REACHED_TRIGGER", observation)
        return None
    if not plan["entry"]["minimum"] <= mark <= plan["entry"]["maximum"]:
        return _finish(db, book, decision, "INVALIDATED", "MARK_OUTSIDE_ENTRY_RANGE", observation=observation)
    reason, terms = _entry_terms(book, lane, plan, metadata, config, mark, decision)
    if reason:
        return _finish(db, book, decision, "NO_FILL", reason, observation=observation)

    # Timestamp the application commit immediately before the state transition. The caller
    # saves the position and this journal event together in one SQLite transaction.
    committed = clock()
    if committed < decision or committed - source > MAX_AGE:
        _reject(db, book, max(decision, committed), "MARK_EXPIRED_BEFORE_COMMIT", observation)
        return expire_pending(db, book, max(decision, committed))
    if expired := expire_pending(db, book, committed):
        return expired
    hold_cap = committed + pd.Timedelta(minutes=plan["max_hold_minutes"])
    if plan["session_flat"]:
        hold_cap = min(hold_cap, utc(plan["session_flat"]))
    # Minute-based exit protection uses the last minute open within the hold cap.
    deadline = hold_cap.floor(STEP)
    if deadline <= committed:
        return _finish(db, book, committed, "NO_FILL", "SESSION_ALREADY_FLAT", observation=observation)

    fee, margin, quantity, notional = (terms[key] for key in ("fee", "margin", "quantity", "notional"))
    pos = Position(side, quantity, mark, book["cash"] - fee, margin, fee, notional)
    entry_at = committed.isoformat()
    details = {**terms, "price": mark, "observed_mark": mark, "entry_basis": MODE, "execution_mode": MODE,
               "execution_protocol": VERSION,
               "entry_adverse_rate": 0., "liquidity_verified": False, "stop": stop, "entry_equity": book["cash"],
               "cash_after_entry_fee": pos.wallet, "available_cash_after_margin": pos.free,
               "deadline": deadline.isoformat(), "hold_cap_at": hold_cap.isoformat(), "exchange_source_at": source.isoformat(),
               "source_received_at": observation["received_at"], "paper_decision_at": decision.isoformat(),
               "entry_committed_at": entry_at, "source_age_seconds": (committed - source).total_seconds(),
               "mark_observation": observation}
    position = {**idea, "entry_at": entry_at, "raw_entry": mark, "entry_fee": fee, "entry_cash": book["cash"],
                "stop": stop, "target": terms["target"], "deadline": deadline.isoformat(), "last_trail": None,
                "planned_risk": terms["planned_risk"], "position": asdict(pos), "last_mark": mark,
                "mark_source_at": source.isoformat(), "mark_received_at": observation["received_at"],
                "entry_evidence": details}
    paper.event(db, book, "ENTRY_OBSERVED", idea["id"], source, committed, {"basis": MODE, **observation})
    paper.event(db, book, "HYPOTHETICAL_FILL", idea["id"], committed, committed, details)
    book["position"], book["pending"], book["cursor"] = position, None, committed.floor(STEP).isoformat()
    book["fees"] += fee
    book["turnover"] += notional
    paper.mark_account(book, pos, mark)
    return "HYPOTHETICAL_FILL"
