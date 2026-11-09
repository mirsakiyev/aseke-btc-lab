"""Delayed hypothetical execution of durable, precommitted bar-based intents."""

import pandas as pd

from lab.config import Execution, Profile
from lab.execution import adverse
from lab.storage import utc

from lab.phase2 import store

MISSING_GRACE = pd.Timedelta("90s")


def price_evidence(bar, observed, profile: Profile) -> dict:
    close = bar.name + profile.step
    late = max(bar.received_at, observed) > close + MISSING_GRACE
    return {
        "execution_bar_open": bar.name, "bar_close": close, "market_received_at": bar.received_at,
        "resolved_at": observed, "data_revision_id": int(bar.revision_id), "data_raw_sha256": bar.raw_sha256,
        "observation_quality": "reconstructed_precommitted" if late else "timely_closed_bar",
        "execution_type": "delayed_hypothetical_bar_fill",
    }


def apply_bar(db, profile: Profile, state: dict, bar: pd.Series, observed: pd.Timestamp) -> None:
    """Advance one observed bar. Caller persists account and events in the same transaction."""
    at = bar.name
    intent = state.get("pending_entry")
    if intent is not None and utc(intent["fill_at"]) == at:
        state["pending_entry"] = None
        cost = Execution(**intent["costs"])
        price = adverse(float(bar.open), cost, buy=True)
        notional = intent["units_btc"] * price
        fee = notional * cost.fee
        details = {"intent": intent, "modeled_fill_at": at, **price_evidence(bar, observed, profile)}
        if notional + fee > state["cash"]:
            store.event(db, profile.id, f"entry-reject:{at}", "entry_rejected_cash", observed, details)
            return
        state["cash"] -= notional + fee
        state["fees_usd"] += fee
        state["entry_fills"] += 1
        state["position"] = {
            "intent": intent, "entry_price": price, "entry_notional": notional, "entry_fee": fee,
            "entry_at": at.isoformat(), "next_open": at.isoformat(), "signal_exit": None,
            "entry_observation_quality": details["observation_quality"],
        }
        stop_price = adverse(intent["stop"], cost, buy=False)
        realized_stop_risk = notional + fee - intent["units_btc"] * stop_price * (1 - cost.fee)
        store.event(db, profile.id, f"entry:{at}", "entry_fill", observed, {
            **details, "raw_open": float(bar.open), "adverse_price": price,
            "notional_usd": notional, "fee_usd": fee,
            "allocation_overrun_usd": max(0, notional - intent["allocation_cap_usd"]),
            "fill_risk_overrun_usd": max(0, realized_stop_risk - intent["risk_budget_usd"]),
        })
    position = state.get("position")
    if position is None:
        return
    plan = position["intent"]
    costs = Execution(**plan["costs"])
    signal_exit = position.get("signal_exit")
    if bar.open <= plan["stop"]:
        raw, when, reason = float(bar.open), at, "stop_open"
    elif at == utc(plan["time_exit"]):
        raw, when, reason = float(bar.open), at, plan["time_reason"]
    elif signal_exit and signal_exit.get("confirmed_at") and at == utc(signal_exit["fill_at"]):
        raw, when, reason = float(bar.open), at, "signal"
    elif bar.low <= plan["stop"]:
        raw, when, reason = plan["stop"], at + profile.step, "stop_intrabar"
    else:
        state["mark_price"], state["mark_at"] = float(bar.close), (at + profile.step).isoformat()
        position["next_open"] = (at + profile.step).isoformat()
        return
    price = adverse(raw, costs, buy=False)
    notional = plan["units_btc"] * price
    fee = notional * costs.fee
    net = notional - fee - position["entry_notional"] - position["entry_fee"]
    state["cash"] += notional - fee
    state["fees_usd"] += fee
    state["realized_usd"] += net
    state["closed_trades"] += 1
    evidence = price_evidence(bar, observed, profile)
    if evidence["observation_quality"] != "timely_closed_bar" or (
        position["entry_observation_quality"] != "timely_closed_bar" or position.get("missed_signal_exit")
    ):
        state["reconstructed_or_uncertain_trades"] += 1
    store.event(db, profile.id, f"exit:{at}", "exit_fill", observed, {
        **evidence, "modeled_fill_at": when, "reason": reason, "originating_intent": plan,
        "raw_price": raw, "adverse_price": price, "units_btc": plan["units_btc"], "fee_usd": fee,
        "realized_usd": net, "realized_loss_overrun_usd": max(0, -net - plan["risk_budget_usd"]),
        "deadline": plan["deadline"], "deadline_status": "met" if when <= utc(plan["deadline"]) else "breached",
        "missed_signal_exit": position.get("missed_signal_exit", False),
    })
    state["position"] = None
    state["mark_price"], state["mark_at"] = float(bar.close), (at + profile.step).isoformat()


def advance(db, profile: Profile, state: dict, boundary: pd.Timestamp, through: pd.Timestamp,
            now: pd.Timestamp) -> None:
    """Recover precommitted events in order. A missing bar freezes uncertain exposure."""
    pending, position = state.get("pending_entry"), state.get("position")
    if state.get("unresolved"):
        blocked = state["unresolved"]
        if now >= utc(blocked["intent"]["deadline"]) and blocked["deadline_status"] != "unresolved_at_deadline":
            with db:
                blocked["deadline_status"] = "unresolved_at_deadline"
                store.event(db, profile.id, "unresolved-deadline", "unresolved_deadline", now, blocked)
                store.set_account(db, profile.id, state)
        return
    if pending and not confirmed_before_open(pending):
        with db:
            state["pending_entry"] = None
            store.event(db, profile.id, f"unconfirmed:{pending['fill_at']}", "unconfirmed_entry_not_filled", now, pending)
            store.set_account(db, profile.id, state)
        return
    if position and position.get("signal_exit") and not confirmed_before_open(position["signal_exit"]):
        with db:
            intent = position["signal_exit"]
            position["signal_exit"], position["missed_signal_exit"] = None, True
            store.event(db, profile.id, f"unconfirmed-exit:{intent['fill_at']}", "unconfirmed_exit", now, intent)
            store.set_account(db, profile.id, state)
    if not (pending or position):
        return
    first = utc(pending["fill_at"] if pending else position["next_open"])
    through = min(through, now.floor(profile.step), first + 512 * profile.step)
    if first + profile.step > through:
        return
    candles = store.bars(db, profile.seconds, first, through)
    scanned = store.checkpoint(db, profile.seconds, boundary)
    for at in pd.date_range(first, through, freq=profile.step, inclusive="left"):
        if at not in candles.index:
            if now < at + profile.step + MISSING_GRACE:
                return
            with db:
                if state.get("pending_entry"):
                    # The entry could have filled at the unobserved open. Freeze the account instead of assuming cash.
                    affected = state["pending_entry"]
                    kind = "unresolved_entry"
                else:
                    affected = state["position"]["intent"]
                    kind = "unresolved_open_position"
                state["unresolved"] = {
                    "kind": kind, "missing_open": at, "scanned_through": scanned, "intent": affected,
                    "deadline_status": "unresolved_at_deadline" if now >= utc(affected["deadline"]) else "unresolved",
                }
                store.event(db, profile.id, f"gap:{at}", kind, now, state["unresolved"])
                store.set_account(db, profile.id, state)
            return
        with db:
            apply_bar(db, profile, state, candles.loc[at], now)
            store.set_account(db, profile.id, state)
        if not (state.get("pending_entry") or state.get("position")):
            return


def confirmed_before_open(intent: dict) -> bool:
    return bool(intent.get("confirmed_at") and utc(intent["confirmed_at"]) < utc(intent["fill_at"]))


def account_view(state: dict) -> dict:
    position = state.get("position")
    unrealized = 0.0
    btc = 0.0
    if position:
        btc = position["intent"]["units_btc"]
        mark = state.get("mark_price") or position["entry_price"]
        unrealized = btc * mark - position["entry_notional"] - position["entry_fee"]
    return {**state, "known_units_btc": btc, "unrealized_usd_at_last_mark": unrealized,
            "equity_usd_at_last_mark": state["starting_cash"] + state["realized_usd"] + unrealized,
            "equity_uncertain": bool(state.get("unresolved"))}
