"""Prospective Kraken Spot EMA decisions over separately approved paper accounts."""

from contextlib import closing
from dataclasses import asdict, replace
import json
from math import isclose, isfinite
from pathlib import Path
import shutil
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.ema_paper import ledger
from lab.execution import execution_open
from lab.learning.simulator import Contract, Cost, Order, Risk, entry_fill, quantize, size_order
from lab.phase15 import paper
from lab.storage import utc, utc_now, writer_lock
from lab.strategy_lab import ema_pullback
from lab.strategy_lab.evaluator import ema_proposal
from lab.strategy_lab.feature_warmup import (
    ForwardEligibility, WarmupPolicy, WarmupSeed, decision_features, ema_indicators, freeze_seed, load_checkpoint,
    record_eligibility, save_checkpoint,
)
from lab.strategy_lab.market_data import ObservationKind, Purpose
from lab.strategy_lab.technical_features import FeatureCache, FeatureIdentity, IncrementalFeatures


MINUTE = pd.Timedelta(minutes=1)
TRADE = ObservationKind.TRADE_OHLCV
MIB = 1024**2
WORKER_CYCLE_SECONDS = 15


def _record(db, table, book_id):
    row = db.execute(f"SELECT body FROM {table} WHERE book=?", (book_id,)).fetchone()
    return json.loads(row[0]) if row else {}


def _save_status(db, book_id, state):
    db.execute("INSERT INTO readiness VALUES (?,?) ON CONFLICT(book) DO UPDATE SET body=excluded.body",
               (book_id, paper.encode(state)))


def _approved_grants(data, lane):
    required = (("warmup_grant_id", Purpose.WARMUP), ("forward_grant_id", Purpose.FORWARD_DECISION),
                ("execution_grant_id", Purpose.EXECUTION_EVIDENCE))
    policy, stamp = lane["source"], utc(lane["registered_at"])
    grants = tuple(data.grant(policy[key]) for key, _ in required)
    for grant, (_, purpose) in zip(grants, required):
        identity = grant.source
        if (grant.book_id != lane["book_id"] or grant.registered_at != stamp or grant.purpose != purpose
                or grant.kinds != {TRADE} or identity.id != policy["source_id"]
                or (identity.provider, identity.market, identity.instrument) != ("Kraken", "SPOT", "BTC-USD")):
            raise PermissionError("Source grants must bind this Kraken Spot book and separate paper purpose")
    seed, forward, execution = grants
    seam = utc(policy["seed_end"])
    if not (seed.end == seam == forward.start and execution.start <= seam < execution.end):
        raise PermissionError("The approved Kraken grants do not share a prospective seam")
    return grants


def _warmup_policy(lane, registration_id, start, content_id, step):
    source, spec = lane["source"], lane["spec"]
    chart_grace = pd.Timedelta(seconds=source["decision_receipt_seconds"])
    return WarmupPolicy(
        registration_id=registration_id, book_id=lane["book_id"], registered_at=lane["registered_at"],
        seed_grant_id=source["warmup_grant_id"], forward_grant_id=source["forward_grant_id"],
        seed_content_id=content_id, seed_start=start, seed_end=source["seed_end"],
        seed_native_step=step, timeframe=step, native_step=step, indicators=ema_indicators(),
        minimum_bars=source["warmup_bars"], chart_grace=chart_grace, native_grace=chart_grace,
        processing=pd.Timedelta(seconds=spec["execution"]["processing_seconds"]),
        publication_grace=pd.Timedelta(seconds=source["publication_max_seconds"]),
        horizon=pd.Timedelta(days=lane["horizon_days"]),
    )


def _trailing_start(snapshot, step):
    start = snapshot.end
    for row in reversed(snapshot.observations):
        if row.end != start:
            break
        start -= step
    return start


def _verify_seed(snapshot, seed, policy):
    frame = snapshot.frame()
    reference = ema_pullback.features(frame, policy.timeframe, policy.native_step)
    if reference.empty or not reference.iloc[-1]["ready"]:
        raise ValueError("Reviewed EMA reference did not qualify the feature-only seed")
    cursor = IncrementalFeatures(
        FeatureIdentity(snapshot.source.id, snapshot.content_id, "BTC-USD", "SPOT", policy.timeframe,
                        authorization_id=snapshot.grant_id),
        dict(policy.indicators),
    )
    incremental = cursor.update(frame, as_of=seed.initialized_at, expected_tip=cursor.tip).iloc[-1]
    for name, value in seed.values:
        if not (isclose(value, reference.iloc[-1][name], rel_tol=1e-13, abs_tol=1e-12)
                and isclose(value, incremental[name], rel_tol=1e-13, abs_tol=1e-12)):
            raise ValueError(f"Kraken warmup indicator parity failed for {name}")
    return cursor.tip


def _load_or_freeze_seed(db, lane, registration_id, state, data, grant, now, step, directory, cache):
    book_id = lane["book_id"]
    stored = _record(db, "warmup", book_id)
    if stored:
        policy = _warmup_policy(lane, registration_id, stored["start"], stored["seed_content_id"], step)
        seed = load_checkpoint(directory, stored["seed_id"])
        if not isinstance(seed, WarmupSeed) or seed.policy_id != policy.id:
            raise ValueError("Frozen Kraken warmup policy does not match its checkpoint")
        state.update(stored)
        return policy, seed
    seam = utc(lane["source"]["seed_end"])
    if now < seam:
        return None, None
    start = max(grant.start, seam - pd.Timedelta(seconds=lane["source"]["history_seconds"]))
    snapshot = data.read(grant.id, book_id, Purpose.WARMUP, TRADE, start, seam, now, step)
    start = _trailing_start(snapshot, step)
    count = int((seam - start) // step)
    state["warmup"] = {"completed": count, "required": lane["source"]["warmup_bars"]}
    if count < lane["source"]["warmup_bars"]:
        return None, None
    if start != snapshot.start:
        snapshot = data.read(grant.id, book_id, Purpose.WARMUP, TRADE, start, seam, now, step)
    policy = _warmup_policy(lane, registration_id, start, snapshot.content_id, step)
    seed = freeze_seed(data, policy, as_of=now, cache=cache)
    tip = _verify_seed(snapshot, seed, policy)
    seed_id = save_checkpoint(directory, seed)
    stored = {"seed_id": seed_id, "seed_content_id": snapshot.content_id, "warmup_snapshot_id": seed.snapshot_id,
              "warmup": {"completed": seed.bars, "required": policy.minimum_bars},
              "start": seed.start.isoformat(), "seed_end": seed.end.isoformat(),
              "initialized_at": seed.initialized_at.isoformat(), "warmup_values": dict(seed.values),
              "warmup_first_receipt_at": seed.first_provider_receipt_at.isoformat(),
              "warmup_last_receipt_at": seed.last_provider_receipt_at.isoformat(), "incremental_seed_tip": tip,
              "warmup_coverage": _receipt_coverage(snapshot)}
    db.execute("INSERT INTO warmup VALUES (?,?)", (book_id, paper.encode(stored)))
    state.update(stored)
    state.setdefault("coverage", stored["warmup_coverage"])
    return policy, seed


def _receipt_coverage(snapshot):
    return {
        "source_id": snapshot.source.id, "latest_close_at": snapshot.end.isoformat(),
        "latest_receipt_at": max(row.available_at for row in snapshot.observations).isoformat(),
        "latest_provider_received_at": max(row.received_at for row in snapshot.observations).isoformat(),
        "latest_handoff_at": max(row.available_at for row in snapshot.observations).isoformat(),
        "snapshot_id": snapshot.id,
    }


def _instrument(lane, at, equity):
    terms = lane["instrument_terms"]
    if utc(terms["received_at"]) > at:
        raise ValueError("Dated Kraken instrument terms were unavailable at the decision")
    if (lane["spec"]["market"], lane["spec"]["instrument"], lane["spec"]["venue"]) != ("SPOT", "BTC-USD", "Kraken"):
        raise ValueError("Kraken paper runtime is restricted to BTC/USD Spot")
    if not isfinite(terms["min_quote"]) or terms["min_quote"] <= 0:
        raise ValueError("Kraken Spot minimum quote is invalid")
    return Contract.load({
        "family": "spot", "quote": "USD", "base_per_contract": 1, "quantity_step": terms["quantity_step"],
        "price_tick": terms["price_tick"], "min_contracts": terms["min_quantity"], "tier_notional": equity,
        "max_leverage": 1, "maintenance_rate": 0., "liquidation_fee_rate": 0.,
    })


def make_plan(lane, setup, equity, now, eligibility, coverage):
    """Build one cash-only hypothetical IOC from reviewed USD rules."""
    if setup.direction != 1 or setup.source_allocation or lane["risk"]["leverage"] != 1:
        return None, "KRAKEN_SPOT_LONG_CASH_ONLY"
    entry = execution_open(now, pd.Timedelta(seconds=lane["execution_clock"]["entry_seconds"]))
    if not setup.decision_at <= setup.available_at <= now < entry < eligibility.primary_end_exclusive:
        return None, "ENTRY_OUTSIDE_PRIMARY_HORIZON"
    contract = _instrument(lane, setup.decision_at, equity)
    fees = lane["cost"]
    cost = Cost(fees["taker"], fees["adverse_proxy"])
    order, reason = size_order(setup, contract, cost, Risk(**lane["risk"]), equity, lane["spec"]["execution"])
    if order is None:
        return None, reason
    stop = quantize(setup.stop, contract.price_tick)
    target = quantize(setup.target_price, contract.price_tick, upward=True)
    maximum = min(target, order.limit_price)
    if not 0 < stop < setup.reference < target or maximum <= stop:
        return None, "INVALID_ROUNDED_KRAKEN_BRACKET"
    if order.contracts * order.limit_price < lane["instrument_terms"]["min_quote"]:
        return None, "BELOW_KRAKEN_MINIMUM_QUOTE"
    hold = lane["spec"]["session"]["max_hold_minutes"]
    return {
        "direction": 1, "eligible_from": entry.isoformat(), "entry_expiry": (entry + MINUTE).isoformat(),
        "entry": {"minimum": stop, "maximum": maximum, "reference": setup.reference, "trigger": setup.reference},
        "entry_basis": "TRADE_MINUTE_OPEN_WITH_ADVERSE", "stop": stop, "target": target,
        "signal_stop": setup.stop, "signal_target": setup.target_price, "target_r": None, "trailing_atr": None,
        "target_basis": "CONFIRMATION_CLOSE", "signal_at": setup.decision_at.isoformat(),
        "signal_available_at": setup.available_at.isoformat(),
        "trigger_basis": "TRADE", "exit_execution_basis": "TRADE", "session_flat": None, "max_hold_minutes": hold,
        "contract": asdict(contract), "cost": fees, "order": asdict(order),
        "sizing": {"contracts": order.contracts, "planned_risk": order.planned_risk, "risk_budget": order.risk_budget,
                   "base": order.contracts, "leverage": order.leverage},
        "execution_terms": {"min_quote": lane["instrument_terms"]["min_quote"]}, "evidence": coverage,
    }, None


def _missing_minute(db, book, at, now, reason):
    if book["pending"]:
        paper.expire(db, book, at, now, reason)
    elif book["position"]:
        paper.problem(db, book, reason, at, now, permanent=True)


def _minute_row(row):
    return {**row.values, "open_at": row.start.isoformat(), "close_at": row.end.isoformat(),
            "received_at": row.received_at.isoformat(), "observed_at": row.available_at.isoformat(),
            "raw_sha256": row.raw_sha256}


def _advance(db, lane, book, data, grant, now, follow, deadline):
    if not book["cursor"] or book["permanent_block"]:
        return
    at = utc(book["cursor"])
    if at >= follow:
        _missing_minute(db, book, at, now, "Kraken follow-through ended with an occupied paper account")
        return
    grace = pd.Timedelta(seconds=lane["source"]["price_receipt_seconds"])
    right = min(at + lane["limits"]["minutes_per_book"] * MINUTE, follow, now.floor(MINUTE), grant.end)
    if at >= right:
        if now > at + MINUTE + grace and at >= grant.end:
            _missing_minute(db, book, at, now, "Kraken execution grant ended before the pending minute")
        return
    snapshot = data.read(grant.id, lane["book_id"], Purpose.EXECUTION_EVIDENCE, TRADE, at, right, now, MINUTE)
    rows = {row.start: row for row in snapshot.observations}
    while book["cursor"] and not book["permanent_block"] and at < right and time.monotonic() < deadline:
        row = rows.get(at)
        if row is None or row.available_at > row.end + grace or row.values["volume"] <= 0:
            if now > at + MINUTE + grace:
                _missing_minute(db, book, at, now, "Kraken minute trade evidence missing, late or without volume")
            break
        record = book["pending"] or book["position"]
        plan = record["plan"]
        contract = Contract.load(plan["contract"])
        if (contract.family, contract.quote, plan["direction"], plan["trigger_basis"]) != ("spot", "USD", 1, "TRADE"):
            raise ValueError("Stored paper order is outside the registered Kraken Spot model")
        if book["pending"]:
            cost = Cost(plan["cost"]["taker"], plan["cost"]["adverse_proxy"])
            quantity, _ = entry_fill(Order(**plan["order"]), row.values["open"], 1, contract, cost, 1.)
            if quantity > row.values["volume"]:
                paper.expire(db, book, at, now, "Modeled entry size exceeds observed total minute trade volume",
                             "INVALIDATED", reason_code="KRAKEN_MINUTE_VOLUME_CAP",
                             source_received_at=row.received_at.isoformat())
                break
        if not ledger.advance(db, book, at, now, _minute_row(row), None):
            break
        at += MINUTE


def _decision(db, lane, book, state, data, grant, policy, seed, eligibility, at, now, cache):
    if now > at + policy.publication_grace:
        state.update(last_evaluated_at=at.isoformat(), last_signal={"decision": "NO_TRADE", "decision_at": at.isoformat(),
                                                                  "reason": "Publication window elapsed"})
        return
    feature = decision_features(data, policy, seed, eligibility, decision_at=at, as_of=now, cache=cache)
    current, prior = dict(feature.current), dict(feature.previous)
    row = pd.Series({**current, **{f"prior_{key}": prior[key] for key in ("ema_fast", "low", "high", "close")},
                     "ready": True}, dtype=object)
    spec = lane["spec"]
    setup, gates = ema_proposal(row, at, spec, spec["session"], spec["execution"], spec["recipe"], policy.timeframe,
                                bool(book["pending"] or book["position"] or book["problem"]))
    read = data.read(grant.id, lane["book_id"], Purpose.FORWARD_DECISION, TRADE,
                     at - policy.timeframe, at, now, policy.native_step)
    coverage = _receipt_coverage(read)
    availability = max(at, feature.evidence_available_at) + policy.processing
    signal = {"decision": "SIGNAL_ONLY" if all(gates["short"].values()) else "NO_TRADE",
              "direction": "SHORT" if all(gates["short"].values()) else None,
              "gates": gates, "values": current, "decision_at": at.isoformat(), "observed_at": now.isoformat(),
              "available_at": availability.isoformat(), "input_hash": feature.snapshot_id}
    state.update(state="ACTIVE", reason="Qualified forward Kraken Spot evidence", coverage=coverage,
                 last_evaluated_at=at.isoformat(), last_signal=signal)
    if setup is None:
        return
    setup = replace(setup, available_at=availability)
    plan, reason = make_plan(lane, setup, book["equity"], now, eligibility, coverage)
    if plan is None:
        signal["reason"] = reason
        return
    idea = paper.publish(db, book, {
        "state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": at, "direction": "LONG", "plan": plan,
        "strategy_version": lane["strategy_hash"], "exit_policy": setup.exit_policy,
    }, now)
    if idea:
        signal.update(decision="PROPOSAL", direction="LONG", idea_id=idea["id"])


def _review_instrument(lane, evidence, now):
    status = evidence["state"]
    if status not in {"READY", "MISMATCH"}:
        return status
    terms = evidence["terms"]
    if not now - pd.Timedelta(days=1) < utc(terms["received_at"]) <= utc(evidence["available_at"]) <= now:
        return "STALE"
    fields = ("price_tick", "quantity_step", "min_quantity", "min_quote")
    if status == "MISMATCH" or any(terms[field] != lane["instrument_terms"][field] for field in fields):
        return "MISMATCH"
    return "READY"


def _cycle_book(db, registration, data, now, directory, deadline, cache, instrument):
    lane, registration_id = registration["body"], registration["id"]
    key = lane["book_id"]
    state = _record(db, "readiness", key)
    state.update(last_checked_at=now.isoformat(), source_permission="APPROVED")
    book = paper.load(db, lane)
    step = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
    processing = pd.Timedelta(seconds=lane["spec"]["execution"]["processing_seconds"])
    at = (now - processing).floor(step)
    state["next_evaluation_at"] = (execution_open(at, step) + processing).isoformat()
    try:
        warm, forward, execution = _approved_grants(data, lane)
        policy, seed = _load_or_freeze_seed(db, lane, registration_id, state, data, warm, now, step, directory, cache)
        evidence_status = _review_instrument(lane, instrument, now)
        state["instrument_status"] = evidence_status
        if seed is None:
            state.update(state="WAITING_FOR_DATA", reason="Awaiting 200 continuous approved closed Kraken warmup bars")
        elif evidence_status != "READY":
            changed = evidence_status == "MISMATCH"
            reason = ("Kraken Spot instrument terms differ from the frozen paper model" if changed else
                      "Awaiting current verified Kraken Spot instrument terms")
            if changed and book["cursor"]:
                _missing_minute(db, book, utc(book["cursor"]), now, reason)
            state.update(state="BLOCKED_SOURCE" if changed else "WAITING_FOR_DATA", reason=reason)
        else:
            fixed = _record(db, "eligibility", key)
            if fixed:
                eligibility = load_checkpoint(directory, fixed["eligibility_id"])
                if (not isinstance(eligibility, ForwardEligibility) or eligibility.seed_id != seed.id
                        or eligibility.first_eligible_at.isoformat() != fixed["first_eligible_at"]):
                    raise ValueError("Frozen eligibility does not match the Kraken book checkpoint")
                follow = utc(fixed["follow_end_exclusive"])
                _advance(db, lane, book, data, execution, now, follow, deadline)
                if now >= eligibility.primary_end_exclusive:
                    closed = db.execute("SELECT COUNT(*) FROM event WHERE book=? AND kind='CLOSED'", (key,)).fetchone()[0]
                    state.update(state="COMPLETE" if now >= follow else "FOLLOW_THROUGH",
                                 reason="Frozen prospective horizon ended", next_evaluation_at=None,
                                 assessment="INSUFFICIENT_EVIDENCE" if closed < lane["min_complete_trades"] else
                                 "UNRESOLVED" if book["problem"] or book["position"] else "REVIEW_REQUIRED")
                elif book["problem"]:
                    state.update(state="BLOCKED_EXECUTION_EVIDENCE", reason=book["problem"])
                elif at < eligibility.first_eligible_at:
                    state.update(state="WAITING_FOR_START", reason="Awaiting the frozen future decision boundary",
                                 next_evaluation_at=(eligibility.first_eligible_at + processing).isoformat())
                elif state.get("last_evaluated_at") != at.isoformat():
                    state.update(state="ACTIVE", reason="Awaiting the next complete forward decision")
                    _decision(db, lane, book, state, data, forward, policy, seed, eligibility, at, now, cache)
            else:
                state.update(state="WAITING_FOR_DATA", reason="Awaiting the independently received Kraken forward bridge")
                if now >= seed.end + step + processing:
                    _instrument(lane, seed.end, book["equity"])
                    follow_delta = pd.Timedelta(minutes=lane["follow_through_minutes"])
                    if execution.end < seed.end + 2 * step + policy.horizon + follow_delta:
                        raise PermissionError("Execution grant does not cover the full frozen follow-through")
                    eligibility = record_eligibility(directory, data, policy, seed, as_of=now, cache=cache)
                    follow = eligibility.primary_end_exclusive + follow_delta
                    fixed = {"eligibility_id": eligibility.id, "source_ready_at": eligibility.recorded_at.isoformat(),
                             "first_eligible_at": eligibility.first_eligible_at.isoformat(),
                             "primary_end_exclusive": eligibility.primary_end_exclusive.isoformat(),
                             "follow_end_exclusive": follow.isoformat(), "source_id": lane["source"]["source_id"]}
                    db.execute("INSERT INTO eligibility VALUES (?,?)", (key, paper.encode(fixed)))
                    bridge = data.read(forward.id, key, Purpose.FORWARD_DECISION, TRADE, seed.end,
                                       seed.end + step, eligibility.recorded_at, step)
                    state.update(state="WAITING_FOR_START", reason="Qualified Kraken bridge; awaiting the future boundary",
                                 coverage=_receipt_coverage(bridge),
                                 next_evaluation_at=(eligibility.first_eligible_at + processing).isoformat())
            state.update(fixed)
    except PermissionError as exc:
        state.update(state="BLOCKED_PERMISSION", source_permission="BLOCKED", reason=str(exc))
    except (ValueError, LookupError, OSError) as exc:
        state.update(state="WAITING_FOR_DATA", reason=str(exc))
    _save_status(db, key, state)
    paper.save(db, book)
    return state


def _blocked(db, records, now, state, reason):
    result = {}
    for profile, record in records.items():
        key = record["body"]["book_id"]
        value = _record(db, "readiness", key)
        value.update(state=state, reason=reason, last_checked_at=now.isoformat())
        if state == "BLOCKED_PERMISSION":
            value["source_permission"] = "BLOCKED"
        _save_status(db, key, value)
        result[profile] = value
    return {"state": state, "reason": reason, "books": result}


def cycle(*, now=None, root=None, source_root=None):
    """Evaluate one bounded paper-only cycle without exchange credentials or requests."""
    from lab.kraken_paper import catalog, retirement, source

    root = catalog.ROOT if root is None else Path(root)
    database = root / "paper.sqlite3"
    if not database.is_file():
        return {"state": "UNREGISTERED", "books": {}}
    if retirement.history(root=root):
        return {"state": retirement.STATE, "books": {}}
    limits = catalog.LIMITS
    process = psutil.Process()
    if (psutil.virtual_memory().available < limits["min_available_mib"] * MIB
            or shutil.disk_usage(root).free < limits["min_free_disk_mib"] * MIB
            or database.stat().st_size >= limits["max_ledger_mib"] * MIB
            or process.memory_info().rss >= limits["worker_rss_mib"] * MIB):
        return {"state": "WAITING_FOR_RESOURCES", "books": {}}
    started = time.monotonic()
    deadline = started + min(WORKER_CYCLE_SECONDS, limits["cycle_seconds"])
    now = utc_now() if now is None else utc(now)
    with writer_lock(root / ".cycle.lock"), closing(catalog.db_open(root=root)) as db, threadpool_limits(limits=1):
        if retirement.audit(db):
            return {"state": retirement.STATE, "books": {}}
        records = catalog.registrations(db)
        try:
            catalog.approved_authorization(db, at=now)
            grants = catalog.grants(db)
        except (PermissionError, ValueError, OSError) as exc:
            with db:
                return _blocked(db, records, now, "BLOCKED_PERMISSION", str(exc))
        source_root = source_root or root / "source"
        data = source.market_data(grants, root=source_root)
        instrument = source.instrument_evidence(grants, as_of=now, root=source_root)
        cache = FeatureCache(max_entries=24, max_bytes=16 * MIB)
        result = {}
        for profile, record in records.items():
            if time.monotonic() >= deadline or process.memory_info().rss >= limits["worker_rss_mib"] * MIB:
                result[profile] = {"state": "WAITING_FOR_RESOURCES"}
                continue
            with db:
                result[profile] = _cycle_book(db, record, data, now, root / "warmup", deadline, cache, instrument)
        return {"state": "CHECKED", "checked_at": now.isoformat(), "books": result,
                "rss_mib": round(process.memory_info().rss / MIB, 1), "wall_seconds": round(time.monotonic() - started, 3)}


def status(*, root=None):
    """Read Kraken paper accounts without advancing the source or paper clocks."""
    from lab.kraken_paper import catalog, retirement

    root = catalog.ROOT if root is None else Path(root)
    if not (root / "paper.sqlite3").is_file():
        return {"state": "UNREGISTERED", "books": {}}
    with closing(catalog.db_open(True, root=root)) as db:
        if retired := retirement.audit(db):
            return {"state": retirement.STATE, "books": retired}
        return {"state": "REGISTERED", "books": {
            profile: {**_record(db, "readiness", item["body"]["book_id"]), "registration": item,
                      "account": {**ledger.report(db, item["body"]), "currency": "USD"}}
            for profile, item in catalog.registrations(db).items()
        }}
