"""Kraken last-trade evidence and EMA signal/accounting rules for successor books."""

from contextlib import closing, nullcontext
from dataclasses import asdict, replace
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from math import ceil, isfinite
from pathlib import Path
import sqlite3
import time

import httpx
import pandas as pd

from lab.config import fingerprint
from lab.ema_paper import catalog, chart_recovery, kraken_reference, position_seam, recovery_protocol
from lab.ema_paper import synthetic_protocol, synthetic_runtime, worker
from lab.kraken_paper import source as kraken_source
from lab.learning.simulator import Contract, Order, liquidation_price, quantize
from lab.phase15 import paper
from lab.storage import utc, utc_now, writer_lock
from lab.strategy_lab import feature_warmup as warmup, synthetic_btc
from lab.strategy_lab.ema_execution import inside_bracket
from lab.strategy_lab.evaluator import proposal
from lab.strategy_lab.market_data import Purpose


SOURCE = "KRAKEN_BTC_USD_LAST_TRADE"
SYMBOL = "BTC/USD"
TRADE_ENDPOINT = "https://api.kraken.com/0/public/Trades"
TRADE_PARAMS = {"pair": "XBTUSD", "count": 1}
MAX_RESPONSE_BYTES = 8192
MAX_OBSERVATION_AGE = pd.Timedelta(seconds=2)
LEVERAGED_MODE = "REALTIME_SYNTHETIC_SPOT_REFERENCE_PAPER"
CASH_MODE = "REALTIME_CASH_SPOT_REFERENCE_PAPER"
SOURCE_ERRORS = (OSError, ValueError, KeyError, sqlite3.Error)
FINANCING_BASIS = "SYNTHETIC_FINANCING_DEBIT"


def init(db):
    """Install EMA feature-only tables in the successor's standalone ledger."""
    db.executescript(synthetic_runtime.SCHEMA)
    chart_recovery.install_schema(db)


def _trade(raw):
    payload = json.loads(raw, parse_float=Decimal)
    if payload.get("error") != []:
        raise ValueError("Kraken returned an unsuccessful Trades response")
    result = payload["result"]
    if set(result) != {"XXBTZUSD", "last"}:
        raise ValueError("Expected exactly the reviewed Kraken BTC/USD Spot pair")
    row, = result["XXBTZUSD"]
    if len(row) != 7 or not isinstance(row[0], str) or type(row[6]) is not int or row[6] <= 0:
        raise ValueError("Kraken trade is missing the price or exchange trade ID")
    if isinstance(row[2], bool) or not isinstance(row[2], (int, Decimal)):
        raise ValueError("Kraken trade is missing a numeric exchange timestamp")
    try:
        price = Decimal(row[0])
        stamp = Decimal(row[2]) * 1_000_000_000
        if not price.is_finite() or price <= 0 or not isfinite(float(price)) or stamp != stamp.to_integral_value():
            raise ValueError("Invalid Kraken reference price or exchange timestamp")
        source_at = pd.Timestamp(int(stamp), unit="ns", tz="UTC")
    except (InvalidOperation, OverflowError) as exc:
        raise ValueError("Invalid Kraken reference price or exchange timestamp") from exc
    return {"source": SOURCE, "symbol": SYMBOL, "price": float(price), "price_text": row[0],
            "source_at": source_at.isoformat(), "event_id": str(row[6]), "endpoint": TRADE_ENDPOINT,
            "raw_sha256": sha256(raw).hexdigest()}


def observe(*, clock=utc_now, monotonic=time.monotonic_ns, transport=None, source_root=None,
            pause=time.sleep, previous_request_at=None):
    """Perform one permission-checked public read under the existing source lock.

    The caller owns call frequency and rate-limit backoff. A failed probe raises
    instead of returning a candle, server time, or an unverified reference.
    """
    root = kraken_reference.ROOT if source_root is None else Path(source_root)
    at = utc(clock())
    identity = kraken_reference.scope_identity(root=root, as_of=at)
    store = root / "source"
    if not (store / kraken_source.FILE).is_file():
        raise PermissionError("Kraken reviewed source and shared request clock are unavailable")
    lock = store / ".kraken-source.lock"
    with writer_lock(lock), closing(kraken_source._read_connection(store)) as db:
        instrument = kraken_source._instrument_result(kraken_source._latest_instrument(db, identity.id, at), at)
        if instrument["state"] != "READY":
            raise PermissionError(f"Current Kraken BTC/USD Spot terms unavailable ({instrument['state']})")
        raw_terms = db.execute("SELECT body FROM raw WHERE sha=?", (instrument["raw_sha256"],)).fetchone()
        if not raw_terms or sha256(raw_terms[0]).hexdigest() != instrument["raw_sha256"]:
            raise PermissionError("Current Kraken BTC/USD Spot terms failed raw checksum verification")
        last, retry, _, _ = kraken_source._rate_state(db, identity.id)
        requested = utc(clock())
        recent = max(pd.Timestamp(last, unit="ns", tz="UTC"),
                     utc(previous_request_at) if previous_request_at is not None else pd.Timestamp(0, unit="ns", tz="UTC"))
        if requested.value < retry or requested - recent < kraken_source.MIN_REQUEST_SPACING:
            raise ValueError("Kraken shared request spacing or rate backoff is active")
        sent_ns = monotonic()
        try:
            with httpx.Client(timeout=1.25, follow_redirects=False, transport=transport) as client:
                with client.stream("GET", TRADE_ENDPOINT, params=TRADE_PARAMS,
                                   headers={"Accept": "application/json", "Cache-Control": "no-cache"}) as response:
                    response.raise_for_status()
                    raw = bytearray()
                    for part in response.iter_bytes():
                        raw.extend(part)
                        if len(raw) > MAX_RESPONSE_BYTES or monotonic() - sent_ns > MAX_OBSERVATION_AGE.value:
                            raise ValueError("Kraken Trades exceeded the response size or time budget")
                    received_ns = monotonic()
                    received = utc(clock())
            result = _trade(bytes(raw))
        finally:
            # Leave the same minimum separation for a following OHLC request.
            remaining = kraken_source.MIN_REQUEST_SPACING.value - (monotonic() - sent_ns)
            if remaining > 0:
                pause(remaining / 1e9)
    return {**result, "received_at": received.isoformat(), "requested_at": requested.isoformat(),
            "elapsed_ns": received_ns - sent_ns, "received_mono_ns": received_ns, "request_mono_ns": sent_ns}


def lanes(*, now=None):
    """Resolve the three immutable EMA predecessors and their reviewed chart scopes."""
    at = utc_now() if now is None else utc(now)
    originals = catalog.registrations()
    if set(originals) != set(catalog.PROFILES):
        raise ValueError("Exactly the three frozen EMA registrations are required")
    synthetic = synthetic_protocol.load(now=at)
    recovery = recovery_protocol.load(synthetic, now=at) if recovery_protocol.active() else None
    records = dict(synthetic["body"]["books"])
    if position_seam.active():
        if not recovery:
            raise PermissionError("The frozen EMA Position seam requires its chart recovery amendment")
        records["position"] = position_seam.load(synthetic, recovery, originals["position"], now=at)["body"]["position"]
    grants = synthetic_protocol.grants({**synthetic, "body": {**synthetic["body"], "books": records}})
    result = {}
    for profile in catalog.PROFILES:
        original, revision = originals[profile], records[profile]
        lane = original["body"]
        if (revision["body"]["registration_id"] != original["id"] or revision["body"]["book_id"] != lane["book_id"]
                or revision["body"]["strategy_hash"] != lane["strategy_hash"]):
            raise PermissionError("EMA strategy or reviewed chart scope differs from its frozen registration")
        scope = next(g for g in grants if g.book_id == lane["book_id"] and g.purpose == Purpose.FORWARD_DECISION)
        mode = CASH_MODE if profile == "position" else LEVERAGED_MODE
        result[f"ema/{profile}"] = {**lane, "source_policy": lane["source"], "source": SOURCE, "symbol": SYMBOL,
            "label": f"EMA {profile.title()}", "family": "EMA", "timeframe": lane["spec"]["timeframe"],
            "execution_mode": mode, "entry_model": mode, "exit_model": mode, "price_basis": SOURCE,
            "long_only": profile == "position",
            "legacy_book_id": lane["book_id"], "legacy_registration": original["id"],
            "feature_revision": revision, "feature_recovery": recovery["id"] if recovery else None,
            "feature_source_id": scope.source.id, "feature_scope_end": scope.end.isoformat(),
            "price_unit": "USD", "ledger_unit": "NOMINAL_USDT", "telegram_lifecycle": False,
            "permissions": {**lane["permissions"], "telegram_lifecycle": False}}
    return result


def _reuse_feature_state(db, seed_path, source_book):
    """Import only immutable chart context from the verified predecessor ledger."""
    with closing(sqlite3.connect(Path(seed_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as prior:
        for table, columns in (("synthetic_seed", "?,?,?"), ("chart_recovery_event", "?,?,?,?,?,?")):
            if not prior.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                continue
            rows = prior.execute(f"SELECT * FROM {table} WHERE book=?", (source_book,)).fetchall()
            db.executemany(f"INSERT OR IGNORE INTO {table} VALUES ({columns})", rows)


def _feature_history(lane, at, now, seed_path, source_root, db):
    revision = lane["feature_revision"]
    if fingerprint(revision["body"]) != revision["id"]:
        raise PermissionError("Frozen EMA feature revision checksum failed")
    original = {**lane, "book_id": lane["legacy_book_id"], "source": lane["source_policy"]}
    scopes = {grant.purpose: grant for grant in map(synthetic_protocol._grant, revision["body"]["grants"])}
    if any(grant.book_id != original["book_id"] or grant.source.id != lane["feature_source_id"]
           for grant in scopes.values()):
        raise PermissionError("EMA features require the original Kraken chart grant")
    if kraken_reference.scope_identity(root=source_root, as_of=now).id != lane["feature_source_id"]:
        raise PermissionError("The verified Kraken feature source changed")
    data = kraken_reference.market_data(scopes.values(), root=source_root)
    with (closing(sqlite3.connect(":memory:")) if db is None else nullcontext(db)) as features_db:
        if db is None:
            init(features_db)
        if not features_db.execute("SELECT 1 FROM synthetic_seed WHERE book=?", (original["book_id"],)).fetchone():
            _reuse_feature_state(features_db, seed_path, original["book_id"])
        policy, seed = synthetic_runtime._seed(features_db, revision, original, scopes, data, now)
        if at <= seed.end:
            return None, "Awaiting the first complete UTC chart after the frozen Kraken feature seam"
        expiry = at + policy.publication_grace
        if now > expiry:
            return None, "The frozen EMA signal publication window elapsed"
        if lane["feature_recovery"]:
            recovered = chart_recovery.recover(features_db, data, policy, seed, decision_at=at, as_of=now,
                                                check_deadline=time.monotonic() + 3.)
            if recovered.state == "READY":
                return (recovered.frame, recovered.evidence_available_at, recovered.snapshot_id, policy.processing), None
            return None, recovered.reason
        _, _, history = warmup._seed_data(data, policy, seed.initialized_at, None)
        source_ids, _, forward, available = warmup._forward(data, policy, seed.end, at, now)
        from lab.strategy_lab import ema_pullback

        frame = ema_pullback.features(pd.concat([history, forward]), policy.timeframe, policy.timeframe)
        return (frame, available, fingerprint([seed.id, *source_ids]), policy.processing), None


def _plan(lane, book, setup, proof, now):
    policy = replace(synthetic_btc.model(lane["profile"]), adverse_execution=0.)
    original = {**lane, "source": lane["source_policy"]}
    plan, reason = synthetic_btc.make_plan(original, setup, book["equity"], now, proof, policy)
    if not plan:
        return None, reason
    # The predecessor model document describes historical minute fills. Persist
    # only the numeric economic assumptions, under the new reference label.
    plan.pop("synthetic_model")
    plan.pop("synthetic_model_id")
    plan["evidence"].pop("synthetic_model_id")
    # The original EMA execution checks the fixed bracket and range, with no
    # extra directional test against the confirmation close.
    plan["entry"]["signal_reference"] = plan["entry"].pop("trigger")
    plan.update(entry_basis=lane["execution_mode"], exit_execution_basis=lane["execution_mode"],
                trigger_basis=SOURCE, atr=setup.atr, model_financing_rate=policy.funding_cost_rate,
                model_financing_interval_minutes=policy.funding_interval_minutes,
                price_unit="USD", ledger_unit="NOMINAL_USDT", nominal_usd_usdt_ratio=1.,
                accounting_model="SYNTHETIC_ISOLATED_MARGIN" if policy.perpetual else "CASH_STYLE")
    return plan, None


def confirmation(lane, book, body, now, *, db=None):
    """Return a causal paper idea from the same frozen Kraken EMA chart rules."""
    step, _ = worker._clocks(lane)
    at = utc(now).floor(step)
    end = utc(lane.get("publication_end_exclusive") or body["publication_end_exclusive"])
    if not utc(body["start"]) <= at < end or at >= utc(lane["feature_scope_end"]):
        return {"state": "WAITING", "reason": "Outside the prospective EMA chart window", "boundary": at.isoformat()}
    if book["position"] or book["pending"] or book["problem"]:
        return {"state": "WAITING", "reason": "EMA account is occupied or unresolved", "boundary": at.isoformat()}
    seed = Path(body.get("ema_seed_ledger", catalog.ROOT / "paper.sqlite3"))
    source_root = Path(body.get("ema_feature_root", kraken_reference.ROOT))
    try:
        chart, reason = _feature_history(lane, at, now, seed, source_root, db)
    except PermissionError as exc:
        return {"state": "BLOCKED", "reason": str(exc), "boundary": at.isoformat()}
    except SOURCE_ERRORS as exc:
        return {"state": "WAITING", "reason": str(exc), "boundary": at.isoformat()}
    if reason:
        return {"state": "WAITING", "reason": reason, "boundary": at.isoformat()}
    frame, available, content_id, processing = chart
    signal = proposal(frame, at, lane["spec"], data_hash=content_id, position_open=False)
    setup = signal.pop("setup")
    result = {"state": "EVALUATED", "boundary": at.isoformat(), "evaluated_at": now.isoformat(), "signal": signal}
    if setup is None:
        return result
    setup = replace(setup, available_at=max(at, available) + processing)
    proof = {"source_mode": synthetic_protocol.SOURCE_MODE, "source_id": lane["feature_source_id"],
             "source_received_at": available.isoformat(), "feature_snapshot_id": content_id,
             "protocol_amendment_id": lane["feature_revision"]["id"],
             "legacy_feature_book_id": lane["legacy_book_id"], "publication_end_exclusive": end.isoformat()}
    plan, reason = _plan(lane, book, setup, proof, now)
    if not plan:
        return {**result, "reason": reason}
    return {**result, "analysis": {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": at,
            "direction": "LONG" if setup.direction == 1 else "SHORT", "plan": plan,
            "strategy_version": lane["strategy_version"], "exit_policy": setup.exit_policy}}


def entry_terms(book, lane, plan, metadata, config, price, now):
    """Recompute synthetic financing, collateral and fixed bracket at the observed price."""
    del metadata, config, now
    policy = replace(synthetic_btc.model(lane["profile"]), adverse_execution=0.)
    if plan["contract"] != asdict(policy.contract) or plan["trigger_basis"] != SOURCE:
        return "EMA_SYNTHETIC_TERMS_CHANGED", None
    side, stop, target = plan["direction"], plan["stop"], plan["target"]
    order, contract = Order(**plan["order"]), Contract.load(plan["contract"])
    risk = lane["risk"]
    if ({1: "long", -1: "short"}.get(side) not in lane["spec"]["sides"]
            or not policy.perpetual and side != 1 or order.leverage != policy.leverage
            or risk["leverage"] != policy.leverage):
        return "EMA_DIRECTION_OR_LEVERAGE_REJECTED", None
    if (not plan["entry"]["minimum"] <= price <= plan["entry"]["maximum"]
            or not inside_bracket(price, stop, target, side)
            or not inside_bracket(price, plan["signal_stop"], plan["signal_target"], side)):
        return "EMA_ENTRY_OUTSIDE_FROZEN_BRACKET", None
    cash, fee_rate = book["cash"], policy.taker_fee
    if cash <= 0 or not all(isfinite(value) and value > 0 for value in (price, stop, target, plan["atr"])):
        return "EMA_INVALID_PRICE_OR_COLLATERAL", None
    funding_periods = ceil(plan["max_hold_minutes"] / policy.funding_interval_minutes) if policy.perpetual else 0
    funding_per_base = max(price, stop, target) * funding_periods * policy.funding_cost_rate
    stop_loss_per_base = side * (price - stop) + fee_rate * (price + stop)
    risk_per_base = stop_loss_per_base + funding_per_base
    reserved_per_base = price / policy.leverage - funding_per_base
    liquidation = liquidation_price(price, reserved_per_base, side, contract, fee_rate) if policy.perpetual else None
    buffer = side * (stop - liquidation) if liquidation is not None else None
    minimum = max(risk["liquidation_buffer_atr"], synthetic_btc.MINIMUM_PERPETUAL_LIQUIDATION_BUFFER_ATR) * plan["atr"]
    if policy.perpetual and (reserved_per_base <= 0 or buffer < minimum):
        return "EMA_STOP_TO_LIQUIDATION_BUFFER", None
    budget = min(order.risk_budget, cash * risk["risk_fraction"])
    cap = min(order.quote_cap, cash * risk["notional_cap"], cash * policy.leverage, contract.tier_notional)
    collateral_per_base = price / policy.leverage + fee_rate * (price + stop) + funding_per_base
    quantity = quantize(min(order.contracts, budget / risk_per_base, cap / price, cash / collateral_per_base),
                        contract.quantity_step)
    if quantity < contract.min_contracts:
        return "EMA_INSUFFICIENT_QUANTITY_OR_COLLATERAL", None
    notional = quantity * price
    return None, {"contracts": quantity, "quantity": quantity, "notional": notional,
                  "fee": notional * fee_rate, "margin": notional / policy.leverage,
                  "reference_taker_fee_rate": fee_rate, "reference_fee_basis": "SYNTHETIC_ASSUMPTION",
                  "planned_risk": quantity * risk_per_base, "risk_budget": budget,
                  "modeled_stop_loss": quantity * stop_loss_per_base, "funding_reserve": quantity * funding_per_base,
                  "projected_liquidation": liquidation, "liquidation_buffer": buffer,
                  "required_liquidation_buffer": minimum if policy.perpetual else None,
                  "target": target, "target_basis": "CONFIRMATION_CLOSE"}


def funding(db, book, record, position, slot, observed_at, received_at, price, now):
    """Apply one modeled financing debit using the qualified forward reference."""
    plan = record["plan"]
    rate = plan["model_financing_rate"]
    interval = pd.Timedelta(minutes=plan["model_financing_interval_minutes"])
    if not rate or slot.value % interval.value or not slot <= observed_at <= now:
        return
    paper.apply_funding(db, book, record, position, {"settlement_at": slot.isoformat(),
        "received_at": received_at.isoformat(), "valuation_source_at": observed_at.isoformat(),
        "rate": position.direction * rate, "modeled_rate": rate, "observed_settlement": False,
        "funding_basis": FINANCING_BASIS, "modeled_interval_minutes": plan["model_financing_interval_minutes"]}, price, now)
