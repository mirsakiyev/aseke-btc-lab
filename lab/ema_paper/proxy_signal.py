"""Inspect an original EMA signal at a strictly prospective boundary."""

from dataclasses import replace

import pandas as pd

from lab.execution import execution_open
from lab.ema_paper import ledger
from lab.ema_paper.proxy_native import KuCoinProxyEvidence
from lab.kraken_paper.authorization import utc
from lab.strategy_lab.cross_venue_warmup import KRAKEN_PROXY_SEEDED, KUCOIN_NATIVE_READY, POLICY_ID, load_proxy_seed
from lab.strategy_lab.evaluator import proposal


def read_transition(amendment, native_reader, *, decision_at, as_of):
    """Replay the checked native prefix for the amendment's frozen seed."""
    if not isinstance(native_reader, KuCoinProxyEvidence):
        raise TypeError("Native transition requires the reviewed KuCoin evidence reader")
    record = amendment["body"]
    seed = load_proxy_seed(record["seed"], record["seed_id"])
    return native_reader.read_transition(seed, decision_at, as_of)


def inspect_decision(registration, amendment, transition, *, decision_at, as_of, ready_at, position_open=False):
    """Apply original rules to verified feature evidence. The result cannot publish or trade."""
    lane, record = registration["body"], amendment["body"]
    evidence = transition.evidence
    at, now, readiness = map(utc, (decision_at, as_of, ready_at))
    step = pd.Timedelta(seconds=lane["execution_clock"]["decision_seconds"])
    if (record["registration_id"] != registration["id"] or record["book_id"] != lane["book_id"]
            or record["policy_id"] != POLICY_ID or evidence["policy_id"] != POLICY_ID
            or evidence["seed_book_id"] != lane["book_id"] or evidence["seed_id"] != record["seed_id"]
            or (evidence["forward_provider"], evidence["forward_market"], evidence["forward_instrument"])
            != ("KuCoin", lane["market"], lane["instrument"])):
        raise PermissionError("Feature evidence does not bind to this original EMA source amendment")
    start = execution_open(max(readiness, utc(record["recorded_at"]), utc(lane["registered_at"])), step)
    end = start + pd.Timedelta(days=lane["horizon_days"])
    mode = evidence["source_mode"]
    result = {"decision": "NO_TRADE", "source_mode": mode, "decision_at": at.isoformat(),
              "first_eligible_at": start.isoformat(), "primary_end_exclusive": end.isoformat(),
              "feature_evidence_id": transition.id, "setup": None}
    if not start <= at < end or readiness > now:
        return {**result, "reason": "DECISION_OUTSIDE_PROSPECTIVE_CLOCK"}
    if utc(evidence["as_of"]) != now or utc(evidence["expected_decision_close"]) != at:
        raise ValueError("Inspect the current closed chart with evidence constructed at the evaluation clock")
    if mode not in {KRAKEN_PROXY_SEEDED, KUCOIN_NATIVE_READY} or at not in transition.frame.index:
        return {**result, "reason": evidence["reason"]}
    row = transition.frame.loc[at]
    processing = pd.Timedelta(seconds=lane["spec"]["execution"]["processing_seconds"])
    signal_available = max(at, utc(row.available_at)) + processing
    publication_deadline = at + pd.Timedelta(seconds=lane["source"]["publication_max_seconds"])
    entry_step = pd.Timedelta(seconds=lane["execution_clock"]["entry_seconds"])
    entry = execution_open(signal_available, entry_step)
    if not signal_available <= now <= publication_deadline or not now < entry < end:
        return {**result, "reason": "MISSED_OR_UNAVAILABLE_PROSPECTIVE_ENTRY"}
    signal = proposal(transition.frame, at, lane["spec"], data_hash=transition.id, position_open=position_open)
    if signal["setup"]:
        expiry = entry + pd.Timedelta(minutes=1)
        deadline = entry + pd.Timedelta(minutes=lane["spec"]["session"]["max_hold_minutes"])
        signal["setup"] = replace(signal["setup"], available_at=signal_available, submit_at=entry,
                                  expires_at=expiry, deadline=deadline)
    return {**result, **signal, "available_at": signal_available.isoformat(), "reason": row.reason}


def inspect_plan(registration, amendment, native_reader, *, decision_at, as_of, ready_at,
                 equity, position_open=False):
    """Check independent execution evidence and form a plan without journal or outbox writes."""
    transition = read_transition(amendment, native_reader, decision_at=decision_at, as_of=as_of)
    decision = inspect_decision(registration, amendment, transition, decision_at=decision_at, as_of=as_of,
                                ready_at=ready_at, position_open=position_open)
    setup = decision.pop("setup")
    if setup is None:
        return {**decision, "plan": None}
    at, now = utc(decision_at), utc(as_of)
    lane = registration["body"]
    native = native_reader.publication_evidence(at, now, decision["primary_end_exclusive"])
    batch, chart = native_reader.read_chart(at, now)
    row = transition.frame.loc[at]
    candle = batch.bars.iloc[-1]
    if (not batch.complete or chart.book_id != lane["book_id"] or chart.id != native.decision_trade.id
            or chart.source.id != transition.evidence["forward_source_id"]
            or chart.grant_id != transition.evidence["forward_grant_id"]
            or not all(row[key] == candle[key] for key in ("open", "high", "low", "close", "volume"))
            or utc(row.available_at) < utc(candle.available_at)):
        raise ValueError("Signal input differs from verified native KuCoin chart evidence")
    processing = pd.Timedelta(seconds=lane["spec"]["execution"]["processing_seconds"])
    economic_availability = max(setup.available_at, native.available_at + processing)
    if economic_availability > now:
        return {**decision, "decision": "NO_TRADE", "plan": None, "reason": "EXECUTION_EVIDENCE_PROCESSING_PENDING"}
    plan, reason = ledger.make_plan(lane, replace(setup, available_at=economic_availability), native.contract, equity, now, {
        "source_mode": decision["source_mode"], "source_id": chart.source.id,
        "source_received_at": max(item.received_at for item in chart.observations).isoformat(),
        "protocol_amendment_id": amendment["id"], "feature_evidence_id": transition.id,
        "chart_snapshot_id": chart.id, "execution_trade_snapshot_id": native.trade.id,
        "mark_snapshot_id": native.marks.id if native.marks else None,
        "contract_snapshot_id": native.dated_terms.id,
        "funding_snapshot_id": native.settled_funding.id if native.settled_funding else None,
        "publication_end_exclusive": decision["primary_end_exclusive"],
        "execution_terms": {"minimum_notional": native.minimum_notional, "maximum_quantity": native.maximum_quantity},
    })
    return {**decision, "decision": "PROPOSAL" if plan else "NO_TRADE", "plan": plan, "reason": reason or row.reason}
