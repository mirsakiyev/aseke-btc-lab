from pathlib import Path

import pandas as pd

from lab.config import Profile, fingerprint
from lab.phase4.config import MarketIdentity, ResearchConfig
from lab.phase4.sensitivity import OUTPUT, source
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock

PROTOCOL_VERSION = "mexc-prospective-swing-draft-v1"


def proposed_market(config: ResearchConfig, strategy_version: str) -> MarketIdentity:
    return MarketIdentity(
        venue="mexc", product="spot", market_type="unlevered_spot",
        symbol=config.published.proposed_symbol, base_currency="BTC",
        quote_currency=config.published.proposed_quote_currency,
        data_version=config.matching_data.adapter_version + ":no_snapshot",
        strategy_version=strategy_version,
        cost_version=config.published.name + ":account_cost_pending",
        execution_version="phase1-closed-bar-v1",
    )


def readiness(config: ResearchConfig) -> dict:
    blockers = []
    state = config.eligibility.state
    if state != "ELIGIBLE":
        blockers.append(f"execution_eligibility_{state.lower()}")
    if not config.published.proposed_pair_confirmed:
        blockers.append("pair_unconfirmed")
    if config.published.execution_channel == "unconfirmed":
        blockers.append("channel_unconfirmed")
    if config.account_fee_evidence is None:
        blockers.append("account_pair_channel_fee_evidence_missing")
    if config.matching_data.coverage != "SUFFICIENT_DATA":
        blockers.append("matching_mexc_data_unverified")
    return {
        "execution_eligibility": state,
        "fee_verification": "PUBLISHED_STANDARD_ONLY",
        "data_status": config.matching_data.coverage,
        "public_data_access": "OFFLINE", "live_schema_validated": False,
        "local_work": "LOCAL_RESEARCH_ONLY",
        "prospective_protocol": "DRAFT", "prospective_activation": "BLOCKED",
        "blockers": blockers, "account_connected_features": "DISABLED",
        "approved_lane": "No validated model",
    }


def build_protocol(config: ResearchConfig) -> dict:
    plan, previous, origin = source()
    strategy = Profile(**plan["profile"])
    sample = previous["scenarios"]["reference"]["methods"]["baseline"]["matched"]
    start = utc(sample["start_mark"])
    end = utc(sample["end_mark"])
    days = (end - start) / pd.Timedelta("1d")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "config_sha256": config.identity,
        "state": readiness(config),
        "market": proposed_market(config, strategy.strategy_version).model_dump(mode="json"),
        "fees": config.published.model_dump(mode="json"),
        "fee_verification_evidence": None,
        "eligibility": config.eligibility.model_dump(mode="json"),
        "matching_data": config.matching_data.model_dump(mode="json"),
        "strategy": {
            "family": "frozen_unfiltered_swing", "profile_template_hash": strategy.identity,
            "source_frozen_design_sha256": plan["design_sha256"], "strategy_code_sha256": plan["phase1_logic_sha256"],
            "reference_artifact_sha256": file_hash(origin / "complete.json"),
            "decision_frame": strategy.decision, "execution_frame": strategy.execution,
            "context": [part.model_dump() for part in strategy.context],
            "entry_range": strategy.entry_range, "exit_range": strategy.exit_range,
            "atr_period": strategy.atr_period, "atr_multiple": strategy.atr_multiple,
            "maximum_hold_minutes": strategy.max_minutes, "entry_risk_fraction": strategy.risk,
            "entry_exposure_cap": strategy.allocation, "processing_seconds": strategy.costs.processing_seconds,
            "delay_bars": strategy.costs.delay_bars, "maximum_open_positions": 1, "positioning": "long_or_cash",
        },
        "hypothetical_account": {
            "starting_amount": strategy.starting_cash, "currency": config.published.proposed_quote_currency,
            "cash_comparator": "Same currency, same start, zero yield.",
            "passive_btc_secondary": "25% BTC at the same opening, identical fee and adverse proxy policy, hold units.",
            "enrollment": None, "account_id": None, "actual_activation_utc": None, "review_at_utc": None,
        },
        "first_review": {
            "observation_budget_calendar_days": 90,
            "activation_rule": "Only after a separate frozen eligible protocol, at the actual next UTC 4h "
                               "decision boundary after approval of the protocol and verified warmup. No backdating.",
            "review_rule": "Actual activation UTC plus 90 calendar days. This is a review budget, not a power claim. "
                           "Do not extend, restart or change the endpoint in response to results.",
            "terminal_mark": "Last valid completed candle at or before review; record observed time, cash, units, "
                             "unrealized value and open status without inventing a sale or using later candles.",
            "historical_accrual_reference": {
                "source": "Preserved Coinbase Phase 3 reference baseline; different venue, quote and costs.",
                "start_utc": start.isoformat(), "end_utc": end.isoformat(), "calendar_days": days,
                "observed_hourly_marks": sample["observed_hourly_marks"], "closed_trades": sample["positions_closed"],
                "naive_90_day_scale": sample["positions_closed"] * 90 / days,
                "mexc_sample_expectation": None,
                "interpretation": "Illustrative only. Outcome dependence, different data, fees and gaps can leave the review inconclusive.",
            },
        },
        "event_policies": {
            "outage": "Block new intents on stale or missing bars. A gap during exposure freezes and flags the account and last valid mark.",
            "revision": "Version new data, retain original timestamps and previously committed observations.",
            "delayed_data": "Mark late catch-up separately; do not add retrospective prospective entries.",
            "restart": "Preserve positions, original exit policy, pending intents, cash and unique event IDs.",
            "fee_change": "Version amendments prospectively. Preserve prior fills and original evidence; "
                          "pause new entries if fee evidence no longer applies.",
            "amendment": "Hash and date the amendment; preserve the original cohort and endpoint. New data, "
                         "currency, cost or execution inputs require compatible identities.",
            "non_trading_costs": "Track conversions, funding arrangements and withdrawals separately if applicable. "
                                 "No derivative funding in the unlevered spot model.",
            "quote_costs": "Observed spread is diagnostic. Combined modeled adverse proxy is charged once. "
                           "No assumption of equal USD and USDT values or common P&L.",
        },
        "holdout": plan["windows"],
        "holdout_performance_revealed": False,
        "existing_forward_experiment": "Separate Coinbase cohort. No enrollment or mutation.",
    }


def register_draft(config: ResearchConfig, root: Path = OUTPUT / "protocols") -> Path:
    draft = build_protocol(config)
    digest = fingerprint(draft)
    path = root / f"draft-{digest[:16]}" / "protocol.json"
    with writer_lock(root / ".writer.lock"):
        if path.exists():
            record = read_json(path)
            if record["protocol_sha256"] != digest or any(record[key] != value for key, value in draft.items()):
                raise ValueError("Previously registered draft has been altered")
        else:
            write_json(path, {**draft, "protocol_sha256": digest, "registered_at_utc": utc_now().isoformat()})
    return path
