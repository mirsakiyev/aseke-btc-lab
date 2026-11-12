"""Reviewable EMA Spot proposals and forward-only signal diagnostics."""

import argparse
import json

import pandas as pd

from lab.strategy_lab import ema_pullback
from lab.strategy_lab.feature_warmup import DecisionFeatures
from lab.strategy_lab.registry import MODE, MODEL_VALIDATION, digest


VERSION = "ema-btc-usd-spot-proposal-v1"
PROFILES = {
    "day": (900, 900, 480, 1.0),
    "swing": (14400, 14400, 10080, 1.0),
    "position": (86400, 3600, 43200, .25),
}
VENUES = {
    "kraken": ("Kraken", "https://www.kraken.com/pro/fees", .004, .008),
    "coinbase": ("Coinbase Advanced Trade",
                 "https://help.coinbase.com/en/coinbase/trading-and-funding/advanced-trade/advanced-trade-fees",
                 None, None),
}


def proposals(venue):
    """Build hashed review drafts with no configured grants or paper dispatch."""
    name, fee_reference, maker, taker = VENUES[venue]
    result = {}
    for profile, (decision, entry, hold, notional_cap) in PROFILES.items():
        body = {
            "schema": VERSION, "status": "BLOCKED_PERMISSION", "paper_only": True,
            "mode": MODE, "model_validation": MODEL_VALIDATION,
            "strategy_id": f"{ema_pullback.FAMILY}/{venue}-btc-usd-spot/{profile}",
            "profile": profile, "venue": name, "instrument": "BTC-USD",
            "market": "SPOT", "base": "BTC", "quote": "USD",
            "recipe": {"entry": ema_pullback.FAMILY, "entry_parameters": dict(ema_pullback.ENTRY_PARAMETERS),
                       "exit": ema_pullback.EXIT, "exit_parameters": dict(ema_pullback.EXIT_PARAMETERS)},
            "executable_sides": ["long"], "short_handling": "SIGNAL_ONLY",
            "session": {"timezone": "UTC", "cadence": "24/7", "max_hold_minutes": hold},
            "capital": {"hypothetical_endowment": 10000, "endowment_currency": "USDT", "ledger_currency": "USD",
                        "initial_usd": None, "conversion_evidence_id": None,
                        "conversion_rule": "Approved causal USDT/USD sell-side quote, net of conversion costs"},
            "risk": {"leverage": 1, "ordinary_cap": 1, "risk_fraction": .0025,
                     "notional_cap": notional_cap, "liquidation_buffer_atr": 0},
            "execution": {
                "decision_seconds": decision, "feature_native_seconds": decision,
                "entry_seconds": entry, "protection_seconds": 60, "processing_seconds": 30,
                "decision_receipt_seconds": 120, "publication_max_seconds": 360,
                "entry_limit_atr": .25, "entry_basis": "TRADE_MINUTE_OPEN",
                "protection_basis": "TRADE", "funding": "NOT_APPLICABLE", "margin": "CASH_ONLY",
                "entry_rule": "One eligible opening strictly after publication",
                "slippage_rule": "Pin a causal venue spread and depth model before approval",
            },
            "cost": {"published_maker": maker, "published_taker": taker, "reference": fee_reference,
                     "approved_maker": None, "approved_taker": None, "approved_slippage": None},
            "research": {"warmup_bars": ema_pullback.WARMUP_BARS, "primary_days": 90,
                         "follow_through_minutes": hold + 120, "min_complete_trades": 25,
                         "source_id": None, "rights_reference": None, "warmup_snapshot_id": None,
                         "warmup_grant_id": None, "decision_grant_id": None, "execution_grant_id": None,
                         "first_eligible_at": None},
            "activation_enabled": False, "registration_id": None, "book_id": None, "approval": None,
        }
        result[profile] = {"draft_id": digest(body), "body": body}
    return result


def signal_diagnostic(features: DecisionFeatures):
    """Reuse the EMA gates on forward feature rows and expose no executable short."""
    if not isinstance(features, DecisionFeatures):
        raise TypeError("EMA Spot diagnostics require eligible forward decision features")
    current, prior = dict(features.current), dict(features.previous)
    prior_values = {f"prior_{name}": prior[name] for name in ("ema_fast", "low", "high", "close")}
    row = pd.Series({**current, **prior_values, "ready": True}, dtype=object)
    gates = ema_pullback.gates(row)
    if all(gates["short"].values()):
        side, state = "SHORT", "SIGNAL_ONLY"
    elif all(gates["long"].values()):
        side, state = "LONG", "LONG_CANDIDATE"
    else:
        side, state = None, "NO_SIGNAL"
    return {"side": side, "state": state, "decision_at": features.decision_at.isoformat(),
            "input_snapshot_id": features.snapshot_id}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--venue", choices=VENUES, default="kraken")
    args = parser.parse_args()
    print(json.dumps(proposals(args.venue), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
