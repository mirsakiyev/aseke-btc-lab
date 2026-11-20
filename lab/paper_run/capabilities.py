"""Declared operational capabilities, independent of strategy names and families.

Sealed legacy lanes retain their exact identifiers and trading settings. New
lanes must explicitly choose supported adapters. A capability declaration is
not source permission, an exchange order authorization, or automatic recovery.
"""

from copy import deepcopy
from math import isfinite

from lab.config import fingerprint


SCHEMA = "PAPER_STRATEGY_CAPABILITIES_V1"
SOURCE_SCHEMA = "PAPER_REFERENCE_SOURCE_V1"
SIGNALS = {"ema_chart_v1", "breakout_v1"}
ENTRIES = {"ema_reference_v1", "breakout_reference_v1"}
EXITS = {"fixed_r_v1", "atr_trail_v1", "signal_atr_bracket_v1"}
MODELS = {
    "REALTIME_MARK_REFERENCE_PAPER": "LINEAR_PERP",
    "REALTIME_SPOT_TICKER_REFERENCE_PAPER": "CASH_SPOT",
    "REALTIME_SYNTHETIC_SPOT_REFERENCE_PAPER": "SYNTHETIC_LINEAR_PERP",
    "REALTIME_CASH_SPOT_REFERENCE_PAPER": "CASH_SPOT",
}
MARK = "KUCOIN_FUTURES_CURRENT_MARK"
SPOT = "KUCOIN_SPOT_TICKER_LAST_REFERENCE"
KRAKEN = "KRAKEN_BTC_USD_LAST_TRADE"


class UnsupportedCapability(ValueError):
    def __init__(self, code, detail):
        self.code = code
        super().__init__(f"{code}: {detail}")


def _text(value, field):
    if not isinstance(value, str) or not value.strip() or len(value) > 500:
        raise UnsupportedCapability("INVALID_CAPABILITY_IDENTITY", field)
    return value


def _source(lane):
    identifier = lane.get("source")
    symbol = lane.get("symbol")
    transport = lane.get("observation_kind", "HTTP_SNAPSHOT")
    basis = _text(lane.get("price_basis"), "price_basis")
    if identifier == MARK and symbol == "XBTUSDTM" and transport == "HTTP_SNAPSHOT":
        venue, product, version = "KuCoin", "LINEAR_PERP", "kucoin-mark-now-v1"
        subscription = {"endpoint": "https://api-futures.kucoin.com/api/v1/mark-price/XBTUSDTM/current",
                        "channel": "mark_now", "instrument": symbol}
    elif identifier == SPOT and symbol == "BTC-USDT" and transport == "HTTP_SNAPSHOT":
        venue, product, version = "KuCoin", "SPOT", "kucoin-level1-ticker-v1"
        subscription = {"endpoint": "https://api.kucoin.com/api/ua/v2/market/ticker",
                        "channel": "ticker", "tradeType": "SPOT", "symbol": symbol}
    elif identifier == KRAKEN and symbol == "BTC/USD" and transport in ("WEBSOCKET_TRADE", "HTTP_SNAPSHOT"):
        venue, product = "Kraken", "SPOT"
        if transport == "WEBSOCKET_TRADE":
            version = "kraken-v2-trade-v1"
            subscription = {"endpoint": "wss://ws.kraken.com/v2", "channel": "trade",
                            "symbol": [symbol], "snapshot": False}
        else:
            version = "kraken-public-trades-v1"
            subscription = {"endpoint": "https://api.kraken.com/0/public/Trades",
                            "channel": "Trades", "pair": "XBTUSD", "count": 1}
    else:
        raise UnsupportedCapability("UNSUPPORTED_REFERENCE_SOURCE", f"{identifier}/{symbol}/{transport}")
    cadence = ({"mode": "EVENT_DRIVEN", "heartbeat_seconds": 1, "protection_gap_seconds": 45}
               if transport == "WEBSOCKET_TRADE" else
               {"mode": "SAMPLED", "minimum_request_interval_seconds": 15, "protection_gap_seconds": 45})
    return {"schema": SOURCE_SCHEMA, "id": identifier, "venue": venue, "instrument": symbol,
            "product": product, "price_basis": basis, "transport": transport,
            "subscription": subscription, "version": version, "cadence": cadence,
            "continuity": "TRADE_EVENT_STREAM" if transport == "WEBSOCKET_TRADE" else "SAMPLED_REFERENCE_POINTS"}


def declare(lane, *, signal_adapter, entry_adapter, trail_adapter=None,
            generation=None, slot=None, legacy=False):
    """Build a complete supported declaration without consulting a strategy name."""
    if signal_adapter not in SIGNALS:
        raise UnsupportedCapability("UNSUPPORTED_SIGNAL_ADAPTER", str(signal_adapter))
    if entry_adapter not in ENTRIES:
        raise UnsupportedCapability("UNSUPPORTED_ENTRY_ADAPTER", str(entry_adapter))
    if (signal_adapter == "ema_chart_v1") != (entry_adapter == "ema_reference_v1"):
        raise UnsupportedCapability("INCOMPATIBLE_STRATEGY_ADAPTERS", "signal and entry adapter")
    source = _source(lane)
    expected_source = KRAKEN if signal_adapter == "ema_chart_v1" else {MARK, SPOT}
    compatible_source = (source["id"] == expected_source if isinstance(expected_source, str)
                         else source["id"] in expected_source)
    if not compatible_source:
        raise UnsupportedCapability("INCOMPATIBLE_REFERENCE_ADAPTER", source["id"])
    model = MODELS.get(lane.get("entry_model"))
    if model is None and legacy:
        model = ("LINEAR_PERP" if source["id"] == MARK else "CASH_SPOT" if source["id"] == SPOT
                 else "CASH_SPOT" if lane.get("market") == "SPOT" else "SYNTHETIC_LINEAR_PERP")
    if model is None or (not legacy and lane.get("exit_model") != lane.get("entry_model")):
        raise UnsupportedCapability("UNSUPPORTED_EXECUTION_MODEL", str(lane.get("entry_model")))
    if ((source["id"] == MARK and model != "LINEAR_PERP") or
            (source["id"] == SPOT and model != "CASH_SPOT") or
            (source["id"] == KRAKEN and model not in ("CASH_SPOT", "SYNTHETIC_LINEAR_PERP"))):
        raise UnsupportedCapability("INCOMPATIBLE_REFERENCE_PRODUCT", model)
    policy = ((lane.get("exit") or "signal_atr_bracket_v1") if signal_adapter == "ema_chart_v1"
              else lane.get("exit", "fixed_r_v1"))
    if policy not in EXITS:
        raise UnsupportedCapability("UNSUPPORTED_EXIT_POLICY", str(policy))
    expected_trail = "breakout_atr_v1" if policy == "atr_trail_v1" else None
    if trail_adapter != expected_trail:
        raise UnsupportedCapability("UNSUPPORTED_TRAIL_ADAPTER", str(trail_adapter))
    if policy == "atr_trail_v1" and signal_adapter != "breakout_v1":
        raise UnsupportedCapability("INCOMPATIBLE_TRAILING_POLICY", signal_adapter)
    funding = "KUCOIN_SETTLED" if model == "LINEAR_PERP" else "SYNTHETIC_DEBIT" if model == "SYNTHETIC_LINEAR_PERP" else "NONE"
    return {
        "schema": SCHEMA, "generation": generation, "slot": slot,
        "book_id": _text(lane.get("book_id"), "book_id"),
        "strategy": {"name": str(lane.get("strategy") or lane.get("family") or "declared-strategy"),
                     "version": str(lane.get("strategy_version") or lane.get("spec", {}).get("implementation_version") or "legacy-sealed")},
        "source": source, "model_product": model,
        "adapters": {"signal": signal_adapter, "entry": entry_adapter, "exit": "reference_bracket_v1",
                     "trail": trail_adapter, "evidence": "kraken_native_trade_bounds_v1" if source["id"] == KRAKEN else "explicit_insufficiency_v1",
                     "reconciliation": "nominal_reference_v1", "notification": "health_outbox_v1", "recovery": "operator_incident_v1"},
        "signal_data": {"source_id": lane.get("feature_source_id") or lane.get("legacy_registration"),
                        "venue": source["venue"], "instrument": source["instrument"],
                        "product": source["product"], "kind": "COMPLETED_TRADE_OHLCV",
                        "timeframe": lane.get("timeframe"),
                        "context": deepcopy((lane.get("spec") or {}).get("context", (lane.get("profile_rules") or {}).get("context", []))),
                        "causal_receipts_required": True},
        "protection_data": {"source": source["id"], "freshness_seconds": 2,
                            "clock_target_ms": 250, "monotonic_tolerance_ms": 100},
        "entry_trigger_basis": source["price_basis"], "exit_trigger_basis": source["price_basis"],
        "liquidation_trigger_basis": source["price_basis"] if model != "CASH_SPOT" else None,
        "exit": {"policy": policy, "target_optional": policy == "atr_trail_v1",
                 "liquidation_required": model != "CASH_SPOT"},
        "funding": funding, "margin_required": model != "CASH_SPOT",
        "recovery": {"operator_policy": "PROTECTION_INCIDENT_POLICY_V1", "automatic_resume_enabled": False,
                     "automatic_policy": None},
        "paper_only": True, "execution_mode": "MANUAL_OUTSIDE_LAB", "approval": "No validated model",
    }


def contract(lane, *, generation=None, slot=None):
    """Resolve a lane's explicit contract or its compatible sealed legacy contract."""
    supplied = lane.get("capabilities")
    if supplied is None:
        family = lane.get("family")
        if family not in ("EMA", "BREAKOUT"):
            raise UnsupportedCapability("EXPLICIT_CAPABILITIES_REQUIRED", str(family))
        return declare(lane, signal_adapter="ema_chart_v1" if family == "EMA" else "breakout_v1",
                       entry_adapter="ema_reference_v1" if family == "EMA" else "breakout_reference_v1",
                       trail_adapter="breakout_atr_v1" if lane.get("exit") == "atr_trail_v1" else None,
                       generation=generation, slot=slot, legacy=True)
    if not isinstance(supplied, dict) or supplied.get("schema") != SCHEMA:
        raise UnsupportedCapability("UNSUPPORTED_CAPABILITY_SCHEMA", str((supplied or {}).get("schema") if isinstance(supplied, dict) else type(supplied)))
    adapters = supplied.get("adapters") or {}
    expected = declare(lane, signal_adapter=adapters.get("signal"), entry_adapter=adapters.get("entry"),
                       trail_adapter=adapters.get("trail"), generation=supplied.get("generation"), slot=supplied.get("slot"))
    if supplied != expected:
        raise UnsupportedCapability("CAPABILITY_DECLARATION_MISMATCH", "source, policy or unsupported capability differs from its admitted adapter")
    for key, value in (("generation", generation), ("slot", slot)):
        if value is not None and supplied.get(key) != value:
            raise UnsupportedCapability("CAPABILITY_IDENTITY_MISMATCH", key)
    return deepcopy(supplied)


def source_scope_id(lane):
    """Stable actual-source identity; subscription epochs are outage evidence only."""
    return fingerprint([SOURCE_SCHEMA, _source(lane)])


def source_identity(lane):
    return _source(lane)


def adapter(lane, role):
    value = contract(lane)["adapters"]
    if role not in value:
        raise UnsupportedCapability("UNSUPPORTED_ADAPTER_ROLE", role)
    return value[role]


def validate_registration(lane, *, generation, slot):
    """Future membership requires an explicit, complete supported declaration."""
    if "capabilities" not in lane:
        raise UnsupportedCapability("EXPLICIT_CAPABILITIES_REQUIRED", slot)
    value = contract(lane, generation=generation, slot=slot)
    permissions = lane.get("permissions") or {}
    if (lane.get("paper_only", True) is not True or permissions.get("live_orders", False) is not False
            or permissions.get("paper_only", True) is not True):
        raise UnsupportedCapability("PAPER_ONLY_REQUIRED", slot)
    if isinstance(lane.get("mode"), str) and lane["mode"] != "MANUAL_OUTSIDE_LAB":
        raise UnsupportedCapability("MANUAL_OUTSIDE_LAB_REQUIRED", slot)
    if lane.get("execution_mode", "MANUAL_OUTSIDE_LAB") != "MANUAL_OUTSIDE_LAB":
        raise UnsupportedCapability("MANUAL_OUTSIDE_LAB_REQUIRED", slot)
    if lane.get("approval", "No validated model") != "No validated model":
        raise UnsupportedCapability("UNVALIDATED_MODEL_APPROVAL_REQUIRED", slot)
    if value["model_product"] == "CASH_SPOT" and lane.get("long_only") is not True:
        raise UnsupportedCapability("CASH_SPOT_REQUIRES_LONG_ONLY", slot)
    capital = (lane.get("risk") or {}).get("starting_equity")
    if type(capital) not in (float, int) or not isfinite(capital) or capital <= 0:
        raise UnsupportedCapability("INVALID_REGISTERED_CAPITAL", slot)
    if not isinstance(lane.get("qualification"), dict) or lane["qualification"].get("state") not in ("QUALIFIED", "BLOCKED"):
        raise UnsupportedCapability("MISSING_SOURCE_QUALIFICATION", slot)
    for field in ("timeframe", "publication_end_exclusive", "follow_end_exclusive"):
        _text(lane.get(field), field)
    return value
