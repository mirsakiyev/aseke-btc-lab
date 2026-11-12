"""Versioned, offline adapter for the documented KuCoin BTC/USDT perpetual."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from math import isfinite

from lab.learning.simulator import Contract


VERSION = "kucoin-xbtusdtm-linear-isolated-v1"
SYMBOL = "XBTUSDTM"
BASE_PER_CONTRACT = Decimal("0.001")
FUNDING_INTERVALS_MS = frozenset(hours * 3_600_000 for hours in (1, 2, 4, 8))


def _finite_float(value, field):
    result = float(value)
    if not isfinite(result) or (result == 0 and value != 0):
        raise ValueError(f"Invalid XBTUSDTM {field}")
    return result


def _decimal(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ValueError(f"Invalid XBTUSDTM {field}")
    try:
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError(f"Invalid XBTUSDTM {field}")
        _finite_float(result, field)
        return result
    except InvalidOperation as exc:
        raise ValueError(f"Invalid XBTUSDTM {field}") from exc


def _number(row, field):
    return _decimal(row.get(field), field)


def _positive(value, field):
    number = _decimal(value, field)
    if number <= 0:
        raise ValueError(f"XBTUSDTM {field} must be positive")
    return number


def _integer(value, field):
    number = _positive(value, field)
    if number != number.to_integral_value():
        raise ValueError(f"XBTUSDTM {field} must be an integer")
    return int(number)


def _assert_fields(row, required):
    if not isinstance(row, Mapping) or any(
        key not in row or row[key] != value for key, value in required.items()
    ):
        raise ValueError("Unsupported XBTUSDTM contract metadata")


def _side(side):
    if type(side) is not int or side not in (-1, 1):
        raise ValueError("Direction must be +1 for long or -1 for short")
    return side


@dataclass(frozen=True)
class XbtusdtmTerms:
    """Exact linear contract economics for a dated, isolated level-one snapshot."""

    contract: Contract
    max_market_contracts: int
    max_limit_contracts: int
    max_order_price: float
    tier_min_initial_rate: float
    published_maker_rate: float
    published_taker_rate: float
    funding_interval_ms: int
    funding_anchor_ms: int
    funding_floor: float
    funding_cap: float

    def _base_quantity(self, contracts):
        quantity = _integer(contracts, "contract quantity")
        if quantity % int(self.contract.quantity_step):
            raise ValueError("XBTUSDTM quantity must follow the dated contract lot")
        return quantity * BASE_PER_CONTRACT

    def base_quantity(self, contracts):
        return _finite_float(self._base_quantity(contracts), "base quantity")

    def notional(self, contracts, price):
        value = self._base_quantity(contracts) * _positive(price, "price")
        return _finite_float(value, "notional")

    def gross_pnl(self, contracts, side, entry_price, exit_price):
        entry = _positive(entry_price, "entry price")
        exit_ = _positive(exit_price, "exit price")
        pnl = _side(side) * self._base_quantity(contracts) * (exit_ - entry)
        return _finite_float(pnl, "gross P&L")

    def initial_margin(self, contracts, entry_price, leverage):
        leverage = float(_positive(leverage, "leverage"))
        price = _positive(entry_price, "entry price")
        quantity = _integer(contracts, "contract quantity")
        value = self._base_quantity(quantity) * price
        if (
            quantity < self.contract.min_contracts
            or quantity > self.max_market_contracts
            or price > Decimal(str(self.max_order_price))
            or value > Decimal(str(self.contract.tier_notional))
            or not 1 <= leverage <= self.contract.max_leverage
        ):
            raise ValueError("Order exceeds the supported XBTUSDTM level-one limits")
        # KuCoin's rounded minimum initial margin can exceed 1/leverage.
        return float(value) * max(1 / leverage, self.tier_min_initial_rate)

    def maintenance_margin(self, contracts, entry_price):
        value = self._base_quantity(contracts) * _positive(entry_price, "entry price")
        if value > Decimal(str(self.contract.tier_notional)):
            raise ValueError("Opening value exceeds the supported XBTUSDTM level-one limit")
        return float(value) * self.contract.maintenance_rate

    def funding_cashflow(self, contracts, side, settlement_mark, settled_rate):
        """Signed USDT cashflow using the observed mark and settled rate."""
        rate = float(_decimal(settled_rate, "settled funding rate"))
        if not self.funding_floor <= rate <= self.funding_cap:
            raise ValueError("Settled funding rate is outside the dated bounds")
        return -_side(side) * self.notional(contracts, settlement_mark) * rate


def _tier_one(rows):
    if (
        not isinstance(rows, Sequence)
        or not rows
        or any(not isinstance(row, Mapping) or row.get("symbol") != SYMBOL for row in rows)
    ):
        raise ValueError("Exact XBTUSDTM isolated risk tiers are required")
    tiers = {_integer(row.get("level"), "isolated tier level"): row for row in rows}
    if len(tiers) != len(rows) or 1 not in tiers:
        raise ValueError("Expected a unique XBTUSDTM isolated level-one tier")
    row = tiers[1]
    minimum, maximum = _number(row, "minRiskLimit"), _number(row, "maxRiskLimit")
    maintenance, initial = _number(row, "maintainMargin"), _number(row, "initialMargin")
    if minimum != 0 or maximum <= 0 or not 0 < maintenance < initial <= 1:
        raise ValueError("Invalid XBTUSDTM isolated level-one tier")
    return maximum, maintenance, initial, _integer(row.get("maxLeverage"), "tier leverage")


def _funding(contract):
    interval = _integer(contract.get("currentFundingRateGranularity"), "current funding interval")
    default = _integer(contract.get("fundingRateGranularity"), "default funding interval")
    anchor = _integer(contract.get("nextFundingRateDateTime"), "funding anchor")
    effective = _integer(contract.get("effectiveFundingRateCycleStartTime"), "funding cycle start")
    floor = _number(contract, "fundingRateFloor")
    cap = _number(contract, "fundingRateCap")
    if (
        interval not in FUNDING_INTERVALS_MS
        or default not in FUNDING_INTERVALS_MS
        or not 10**12 <= effective <= anchor < 10**13
        or anchor % 60_000
        or effective % 60_000
        or not -1 < floor < 0 < cap < 1
    ):
        raise ValueError("Invalid XBTUSDTM dated funding schedule")
    return interval, anchor, float(floor), float(cap)


def adapt_xbtusdtm(contract, instrument, isolated_tiers, *, modeled_liquidation_fee):
    """Translate decoded Classic and UTA metadata into the reviewed linear model.

    The caller must verify source rights, receipt hashes, timestamps, and
    freshness before supplying these three payloads.
    """
    _assert_fields(contract, {
        "symbol": SYMBOL, "type": "FFWCSX", "quoteCurrency": "USDT", "settleCurrency": "USDT",
        "status": "Open", "expireDate": None, "settleDate": None, "settlementFee": None,
        "markMethod": "FairPrice",
    })
    _assert_fields(instrument, {
        "symbol": SYMBOL, "quoteCurrency": "USDT", "settlementCurrency": "USDT",
        "tradingStatus": "Open", "marketStage": "NORMAL", "contractType": "0",
        "expiryTime": None, "settlementTime": None, "settlementFeeRate": None,
    })
    if (
        contract.get("isQuanto") is not True
        or contract.get("isInverse") is not False
        or instrument.get("isInverse") is not False
        or contract.get("baseCurrency") not in ("BTC", "XBT")
        or instrument.get("baseCurrency") not in ("BTC", "XBT")
        or _number(contract, "multiplier") != BASE_PER_CONTRACT
        or _number(instrument, "unitSize") != BASE_PER_CONTRACT
    ):
        raise ValueError("Unsupported XBTUSDTM payoff, currencies, or contract multiplier")
    lot = _integer(contract.get("lotSize"), "lotSize")
    tick = _positive(contract.get("tickSize"), "tickSize")
    market_cap = _integer(contract.get("marketMaxOrderQty"), "marketMaxOrderQty")
    limit_cap = _integer(contract.get("maxOrderQty"), "maxOrderQty")
    max_price = _positive(contract.get("maxPrice"), "maxPrice")
    max_leverage = _integer(contract.get("maxLeverage"), "maxLeverage")
    minimum = _integer(instrument.get("minOrderSize", lot), "minimum order size")
    if (
        _number(instrument, "lotSize") != lot
        or _number(instrument, "tickSize") != tick
        or _number(instrument, "maxMarketOrderSize") != market_cap
        or _number(instrument, "maxBaseOrderSize") != limit_cap
        or _number(instrument, "maxPrice") != max_price
        or _number(instrument, "maxLeverage") != max_leverage
        or minimum > min(market_cap, limit_cap)
        or any(size % lot for size in (minimum, market_cap, limit_cap))
    ):
        raise ValueError("Classic and UTA XBTUSDTM size, tick, or capacity rules disagree")
    tier_limit, maintenance, tier_initial, tier_leverage = _tier_one(isolated_tiers)
    if (
        _number(contract, "initialMargin") != tier_initial
        or _number(contract, "maintainMargin") != maintenance
        or tier_leverage > max_leverage
    ):
        raise ValueError("Classic and isolated XBTUSDTM margin rules disagree")
    maker, taker = _number(contract, "makerFeeRate"), _number(contract, "takerFeeRate")
    liquidation = _decimal(modeled_liquidation_fee, "modeled liquidation fee")
    if (
        not 0 <= maker <= taker < 1
        or not 0 <= liquidation < 1 - maintenance
        or _number(instrument, "makerFeeRate") != maker
        or _number(instrument, "takerFeeRate") != taker
        or _number(contract, "makerFixFee") != 0
        or _number(contract, "takerFixFee") != 0
    ):
        raise ValueError("Unsupported XBTUSDTM fee metadata")
    interval, anchor, floor, cap = _funding(contract)
    # The simulator's reviewed maximum leverage remains 100, even if the
    # exchange's dated venue limit is higher.
    model = Contract.load({
        "family": "linear_perp", "quote": "USDT", "base_per_contract": float(BASE_PER_CONTRACT),
        "quantity_step": lot, "price_tick": float(tick), "min_contracts": minimum,
        "tier_notional": float(tier_limit), "max_leverage": min(100, tier_leverage),
        "maintenance_rate": float(maintenance), "liquidation_fee_rate": float(liquidation),
    })
    return XbtusdtmTerms(model, market_cap, limit_cap, float(max_price), float(tier_initial),
                        float(maker), float(taker), interval, anchor, floor, cap)
