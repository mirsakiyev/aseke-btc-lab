import pytest

from lab.learning.simulator import Position, liquidation_price
from lab.strategy_lab.kucoin_contract import VERSION, adapt_xbtusdtm


@pytest.fixture
def terms_metadata():
    # Synthetic payloads shaped like KuCoin's published Classic and UTA responses.
    contract = {
        "symbol": "XBTUSDTM", "type": "FFWCSX", "baseCurrency": "XBT",
        "quoteCurrency": "USDT", "settleCurrency": "USDT", "status": "Open",
        "expireDate": None, "settleDate": None, "settlementFee": None,
        "isQuanto": True, "isInverse": False, "multiplier": 0.001, "markMethod": "FairPrice",
        "lotSize": 1, "tickSize": 0.1, "maxPrice": 1_000_000,
        "marketMaxOrderQty": 1_000_000, "maxOrderQty": 1_000_000, "maxLeverage": 125,
        "initialMargin": 0.008, "maintainMargin": 0.004,
        "makerFeeRate": 0.0002, "takerFeeRate": 0.0006, "makerFixFee": 0, "takerFixFee": 0,
        "fundingRateGranularity": 28_800_000, "currentFundingRateGranularity": 28_800_000,
        "effectiveFundingRateCycleStartTime": 1_750_147_200_000, "nextFundingRateDateTime": 1_788_854_400_000,
        "fundingRateFloor": -0.003, "fundingRateCap": 0.003,
    }
    instrument = {
        "symbol": "XBTUSDTM", "baseCurrency": "XBT", "quoteCurrency": "USDT", "settlementCurrency": "USDT",
        "contractType": "0", "marketStage": "NORMAL", "tradingStatus": "Open", "isInverse": False,
        "expiryTime": None, "settlementTime": None, "settlementFeeRate": None, "unitSize": "0.001",
        "lotSize": "1", "tickSize": "0.10", "maxPrice": "1000000.0000",
        "maxMarketOrderSize": "1000000", "maxBaseOrderSize": "1000000", "maxLeverage": "125",
        "makerFeeRate": "0.00020", "takerFeeRate": "0.00060",
    }
    isolated = [{
        "symbol": "XBTUSDTM", "level": 1, "minRiskLimit": 0, "maxRiskLimit": 100_000,
        "initialMargin": 0.008, "maintainMargin": 0.004, "maxLeverage": 125,
    }]
    return contract, instrument, isolated


def test_documented_quanto_contract_retains_linear_economics(terms_metadata):
    terms = adapt_xbtusdtm(*terms_metadata, modeled_liquidation_fee=0.003)
    assert VERSION == "kucoin-xbtusdtm-linear-isolated-v1"
    assert terms_metadata[0]["isQuanto"] is True
    assert terms.contract.family == "linear_perp"
    assert terms.contract.quote == "USDT"
    assert terms.contract.max_leverage == 100
    assert terms.funding_interval_ms == 28_800_000
    assert terms.funding_anchor_ms == 1_788_854_400_000

    # KuCoin's isolated-margin example: 1 lot at 50,000 USDT, 10x leverage.
    assert terms.base_quantity(1) == 0.001
    assert terms.notional(1, 50_000) == pytest.approx(50)
    margin = terms.initial_margin(1, 50_000, 10)
    assert margin == pytest.approx(5)
    assert margin + terms.published_taker_rate * terms.notional(1, 50_000) == pytest.approx(5.03)

    # KuCoin's PnL, funding and maintenance margin examples.
    assert terms.gross_pnl(100, 1, 5_000, 5_100) == pytest.approx(10)
    assert terms.gross_pnl(100, -1, 5_000, 5_100) == pytest.approx(-10)
    assert terms.maintenance_margin(1000, 30_000) == pytest.approx(120)
    assert terms.funding_cashflow(10, 1, 5_000, 0.0001) == pytest.approx(-0.005)
    assert terms.funding_cashflow(10, -1, 5_000, 0.0001) == pytest.approx(0.005)
    assert terms.funding_cashflow(10, 1, 5_000, -0.0001) == pytest.approx(0.005)


@pytest.mark.parametrize("side", [1, -1])
def test_adapter_and_existing_linear_position_agree(terms_metadata, side):
    terms = adapt_xbtusdtm(*terms_metadata, modeled_liquidation_fee=0.003)
    qty = terms.base_quantity(7)
    notional = terms.notional(7, 61_950)
    margin = terms.initial_margin(7, 61_950, 3)
    pos = Position(side, qty, 61_950, 10_000, margin, 0, notional)
    assert margin == pytest.approx(notional / 3)
    assert pos.unrealized(61_834) == pytest.approx(terms.gross_pnl(7, side, 61_950, 61_834))
    pos.fund(0.000315, 62_107)
    assert pos.funding == pytest.approx(terms.funding_cashflow(7, side, 62_107, 0.000315))
    boundary = liquidation_price(pos.entry, pos.reserved / qty, side, terms.contract)
    assert (boundary < pos.entry) is (side == 1)


def test_isolated_liquidation_price_matches_documented_long_example(terms_metadata):
    terms = adapt_xbtusdtm(*terms_metadata, modeled_liquidation_fee=0.0006)
    quantity = terms.base_quantity(1000)
    margin = terms.initial_margin(1000, 30_000, 50)
    assert liquidation_price(30_000, margin / quantity, 1, terms.contract) == pytest.approx(
        29_535.9, abs=0.1
    )


@pytest.mark.parametrize(("source", "field", "value"), [
    (0, "symbol", "ETHUSDTM"), (0, "type", "delivery"),
    (0, "isQuanto", False), (0, "isQuanto", 1), (0, "isInverse", True), (0, "isInverse", 0),
    (0, "baseCurrency", "ETH"), (0, "quoteCurrency", "USD"), (0, "settleCurrency", "XBT"),
    (0, "multiplier", 1), (0, "settleDate", 1_790_000_000_000), (0, "expireDate", 1_790_000_000_000),
    (0, "status", "Closed"), (0, "currentFundingRateGranularity", 0),
    (0, "nextFundingRateDateTime", 1_788_854_400), (0, "fundingRateFloor", 0),
    (0, "takerFixFee", 0.001), (0, "lotSize", 0.5), (0, "initialMargin", 0.009),
    (1, "isInverse", True), (1, "unitSize", "0.01"), (1, "tickSize", "0.01"),
    (1, "maxMarketOrderSize", "200"), (1, "minOrderSize", "1000001"),
    (1, "marketStage", "PRE_MARKET"), (1, "takerFeeRate", "0.0002"),
])
def test_metadata_mismatch_fails_closed(terms_metadata, source, field, value):
    terms_metadata[source][field] = value
    with pytest.raises(ValueError, match="XBTUSDTM"):
        adapt_xbtusdtm(*terms_metadata, modeled_liquidation_fee=0.003)


def test_missing_fields_and_invalid_isolated_tier_fail_closed(terms_metadata):
    contract, instrument, tiers = terms_metadata
    for missing in ("expireDate", "isQuanto", "makerFixFee", "currentFundingRateGranularity"):
        partial = {key: val for key, val in contract.items() if key != missing}
        with pytest.raises(ValueError, match="XBTUSDTM"):
            adapt_xbtusdtm(partial, instrument, tiers, modeled_liquidation_fee=0.003)
    for bad_tiers in ([], [tiers[0], tiers[0]], [{**tiers[0], "minRiskLimit": 10}],
                      [{**tiers[0], "symbol": "XBTUSDM"}], [{**tiers[0], "maintainMargin": "NaN"}]):
        with pytest.raises(ValueError, match="XBTUSDTM"):
            adapt_xbtusdtm(contract, instrument, bad_tiers, modeled_liquidation_fee=0.003)


def test_math_does_not_silently_cross_dated_limits(terms_metadata):
    terms = adapt_xbtusdtm(*terms_metadata, modeled_liquidation_fee=0.003)
    with pytest.raises(ValueError, match="integer"):
        terms.base_quantity(0.5)
    with pytest.raises(ValueError, match="level-one"):
        terms.initial_margin(2_001, 50_000, 3)
    with pytest.raises(ValueError, match="level-one"):
        terms.initial_margin(10, 50_000, 125)
    with pytest.raises(ValueError, match="level-one"):
        terms.maintenance_margin(2_001, 50_000)
    with pytest.raises(ValueError, match="dated bounds"):
        terms.funding_cashflow(10, 1, 5_000, 0.004)
