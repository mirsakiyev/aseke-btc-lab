from decimal import Decimal

import pandas as pd
import pytest

from lab.backtest import Simulation
from lab.config import load_profiles
from lab.phase2 import store
from lab.phase4 import protocol
from lab.phase4.config import AccountFeeEvidence, MarketIdentity, REFERENCE, ResearchConfig, bps_fraction, load_config
from lab.phase4.mexc import (
    INTERVAL_MS, PROFILE_INTERVALS, CandleWindow, candle_windows, offline_quantity_check, parse_book_ticker,
    parse_candles, parse_exchange_info,
)
from lab.phase4.sensitivity import fixed_ledger_reprice
from lab.storage import file_hash, read_json, utc


def test_bps_and_published_fees_do_not_assert_account_verification():
    config = load_config()
    fee = config.published
    execution = fee.execution_sensitivity(REFERENCE)
    assert bps_fraction(50) == Decimal("0.005")
    assert bps_fraction(5) == Decimal("0.0005")
    assert 2 * bps_fraction(execution.fee_bps + execution.proxy_bps) == Decimal("0.002")
    assert fee.name == "mexc_published_spot_taker_research"
    assert fee.fee_provenance == "official_published_standard" and not fee.account_pair_channel_fee_verified
    assert config.account_fee_evidence is None and fee.proposed_symbol == "BTCUSDT" and not fee.proposed_pair_confirmed
    assert fee.simulated_entry_liquidity == fee.simulated_exit_liquidity == "taker"
    assert not fee.mx_deduction_enabled and not fee.promotional_zero_fee_assumed


def test_manual_promotion_evidence_does_not_transfer_to_api_or_imply_mx_discount():
    record = {
        "symbol": "BTCUSDT", "channel": "manual_web_app", "maker_fee_bps": 0, "taker_fee_bps": 0,
        "evidence_date": "2026-11-08", "evidence_reference": "synthetic manual fee screen",
        "account_pair_qualification": "fixture qualification", "promotion_applied": True,
        "promotion_expires_on": "2026-11-09", "mx_deduction_enabled": False,
        "mx_balance_qualification_evidence": None,
    }
    assert AccountFeeEvidence(**record).channel == "manual_web_app"
    with pytest.raises(ValueError, match="excludes API"):
        AccountFeeEvidence(**{**record, "channel": "api"})
    with pytest.raises(ValueError, match="known expiry"):
        AccountFeeEvidence(**{**record, "promotion_expires_on": None})
    with pytest.raises(ValueError, match="balance evidence"):
        AccountFeeEvidence(**{**record, "mx_deduction_enabled": True})
    config = load_config().model_dump()
    config["account_fee_evidence"] = record
    with pytest.raises(ValueError, match="requires permitted use"):
        ResearchConfig(**config)


def test_venue_currency_data_and_cost_are_model_compatibility_boundaries():
    market = protocol.proposed_market(load_config(), "1")
    reference = MarketIdentity(**{**market.model_dump(), "venue": "coinbase_exchange",
                                 "symbol": "BTC-USD", "quote_currency": "USD", "data_version": "original_snapshot",
                                 "cost_version": "reference"})
    with pytest.raises(ValueError, match="venue.*symbol.*quote_currency.*data_version.*cost_version"):
        reference.require_compatible(market)
    with pytest.raises(ValueError, match="cost_version"):
        market.require_compatible(MarketIdentity(**{**market.model_dump(), "cost_version": "another_cost"}))
    with pytest.raises(ValueError, match="must agree"):
        MarketIdentity(**{**market.model_dump(), "quote_currency": "USD"})


def fixture_candle(open_ms, step):
    return [open_ms, "100.05", "105.00", "99.90", "103.50", "0.01234567", open_ms + step, "1.27000000"]


def test_spot_v3_millisecond_pagination_and_closed_bar_schema():
    market = protocol.proposed_market(load_config(), "1")
    start = int(utc("2026-11-08").timestamp() * 1000)
    step = INTERVAL_MS["60m"]
    assert set(INTERVAL_MS) == {"5m", "15m", "60m", "4h", "1d", "1W"}
    assert PROFILE_INTERVALS["swing"] == {"decision": "4h", "context": ("1d",), "execution": "60m"}
    pages = list(candle_windows(market, "60m", start, start + 1001 * step, anchor_ms=0, max_pages=3))
    assert [p.parameters["limit"] for p in pages] == [500, 500, 1]
    assert pages[0].parameters["endTime"] + 1 == pages[1].parameters["startTime"]
    with pytest.raises(ValueError, match="page budget"):
        list(candle_windows(market, "60m", start, start + 1001 * step, anchor_ms=0, max_pages=2))
    with pytest.raises(ValueError, match="millisecond"):
        list(candle_windows(market, "60m", start // 1000, start + step, anchor_ms=0))
    window = CandleWindow(market, "60m", start, start + 3 * step, 0)
    first = fixture_candle(start, step)
    forming = fixture_candle(start + 2 * step, step)
    parsed = parse_candles([forming, first, first], window, start + 2 * step)
    assert parsed.candles[0].close_ms == start + step
    assert parsed.candles[0].base_volume == Decimal("0.01234567")
    assert parsed.candles[0].quote_volume == Decimal("1.27000000")
    assert parsed.incomplete_open_ms == (start + 2 * step,)
    assert parsed.missing_closed_open_ms == (start + step,) and parsed.duplicate_rows == 1
    alternative = first.copy()
    alternative[6] -= 1
    with pytest.raises(ValueError, match="close time"):
        parse_candles([alternative], window, start + 2 * step)
    alternative = first.copy()
    alternative[4] = "102"
    with pytest.raises(ValueError, match="Conflicting"):
        parse_candles([first, alternative], window, start + 2 * step)


def synthetic_exchange_info() -> dict:
    return {"symbols": [{
        "symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "1", "tradeSideType": 1,
        "baseAssetPrecision": 4, "quotePrecision": 2, "quoteAssetPrecision": 2,
        "isSpotTradingAllowed": True, "orderTypes": ["LIMIT", "MARKET", "LIMIT_MAKER"],
        "baseSizePrecision": "0.0003", "quoteAmountPrecision": "5", "maxQuoteAmount": "100000",
        "quoteAmountPrecisionMarket": "10", "maxQuoteAmountMarket": "100000",
        "makerCommission": "0", "takerCommission": "0.0005",
    }]}


def test_public_metadata_rounds_down_and_keeps_spread_diagnostic():
    config = load_config()
    market = protocol.proposed_market(config, "1")
    metadata = parse_exchange_info(synthetic_exchange_info(), market)
    assert metadata.minimum_base_quantity == Decimal("0.0003") and metadata.price_decimal_places == 2
    assert metadata.public_taker_commission == Decimal("0.0005") and config.account_fee_evidence is None
    # At this price the exact risk limit would cover 10 USDT, but rounding down fails the minimum.
    qty, state = offline_quantity_check(metadata, Decimal("0.00129"), Decimal("8000"))
    assert qty == Decimal("0.0012") and state == "below_minimum"
    qty, state = offline_quantity_check(metadata, Decimal("0.00139"), Decimal("8000"))
    assert qty == Decimal("0.0013") and state == "passes_offline_metadata_checks"
    bad = synthetic_exchange_info()
    bad["symbols"][0]["tradeSideType"] = 2
    assert offline_quantity_check(parse_exchange_info(bad, market), qty, Decimal("8000"))[1] == (
        "spot_market_capability_unavailable"
    )
    ticker = parse_book_ticker({"symbol": "BTCUSDT", "bidPrice": "99", "bidQty": "1", "askPrice": "101",
                                "askQty": "2"}, market, int(utc("2026-11-08").timestamp() * 1000))
    assert ticker.observed_spread_bps == Decimal(200)
    assert config.published.execution_sensitivity(REFERENCE).proxy_bps == 5


def test_fixed_repricing_changes_fee_cash_and_never_changes_fills():
    at = utc("2025-01-01")
    step = pd.Timedelta("1h")
    buy, sell = 100.05, 109.945
    fee = (buy + sell) * .005
    net = sell - buy - fee
    after_buy = 1000 - buy * 1.005
    equity = pd.DataFrame({"cash": [after_buy, 1000 + net], "equity": [after_buy + 100, 1000 + net],
                           "units": [1, 0], "btc_weight": [100 / (after_buy + 100), 0], "drawdown": 0.,
                           "cash_benchmark": 1000., "full_btc": 1000., "passive_25pct": 1000.},
                          index=[at + step, at + 2 * step])
    trades = pd.DataFrame({"signal_at": [at], "entry_at": [at], "exit_at": [at + step], "exit_bar_open": [at + step],
                           "entry_price": [buy], "exit_price": [sell], "entry_notional": [buy], "exit_notional": [sell],
                           "units_btc": [1.], "fees_usd": [fee], "net_usd": [net]})
    repriced = fixed_ledger_reprice(Simulation({}, equity, trades, pd.DataFrame()), REFERENCE,
                                   load_config().published.execution_sensitivity(REFERENCE), 1000, step)
    pd.testing.assert_frame_equal(repriced.trades[["entry_price", "exit_price", "units_btc"]],
                                  trades[["entry_price", "exit_price", "units_btc"]])
    assert repriced.trades.fees_usd.iloc[0] == pytest.approx((buy + sell) * .0005)
    assert repriced.equity.equity.iloc[-1] == pytest.approx(1000 + sell - buy - (buy + sell) * .0005)
    assert trades.fees_usd.iloc[0] == fee


def test_blocked_draft_is_idempotent_and_preserves_existing_experiment(tmp_path, monkeypatch):
    config = load_config()
    state = protocol.readiness(config)
    assert state["execution_eligibility"] == "RESTRICTED" and state["prospective_activation"] == "BLOCKED"
    assert state["public_data_access"] == "OFFLINE" and state["data_status"] == "INSUFFICIENT_DATA"
    unresolved = config.model_dump()
    unresolved["eligibility"]["residency_country_context"] = None
    assert protocol.readiness(ResearchConfig(**unresolved))["execution_eligibility"] == "UNRESOLVED"
    snapshot = tmp_path / "observation.sqlite3"
    experiment = {"actual_activation": "preserved", "model_versions": {"swing": "original"}}
    with store.connect(snapshot) as db, db:
        store.set_meta(db, "experiment", experiment)
        store.set_account(db, "swing", {"cash": 9234.56, "pending_entry": {"fill": "original"}})
    before = file_hash(snapshot)
    profile = load_profiles()["swing"]
    origin = tmp_path / "original"
    origin.mkdir()
    (origin / "complete.json").write_text("{}", encoding="utf-8")
    plan = {"profile": profile.model_dump(), "design_sha256": "fixture", "phase1_logic_sha256": "fixture",
            "windows": {"holdout_start": "2025-11-08", "data_cutoff": "2026-11-08"}}
    sample = {"start_mark": "2023-07-01", "end_mark": "2025-11-08", "observed_hourly_marks": 20659,
              "positions_closed": 68}
    monkeypatch.setattr(protocol, "source", lambda: (
        plan, {"scenarios": {"reference": {"methods": {"baseline": {"matched": sample}}}}}, origin,
    ))
    draft = protocol.register_draft(config, tmp_path / "protocols")
    saved = file_hash(draft)
    assert protocol.register_draft(config, tmp_path / "protocols") == draft and file_hash(draft) == saved
    account = read_json(draft)["hypothetical_account"]
    assert account["currency"] == "USDT" and account["actual_activation_utc"] is None and account["account_id"] is None
    assert file_hash(snapshot) == before
