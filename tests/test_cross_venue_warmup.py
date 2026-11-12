from hashlib import sha256

import numpy as np
import pandas as pd
import pytest

from lab.strategy_lab import cross_venue_warmup as cross, ema_pullback
from lab.strategy_lab.market_data import MarketData, Observation, ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity


RAW = b"synthetic independent receipt for cross-venue feature tests"
RAW_HASH = sha256(RAW).hexdigest()
TRADE = ObservationKind.TRADE_OHLCV
SEAM = pd.Timestamp("2027-01-04T00:00Z")
STEP = pd.Timedelta("15min")


def snapshot(step=STEP, *, count=220, source_provider="Kraken", provenance=ProvenanceClass.INDEPENDENT):
    source = SourceIdentity(source_provider, "test feed", "BTC-USD", "SPOT", "test grant", RAW_HASH, provenance)
    start = SEAM - step * count
    seen = SEAM + pd.Timedelta(seconds=5)
    grant = SourceGrant(source, "original logical book", Purpose.WARMUP, frozenset({TRADE}), start, SEAM,
                        start, start, "test grant")
    rows = []
    for i, at in enumerate(pd.date_range(start, SEAM, freq=step, inclusive="left")):
        price = 100 + i * .025 + (i % 7) * .06
        rows.append(Observation(source.id, TRADE, at, at + step, seen, seen, RAW_HASH,
                                {"open": price, "high": price + 2, "low": price - 1, "close": price,
                                 "volume": 10_000.}))

    class Provider:
        def read(self, kind, start, end, as_of, interval):
            return iter(rows)

        def raw(self, raw_hash):
            assert raw_hash == RAW_HASH
            return RAW

    return MarketData([grant], {source.id: Provider()}).read(
        grant_id=grant.id, book_id=grant.book_id, purpose=Purpose.WARMUP, kind=TRADE,
        start=start, end=SEAM, as_of=seen, interval=step)


def native(seed, count, *, basis=.50):
    step = pd.Timedelta(seconds=seed.timeframe_seconds)
    opens = pd.date_range(SEAM, periods=count, freq=step)
    first = seed.last_close + basis
    closing = first + np.arange(count) * .05
    return pd.DataFrame({"open": closing, "high": closing + 1., "low": closing - .5, "close": closing,
                         "volume": np.arange(count) + 1., "close_at": opens + step,
                         "available_at": opens + step + pd.Timedelta(seconds=3),
                         "raw_sha256": RAW_HASH}, index=opens)


def evaluate(seed, data, *, as_of=None, source_blocked=False):
    market, instrument = ("SPOT", "BTC-USDT") if seed.timeframe_seconds == 86_400 else ("FUTURES", "XBTUSDTM")
    return cross.evaluate_transition(seed, data, as_of=as_of or data.available_at.max(), forward_provider="KuCoin",
                                     forward_market=market, forward_instrument=instrument, source_blocked=source_blocked)


@pytest.mark.parametrize("step", [STEP, pd.Timedelta("4h"), pd.Timedelta("1D")])
def test_proxy_keeps_exact_wilder_and_ema_state_and_uses_only_native_windows(step):
    source = snapshot(step)
    seed = cross.freeze_proxy_seed(source, timeframe=step)
    reference = ema_pullback.features(source.frame(), step, step).iloc[-1]
    assert (seed.ema_fast, seed.ema_slow, seed.atr) == (reference.ema_fast, reference.ema_slow, reference.atr)
    assert seed.end == SEAM and seed.count == 220
    assert cross.load_proxy_seed(seed.body(), seed.id) == seed
    with pytest.raises(ValueError, match="checkpoint or policy hash"):
        cross.load_proxy_seed({**seed.body(), "atr": seed.atr + 1}, seed.id)

    data = native(seed, 20)
    result = evaluate(seed, data)
    first, second, final = result.frame.iloc[[0, 1, -1]].itertuples()
    assert first.ema_fast == pytest.approx(seed.ema_fast * (49 / 51) + data.close.iloc[0] * (2 / 51))
    assert first.ema_slow == pytest.approx(seed.ema_slow * (199 / 201) + data.close.iloc[0] * (2 / 201))
    assert first.rsi == pytest.approx(reference.rsi)
    gain = (13 * seed.average_gain + .05) / 14
    loss = 13 * seed.average_loss / 14
    assert second.rsi == pytest.approx(100 - 100 / (1 + gain / loss))
    assert first.atr == pytest.approx((13 * seed.atr + 1.5) / 14)
    assert pd.isna(first.prior_low) and pd.isna(first.prior_close) and pd.isna(first.prior_ema_fast)
    assert second.prior_low == first.low and second.prior_ema_fast == first.ema_fast
    assert final.volume_sma == pytest.approx(10.5)
    assert final.swing_low == data.low.tail(5).min() and final.swing_high == data.high.tail(5).max()
    assert result.frame.source_mode.iloc[:19].eq(cross.DATA_INCOMPLETE).all()
    assert final.source_mode == cross.KRAKEN_PROXY_SEEDED and final.ready
    assert result.evidence["native_open_minus_kraken_close"] == pytest.approx(.50)
    assert result.evidence["native_volume_progress"] == 20 and result.evidence["native_swing_progress"] == 5
    assert result.evidence["first_native_candle"]["raw_sha256"] == RAW_HASH
    assert result.evidence["seed_state_id"] == seed.state_id
    assert result.id == evaluate(cross.load_proxy_seed(seed.body(), seed.id), data).id


def test_availability_and_gap_fail_closed_then_exact_native_only_replacement():
    seed = cross.freeze_proxy_seed(snapshot(), timeframe=STEP)
    data = native(seed, 223)
    early = evaluate(seed, data, as_of=data.close_at.iloc[19])
    assert early.evidence["source_mode"] == cross.DATA_INCOMPLETE
    assert early.evidence["reason"] == "LATEST_COMPLETED_NATIVE_CANDLE_UNAVAILABLE"
    pd.testing.assert_frame_equal(early.frame, evaluate(seed, data.iloc[:19], as_of=data.close_at.iloc[19]).frame)
    assert early.id == evaluate(seed, data.iloc[:19], as_of=data.close_at.iloc[19]).id

    data = data.drop(data.index[21])
    before_native_ready = evaluate(seed, data, as_of=data.available_at.iloc[-3])
    assert before_native_ready.evidence["source_mode"] == cross.DATA_INCOMPLETE
    at_native_ready = evaluate(seed, data, as_of=data.available_at.iloc[-1])
    assert at_native_ready.evidence["source_mode"] == cross.KUCOIN_NATIVE_READY
    assert at_native_ready.frame.source_mode.iloc[:21].eq(cross.KRAKEN_PROXY_SEEDED).sum() == 2
    reference = ema_pullback.features(data, STEP, STEP)
    for name in ("ema_fast", "ema_slow", "rsi", "atr", "volume_sma", "swing_low", "prior_low"):
        assert at_native_ready.frame[name].iloc[-1] == reference[name].iloc[-1]
    assert at_native_ready.frame.contiguous_bars.iloc[-1] == 201


def test_large_basis_blocks_proxy_and_recovery_is_native_at_exact_threshold():
    seed = cross.freeze_proxy_seed(snapshot(), timeframe=STEP)
    data = native(seed, 200, basis=seed.atr + 20)
    pending = evaluate(seed, data.iloc[:199])
    assert pending.evidence["source_mode"] == cross.DATA_INCOMPLETE
    assert pending.evidence["reason"] == "ABNORMAL_CROSS_VENUE_PRICE_BASIS"
    assert pending.evidence["abnormal_basis"]
    assert pending.frame.signal_side.eq(0).all()
    ready = evaluate(seed, data)
    assert ready.evidence["source_mode"] == cross.KUCOIN_NATIVE_READY
    assert ready.evidence["current_comparison"]["entry_signal_agreement"] in (True, False)
    assert ready.evidence["current_comparison"]["ema_slow_difference"] != 0


def test_native_late_receipt_delays_features_and_invalid_bracket_suppresses_signal():
    seed = cross.freeze_proxy_seed(snapshot(), timeframe=STEP)
    data = native(seed, 20)
    data.loc[data.index[-2], "low"] = .02
    late = data.available_at.iloc[-1] + pd.Timedelta(seconds=20)
    data.loc[data.index[-2], "available_at"] = late
    early = evaluate(seed, data, as_of=late - pd.Timedelta(seconds=1))
    assert early.frame.reason.iloc[-1] == "CONTIGUOUS_NATIVE_HISTORY_REQUIRED"
    assert not early.frame.ready.iloc[-1]
    result = evaluate(seed, data, as_of=late)
    last = result.frame.iloc[-1]
    assert last.available_at == late
    assert all(ema_pullback.gates(last)["long"].values())
    assert last.reason == "INVALID_SIGNAL_BRACKET" and last.signal_side == 0
    assert pd.isna(last.stop) and pd.isna(last.target)


def test_malformed_and_duplicate_visible_candles_break_proxy_continuity():
    seed = cross.freeze_proxy_seed(snapshot(), timeframe=STEP)
    data = native(seed, 20)
    data.loc[data.index[0], "volume"] = -1.
    duplicate = data.iloc[[5]].assign(close=data.close.iloc[5] + .05)
    result = evaluate(seed, pd.concat([data, duplicate]))
    assert result.evidence["source_mode"] == cross.DATA_INCOMPLETE
    assert result.evidence["invalid_native_opens"] == [data.index[0].isoformat(), data.index[5].isoformat()]
    assert result.evidence["first_native_candle"] is None and len(result.frame) == 18


def test_no_native_data_is_used_while_source_blocked_and_wrong_seed_rejected():
    seed = cross.freeze_proxy_seed(snapshot(), timeframe=STEP)
    data = native(seed, 20)
    result = evaluate(seed, data, source_blocked=True)
    assert result.evidence["source_mode"] == cross.SOURCE_BLOCKED
    assert result.evidence["first_native_candle"] is None and result.evidence["native_count"] == 0
    assert result.frame.empty and result.comparison.empty
    with pytest.raises(PermissionError, match="Kraken BTC-USD"):
        cross.freeze_proxy_seed(snapshot(source_provider="Coinbase"), timeframe=STEP)
    with pytest.raises(PermissionError, match="Protected or unverified"):
        cross.freeze_proxy_seed(snapshot(provenance=ProvenanceClass.PROTECTED), timeframe=STEP)
    with pytest.raises(ValueError, match="at least 200 consecutive"):
        cross.freeze_proxy_seed(snapshot(count=199), timeframe=STEP)
