"""Small, deterministic signal contract shared by operator and automated tests."""
import pandas as pd

from lab.strategy_lab import ema_pullback
from lab.strategy_lab.evaluator import proposal
from lab.strategy_lab.specification import SOURCE_POLICY


def signal_contract(spec, profile):
    # Tuesday midnight satisfies Day's entry cutoff and all profile grids.
    at = pd.Timestamp("2024-04-02T00:00:00Z")
    entry = spec["recipe"]["entry"]
    if entry == ema_pullback.FAMILY:
        return ema_signal_contract(spec, at)
    sign = 1 if "long" in spec["sides"] else -1
    params = spec["recipe"]["entry_parameters"]
    distance = params.get("deviation_atr", 1) + 1
    retracement = params.get("prior_retracement_atr", 1) + 1
    close = 100. - sign * distance if entry == "range_reversion_v1" else 100.
    row = {"ready": True, "uptrend": sign == 1, "downtrend": sign == -1, "close": close, "atr": 1.,
           "prior_high": 99., "prior_low": 101., "pivot_low": close - 1., "pivot_high": close + 1.,
           "prior_ema": 100., "prior_atr": 1., "prior_close": 100. - sign * retracement,
           "return_1": sign * .01, "context_abs": 0., "ema": 100., "rsi": .1 if sign > 0 else .9}
    frame = pd.DataFrame([row], index=[at])
    good = proposal(frame, at, spec, data_hash="controlled-fixture")
    assert good == proposal(frame, at, spec, data_hash="controlled-fixture")
    assert good["decision"] == "PROPOSAL" and good["setup"].direction == sign
    assert good["setup"].submit_at > good["setup"].available_at
    assert proposal(frame.assign(ready=False), at, spec, data_hash="controlled-fixture")["decision"] == "NO_TRADE"
    assert proposal(frame.iloc[:0], at, spec, data_hash="controlled-fixture")["decision"] == "NO_TRADE"
    if "entry_cutoff_minute_utc" in profile:
        blocked = at + pd.Timedelta(minutes=profile["entry_cutoff_minute_utc"])
        frame.index = [blocked]
        assert proposal(frame, blocked, spec, data_hash="controlled-fixture")["gates"]["session"] is False
    return ["determinism", "supported direction", "causal submit", "warmup", "missing decision", "applicable session"]


def ema_signal_contract(spec, at):
    sides = [sign for label, sign in (("long", 1), ("short", -1)) if label in spec["sides"]]
    for side in sides:
        row = {"ready": True, "close": 100., "ema_fast": 100. - side, "ema_slow": 100. - 2 * side,
               "prior_ema_fast": 100., "prior_close": 100., "prior_low": 99., "prior_high": 101.,
               "volume": 2., "volume_sma": 1., "rsi": 50., "atr": 2., "swing_low": 97., "swing_high": 103.}
        frame = pd.DataFrame([row], index=[at])
        good = proposal(frame, at, spec, data_hash="controlled-ema-fixture")
        assert good == proposal(frame, at, spec, data_hash="controlled-ema-fixture")
        assert good["decision"] == "PROPOSAL"
        setup = good["setup"]
        assert setup.direction == side and setup.stop == 100. - 4 * side and setup.target_price == 100. + 5 * side
        if spec["execution"]["version"] == SOURCE_POLICY:
            assert setup.submit_at == at and setup.deadline is None
        else:
            assert setup.submit_at > setup.available_at and setup.deadline > setup.submit_at
        occupied = proposal(frame, at, spec, data_hash="controlled-ema-fixture", position_open=True)
        assert occupied["decision"] == "NO_TRADE" and occupied["gates"]["account_flat"] is False
        for rejected in (frame.assign(ready=False), frame.assign(volume=1.),
                         frame.assign(rsi=40. if side == 1 else 60.), frame.iloc[:0]):
            assert proposal(rejected, at, spec, data_hash="controlled-ema-fixture")["decision"] == "NO_TRADE"
    return ["determinism", "profile sides", "strict volume and RSI", "signal-priced bracket", "entry timing",
            "flat-position gate", "warmup", "missing decision"]
