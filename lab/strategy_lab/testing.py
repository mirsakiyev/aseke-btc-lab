"""Small, deterministic signal contract shared by operator and automated tests."""
import pandas as pd

from lab.strategy_lab.evaluator import proposal


def signal_contract(spec, profile):
    # Tuesday midnight satisfies Day's entry cutoff and all profile grids.
    at = pd.Timestamp("2024-04-02T00:00:00Z")
    entry = spec["recipe"]["entry"]
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
