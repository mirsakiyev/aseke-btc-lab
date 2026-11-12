from base64 import b64decode
from copy import deepcopy
import gzip
from hashlib import sha256
import json
import socket

import numpy as np
import pandas as pd
import pytest

from lab.learning.data import Market
from lab.learning.simulator import Contract
from lab.strategy_lab import ema_pullback, ema_registration
from lab.strategy_lab.evaluator import compile_spec, evaluate, proposal, test_version as check_version
from lab.strategy_lab.prospective import propose
from lab.strategy_lab.registry import connect, register
from lab.strategy_lab.replay import run_case
from test_ema_pullback_registration import baseline_spec


@pytest.fixture
def registered_ema(tmp_path):
    with connect(tmp_path, write=True) as db:
        for profile in ("position", "swing"):
            register(db, baseline_spec(profile))
        records = ema_registration.register_ema_pullback(db)
        assert all(check_version(db, row["hash"])["state"] == "UNIT_TESTED" for row in records)
        return {row["strategy_id"].rsplit("/", 1)[1]: row for row in records}


def bullish_market():
    step = pd.Timedelta("1h")
    index = pd.date_range("2023-01-01", periods=300 * 24, freq=step, tz="UTC", name="open_at")
    chart_day = np.arange(len(index)) // 24
    daily = 100. + np.arange(300) * .1
    daily[213] = daily[212]
    close = daily[chart_day]
    frame = pd.DataFrame({"open": close, "high": close + .12, "low": close - .12, "close": close,
                          "volume": 1., "close_at": index + step, "received_at": index + step}, index=index)
    frame.loc[index[211 * 24 + 23], "low"] -= 7.
    frame.loc[index[212 * 24:213 * 24], "volume"] = 2.
    return frame, index[213 * 24], step


def test_causal_proposal_and_spot_profile(registered_ema):
    frame, at, step = bullish_market()
    spec = registered_ema["position"]["spec"]
    as_of = at + pd.Timedelta(seconds=30)
    result = evaluate(frame, spec, at, as_of=as_of, data_hash="saved-fixture")
    assert result["decision"] == "PROPOSAL"
    assert all(result["gates"]["long"].values())
    assert result["values"]["ema_fast"] > result["values"]["ema_slow"]
    assert result["setup"]["submit_at"] == at + step
    assert result["setup"]["stop"] == pytest.approx(result["values"]["swing_low"] - .5 * result["values"]["atr"])
    assert result["setup"]["target_price"] == pytest.approx(result["values"]["close"] + 2.5 * result["values"]["atr"])
    future_changed = frame.copy()
    future_changed.loc[future_changed.index >= at, "volume"] *= 50
    assert evaluate(future_changed, spec, at, as_of=as_of, data_hash="saved-fixture") == result
    assert evaluate(frame, spec, at, as_of=at, data_hash="saved-fixture")["gates"] == {"decision_available": False}
    occupied = evaluate(frame, spec, at, as_of=as_of, data_hash="saved-fixture", position_open=True)
    assert occupied["decision"] == "NO_TRADE" and occupied["gates"]["account_flat"] is False
    late = frame.copy()
    late.loc[at - step, "received_at"] = as_of + pd.Timedelta(seconds=1)
    assert evaluate(late, spec, at, as_of=as_of, data_hash="saved-fixture")["gates"] == {"completed_decision": False}
    invalid = frame.copy()
    invalid.loc[at - step, "volume"] = np.nan
    assert evaluate(invalid, spec, at, as_of=as_of, data_hash="saved-fixture")["decision"] == "NO_TRADE"

    bullish = ema_pullback.features(frame.loc[frame.close_at <= at], "1D", step).loc[[at]].copy()
    bearish = bullish.assign(ema_fast=125., ema_slow=130., prior_high=126., prior_ema_fast=125., rsi=50.)
    assert ema_pullback.sides(bearish).iloc[0] == -1
    blocked = proposal(bearish, at, spec, data_hash="saved-fixture")
    assert blocked["decision"] == "NO_TRADE" and blocked["gates"]["eligible_side"] is False


def test_isolated_spot_replay_preserves_bracket_and_audit(registered_ema, monkeypatch, tmp_path):
    frame, at, step = bullish_market()
    spec = registered_ema["position"]["spec"]
    market = Market(frame, frame, {}, step, "CONTROLLED_FIXTURE")
    contract = Contract("spot", "USDT", 1., .001, .01, .001, 1e7, 1, 0., 0.)
    first, end = at - pd.Timedelta(days=2), at + pd.Timedelta(days=40)
    intervals = {"development": [first - pd.Timedelta(days=10), first], "validation": [first, end]}
    untouched = tmp_path / "protected-phase15-paper.sqlite3"
    untouched.write_bytes(b"synthetic phase 15 equity sentinel")

    def prohibit_network(*args, **kwargs):
        pytest.fail("Synthetic replay must not connect to a network, including Telegram")

    monkeypatch.setattr(socket.socket, "connect", prohibit_network)
    result = run_case(market, contract, spec, "validation/base", intervals, "synthetic", {})
    assert result["eligible_setups"] == 1 and result["metrics"]["closed_trades"] == 1
    assert result["execution_reasons"] == {"target": 1}
    assert result["status"] == "INCONCLUSIVE"  # One fixture trade is not a historical screen.
    audit = result["decision_audit"]
    raw = gzip.decompress(b64decode(audit["data"]))
    assert sha256(raw).hexdigest() == audit["sha256"]
    rows = [dict(zip(audit["columns"], row)) for row in json.loads(raw)]
    assert len(rows) == 42 and rows[2]["long.prior_touch"] is True
    assert rows[2]["profile_signal"] == 1 and rows[2]["account_replay"] == "COMPLETE/target"
    assert rows[0]["long.volume"] is False and rows[0]["profile_signal"] == 0 and rows[0]["account_replay"] is None
    assert untouched.read_bytes() == b"synthetic phase 15 equity sentinel"


def test_reference_timing_and_futures_dependencies(registered_ema, tmp_path):
    spec = registered_ema["source"]["spec"]
    at = pd.Timestamp("2024-04-02T00:00:00Z")
    row = pd.DataFrame([{"ready": True, "close": 100., "ema_fast": 101., "ema_slow": 103.,
                         "prior_ema_fast": 100., "prior_close": 101., "prior_low": 99., "prior_high": 102.,
                         "volume": 2., "volume_sma": 1., "rsi": 59., "atr": 2.,
                         "swing_low": 95., "swing_high": 105.}], index=[at])
    result = proposal(row, at, spec, data_hash="saved-fixture")
    assert result["setup"].submit_at == at and result["setup"].deadline is None
    assert result["setup"].source_allocation and result["setup"].direction == -1
    assert result["setup"].stop == 106 and result["setup"].target_price == 95
    incompatible = deepcopy(spec)
    incompatible["data"]["dependencies"].remove("settled_funding")
    with pytest.raises(ValueError, match="Futures requires trade, mark, settled funding"):
        compile_spec(incompatible)

    with connect(tmp_path, write=True) as db:
        record = registered_ema["swing"]
        assert record["flags"]["live_permission"] == "DISABLED"
        assert record["flags"]["prospective_permission"] == "DISABLED"
        with pytest.raises(ValueError, match="Unknown job"):
            propose(db, record["hash"], "no-evaluated-job", {})
        assert db.execute("SELECT COUNT(*) FROM proposal").fetchone()[0] == 0
