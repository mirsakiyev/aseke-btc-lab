from copy import deepcopy

import httpx
import numpy as np
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from lab.learning.design import catalog, config
from lab.learning.simulator import Contract, Cost, Order, Setup
from lab.phase9 import data, ideas, public, reference, research
from lab.storage import file_hash, utc, write_json


@pytest.fixture
def parent():
    settings = config()
    folds = deepcopy(settings["evaluation"]["folds"])
    folds[-1]["outer_end"] = "2025-11-08T00:00:00Z"
    return {"id": "phase8", "design": {"settings": settings, "folds": folds, "seed": 308,
            "holdout": {"start": "2025-11-08T00:00Z", "end_exclusive": "2026-11-08T00:00Z"},
            "available_at_cutoff_exclusive": "2025-11-08T00:00Z", "trials": list(catalog(settings)),
            "feature_version": "fixture-features", "label_version": "fixture-labels", "code_sha256": {"fixture": "same"}}}


def test_kucoin_public_candle_funding_and_quote_units():
    left, right = utc("2025-01-02T00:00Z"), utc("2025-01-02T00:02Z")
    opened = int(left.timestamp())
    perp = public.kline("p", "XBTUSDTM", "FUTURES", "TRADE", "1min", left, right)
    payload = {"tradeType": "FUTURES", "symbol": "XBTUSDTM", "list": [[opened, "100", "104", "98", "102", "2345", "238"]]}
    result = data.candles(payload, perp, right, .001)
    assert result[["open", "high", "low", "close"]].iloc[0].tolist() == [100, 104, 98, 102]
    assert result.volume.iloc[0] == pytest.approx(2.345)
    assert result.turnover_quote.iloc[0] == 238
    assert perp["params"]["startAt"] == opened
    with pytest.raises(ValueError, match="symbol"):
        data.candles({**payload, "symbol": "BTC-USDT"}, perp, right, .001)
    with pytest.raises(ValueError, match="Timestamp"):
        data.candles({**payload, "list": [[opened * 1000, 100, 104, 98, 102, 1, 2]]}, perp, right, .001)
    mark = public.kline("m", "XBTUSDTM", "FUTURES", "MARK_PRICE", "1min", left, right)
    assert "volume" not in data.candles(payload, mark, right)
    funding = public.funding("f", left, right)
    rates = data.settled_rates({"symbol": "XBTUSDTM", "list": [{"ts": opened * 1000, "fundingRate": "-0.0001"}]}, funding)
    assert funding["params"]["startAt"] == opened * 1000 and rates.rate.iloc[0] == -.0001
    spot = public.kline("s", "BTC-USDT", "SPOT", "TRADE", "1min", left, right)
    assert data.candles({**payload, "tradeType": "SPOT", "symbol": "BTC-USDT"}, spot, right).volume.iloc[0] == 2345
    quote = data.market_quote({"tradeType": "SPOT", "ts": left.value, "list": [
        {"symbol": "BTC-USDT", "lastPrice": "101", "bestBidPrice": "100", "bestAskPrice": "102"},
    ]}, "BTC-USDT", "SPOT", right)
    assert utc(quote["at"]) == left


def test_taker_decimal_costs_on_both_legs():
    cfg = reference.reference()
    assert cfg["fees"]["kucoin_spot_class_a_vip0_v1"]["taker"] == .001
    assert cfg["fees"]["kucoin_linear_perp_vip0_v1"]["taker"] == .0006
    at = utc("2026-11-09")
    setup = Setup(at, at, at, at, at, 100, 5, 90, 1, 2, None, "breakout", "fixed")
    contract = Contract("linear_perp", "USDT", .001, 1, .1, 1, 250000, 100, .004, .003)
    order = Order(1000, 100, 1000, 25, 25, 3, None, None)
    result = ideas.estimate(order, setup, contract, Cost(.0006, 0))
    assert result["base_quantity"] == 1
    assert result["net_loss_scenario"] == pytest.approx(10 + .0006 * 190)
    assert result["net_reward_scenario"] == pytest.approx(20 - .0006 * 220)


def test_prospective_features_exclude_future_candle_and_fail_stale_quote(parent):
    cutoff = utc("2026-11-09")
    now = cutoff + pd.Timedelta("8h")
    index = pd.date_range(end=cutoff - pd.Timedelta("1D"), periods=168, freq="1D")
    values = 100 + np.arange(len(index)) * .05 + np.sin(np.arange(len(index)) / 4) * 3
    frame = pd.DataFrame({"open_at": index, "close_at": index + pd.Timedelta("1D"), "open": values,
                          "high": values + 1, "low": values - 1, "close": values, "volume": 10})
    future = pd.DataFrame([{"open_at": cutoff, "close_at": cutoff + pd.Timedelta("1D"),
                            "open": 9999, "high": 9999, "low": 9999, "close": 9999, "volume": 100}])
    at = now - pd.Timedelta("1s")
    quote = {"at": at.isoformat(), "received_at": now.isoformat(), "bid": 101., "ask": 102., "last": 101.}
    args = (quote, None, {}, reference.reference(), parent, now)
    original = ideas.compute("position", frame, *args)
    later = ideas.compute("position", pd.concat([frame, future]), *args)
    assert original["input_digest"] == later["input_digest"]
    assert original["structure"] == later["structure"]
    assert original["structure"]["feature_ready"] and original["classification"] == "EXPERIMENTAL"
    assert original["validation"] == "No validated model" and original["model_version"] is None
    stale = {**quote, "at": (now - pd.Timedelta("3min")).isoformat()}
    assert ideas.compute("position", frame, stale, *args[1:])["status"] == "DATA_NOT_READY"
    assert ideas.compute("position", frame.iloc[:-1], *args)["status"] == "DATA_NOT_READY"


def test_qualified_rules_only_idea_contains_frozen_manual_risk(parent, monkeypatch):
    cutoff = utc("2026-11-09")
    now = cutoff + pd.Timedelta("8h")
    row = {"close": 100., "prior_high": 99., "prior_low": 94., "atr": 4., "return_1": .01,
           "ema_distance": .03, "rsi": .6, "context_abs": .03, "context_distance_0": .03,
           "uptrend": True, "downtrend": False, "ready": True, "pivot_low": 95.}
    monkeypatch.setattr(ideas, "build_features", lambda *_: (pd.DataFrame([row], index=[cutoff]), []))
    frame = pd.DataFrame([{"open_at": cutoff - pd.Timedelta("1D"), "close_at": cutoff, "open": 98.,
                           "high": 101., "low": 97., "close": 100., "volume": 30.}])
    quote = {"at": now.isoformat(), "received_at": now.isoformat(), "ask": 100.1, "last": 100.}
    rules = {"as_of": now.isoformat(), "spot": {"feeCategory": "classA", "tradingStatus": "TradingEnabled",
              "baseCurrency": "BTC", "quoteCurrency": "USDT", "baseOrderStep": "0.00000001", "tickSize": ".1",
              "minBaseOrderSize": ".00001", "maxQuoteOrderSize": "1000000", "maxBaseOrderSize": "1000",
              "minFunds": ".1"}}
    result = ideas.compute("position", frame, quote, None, rules, reference.reference(), parent, now)
    assert result["status"] == "CONDITIONAL" and result["direction"] == "LONG"
    assert result["entry"]["minimum"] == 99 and result["entry"]["maximum"] == 101
    assert result["stop"] == {"price": 95., "basis": "LAST"}
    assert result["take_profit"] == [{"price": 113., "allocation_fraction": 1.}]
    assert result["paper"]["risk_budget"] == 25 and result["paper"]["planned_risk"] <= 25
    assert result["execution_mode"] == "MANUAL_OUTSIDE_LAB" and result["evidence_basis"] == "RULES_ONLY"
    assert result["model_version"] is None and "win_probability" not in result


def test_idea_market_events_are_distinct_from_simulated_and_user_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(ideas, "OUTPUT", tmp_path)
    monkeypatch.setattr(ideas, "RECORDS", tmp_path / "ideas")
    at = utc("2026-11-09T12:00Z")
    now = at + pd.Timedelta(minutes=1)
    monkeypatch.setattr(ideas, "utc_now", lambda: now)
    plan = ideas.publish({"profile": "swing", "source_cutoff": at.isoformat(), "decision_at": at.isoformat(),
                          "status": "CONDITIONAL", "direction": "LONG", "expires_at": (at + pd.Timedelta("5min")).isoformat(),
                          "stop": {"price": 90, "basis": "MARK"},
                          "entry": {"trigger": 100, "minimum": 100, "maximum": 102, "price_basis": "ASK"}}, "ref")
    quote = {"at": now.isoformat(), "received_at": (now - pd.Timedelta("300ms")).isoformat(), "ask": 101, "last": 91}
    mark = {"at": now.isoformat(), "received_at": now.isoformat(), "mark": 95}
    assert ideas.observation_state(plan, quote, mark, now, 120) is None
    assert ideas.observation_state(plan, quote, mark, now, 120, 1) == "ENTRY_CONDITION_OBSERVED"
    assert ideas.observation_state(plan, {**quote, "ask": 103}, mark, now, 120, 1) == "ENTRY_OUTSIDE_RANGE"
    assert ideas.observation_state(plan, {**quote, "ask": 99}, mark, now, 120, 1, True) == "ENTRY_OUTSIDE_RANGE"
    assert ideas.observation_state(plan, quote, {**mark, "mark": 89}, now, 120, 1) == "INVALIDATED"
    assert ideas.observation_state(plan, quote, mark, at + pd.Timedelta("5min"), 120, 1) == "EXPIRED"
    assert not ideas.fresh({**quote, "at": (now + pd.Timedelta("1ms")).isoformat()}, now, 120, 1)
    assert ideas.append_event(plan, "ENTRY_CONDITION_OBSERVED", {"quote": quote}, now)["classification"] == "MARKET_OBSERVATION"
    assert ideas.recent("swing")[0]["user_reported_events"] == []
    ideas.append_event(plan, "SIMULATED_FILL", {"scope": "SIMULATOR_DIAGNOSTIC"}, now)
    ideas.user_execution(plan["idea_id"], {"executed_at": now.isoformat(), "unit": "contracts", "price": 101, "quantity": 2})
    record = ideas.recent("swing")[0]
    assert record["market_state"] == "ENTRY_CONDITION_OBSERVED"
    assert len(record["simulation_events"]) == len(record["user_reported_events"]) == 1
    assert ideas.publish({**plan, "published_at": now.isoformat()}, "ref") == plan


def test_controls_persist_independently(tmp_path, monkeypatch):
    monkeypatch.setattr(reference, "ROOT_DATA", tmp_path)
    reference.set_control("monitor", True)
    reference.set_control("ideas", True, "swing")
    assert reference.controls() == {"monitor": True, "training": False,
                                     "ideas": {"position": False, "swing": True, "day": False}}
    reference.set_control("training", True)
    reference.set_control("monitor", False)
    assert reference.controls()["ideas"]["swing"] and reference.controls()["training"]


def test_import_respects_holdout_and_preserves_alternative_venue(tmp_path, monkeypatch, parent):
    monkeypatch.setattr(data, "OUTPUT", tmp_path)
    monkeypatch.setattr(data, "parent_design", lambda: parent)
    raw, desc = tmp_path / "sample.csv", tmp_path / "description.json"
    raw.write_text("open_at,open,high,low,close,volume\n2025-11-07T23:00:00Z,1,2,1,2,10\n"
                   "2025-11-08T00:00:00Z,5,6,5,6,10\n", encoding="utf-8")
    descriptor = {"venue": "Permitted fixture venue", "trade_type": "SPOT", "symbol": "BTC-USDT",
                  "quote": "USDT", "kind": "trade", "interval": "1hour", "timestamp_unit": "iso_utc",
                  "volume_unit": "base", "retrieved_at": "2025-11-09T00:00Z", "permission_evidence": "fixture",
                  "source": "fixture.csv", "start": "2025-11-07T23:00Z", "end_exclusive": "2025-11-08T00:00Z"}
    write_json(desc, descriptor)
    record = data.import_data(raw, desc)
    assert record["rows"] == 1 and record["descriptor"]["venue"] == "Permitted fixture venue"
    assert data.import_data(raw, desc) == record
    write_json(desc, {**descriptor, "end_exclusive": "2025-11-08T01:00Z"})
    with pytest.raises(ValueError, match="sealed"):
        data.import_data(raw, desc)


def test_public_raw_payload_digest_and_retry_boundary(tmp_path):
    spec = public.request("spot", "instrument", tradeType="SPOT", symbol="BTC-USDT")
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"code": "200000", "data": []}))) as client:
        row = public.send(client, spec, 1000, tmp_path / "raw")
    assert row["status"] == "RECEIVED"
    assert file_hash(tmp_path / "raw" / f"{row['raw_sha256']}.json") == row["raw_sha256"]
    assert public.load_payload(tmp_path, row) == []
    now = utc("2026-11-09")
    assert public.retry_boundary("90", now) == now + pd.Timedelta(seconds=90)


def test_lineage_uses_only_selected_profile_datasets_and_kucoin_costs(monkeypatch, parent):
    monkeypatch.setattr(research, "parent_design", lambda: parent)
    monkeypatch.setattr(research, "freeze_reference", lambda _: "ref")
    monkeypatch.setattr(research, "run_folder", lambda _: None)
    monkeypatch.setattr(research, "metadata", lambda _: {"as_of": "2026-11-09", "sources": {},
                        "spot": {"minFunds": ".1"}})
    monkeypatch.setattr(research, "current_rules", lambda *_: Contract("spot", "USDT", 1, 1e-8, .1, 1e-5, 1e6, 1, 0, 0))
    source = {"trade": [{"rows": 10, "first": "2022-06-30T00:00Z", "last": "2025-11-07T23:00Z", "id": "spot"}]}
    monkeypatch.setattr(research, "assets", lambda *_: deepcopy(source))
    bundle = {"metadata_plan": "x", "rules_basis": "OBSERVED_CURRENT_AS_HYPOTHETICAL", "unrelated_future": "v1"}
    first = research.design_for("position", bundle)
    assert first["id"] == research.design_for("position", {**bundle, "unrelated_future": "v2"})["id"]
    assert first["body"]["design"]["settings"]["profiles"]["position_spot_research_v1"]["instrument"] == (
        "kucoin_btc_usdt_spot_current_v1"
    )
    assert all(trial["cost"] == "kucoin_spot_class_a_vip0_v1" for trial in first["body"]["design"]["trials"])
    assert len([trial for trial in first["body"]["design"]["trials"] if trial["role"] == "primary"]) == 18


def test_post_inner_futures_history_does_not_qualify_even_with_outer_coverage(parent):
    sample = [{"first": "2024-12-30T12:00Z", "last": "2025-11-07T23:59Z"}]
    gates = research.coverage_gates({"trade": sample, "mark": sample}, parent["design"]["folds"])
    assert gates["y2025"]["outer_interval_bounded"]
    assert not any(row["train_price_interval_present"] for row in gates.values())


def test_dashboard_persists_controls_and_preserves_validated_lane(tmp_path, monkeypatch):
    monkeypatch.setattr(reference, "ROOT_DATA", tmp_path)
    monkeypatch.setattr(ideas, "RECORDS", tmp_path / "ideas")
    app = AppTest.from_string("from lab.phase9.dashboard import panel\npanel()").run()
    assert not app.exception
    assert app.info[0].value == "No validated model"
    app.toggle(key="phase9-ideas-position").set_value(True).run()
    assert not app.exception and reference.controls()["ideas"]["position"]
    assert not reference.controls()["training"] and not reference.controls()["monitor"]
