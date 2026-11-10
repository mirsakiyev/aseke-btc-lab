from contextlib import closing
from dataclasses import asdict
import json
from types import SimpleNamespace

import httpx
import numpy as np
import pandas as pd
import pytest

from lab.learning.data import Market
from lab.learning.design import config
from lab.learning.simulator import Contract, Cost, Risk, Setup
from lab.phase10.trading import run_trade
from lab.phase13 import observation
from lab.phase16 import allocation, control, evaluate, plan
from lab.phase17 import admission, collection, labels, registry, source
from lab.storage import immutable_bytes, read_json, utc, write_json


def test_native_funding_and_receipt_maturity_across_midnight():
    start = utc("2027-03-01T23:59Z")
    minutes = pd.date_range(start, periods=8, freq="1min")
    step = pd.Timedelta(minutes=1)
    candles = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100., "volume": 10.,
                            "close_at": minutes+step}, index=minutes)
    market = Market(candles, candles, {minutes[1]: .001}, step, "SYNTHETIC")
    market.intrabar_funding = {}
    receipt = pd.Series(minutes+step+pd.Timedelta(seconds=45), index=minutes)
    settled = {minutes[1]: (minutes[1], minutes[5], .001)}
    setup = Setup(start-step, start-step, start, start+step, minutes[3], 100., 5., 90., 1, 2., None, "breakout_v1", "fixed_r_v1")
    contract = Contract("linear_perp", "USDT", .001, 1., .1, 1., 100000., 20., .005, .001)
    design = config()
    risk = Risk(**design["risk"]["perp_equal_risk_v1"])
    args = (market, setup, contract, Cost(.0006, .0005), risk, 10000., design["execution"])
    interval = [(start-step, minutes[-1]+step)]
    executor = labels.audited_executor(run_trade, interval, settled, [], receipt, receipt, step=step)
    valid = executor(*args, minutes[7])
    assert valid["status"] == "COMPLETE" and valid["exit_at"].day != start.day
    assert valid["mature_at"] == minutes[5] and valid["funding"] < 0 and valid["adverse_cost"] > 0
    assert valid["net"] == pytest.approx(valid["gross_realized"]-valid["fees"]+valid["funding"])
    immature = executor(*args, minutes[5])
    assert immature["status"] == "INCOMPLETE" and "net_r" not in immature
    missing = labels.audited_executor(run_trade, interval, {}, [minutes[1]], receipt, receipt, step=step)
    missing_result = missing(*args, minutes[7])
    assert missing_result["reason"] == "funding_path_missing_or_conflicting" and "net_r" not in missing_result
    unpaired = Market(candles, candles.drop(minutes[1]), {minutes[1]: .001}, step, "SYNTHETIC")
    unpaired.intrabar_funding = {}
    mark_gap = executor(unpaired, *args[1:], minutes[7])
    assert mark_gap["status"] == "INCOMPLETE" and mark_gap["reason"] == "mark_feed_gap_while_exposed"
    assert mark_gap["remaining_base"] > 0 and "net_r" not in mark_gap
    assert labels.settlement_map([], set()) == ({}, {}, [])


def test_managed_spot_one_request_and_immutable_source_revision(tmp_path, monkeypatch):
    for module in (registry, collection, source):
        monkeypatch.setattr(module, "ROOT", tmp_path)
    start = utc("2027-04-01T00:00Z")
    end = start+collection.HOUR
    now = end+pd.Timedelta(seconds=50)
    monkeypatch.setattr(observation, "save_metadata", lambda *_: None)
    # The public client records actual receipt time, which is scoped here to the fixture.
    monkeypatch.setattr("lab.phase9.public.utc_now", lambda: now)
    body = {"profiles": {"position": {"start": start.isoformat(), "end_exclusive": (end+collection.HOUR).isoformat(),
                                     "market": "SPOT", "instrument": "BTC-USDT"}},
            "budget": {"spot_requests": 2, "min_free_disk_mib": 0, "spot_response_bytes": 32768, "spot_retained_bytes": 1048576,
                       "max_native_rows": 650000}, "rights": {}, "holdout": {}, "phase14_evaluation": {},
            "phase15_spot_overlap": {}, "parent_protocols": {}}
    record = {"id": "fixture", "body": body}
    sends = []
    def answer(request):
        sends.append(request.url)
        return httpx.Response(200, json={"code":"200000", "data":{"tradeType":"SPOT", "symbol":"BTC-USDT",
            "list":[[int(start.timestamp()), "100", "101", "99", "100", "4", "400"]]}})
    transport = httpx.MockTransport(answer)
    assert collection.capture_spot(record, now, transport)["receipt"]["rows"] == 1
    assert collection.capture_spot(record, now, transport)["state"] == "SPOT_HOUR_ALREADY_RESERVED"
    assert len(sends) == 1
    coverage = collection.coverage([collection.spot_store(record)], start, end, collection.HOUR, now, mark_required=False)
    assert coverage["trade_gaps"] == 0 and coverage["mark_gaps"] is None
    before = source.snapshot(record, "position", end, now)
    assert source.snapshot(record, "position", end, now)["id"] == before["id"]
    store = collection.spot_store(record)
    changed = now+pd.Timedelta(minutes=1)
    root = observation.directory(store)
    revision_sha = immutable_bytes(root/"raw", b'{"corrected":true}', "json")
    with closing(observation.db_open(store)) as db:
        raw = db.execute("SELECT body FROM item").fetchone()[0]
        row = json.loads(raw)
        row.update(open_at=start, close_at=end, close=100.5)
        observation.store_frame(db, "trade", pd.DataFrame([row]), {"raw_sha256": revision_sha,
                                "received_at": changed.isoformat()}, {"step_minutes":60, "decision_max_receipt_delay_seconds":120})
        db.commit()
    after = source.snapshot(record, "position", end, changed)
    assert after["body"]["parent_bundle"] == before["id"] and after["id"] != before["id"]
    assert after["body"]["corrections"][0]["revisions"][0][1] == revision_sha
    assert not after["body"]["parts"] and before["body"]["parts"][0]["bases"]["trade"]["rows"] == 1
    assert source.snapshot(record, "position", end, now)["id"] == before["id"]


def test_native_continuation_waits_for_protected_funding_closeout(tmp_path, monkeypatch):
    monkeypatch.setattr(collection, "ROOT", tmp_path)
    monkeypatch.setattr(observation, "PULSE", tmp_path/"schedule.json")
    monkeypatch.setattr(collection.protocol, "control", lambda: {"enabled": True})
    monkeypatch.setattr(collection.protocol, "active", lambda: {"id":"original"})
    monkeypatch.setattr(collection, "managed_observe", lambda _: {"state":"FUNDING_CLOSEOUT_RECORDED"})
    record = {"id":"native-fixture", "body":{"parent_protocols":{"phase14":"protected-fixture"}}}
    now = utc("2027-02-08T00:00Z")
    write_json(observation.PULSE, {"wake": (now+pd.Timedelta(minutes=1)).isoformat()})
    collection.price_observe(record, "scheduled:fixture", now)
    assert not (tmp_path/"protected-closeout.json").exists()
    write_json(observation.PULSE, {"wake": (now+pd.Timedelta(days=3650)).isoformat()})
    collection.price_observe(record, "scheduled:fixture", now)
    assert read_json(tmp_path/"protected-closeout.json")["phase14"] == "protected-fixture"
    assert utc(read_json(observation.PULSE)["wake"]) == now+collection.HOUR+collection.GRACE


def test_spot_partitions_reach_frozen_features_and_real_labels(tmp_path, monkeypatch):
    monkeypatch.setattr(source, "ROOT", tmp_path)
    start = utc("2027-03-01T00:00Z")
    end = start+pd.Timedelta(days=220)
    hours = pd.date_range(start, end, freq="1h", inclusive="left")
    tick = np.arange(len(hours), dtype=float)
    mid = 10000 + 2.5*tick + 250*np.sin(tick/72)
    trade = pd.DataFrame({"open_at":hours, "close_at":hours+collection.HOUR, "open":mid-.3, "high":mid+1., "low":mid-1.,
                          "close":mid, "volume":4.+np.sin(tick/8), "received_at":hours+collection.HOUR+pd.Timedelta(seconds=50)})
    partition = source.put_frame(trade)
    settings = config()
    name = "position_spot_research_v1"
    trial = {"profile":name, "entry":"breakout_v1", "exit_policy":"fixed_r_v1", "cost":"fixture"}
    cfg = {"recipe":trial, "risk":settings["risk"]["spot_equal_risk_v1"], "costs":{"taker_bps":10,"adverse_bps":5},
           "start": start.isoformat(), "inner_start": (start+pd.Timedelta(days=170)).isoformat(),
           "outer_start": (start+pd.Timedelta(days=190)).isoformat(), "market":"SPOT"}
    monkeypatch.setattr(labels, "parent_design", lambda: {"design":{"settings":settings, "feature_version":"fixture"}})
    monkeypatch.setattr(labels, "freeze_initial", lambda: {"body":{"profiles":{"position":{"profile":settings["profiles"][name]}}}})
    spot_rules = {"spot": {"feeCategory":"classA", "tradingStatus":"TradingEnabled", "baseCurrency":"BTC", "quoteCurrency":"USDT",
                           "baseOrderStep":.0001, "tickSize":.1, "minBaseOrderSize":.0001, "maxQuoteOrderSize":1000000.}}
    monkeypatch.setattr(labels, "open_metadata", lambda _: {"metadata":spot_rules})
    monkeypatch.setattr(labels, "expected_funding", lambda *_: pytest.fail("Spot must not require funding evidence"))
    metadata = [{"as_of":at.isoformat(), "received_at":at.isoformat()} for at in pd.date_range(start, end, freq="1D", inclusive="left")]
    native = {"id":"synthetic-only", "body":{"profile":"position", "market":"SPOT", "metadata":metadata,
                                             "start":start.isoformat(), "end_exclusive":end.isoformat(),
                                             "parts":[{"bases":{"trade":partition}}]}}
    record = {"body":{"profiles":{"position":cfg}}}
    design, data, (setups, _, mature, records, counts), _, contract = labels.label(record, native)
    assert design["label_version"] == labels.VERSION and counts["feature_ready"] > 0 and len(setups) > 0
    assert len(mature) > 0 and (mature.mature_at < end).all()
    assert contract["family"] == "spot" and data["market"].funding == {} and counts["unresolved_funding_slots"] == 0
    assert counts["source_state"]["missing_trade"] == 0
    assert all(row["input_identity"] and row["source_bundle"] == "synthetic-only" for row in records)


def test_sealed_native_readiness_enters_phase16_once_after_interval(tmp_path, monkeypatch):
    root = tmp_path / "phase17"
    for module in (admission, registry):
        monkeypatch.setattr(module, "ROOT", root)
    for module in (allocation, plan, control):
        monkeypatch.setattr(module, "ROOT", tmp_path / "phase16")
    start = utc("2027-01-01T00:00Z")
    inner, outer, end = (start+pd.Timedelta(days=days) for days in (20, 40, 60))
    now = end+pd.Timedelta(days=2)
    monkeypatch.setattr(admission, "utc_now", lambda: now)
    monkeypatch.setattr(allocation, "utc_now", lambda: now)
    monkeypatch.setattr(control, "utc_now", lambda: now)
    previous = {"data_manifest":"prior-input", "mature_ids":[], "campaign":"prior", "cutoff":"2025-11-08T00:00Z",
                "completed_at": (now-pd.Timedelta(days=29)).isoformat()}
    monkeypatch.setattr(allocation, "completed_for", lambda _: previous)
    monkeypatch.setattr(allocation, "phase14", lambda _: {"body":{"end_exclusive":"2027-04-01T00:00Z"}})
    monkeypatch.setattr(allocation, "phase15", lambda: {"body":{"follow_end_exclusive":"2027-02-01T00:00Z"}})
    settings = config()
    profile = "position_spot_research_v1"
    trial = {"profile":profile, "entry":"breakout_v1", "exit_policy":"fixed_r_v1", "cost":"fixture"}
    cfg = {"recipe":trial, "trial":trial, "profile":settings["profiles"][profile], "risk":settings["risk"]["spot_equal_risk_v1"],
           "costs":{"taker_bps":10,"adverse_bps":5}, "queue": True, "minimum_new_mature": 10,
           "minimum_train": 35, "minimum_inner": 10, "minimum_outer": 10, "interval_days": 30,
           "instrument":"BTC-USDT", "start":start.isoformat(), "first_cutoff":end.isoformat(),
           "inner_start":inner.isoformat(), "outer_start":outer.isoformat(), "context_lower_bound":start.isoformat()}
    initial = {"profiles":{"position": cfg}, "holdout":{"end_exclusive":"2026-11-08T00:00Z"}}
    monkeypatch.setattr(allocation, "freeze_initial", lambda: {"body":initial})
    monkeypatch.setattr(allocation, "parent_design", lambda: {"design": {"settings":settings,
                                                                         "feature_version":"fixture", "label_version":"fixture"}})
    record = registry.save("registrations", {"profiles":{"position":cfg}, "docs":{"kline":"fixture", "terms":"fixture"},
                                                "rights":{"source":"fixture"}})
    write_json(root/"registration.json", {"id": record["id"]})
    source_record = registry.save("sources", {"content_identity":"fixture-content", "parts":[{"bases":{"trade":{"sha256":"fixture", "rows":1000}}}],
                                             "end_exclusive":end.isoformat()})
    monkeypatch.setattr(admission, "snapshot", lambda *_: source_record)
    observations = pd.date_range(start, end, freq="6h", inclusive="left")
    mature = pd.DataFrame({"net_r": .05, "mature_at": observations+pd.Timedelta(hours=2)}, index=observations)
    design = {"available_at_cutoff_exclusive":end.isoformat(), "folds":[{"id":"allocated", "inner_start":inner,
                                                                     "outer_start":outer, "outer_end":end}]}
    contract = asdict(Contract("spot", "USDT", 1., .0001, .1, .0001, 100000., 1., 0., 0.))
    def provider(*_):
        return design, {}, ({}, None, mature, [], {}), None, contract
    monkeypatch.setattr(admission, "label", provider)
    def interrupt_on_publication(path, value):
        if path.parent.name == "admitted":
            raise OSError("synthetic interruption after the queue commit")
        write_json(path, value)
    monkeypatch.setattr(admission, "write_json", interrupt_on_publication)
    with pytest.raises(OSError, match="synthetic interruption"):
        admission.run_labels("position", end.isoformat())
    assert len(list((plan.ROOT/"queue").glob("*.json"))) == 1
    monkeypatch.setattr(admission, "write_json", write_json)
    receipt = admission.run_labels("position", end.isoformat())
    assert receipt["state"] == "ADMITTED" and receipt["queue_state"] == "WAITING_FOR_INTERVAL"
    assert control.queued() == []
    duplicate = admission.run_labels("position", end.isoformat())
    assert duplicate["bundle"] == receipt["bundle"] and len(list((plan.ROOT/"queue").glob("*.json"))) == 1
    now += pd.Timedelta(days=1)
    assert [row["id"] for row in control.queued()] == [receipt["campaign"]]
    manifest = read_json(plan.folder(receipt["campaign"])/"manifest.json")
    design_back, _, batch, _ = evaluate.open_profile(manifest, "position")
    assert design_back["folds"][0]["id"] == manifest["body"]["profiles"]["position"]["folds"][0]["id"]
    assert allocation.incremental_check(manifest, "position", evaluate.mature_ids(batch[2])) is None
    monkeypatch.setattr(control, "process_identity", lambda pid, created: pid == 123 and created == 1.)
    monkeypatch.setattr(control.psutil, "Process", lambda pid: SimpleNamespace(pid=pid, create_time=lambda:1.))
    starts = []
    monkeypatch.setattr(control.subprocess, "Popen", lambda args, **kwargs: starts.append(args) or SimpleNamespace(pid=123))
    control.change("enable")
    control.managed_tick()
    control.managed_tick()
    assert len(starts) == 1 and starts[0][-1] == receipt["campaign"]
