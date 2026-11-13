import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from lab.learning.data import Market
from lab.learning.design import config
from lab.learning.diagnostics import fixture
from lab.learning.evaluation import account_replay as flat_account
from lab.learning.features import build_features
from lab.learning.simulator import ExecutionAssumptions, run_trade as flat_trade
from lab.phase10 import observer, worker
from lab.phase10.archive import calendar
from lab.phase10.evaluation import account_replay
from lab.phase10.partition import Chunk, PartitionedMarket, checked_funding, partition_features
from lab.phase10.trading import run_trade
from lab.storage import file_hash, utc


def partition(tmp_path, trade, mark, funding, widths, total_limit=None):
    chunks, start = [], 0
    for i, width in enumerate(widths):
        piece = trade.iloc[start:start + width]
        prices = mark.loc[mark.index.intersection(piece.index)]
        path, fair = tmp_path / f"trade{i}.parquet", tmp_path / f"mark{i}.parquet"
        piece.rename_axis("open_at").reset_index().to_parquet(path, index=False)
        prices.rename_axis("open_at").reset_index().to_parquet(fair, index=False)
        chunks.append(Chunk(path, fair, file_hash(path), file_hash(fair)))
        start += width
    assert start == len(trade)
    return PartitionedMarket(chunks, funding, utc("2025-11-08"), total_limit or len(trade))


def test_partition_replay_keeps_positions_pending_exits_and_account_economics(tmp_path):
    settings = config()
    trade, mark, funding, setup, contract, risk, cost = fixture(settings)
    assumptions = ExecutionAssumptions(exit_fraction=.5, delay_exit_bars=1)
    step, cutoff = pd.Timedelta("1min"), trade.index[-1] + pd.Timedelta("2min")
    full = Market(trade, mark, funding, step, "HYPOTHETICAL_REPLAY")
    split = partition(tmp_path, trade, mark, funding, [2, 2, 2, len(trade) - 6])
    inputs = setup, contract, cost, risk, 10000., settings["execution"], cutoff
    expected = flat_trade(full, *inputs, assumptions=assumptions, keep_marks=True)
    actual = run_trade(split, *inputs, assumptions=assumptions, keep_marks=True)
    settlements = actual.pop("settlements")
    assert actual == expected
    assert actual["status"] == "COMPLETE" and actual["funding"] < 0
    assert len(actual["fills"]) == 2 and len(settlements) == 1
    assert actual["entry_at"] < split.index[2] < actual["exit_at"]
    assert any(fill["resolved_at"] >= split.index[6] for fill in actual["fills"])
    assert split.read.cache_info().currsize <= 2

    fold = {"id": "synthetic", "outer_start": (setup.available_at - step).isoformat(), "outer_end": cutoff.isoformat()}
    design = {"folds": [fold], "settings": settings, "seed": 308, "feature_version": "fixture", "label_version": "fixture"}
    trial = {"model": "rules_v1", "cost": "fixture", "id": "fixture"}
    batch = {setup.decision_at: setup}, cost, None, None, None
    base = {"risk": risk, "contract": contract, "rules": settings["execution"], "trail": {}}
    predictions = {"synthetic": pd.Series([1.], index=[setup.decision_at])}
    metrics, trades = account_replay(design, {**base, "market": split}, trial, batch, predictions, {})
    flat_metrics, flat_trades = flat_account(design, {**base, "market": full}, trial, batch, predictions, {})
    assert metrics == flat_metrics and metrics["closed_trades"] == 1 and metrics["account_state"] == "FLAT"
    assert {k: v for k, v in trades[0].items() if k != "settlements"} == flat_trades[0]


def test_off_grid_settlement_retains_timestamp_and_gap_keeps_exposure(tmp_path):
    settings = config()
    trade, mark, funding, setup, contract, risk, cost = fixture(settings)
    off_grid = next(iter(funding)) + pd.Timedelta(milliseconds=500)
    actual = pd.Series([.001], index=pd.DatetimeIndex([off_grid]))
    schedule = pd.DatetimeIndex([off_grid.floor("min")])
    assert checked_funding(actual, schedule, "1s", trade.index[0], trade.index[-1]).index[0] == off_grid
    with pytest.raises(ValueError, match="Missing"):
        checked_funding(actual.iloc[:0], schedule, "1s", trade.index[0], trade.index[-1])
    assert checked_funding(actual * 0, schedule, "1s", trade.index[0], trade.index[-1]).iloc[0] == 0.
    split = partition(tmp_path, trade, mark, actual.to_dict(), [3, len(trade)-3])
    args = setup, contract, cost, risk, 10000., settings["execution"], trade.index[-1] + pd.Timedelta("2min")
    result = run_trade(split, *args)
    assert result["settlements"][0]["settlement_at"] == off_grid
    assert result["funding"] == pytest.approx(-result["signed_base_quantity"] * mark.loc[off_grid.floor("min"), "open"] * .001)
    gap = trade.drop(trade.index[4])
    broken = partition(tmp_path, gap, mark, funding, [4, len(gap)-4])
    result = run_trade(broken, *args)
    assert result["reason"] == "trade_feed_gap_while_exposed" and result["remaining_base"] > 0
    assert "net" not in result


def test_feature_warmup_and_gap_resets_match_flat_reference(tmp_path):
    index = pd.date_range("2024-01-01", periods=1600, freq="min", tz="UTC").delete(849)
    close = 100 + np.arange(len(index)) / 10 + np.sin(np.arange(len(index)) / 8)
    frame = pd.DataFrame({"open": close, "high": close+1, "low": close-1, "close": close,
                          "volume": 1., "close_at": index + pd.Timedelta("1min")}, index=index)
    step = pd.Timedelta("1min")
    profile = {"decision": "15min", "context": [{"frame": "1h", "sma": 3}]}
    parts = partition(tmp_path, frame, frame, {}, [277, 430, 232, 660])
    got, got_columns = partition_features(parts, profile)
    expected, columns = build_features(frame, profile, step)
    assert got_columns == columns
    pd.testing.assert_frame_equal(got, expected, check_freq=False)
    assert parts.coverage()["missing_trade"] == 1
    with pytest.raises(ValueError, match="workload cap"):
        partition(tmp_path, frame, frame, {}, [800, 799], total_limit=1500)


def test_schedule_id_retries_and_independent_controls(monkeypatch, tmp_path):
    monkeypatch.setattr(worker, "STATE", tmp_path)
    monkeypatch.setattr(worker, "CONTROL", tmp_path / "control.json")
    monkeypatch.setattr(worker, "phase9_controls", lambda: {"monitor": True, "training": False,
                                                          "ideas": dict.fromkeys(("day", "position", "swing"), True)})
    close = utc("2026-11-09T16:15Z")
    assert worker.boundary(utc("2026-11-09T16:15:39Z"), pd.Timedelta("15min"), 40) < close
    assert worker.boundary(utc("2026-11-09T16:15:40Z"), pd.Timedelta("15min"), 40) == close
    assert [worker.timing(close + pd.Timedelta(seconds=x), close) for x in (45, 130, 301)] == [
        "TIMELY", "DELAYED", "CATCH_UP"]
    assert worker.decision_key("day", close, "v1") == worker.decision_key("day", close, "v1")
    assert worker.decision_key("swing", close, "v1") != worker.decision_key("day", close, "v1")
    worker.set_control("training", True, "swing")
    worker.set_control("ideas", False, "day")
    assert worker.controls()["monitor"] and worker.controls()["training"]["swing"]
    assert not worker.controls()["training"]["day"] and not worker.controls()["ideas"]["day"]
    assert worker.controls()["ideas"]["swing"]
    assert calendar()["max_joined_rows_per_basis"] == 650000
    assert min(row["minimum_minutes_per_price_basis_before_training_prefix"] for row in calendar()["folds"]) == 712800


def test_one_shot_observer_terminates_without_blocking_or_overlapping(monkeypatch, tmp_path):
    clock = 0.
    children = []

    def launch(args, **options):
        assert args[-3:] == ["lab.phase13", "_observe", "test-launch"]
        assert options["env"]["__PYVENV_LAUNCHER__"] == observer.sys.executable
        assert options["creationflags"] & observer.subprocess.NORMAL_PRIORITY_CLASS
        proc = SimpleNamespace(pid=len(children) + 1, exit_code=None, terminate_calls=0)
        proc.poll = lambda: proc.exit_code

        def terminate():
            proc.terminate_calls += 1

        proc.terminate = terminate
        children.append(proc)
        return proc

    monkeypatch.setattr(observer.time, "monotonic", lambda: clock)
    monkeypatch.setattr(observer.subprocess, "Popen", launch)
    supervision = observer.Observer(tmp_path)
    supervision.tick("test-launch", lambda: True)
    assert supervision.status["state"] == "RUNNING" and len(children) == 1

    clock = observer.DEADLINE_SECONDS
    supervision.tick("test-launch", lambda: pytest.fail("Active observer must be checked before new work"))
    assert supervision.status["state"] == "TIMEOUT_TERMINATING" and children[0].terminate_calls == 1
    clock += 20
    supervision.tick("test-launch", lambda: pytest.fail("Termination must be acknowledged before restart"))
    assert len(children) == 1
    assert children[0].terminate_calls == 1

    children[0].exit_code = 1
    supervision.tick("test-launch", lambda: True)
    assert supervision.status["state"] == "TIMEOUT" and supervision.status["exit_code"] == 1
    clock += observer.RETRY_SECONDS - 1
    supervision.tick("test-launch", lambda: pytest.fail("Timeout retries are paced"))
    clock += 1
    supervision.tick("test-launch", lambda: True)
    assert supervision.status["pid"] == 2 and len(children) == 2
    children[1].exit_code = 0
    supervision.tick("test-launch", lambda: True)
    assert supervision.status["state"] == "EXITED"


def test_operator_drain_keeps_observer_ownership_until_child_exits(tmp_path):
    child = SimpleNamespace(exit_code=None)
    child.poll = lambda: child.exit_code
    child.terminate = lambda: pytest.fail("A graceful drain must not terminate the observer")
    supervision = observer.Observer(tmp_path)
    supervision.child = child
    supervision.status = {"state": "RUNNING", "pid": 10}
    assert not supervision.drain()
    assert supervision.child is child
    assert supervision.status == {"state": "DRAINING", "pid": 10}
    child.exit_code = 0
    assert supervision.drain()
    assert supervision.child is None
    assert supervision.status == {"state": "EXITED", "pid": 10, "exit_code": 0}


def test_graceful_stop_waits_for_actual_lock_and_preserves_pause(monkeypatch, tmp_path):
    monkeypatch.setattr(worker, "STATE", tmp_path)
    monkeypatch.setattr(worker, "CONTROL", tmp_path / "control.json")
    monkeypatch.setattr(worker, "RUNTIME", tmp_path / "runtime.json")
    monkeypatch.setattr(worker, "LOCK", tmp_path / ".worker.lock")
    monkeypatch.setattr(worker, "OUTPUT", tmp_path / "runs")
    monkeypatch.setattr(worker, "phase9_controls", lambda: {"monitor": True, "ideas": {}})
    calls = []
    clock = [100.]
    monkeypatch.setattr(worker.time, "monotonic", lambda: clock[0])

    def sleep(seconds):
        calls.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(worker.time, "sleep", sleep)
    worker.set_control("paused", False)
    active = {"key": "perp-mark", "started_at": "2026-11-13T16:00:00Z"}
    worker.write_json(worker.RUNTIME, {"pid": 3210, "active_request": active, "worker_stage": "COLLECTING"})
    with worker.writer_lock(worker.LOCK):
        result = worker.stop_local(drain_seconds=1)
        assert result["state"] == "BLOCKED"
        assert result["writer_lock_held"] and result["paused"]
        assert result["active_request"] == active and result["pid"] == 3210
        assert calls == [.5, .5]
        with pytest.raises(RuntimeError, match="Another writer owns"):
            with worker.writer_lock(worker.LOCK):
                pytest.fail("Stopping must never clear an owned writer lock")
    assert worker.controls()["paused"] and worker.controls()["monitor"]
    with worker.writer_lock(worker.OUTPUT / ".observer-cycle.lock"):
        result = worker.stop_local(drain_seconds=1)
        assert result["state"] == "BLOCKED"
        assert not result["writer_lock_held"] and result["observer_lock_held"]
    assert worker.stop_local(drain_seconds=1) == {
        "state": "STOPPED", "writer_lock_held": False, "observer_lock_held": False, "paused": True,
    }


def test_paused_worker_will_not_spawn_observer_or_start_spaced_request(monkeypatch, tmp_path):
    monkeypatch.setattr(worker, "STATE", tmp_path)
    flags = {"paused": True, "monitor": True}
    monkeypatch.setattr(worker, "controls", lambda: flags)
    supervision = SimpleNamespace(status={"state": "WAITING"}, tick=lambda *args: pytest.fail("Observer was spawned"))
    instance = SimpleNamespace(phase13_observer=supervision, rt={"launch_id": "old-worker"}, remaining=1,
                               used=0, budget={"maximum_response_bytes": 1, "minimum_spacing_seconds": 1})
    worker.Worker.observe_registered_research(instance)
    flags["paused"] = False
    monkeypatch.setattr(worker.time, "sleep", lambda _: flags.update(paused=True))
    monkeypatch.setattr(worker, "send", lambda *args: pytest.fail("Request started after the pause"))
    assert worker.Worker.fetch(instance, None, {"key": "trade"}) is None
    assert instance.remaining == 1


def test_worker_records_admitted_request_until_source_receipt(monkeypatch, tmp_path):
    monkeypatch.setattr(worker, "STATE", tmp_path)
    monkeypatch.setattr(worker, "controls", lambda: {"paused": False, "monitor": True})
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)
    clock = utc("2026-11-13T16:00:00Z")
    monkeypatch.setattr(worker, "utc_now", lambda: clock)
    saved = []
    instance = SimpleNamespace(rt={}, remaining=1, used=0, raw=tmp_path,
                               budget={"maximum_response_bytes": 1, "minimum_spacing_seconds": 1})
    instance.save = lambda: saved.append(instance.rt["active_request"].copy())
    request = {"key": "perp-mark"}

    def source(*_):
        assert saved == [{"key": "perp-mark", "started_at": clock.isoformat()}]
        return {"status": "RECEIVED", "received_at": clock.isoformat()}

    monkeypatch.setattr(worker, "send", source)
    receipt = worker.Worker.fetch(instance, None, request)
    assert receipt["request"] == request
    assert instance.rt["active_request"] is None


def test_observer_receipts_keep_only_operational_fields(tmp_path):
    audit = observer.ObservationAudit(tmp_path, "fixture")
    with audit.open(), audit.stage("phase15_paper_cycle"):
        audit.phase15({"opportunities": {"day/fixed": {
            "state": "WATCHING", "as_of": "2026-11-13T12:02:00+00:00",
            "source_cutoff": "2026-11-13T12:00:00+00:00", "latest_candle_receipt": "2026-11-13T12:00:45+00:00",
            "gates": {"completed candle": "PASSED", "feature readiness": "PASSED"},
            "decision": "NO_TRADE", "category": "NO_QUALIFYING_SETUP",
            "reason": "private detail", "chart": [{"close": 12345}],
        }}})
        with pytest.raises(RuntimeError), audit.stage("source_import"):
            raise RuntimeError("private source details")
    messages = (tmp_path / "observer.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(row) for row in messages]
    assert [row["state"] for row in rows] == ["STARTED", "WATCHING", "STARTED", "FAILED", "COMPLETED"]
    assert rows[1]["book"] == "day/fixed" and rows[1]["decision"] == "NO_TRADE"
    assert rows[1]["receipt"] == "2026-11-13T12:00:45+00:00" and rows[1]["feature_readiness"] == "PASSED"
    assert rows[3]["error"] == "RuntimeError"
    assert all("private" not in row and "12345" not in row for row in messages)


def test_dashboard_changes_controls_without_starting_a_worker(monkeypatch, tmp_path):
    from lab.phase10 import dashboard

    monkeypatch.setattr(worker, "STATE", tmp_path)
    monkeypatch.setattr(worker, "CONTROL", tmp_path / "control.json")
    monkeypatch.setattr(worker, "RUNTIME", tmp_path / "runtime.json")
    monkeypatch.setattr(worker, "OUTPUT", tmp_path / "runs")
    monkeypatch.setattr(dashboard, "recent", lambda _: [])
    app = AppTest.from_string("from lab.phase10.dashboard import panel\npanel()").run()
    assert not app.exception and app.info[0].value == "No validated model"
    assert "STOPPED" in app.markdown[0].value
    app.toggle(key="phase10-monitor-all").set_value(False).run()
    assert not worker.controls()["monitor"] and not (tmp_path / "runtime.json").exists()


def test_quote_lifecycle_keeps_market_and_simulated_evidence_separate(monkeypatch, tmp_path):
    from lab.phase9 import ideas

    monkeypatch.setattr(ideas, "OUTPUT", tmp_path)
    start = utc("2026-11-09T16:30:45Z")
    idea = {"idea_id": "fixture", "status": "CONDITIONAL", "market": "SPOT", "direction": "LONG",
            "published_at": start.isoformat(), "expires_at": (start + pd.Timedelta("10min")).isoformat(),
            "entry": {"trigger": 100., "minimum": 100., "maximum": 102., "price_basis": "ASK"},
            "stop": {"price": 96., "basis": "LAST"}, "take_profit": [{"price": 106.}],
            "paper": {"base_quantity": 1.}, "cost": {"taker": .001}, "time_exit": {"maximum_hold_minutes": 60}}
    policy = {"maximum_quote_age_seconds": 120, "maximum_quote_clock_skew_seconds": 1}

    def quote(seconds, price):
        stamp = (start + pd.Timedelta(seconds=seconds)).isoformat()
        return {"at": stamp, "received_at": stamp, "ask": price, "bid": price - .1, "last": price}

    assert not worker.observe_idea(idea, quote(1, 101.), None, start + pd.Timedelta("2s"), policy)
    assert worker.observe_idea(idea, quote(3, 107.), None, start + pd.Timedelta("4s"), policy)
    assert worker.observe_idea(idea, quote(5, 110.), None, start + pd.Timedelta("6s"), policy)
    rows = ideas.events("fixture")
    by_type = {row["type"]: row for row in rows}
    assert len(rows) == 4  # Entry condition, entry range retirement, fill and outcome.
    assert by_type["ENTRY_CONDITION_OBSERVED"]["classification"] == "MARKET_OBSERVATION"
    assert by_type["SIMULATED_OUTCOME"]["classification"] == "SIMULATED_EXECUTION"
    assert by_type["SIMULATED_OUTCOME"]["evidence"]["net"] == pytest.approx(5.9 - .2079)
    assert "USER_REPORTED_EXECUTION" not in by_type
