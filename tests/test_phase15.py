from contextlib import closing
from dataclasses import asdict
import httpx
import pandas as pd
import pytest

from lab.learning.design import config
from lab.learning.simulator import Contract, Order
from lab.phase9.reference import reference
from lab.phase15 import catalog, feeds, opportunities, paper, worker
from lab.storage import utc


@pytest.fixture
def state(tmp_path, monkeypatch):
    for module in (catalog, feeds, worker):
        monkeypatch.setattr(module, "ROOT", tmp_path)
    lanes = catalog.rules(reference(), {"design": {"settings": config()}})
    for key, lane in lanes.items():
        lane["book_id"] = key
    return lanes


def bar(at, price=100., high=None, low=None):
    return {"open_at": at.isoformat(), "close_at": (at+catalog.STEP).isoformat(),
            "received_at": ((at+catalog.STEP).ceil("15min")+pd.Timedelta(seconds=50)).isoformat(),
            "open": price, "high": high if high is not None else price+1,
            "low": low if low is not None else price-1, "close": price, "raw_sha256": "fixture"}


def order_plan(at, *, direction=1, trailing=False, flat=None, hold=10080):
    edge = 102. if direction > 0 else 98.
    contract = Contract("linear_perp", "USDT", .001, 1., .1, 1., 250000., 100., .004, .003)
    plan = {"eligible_from": at+catalog.STEP, "entry_expiry": at+3*catalog.STEP,
            "entry": {"trigger": 99. if direction > 0 else 101., "minimum": 98., "maximum": 102.},
            "stop": 95. if direction > 0 else 105., "target_r": None if trailing else 2,
            "trailing_atr": 2.5 if trailing else None, "trigger_basis": "MARK", "exit_execution_basis": "TRADE",
            "direction": direction, "max_hold_minutes": hold, "session_flat": flat, "contract": asdict(contract),
            "cost": {"taker": .0006, "adverse_proxy": .0005},
            "order": asdict(Order(1000., edge, 1000., 25., 8., 3., 65., 30.))}
    return {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": at, "direction": "LONG" if direction > 0 else "SHORT",
            "strategy_version": "test-trail" if trailing else "test-fixed", "exit_policy": "atr_trail_v1" if trailing else "fixed_r_v1",
            "plan": plan}


def tick(db, book, at, trade=None, mark=None, **options):
    return paper.minute(db, book, at, at+pd.Timedelta(minutes=20), trade or bar(at), mark or bar(at), **options)


def test_watchlist_publishes_no_fill_and_expiry_does_not_close_open_trade(state):
    at = utc("2026-11-09T20:00Z")
    with closing(catalog.db_open()) as db:
        book = paper.initial(state["day/fixed"])
        draft = order_plan(at, flat=at+pd.Timedelta(minutes=6))
        assert paper.publish(db, book, {**draft, "state": "AWAITING_CONFIRMATION"}, at) is None
        assert not tick(db, book, at)
        assert not db.execute("SELECT 1 FROM event").fetchone()
        assert paper.publish(db, book, draft, at)
        assert not tick(db, book, at)
        assert tick(db, book, at+catalog.STEP)
        assert not tick(db, book, at+catalog.STEP)
        for i in range(2, 6):
            assert tick(db, book, at+i*catalog.STEP)
        assert book["position"] and not book["pending"]
        assert utc(book["position"]["deadline"]) == at+6*catalog.STEP
        assert tick(db, book, at+6*catalog.STEP)
        paper.save(db, book)
        snapshot = paper.book_report(db, state["day/fixed"])
        assert snapshot["counts"] == {"PUBLISHED": 1, "ENTRY_OBSERVED": 1, "HYPOTHETICAL_FILL": 1, "CLOSED": 1}
        assert snapshot["recent_closed"][0]["reason"] == "time_exit"


def test_entry_expiry_and_gap_beyond_planned_stop(state):
    at = utc("2026-11-09T20:00Z")
    with closing(catalog.db_open()) as db:
        day = paper.initial(state["day/fixed"])
        paper.publish(db, day, order_plan(at), at)
        for i in range(1, 3):
            assert tick(db, day, at+i*catalog.STEP, trade=bar(at+i*catalog.STEP, 98.))
        assert tick(db, day, at+3*catalog.STEP)
        assert not day["position"] and not day["pending"]
        swing = paper.initial(state["swing/fixed"])
        paper.publish(db, swing, order_plan(at), at)
        assert tick(db, swing, at+catalog.STEP)
        gap = at+2*catalog.STEP
        assert tick(db, swing, gap, bar(gap, 80.), bar(gap, 80.))
        paper.save(db, swing)
        record = paper.book_report(db, state["swing/fixed"])["recent_closed"][0]
        assert record["reason"] == "stop" and record["loss_over_plan"] > 0
        assert record["gross_before_adverse"]-record["adverse_cost_in_gross"] == pytest.approx(record["gross_price_pnl"])
        assert record["gross_price_pnl"]-record["fees"] == pytest.approx(record["net"])


@pytest.mark.parametrize("side", [1, -1])
def test_trailing_restart_and_funding_exactly_once(state, side):
    at = utc("2026-11-09T23:58Z")
    lane = state["swing/trail"]
    stamp = at+2*catalog.STEP
    with closing(catalog.db_open()) as db:
        book = paper.initial(lane)
        paper.publish(db, book, order_plan(at, direction=side, trailing=True), at)
        assert tick(db, book, at+catalog.STEP)
        paper.save(db, book)
        db.commit()
    with closing(catalog.db_open()) as db:
        book = paper.load(db, lane)
        assert utc(book["position"]["deadline"]) > stamp+pd.Timedelta(days=6)
        source = {"settlement_at": stamp.isoformat(), "rate": .001, "received_at": (stamp+catalog.STEP).isoformat()}
        trail = {"close_at": stamp.isoformat(), "available_at": (stamp+catalog.STEP).isoformat(),
                 "close": 104. if side > 0 else 96., "atr": 2.}
        assert tick(db, book, stamp, funding=(source,source), trail=trail)
        assert book["position"]["last_trail"] is None
        assert book["funding"] == pytest.approx(-side*.1)
        assert not tick(db, book, stamp, funding=(source,))
        third = stamp+catalog.STEP
        safe = 102. if side > 0 else 98.
        assert tick(db, book, third, bar(third, safe), bar(third, safe), trail=trail)
        assert book["position"]["stop"] == (99. if side > 0 else 101.)
        fourth = third+catalog.STEP
        exit_price = 98. if side > 0 else 102.
        assert tick(db, book, fourth, bar(fourth, exit_price), bar(fourth, exit_price))
        paper.save(db, book)
        result = paper.book_report(db, lane)
        assert result["counts"]["FUNDING"] == result["counts"]["CLOSED"] == 1
        assert result["recent_closed"][0]["reason"] == "stop"
        assert paper.book_report(db, state["swing/fixed"])["cash"] == 10000


def test_fixed_target_and_ambiguous_paths_remain_distinct(state):
    at = utc("2026-11-09T20:00Z")
    with closing(catalog.db_open()) as db:
        for key, high, low in (("day/fixed", 113., 99.), ("swing/fixed", 115., 92.)):
            book = paper.initial(state[key])
            paper.publish(db, book, order_plan(at), at)
            opened = at+catalog.STEP
            assert tick(db, book, opened)
            next_at = opened+catalog.STEP
            ambiguous = key == "swing/fixed"
            assert tick(db, book, next_at, bar(next_at, high=high, low=low), bar(next_at, high=high, low=low)) != ambiguous
            paper.save(db, book)
            snapshot = paper.book_report(db, state[key])
            if ambiguous:
                assert snapshot["book_status"] == "UNRESOLVED" and snapshot["position"]
                assert not snapshot["accounting_complete"] and not snapshot["recent_closed"]
            else:
                assert snapshot["recent_closed"][0]["reason"] == "target"


def test_short_fixed_target_at_exact_price_tick(state):
    at = utc("2026-11-10T03:45Z")
    with closing(catalog.db_open()) as db:
        book = paper.initial(state["day/fixed"])
        draft = order_plan(at, direction=-1)
        draft["plan"].update(
            entry={"trigger": 74600.1, "minimum": 74355.6, "maximum": 74441.4},
            stop=74742.0,
            order=asdict(Order(42., 74355.6, 10000., 25., 24.68650968, 3., 98319.1, 23577.1)),
        )
        assert paper.publish(db, book, draft, at)
        opened = at + catalog.STEP
        assert tick(db, book, opened, bar(opened, 74418.5), bar(opened, 74420.62))
        assert book["position"]["position"]["entry"] == 74381.2
        assert book["position"]["target"] == 73659.6
        next_at = opened + catalog.STEP
        touch = bar(next_at, 73700., low=73659.6)
        assert tick(db, book, next_at, touch, touch)
        assert book["position"] is None
        assert db.execute("SELECT 1 FROM event WHERE kind='CLOSED' AND json_extract(body, '$.reason')='target'").fetchone()


def test_missing_funding_and_price_hold_the_durable_cursor(state, monkeypatch):
    at = utc("2026-11-09T23:58Z")
    lane = state["swing/fixed"]
    stamp = at+2*catalog.STEP
    end = stamp+catalog.STEP
    body = {"config": reference(), "max_batch_minutes": 180, "gap_grace_seconds": 2700,
            "follow_end_exclusive": (stamp+pd.Timedelta(days=2)).isoformat()}
    with closing(catalog.db_open()) as db:
        book = paper.initial(lane)
        paper.publish(db, book, order_plan(at), at)
        assert tick(db, book, at+catalog.STEP)
        monkeypatch.setattr(worker, "futures_window", lambda *_: ({"trade": {}, "mark": {}}, {}, {stamp: ("Expected public funding settlement is missing", False)}, True))
        worker.advance(db, book, lane, body, end, end+pd.Timedelta(hours=3), {})
        assert book["problem"] == "Expected public funding settlement is missing"
        assert utc(book["cursor"]) == stamp and book["funding"] == 0
        monkeypatch.setattr(worker, "futures_window", lambda *_: ({"trade": {stamp: bar(stamp)}, "mark": {}}, {}, {}, True))
        worker.advance(db, book, lane, body, end, end+pd.Timedelta(hours=3), {})
        assert "missing or invalid minute" in book["problem"] and utc(book["cursor"]) == stamp
        settled = {"settlement_at": stamp.isoformat(), "rate": 0., "received_at": end.isoformat()}
        monkeypatch.setattr(worker, "futures_window", lambda *_: ({"trade": {stamp: bar(stamp)}, "mark": {stamp: bar(stamp)}}, {stamp: [settled]}, {}, True))
        worker.advance(db, book, lane, body, end, end+pd.Timedelta(hours=3), {})
        assert book["problem"] is None and utc(book["cursor"]) == end
        assert db.execute("SELECT count(*) FROM event WHERE kind='FUNDING'").fetchone()[0] == 1


def test_spot_public_minute_capture_reserves_once(state, monkeypatch):
    at = utc("2026-11-09T21:30Z")
    end = at+catalog.STEP
    now = end+pd.Timedelta(seconds=50)
    monkeypatch.setattr("lab.phase9.public.utc_now", lambda: now)
    body = {"spot_budget": {"requests": 2, "spacing_seconds": 0, "timeout_seconds": 2,
                            "response_bytes": 2048, "retained_bytes": 8192, "min_free_bytes": 0}}
    def respond(request):
        assert request.url.params["tradeType"] == "SPOT" and request.url.params["symbol"] == "BTC-USDT"
        return httpx.Response(200, json={"code": "200000", "data": {"tradeType":"SPOT", "symbol":"BTC-USDT",
            "list":[[int(at.timestamp()), "100", "101", "99", "100", "1.5", "150"]]}})
    with closing(catalog.db_open()) as db:
        assert feeds.capture_spot(db, body, at, end, now, httpx.MockTransport(respond))["rows"] == 1
        assert feeds.capture_spot(db, body, at, end, now, httpx.MockTransport(respond))["status"] == "REUSED"
        rows, corrections = feeds.spot_bars(db, at, end)
        assert rows[at]["volume"] == 1.5 and not corrections
        assert db.execute("SELECT count(*) FROM spot_request").fetchone()[0] == 1


def test_actual_rule_gates_show_watch_and_day_session_exclusion(state, monkeypatch):
    cfg = reference()
    at = utc("2026-11-09T22:00Z")
    now = at+pd.Timedelta(seconds=45)
    frame = pd.DataFrame([{**bar(at-catalog.QUARTER), "open_at": at-catalog.QUARTER, "close_at": at,
                          "received_at": now, "volume": 1}])
    features = pd.DataFrame([{"close": 98., "prior_high": 100., "prior_low": 95., "atr": 2., "pivot_high": 101., "pivot_low": 96.,
                              "ready": True, "uptrend": True, "downtrend": False, "context_distance_0": .03, "context_distance_1": .02}], index=[at])
    monkeypatch.setattr(opportunities, "build_features", lambda *_: (features, []))
    row = opportunities.analyze(state["day/fixed"], cfg, frame, at, now, None, None, {}, 10000., holdout_end="2026-11-08")
    assert row["category"] == "SESSION_EXCLUDED" and row["confirmation_level"] == 100 and row["distance_to_confirmation"] == 2
    row = opportunities.analyze(state["swing/trail"], cfg, frame, at, now, None, None, {}, 10000., holdout_end="2026-11-08")
    assert row["state"] == "AWAITING_CONFIRMATION" and "plan" not in row and "rsi" not in row["gates"]
    assert row["strategy_version"] != state["swing/fixed"]["strategy_version"]


def test_eligible_trade_preview_uses_same_breakout_and_distinct_exit(state, monkeypatch):
    cfg = reference()
    at = utc("2026-11-09T20:00Z")
    now = at+pd.Timedelta(seconds=45)
    frame = pd.DataFrame([{**bar(at-pd.Timedelta(hours=4)), "open_at": at-pd.Timedelta(hours=4), "close_at": at,
                           "received_at": now, "volume": 1}])
    features = pd.DataFrame([{"close": 100., "prior_high": 99., "prior_low": 94., "atr": 4., "pivot_high": 102., "pivot_low": 95.,
                              "ready": True, "uptrend": True, "downtrend": False, "context_distance_0": .03}], index=[at])
    monkeypatch.setattr(opportunities, "build_features", lambda *_: (features, []))
    quote = {"at": now.isoformat(), "received_at": now.isoformat(), "ask": 100.1, "bid": 100., "last": 100.}
    mark = {"at": now.isoformat(), "received_at": now.isoformat(), "mark": 100.}
    metadata = {"as_of": now.isoformat(), "contract": {"status": "Open", "multiplier": .001, "lotSize": 1, "tickSize": .1,
        "maxLeverage": 100, "marketMaxOrderQty": 1000000}, "isolated": [{"level": 1, "maxRiskLimit": 250000,
            "maxLeverage": 100, "maintainMargin": .004}]}
    fixed, trailing = [opportunities.analyze(state[lane], cfg, frame, at, now, quote, mark, metadata, 10000.,
                          holdout_end="2026-11-08") for lane in ("swing/fixed", "swing/trail")]
    assert fixed["state"] == trailing["state"] == "ELIGIBLE_CONDITIONAL_IDEA"
    assert fixed["plan"]["entry"] == trailing["plan"]["entry"]
    assert fixed["plan"]["order"]["planned_risk"] <= 25
    assert fixed["plan"]["fixed_target"] == 113 and trailing["plan"]["fixed_target"] is None
    assert trailing["plan"]["trailing_atr"] == 2.5 and trailing["plan"]["sizing"]["double_cost_stop_loss"] > trailing["plan"]["sizing"]["initial_stop_loss"]


def test_registry_is_prospective_idempotent_and_does_not_write_phase14(state, monkeypatch, tmp_path):
    from lab.storage import read_json, write_json

    now = utc("2026-11-09T21:35Z")
    original = {"id": "parent", "body": {"end_exclusive": "2027-02-07T18:56Z"}}
    amendment = {"id": "fixed-cohort", "body": {"original_protocol": "parent", "start": "2026-11-09T20:00Z"}}
    path = tmp_path / "phase14" / "amendment.json"
    write_json(path, amendment)
    before = path.read_bytes()
    monkeypatch.setattr(catalog, "active", lambda: original)
    monkeypatch.setattr(catalog, "phase14_pointer", lambda _: read_json(path))
    monkeypatch.setattr(catalog, "utc_now", lambda: now)
    monkeypatch.setattr(catalog, "parent_design", lambda: {"id": "parent-design", "design": {"settings": config(),
                        "holdout": {"end_exclusive": "2026-11-08T00:00Z"}}})
    saved = catalog.register()
    assert saved == catalog.register() and path.read_bytes() == before
    assert utc(saved["body"]["start"]) == now.ceil("15min") > now
    assert len({lane["book_id"] for lane in saved["body"]["lanes"].values()}) == 4
    assert saved["body"]["phase14_amendment"] == amendment["id"]


def test_cycle_publishes_then_accounts_once_and_keeps_user_reporting_separate(state, monkeypatch):
    at = utc("2026-11-09T20:00Z")
    now = {"value": at+pd.Timedelta(seconds=55)}
    record = {"id": "isolated", "body": {"lanes": state, "start": at.isoformat(),
              "publication_end_exclusive": (at+pd.Timedelta(days=1)).isoformat(),
              "follow_end_exclusive": (at+pd.Timedelta(days=2)).isoformat(), "config": reference(),
              "max_live_delay_seconds": 300, "max_batch_minutes": 180, "gap_grace_seconds": 2700}}
    flags = {"analysis": True, "publication": True, "paper": True, "spot_minutes": False}
    monkeypatch.setattr(worker, "registered", lambda: record)
    monkeypatch.setattr(worker, "utc_now", lambda: now["value"])
    monkeypatch.setattr(worker, "controls", lambda: flags)
    monkeypatch.setattr(worker, "monitoring_controls", lambda: {"monitor": True, "ideas": {"position": True, "day": True, "swing": True}})
    monkeypatch.setattr(worker.protocol, "current_metadata", lambda: {})
    draft = {**order_plan(at, flat=at+pd.Timedelta(hours=3)), "category": "CLOSED_BREAKOUT"}
    monkeypatch.setattr(worker, "opportunity_rows", lambda *_: {"day/fixed": draft})
    first = worker.cycle("scheduled:isolated")
    idea_id = first["published"]["day/fixed"]
    assert worker.cycle("scheduled:isolated")["published"] == {}
    with closing(catalog.db_open()) as db:
        book = paper.load(db, state["day/fixed"])
        paper.user_execution(db, book, idea_id, {"executed_at": now["value"], "unit": "contracts", "quantity": 1, "price": 99.}, now["value"])
        db.commit()
        assert paper.book_report(db, state["day/fixed"])["counts"]["USER_REPORTED_EXECUTION"] == 1
        assert book["cash"] == 10000 and not book["position"]
    now["value"] = at+pd.Timedelta(minutes=16)
    entry_at, exit_at = at+catalog.STEP, at+2*catalog.STEP
    sample = {entry_at: bar(entry_at), exit_at: bar(exit_at, price=114.)}
    monkeypatch.setattr(worker, "futures_window", lambda *_: ({"trade": sample, "mark": sample}, {}, {}, True))
    worker.cycle("scheduled:isolated")
    worker.cycle("scheduled:isolated")
    with closing(catalog.db_open()) as db:
        result = paper.book_report(db, state["day/fixed"])
        assert result["counts"] == {"PUBLISHED": 1, "USER_REPORTED_EXECUTION": 1, "ENTRY_OBSERVED": 1,
                                    "HYPOTHETICAL_FILL": 1, "CLOSED": 1}
        assert result["closed_net"] == pytest.approx(result["cash"]-10000)
        assert result["gross"]-result["fees"] == pytest.approx(result["closed_net"])
        assert result["cash_reconciliation_difference"] == pytest.approx(0., abs=1e-9)
        assert paper.book_report(db, state["swing/fixed"])["counts"] == {}


def test_public_funding_schedule_keeps_missing_and_verified_zero_distinct(state, monkeypatch, tmp_path):
    slot = utc("2026-11-10T00:00Z")
    start, end = slot-catalog.STEP, slot+2*catalog.STEP
    body = {"start": (slot-pd.Timedelta(hours=2)).isoformat(), "funding_grace_seconds": 7200}
    stamp = slot+pd.Timedelta(milliseconds=550)
    received = slot+pd.Timedelta(minutes=10)
    contract = {"currentFundingRateGranularity": 28800000, "nextFundingRateDateTime": int(slot.timestamp()*1000)}
    snapshot = slot-pd.Timedelta(hours=3)
    source = []
    monkeypatch.setattr(feeds, "futures_source", lambda _: {})
    monkeypatch.setattr(feeds, "stores", lambda _: [])
    monkeypatch.setattr(feeds, "checked", lambda *_: None)
    monkeypatch.setattr(feeds, "metadata_and_funding_queries", lambda *_: ([(snapshot,snapshot,{"contract": contract})], []))
    monkeypatch.setattr(feeds, "market_evidence", lambda *_, basis: source if basis == "funding" else [])
    _, settled, flags, available = feeds.futures_window(body, start, end, end)
    assert available and not settled and flags[start][0] == "Waiting for expected public funding settlement"
    _, settled, flags, _ = feeds.futures_window(body, start, end, slot+pd.Timedelta(hours=3))
    assert not settled and flags[slot][0] == "Expected public funding settlement is missing"
    source.append((stamp, stamp, received, tmp_path / "source.json", {"rate": 0.}))
    _, settled, flags, _ = feeds.futures_window(body, start, end, received)
    assert not flags and settled[slot][0]["settlement_at"] == stamp.isoformat()
    assert settled[slot][0]["rate"] == 0.
