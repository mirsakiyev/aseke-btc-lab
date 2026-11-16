from contextlib import closing
from dataclasses import asdict
import json

import pandas as pd
import pytest

from lab.learning.simulator import Order, Risk, liquidation_price
from lab.phase9.ideas import current_rules
from lab.phase15 import catalog, paper, realtime, realtime_worker
from lab.storage import utc


BOUNDARY = utc("2026-11-16T06:15:00Z")
CONFIRMED = utc("2026-11-16T06:15:52.149797Z")
FIRST_MINUTE = utc("2026-11-16T06:16:00Z")
REQUESTED = FIRST_MINUTE + pd.Timedelta(milliseconds=100)
SOURCE = FIRST_MINUTE + pd.Timedelta(milliseconds=200)
RECEIVED = FIRST_MINUTE + pd.Timedelta(milliseconds=400)
DECIDED = FIRST_MINUTE + pd.Timedelta(milliseconds=500)
COMMITTED = FIRST_MINUTE + pd.Timedelta(milliseconds=600)
EXPIRY = FIRST_MINUTE + pd.Timedelta(minutes=10)
MARK = 100.123


@pytest.fixture
def engine(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "ROOT", tmp_path)
    with closing(catalog.db_open()) as db:
        realtime.init_observations(db)
        lane = {"book_id": "realtime-test-day", "profile": "day", "execution_protocol": realtime.VERSION,
                "mode": {"trade_type": "FUTURES", "symbol": "XBTUSDTM"},
                "execution": {"funding_reserve_bps": 10.},
                "risk": asdict(Risk(10000., .0025, 1., 3., 5., .5))}
        config = {"ideas": {"maximum_metadata_age_hours": 24, "modeled_liquidation_fee": .003}}
        metadata = {"as_of": BOUNDARY.isoformat(), "contract": {"status": "Open", "symbol": "XBTUSDTM",
                    "multiplier": .001, "lotSize": 1., "tickSize": .1, "maxLeverage": 100.,
                    "marketMaxOrderQty": 2000.},
                    "isolated": [{"level": 1, "maxRiskLimit": 250000., "maxLeverage": 100.,
                                  "maintainMargin": .004}]}
        yield db, paper.initial(lane), lane, metadata, config


def analysis(metadata, config, side, *, expiry=EXPIRY):
    stop = 95. if side > 0 else 105.
    limit = 102. if side > 0 else 98.
    contract = current_rules("FUTURES", metadata, config)
    return {"state": "ELIGIBLE_CONDITIONAL_IDEA", "category": "CLOSED_BREAKOUT", "source_cutoff": BOUNDARY,
            "latest_completed": BOUNDARY, "latest_candle_receipt": CONFIRMED, "as_of": CONFIRMED,
            "direction": "LONG" if side > 0 else "SHORT", "strategy_version": "breakout-fixture",
            "exit_policy": "fixed_r_v1", "structure": {"atr": 2.},
            "plan": {"eligible_from": FIRST_MINUTE, "entry_expiry": expiry, "stop": stop,
                     "entry": {"minimum": 98., "maximum": 102., "trigger": 99. if side > 0 else 101.},
                     "direction": side, "target_r": 2., "trailing_atr": None, "max_hold_minutes": 90,
                     "session_flat": None, "trigger_basis": "MARK", "exit_execution_basis": "TRADE",
                     "contract": asdict(contract), "cost": {"taker": .0006, "adverse_proxy": .0005},
                     "order": asdict(Order(1000., limit, 1000., 25., 8., 3., None, None))}}


def observation(*, source=SOURCE, received=RECEIVED, requested=REQUESTED, price=MARK):
    return {"status": "RECEIVED", "symbol": "XBTUSDTM", "source": "KUCOIN_FUTURES_CURRENT_MARK",
            "source_at": source.isoformat(), "received_at": received.isoformat(),
            "requested_at": requested.isoformat(), "mark": price, "raw_sha256": "a" * 64}


def submit(engine, snapshot, *, decision=DECIDED, commit=COMMITTED):
    moments = iter((decision, commit))
    return realtime.consider(*engine, snapshot, clock=lambda: next(moments, commit))


def confirm(engine, side=1, *, expiry=EXPIRY):
    db, book, lane, metadata, config = engine
    return realtime.confirm(db, book, lane, analysis(metadata, config, side, expiry=expiry), CONFIRMED)


def events(db, kind):
    return [{"source_at": at, "recorded_at": recorded, **json.loads(body)}
            for at, recorded, body in db.execute("SELECT source_at,recorded_at,body FROM event WHERE kind=? "
                                                "ORDER BY rowid", (kind,))]


def minute_bar(at, price):
    close = at + catalog.STEP
    return {"open_at": at.isoformat(), "close_at": close.isoformat(), "received_at": close.isoformat(),
            "open": price, "high": price, "low": price, "close": price, "raw_sha256": "b" * 64}


@pytest.mark.parametrize("side,expected_target", [(1, 110.4), (-1, 90.3)])
@pytest.mark.parametrize("exit_kind", ["stop", "target"])
def test_uses_exact_mark_for_both_sides_and_reconciles_exit(engine, monkeypatch, side, expected_target, exit_kind):
    db, book, lane, metadata, config = engine
    idea = confirm(engine, side)
    assert idea and realtime.eligible(book, DECIDED)
    assert submit(engine, observation()) == "HYPOTHETICAL_FILL"

    fill, = events(db, "HYPOTHETICAL_FILL")
    record = book["position"]
    position = record["position"]
    assert not book["pending"]
    assert fill["entry_basis"] == realtime.MODE == "REALTIME_MARK_REFERENCE_PAPER"
    assert fill["entry_adverse_rate"] == 0 and not fill["liquidity_verified"]
    assert fill["price"] == record["raw_entry"] == position["entry"] == MARK
    assert position["direction"] == side
    assert fill["target"] == record["target"] == expected_target
    assert record["stop"] == idea["plan"]["stop"]
    assert utc(fill["exchange_source_at"]) == SOURCE
    assert utc(fill["source_received_at"]) == RECEIVED
    assert utc(fill["paper_decision_at"]) == DECIDED
    assert utc(fill["entry_committed_at"]) == utc(record["entry_at"]) == COMMITTED
    assert utc(fill["source_at"]) == utc(fill["recorded_at"]) == COMMITTED
    assert utc(fill["hold_cap_at"]) == COMMITTED + pd.Timedelta(minutes=90)
    assert utc(record["deadline"]) == (COMMITTED + pd.Timedelta(minutes=90)).floor("min")
    assert fill["fee"] == pytest.approx(MARK * fill["quantity"] * .0006)
    assert fill["margin"] == pytest.approx(MARK * fill["quantity"] / 3)
    assert book["equity"] == book["cash"] == pytest.approx(10000. - fill["fee"])
    contract = current_rules("FUTURES", metadata, config)
    liq = liquidation_price(MARK, fill["margin"] / fill["quantity"], side, contract, .0006)
    assert side * (fill["stop"] - liq) > 1.
    assert 0 < fill["planned_risk"] <= 25.
    assert fill["planned_risk"] == pytest.approx(fill["modeled_stop_loss"] + fill["funding_reserve"])
    assert fill["liquidation_buffer"] >= fill["required_liquidation_buffer"] == 1.

    partial = minute_bar(FIRST_MINUTE, MARK)
    paired = {"trade": {FIRST_MINUTE: partial}, "mark": {FIRST_MINUTE: partial}}
    monkeypatch.setattr(realtime_worker, "futures_window", lambda *_: (paired, {}, {}, True))
    assert realtime_worker.protect_entry_minute(db, book, {"config": config}, FIRST_MINUTE,
                                                utc(partial["received_at"]), metadata)
    first_full = utc(book["cursor"])
    assert first_full == COMMITTED.ceil(catalog.STEP)
    bar = minute_bar(first_full, record[exit_kind])
    assert paper.minute(db, book, first_full, utc(bar["received_at"]), bar, bar)
    assert not book["position"]
    closed, = events(db, "CLOSED")
    assert closed["reason"] == exit_kind
    assert closed["entry_price"] == MARK
    assert closed["gross_before_adverse"] == pytest.approx(side * fill["quantity"] * (record[exit_kind] - MARK))
    assert closed["net"] == pytest.approx(closed["gross_price_pnl"] - closed["fees"])
    paper.save(db, book)
    db.commit()
    report = paper.book_report(db, lane)
    assert report["cash_reconciliation_difference"] == pytest.approx(0., abs=1e-10)


@pytest.mark.parametrize("snapshot", [
    None,
    observation(source=DECIDED + pd.Timedelta(milliseconds=1)),
    observation(received=DECIDED + pd.Timedelta(milliseconds=1)),
    observation(received=REQUESTED - pd.Timedelta(milliseconds=1)),
    observation(requested=CONFIRMED - pd.Timedelta(milliseconds=1)),
    observation(source=CONFIRMED - pd.Timedelta(milliseconds=1)),
    {**observation(), "source": "HISTORICAL_TRADE_KLINE"},
    {**observation(), "symbol": "ETHUSDTM"},
    {**observation(), "mark": float("nan")},
    {key: value for key, value in observation().items() if key != "source_at"},
])
def test_rejects_missing_stale_future_or_inconsistent_evidence(engine, snapshot):
    db, book, *_ = engine
    assert confirm(engine)
    assert submit(engine, snapshot) != "HYPOTHETICAL_FILL"
    assert not book["position"] and not events(db, "HYPOTHETICAL_FILL")
    assert book["cash"] == 10000. and book["fees"] == 0.


@pytest.mark.parametrize("decision_delay,commit_delay", [(3., 3.), (0., 3.)])
def test_rechecks_two_second_observation_limit_until_commit(engine, decision_delay, commit_delay):
    db, book, *_ = engine
    assert confirm(engine)
    decision = DECIDED + pd.Timedelta(seconds=decision_delay)
    commit = COMMITTED + pd.Timedelta(seconds=commit_delay)
    assert submit(engine, observation(), decision=decision, commit=commit) != "HYPOTHETICAL_FILL"
    assert not book["position"] and not events(db, "HYPOTHETICAL_FILL")


def test_accepts_observation_at_the_two_second_commit_boundary(engine):
    db, *_ = engine
    assert confirm(engine)
    commit = SOURCE + pd.Timedelta(seconds=2)
    assert submit(engine, observation(), commit=commit) == "HYPOTHETICAL_FILL"
    fill, = events(db, "HYPOTHETICAL_FILL")
    assert fill["source_age_seconds"] == 2.


def test_delayed_historical_minute_cannot_create_a_realtime_entry(engine):
    db, book, *_ = engine
    received = utc("2026-11-16T06:30:50.813842Z")
    decision = utc("2026-11-16T06:30:51.662791Z")
    # A deliberately extended expiry isolates evidence rejection from the expiry gate.
    assert confirm(engine, -1, expiry=FIRST_MINUTE + pd.Timedelta(minutes=20))
    candle = {**minute_bar(FIRST_MINUTE, 64279.8), "received_at": received.isoformat()}
    assert submit(engine, candle, decision=decision, commit=decision) != "HYPOTHETICAL_FILL"
    assert not events(db, "ENTRY_OBSERVED") and not events(db, "HYPOTHETICAL_FILL")
    assert not book["position"]


@pytest.mark.parametrize("decision,commit", [(EXPIRY, EXPIRY), (EXPIRY - pd.Timedelta(milliseconds=100), EXPIRY)])
def test_expiry_rejects_even_fresh_observation(engine, decision, commit):
    db, book, *_ = engine
    assert confirm(engine)
    fresh = observation(source=EXPIRY - pd.Timedelta(milliseconds=300),
                        received=EXPIRY - pd.Timedelta(milliseconds=200),
                        requested=EXPIRY - pd.Timedelta(milliseconds=400))
    assert submit(engine, fresh, decision=decision, commit=commit) != "HYPOTHETICAL_FILL"
    if book["pending"]:
        realtime.expire_pending(db, book, EXPIRY)
    assert not book["pending"] and not book["position"] and not events(db, "HYPOTHETICAL_FILL")
    expired = events(db, "EXPIRED") + events(db, "NO_FILL")
    assert expired and all(item["reason"] for item in expired)


def test_revalidates_current_risk_budget_and_lots(engine):
    db, book, lane, _, _ = engine
    assert confirm(engine)
    book["cash"] = book["equity"] = 100.
    assert submit(engine, observation()) == "HYPOTHETICAL_FILL"
    fill, = events(db, "HYPOTHETICAL_FILL")
    assert 0 < fill["contracts"] < 1000.
    assert fill["contracts"] == int(fill["contracts"])
    assert fill["planned_risk"] <= 100. * lane["risk"]["risk_fraction"]
    assert fill["margin"] + fill["fee"] < 100.


def test_rejects_entry_if_available_capital_cannot_fund_one_lot(engine):
    db, book, *_ = engine
    assert confirm(engine)
    book["cash"] = book["equity"] = .001
    assert submit(engine, observation()) != "HYPOTHETICAL_FILL"
    assert not book["position"] and not events(db, "HYPOTHETICAL_FILL")


def test_revalidates_exchange_max_order_quantity(engine):
    db, _, _, metadata, _ = engine
    assert confirm(engine)
    metadata["contract"]["marketMaxOrderQty"] = 5.
    assert submit(engine, observation()) == "HYPOTHETICAL_FILL"
    fill, = events(db, "HYPOTHETICAL_FILL")
    assert fill["contracts"] == 5.


def test_rejects_entry_when_instrument_terms_change(engine):
    db, book, _, metadata, _ = engine
    assert confirm(engine)
    metadata["contract"]["lotSize"] = 2.
    assert submit(engine, observation()) == "NO_FILL"
    rejected, = events(db, "NO_FILL")
    assert rejected["reason"] == "INSTRUMENT_RULES_CHANGED"
    assert not book["position"] and not events(db, "HYPOTHETICAL_FILL")


def test_rejects_entry_when_mark_based_liquidation_buffer_is_too_small(engine):
    db, book, lane, *_ = engine
    lane["risk"]["liquidation_buffer_atr"] = 20.
    assert confirm(engine)
    assert submit(engine, observation()) == "NO_FILL"
    rejected, = events(db, "NO_FILL")
    assert rejected["reason"] == "STOP_TO_LIQUIDATION_BUFFER"
    assert not book["position"] and not events(db, "HYPOTHETICAL_FILL")


@pytest.mark.parametrize("price", [94., 98.5, 102.1])
def test_revalidates_original_range_trigger_and_stop(engine, price):
    db, book, *_ = engine
    assert confirm(engine)
    assert submit(engine, observation(price=price)) != "HYPOTHETICAL_FILL"
    assert not book["position"] and not events(db, "HYPOTHETICAL_FILL")


def test_watchlist_does_not_create_a_pending_idea(engine):
    db, book, lane, metadata, config = engine
    watchlist = {**analysis(metadata, config, 1), "state": "AWAITING_CONFIRMATION"}
    assert realtime.confirm(db, book, lane, watchlist, CONFIRMED) is None
    assert not book["pending"] and not events(db, "PUBLISHED")
