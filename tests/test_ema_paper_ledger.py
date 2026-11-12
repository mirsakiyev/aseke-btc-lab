from dataclasses import replace
import json
import sqlite3

import pandas as pd
import pytest

from lab.ema_paper.ledger import advance, make_plan, report
from lab.learning.simulator import Contract
from lab.phase15 import paper
from lab.strategy_lab.ema_execution import BracketSetup


MINUTE = pd.Timedelta(minutes=1)


@pytest.fixture
def db():
    with sqlite3.connect(":memory:") as connection:
        connection.executescript("""
        CREATE TABLE book (id TEXT PRIMARY KEY, state TEXT NOT NULL);
        CREATE TABLE event (key TEXT PRIMARY KEY, book TEXT NOT NULL, kind TEXT NOT NULL,
            idea TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL, body TEXT NOT NULL);
        """)
        yield connection


def lane(profile="day"):
    spot = profile == "position"
    risk = {
        "starting_equity": 10000., "risk_fraction": .0025, "notional_cap": .25 if spot else 1.,
        "leverage": 1 if spot else 3, "ordinary_cap": 3, "liquidation_buffer_atr": 0.5,
    }
    return {
        "book_id": f"{profile}-fixture", "risk": risk, "profile": profile,
        "execution_clock": {"entry_seconds": {"day": 900, "swing": 14400, "position": 3600}[profile]},
        "spec": {
            "market": "SPOT" if spot else "FUTURES", "sides": ["long"] if spot else ["long", "short"],
            "cost": {"taker": .001, "adverse_proxy": .0005},
            "execution": {"entry_limit_atr": .25, "funding_reserve_bps": 10},
            "session": {"max_hold_minutes": {"day": 480, "swing": 10080, "position": 43200}[profile]},
        },
    }


def contract(spot=False):
    return Contract("spot" if spot else "linear_perp", "USDT", 1. if spot else .01, .001 if spot else 1.,
                    .01, .001 if spot else 1., 100000., 1. if spot else 10., .01 if not spot else 0., .003)


def setup(at, side=1):
    decision = pd.Timestamp(at)
    available = decision + pd.Timedelta(seconds=30)
    return BracketSetup(decision, available, available + MINUTE, available + 2*MINUTE, available + 480*MINUTE,
                        100., 4., 100. - 5*side, side, None, None, "ema", "signal_atr_bracket_v1",
                        100. + 10*side, False)


def bar(at, open_=100., low=None, high=None, close=None, delay=MINUTE):
    return {
        "open_at": at.isoformat(), "close_at": (at + MINUTE).isoformat(), "received_at": (at + delay).isoformat(),
        "open": open_, "low": open_ if low is None else low, "high": open_ if high is None else high,
        "close": open_ if close is None else close,
    }


def start(db, profile, candidate, *, now=None):
    spec = lane(profile)
    book = paper.initial(spec)
    now = now or candidate.available_at
    plan, reason = make_plan(spec, candidate, contract(profile == "position"), book["equity"], now, {})
    assert reason is None
    idea = paper.publish(db, book, {
        "state": "ELIGIBLE_CONDITIONAL_IDEA", "plan": plan, "source_cutoff": candidate.decision_at,
        "direction": "LONG" if candidate.direction > 0 else "SHORT",
        "strategy_version": "fixture", "exit_policy": candidate.exit_policy,
    }, now)
    assert idea
    return book, pd.Timestamp(plan["eligible_from"])


def bodies(db, kind):
    return [json.loads(row[0]) for row in db.execute("SELECT body FROM event WHERE kind=? ORDER BY rowid", (kind,))]


@pytest.mark.parametrize(("profile", "decision", "expected"), [
    ("day", "2026-11-11T23:30Z", "2026-11-11T23:45Z"),
    ("swing", "2026-11-11T20:00Z", "2026-11-12T00:00Z"),
    ("position", "2026-11-12T00:00Z", "2026-11-12T01:00Z"),
])
def test_profile_entry_clock_and_horizon(profile, decision, expected):
    lane_spec, candidate = lane(profile), setup(decision)
    product = contract(profile == "position")
    plan, reason = make_plan(lane_spec, candidate, product, 10000., candidate.available_at, {})
    assert reason is None and plan["eligible_from"] == pd.Timestamp(expected)
    assert plan["entry_expiry"] == pd.Timestamp(expected) + MINUTE
    assert plan["target"] == 110. and plan["stop"] == 95.
    assert make_plan(lane_spec, candidate, product, 10000., pd.Timestamp(expected), {})[1] == (
        "MISSED_OR_UNAVAILABLE_ENTRY_BOUNDARY"
    )
    assert make_plan(lane_spec, candidate, product, 10000., candidate.available_at, {
        "publication_end_exclusive": expected
    })[1] == "ENTRY_OUTSIDE_PRIMARY_HORIZON"


def test_24_hour_day_book_holds_across_midnight_and_closes_at_480_minutes(db):
    book, at = start(db, "day", setup("2026-11-11T23:30Z"))
    for minute in pd.date_range(at, periods=480, freq=MINUTE):
        quote = bar(minute)
        assert advance(db, book, minute, minute + MINUTE, quote, quote)
        assert book["position"]
    end = at + 480 * MINUTE
    assert book["position"]["deadline"] == end.isoformat()
    assert book["position"]["target"] == 110.
    assert advance(db, book, end, end + MINUTE, bar(end), bar(end))
    assert book["position"] is None
    closed, = bodies(db, "CLOSED")
    assert closed["exit_kind"] == "time_exit" and closed["holding_minutes"] == 480
    assert closed["net"] == pytest.approx(closed["gross_price_pnl"] - closed["fees"])
    assert closed["net"] == pytest.approx(closed["gross_before_adverse"] - closed["fees"] - closed["adverse_cost_in_gross"])
    paper.save(db, book)
    metrics = report(db, lane())
    assert metrics["closed_count"] == 1 and metrics["occupied_complete_minutes"] == 480
    assert metrics["maximum_adverse_excursion"] < 0
    assert len(metrics["equity_curve"]) >= 8


def test_futures_short_settled_funding_and_mark_trigger_trade_exit(db):
    book, at = start(db, "swing", setup("2026-11-11T20:00Z", side=-1))
    entered = bar(at, open_=99.5)
    assert advance(db, book, at, at + MINUTE, entered, entered)
    fill, = bodies(db, "HYPOTHETICAL_FILL")
    assert fill["contracts"] == int(fill["contracts"])
    assert fill["target"] == 90. and fill["stop"] == 105.
    later = at + MINUTE
    trade, mark = bar(later, 100., high=103.), bar(later, 101., high=106.)
    settlement = {"settlement_at": later.isoformat(), "received_at": (later + MINUTE).isoformat(), "rate": .001}
    assert advance(db, book, later, later + MINUTE, trade, mark, [settlement])
    funding, = bodies(db, "FUNDING")
    closed, = bodies(db, "CLOSED")
    assert funding["cash_flow"] == pytest.approx(fill["quantity"] * 101 * .001)
    assert closed["trigger_basis"] == "MARK" and closed["execution_basis"] == "TRADE"
    assert closed["reason"] == "stop" and closed["raw_exit"] == 103.
    assert closed["net"] == pytest.approx(closed["gross_price_pnl"] - closed["fees"] + closed["funding"])


def test_spot_long_only_and_trade_gap_invalidation(db):
    spot_lane = lane("position")
    short = setup("2026-11-12T00:00Z", -1)
    assert make_plan(spot_lane, short, contract(True), 10000., short.available_at, {})[1] == "INCOMPATIBLE_MARKET_OR_SIDE"
    book, at = start(db, "position", setup("2026-11-12T00:00Z"))
    gap = bar(at, open_=112.)
    assert advance(db, book, at, at + MINUTE, gap, gap)
    assert not book["position"] and not book["pending"]
    assert not bodies(db, "HYPOTHETICAL_FILL")
    expiry, = bodies(db, "INVALIDATED")
    assert expiry["reason_code"] == "ENTRY_GAP_OUTSIDE_BRACKET"


def test_delayed_entry_is_labeled_and_restart_replay_is_idempotent(db):
    spec = lane()
    book, at = start(db, "day", setup("2026-11-12T00:00Z"))
    quote = bar(at, delay=pd.Timedelta(minutes=15))
    assert not advance(db, book, at, at + MINUTE, quote, quote)
    assert advance(db, book, at, at + 15*MINUTE, quote, quote)
    paper.save(db, book)
    restarted = paper.load(db, spec)
    assert not advance(db, restarted, at, at + 16*MINUTE, quote, quote)
    fill, = bodies(db, "HYPOTHETICAL_FILL")
    assert fill["delayed_confirmation"] and fill["entry_at"] == at.isoformat()
    assert fill["source_received_at"] == (at + 15*MINUTE).isoformat()


def test_late_after_receipt_allowance_expires_without_fill(db):
    book, at = start(db, "day", setup("2026-11-12T00:00Z"))
    late = bar(at, delay=pd.Timedelta(minutes=18))
    assert advance(db, book, at, at + 18*MINUTE, late, late)
    assert not bodies(db, "HYPOTHETICAL_FILL") and not book["pending"]
    assert bodies(db, "EXPIRED")[0]["reason_code"] == "ENTRY_RECEIPT_TOO_LATE"


def test_ambiguous_bracket_protection_leaves_open_exposure_unresolved(db):
    book, at = start(db, "day", setup("2026-11-12T00:00Z"))
    dual = bar(at, 100., low=94., high=111.)
    assert not advance(db, book, at, at + MINUTE, dual, dual)
    assert book["permanent_block"] and book["position"]
    assert not bodies(db, "CLOSED")
    assert bodies(db, "UNRESOLVED")[0]["permanent"]


def test_invalid_signal_and_strictly_after_publication(db):
    candidate = setup("2026-11-12T00:00Z")
    broken = replace(candidate, target_price=99.)
    assert make_plan(lane(), broken, contract(), 10000., broken.available_at, {})[1] == (
        "INVALID_SIGNAL_BRACKET"
    )
    book, at = start(db, "day", candidate)
    book["pending"]["published_at"] = at.isoformat()
    with pytest.raises(ValueError, match="after publication"):
        advance(db, book, at, at + MINUTE, bar(at), bar(at))


def test_three_books_keep_independent_balances_and_journals(db):
    for profile in ("day", "swing", "position"):
        book = paper.initial(lane(profile))
        assert book["cash"] == book["starting_equity"] == 10000.
        paper.save(db, book)
    book, at = start(db, "day", setup("2026-11-12T00:00Z"))
    price = bar(at)
    assert advance(db, book, at, at + MINUTE, price, price)
    paper.save(db, book)
    assert report(db, lane("day"))["cash"] < 10000.
    for profile in ("swing", "position"):
        untouched = report(db, lane(profile))
        assert untouched["cash"] == untouched["equity"] == 10000.
        assert not untouched["counts"]


def test_unavailable_funding_cannot_commit_paper_fill(db):
    book, at = start(db, "day", setup("2026-11-12T00:00Z"))
    price = bar(at)
    funding = [{"settlement_at": at.isoformat(), "received_at": (at + 2*MINUTE).isoformat(), "rate": .001}]
    assert not advance(db, book, at, at + MINUTE, price, price, funding)
    assert not book["permanent_block"] and book["pending"]
    assert not bodies(db, "HYPOTHETICAL_FILL")
    assert advance(db, book, at, at + 2*MINUTE, price, price, funding)
    assert len(bodies(db, "HYPOTHETICAL_FILL")) == 1
