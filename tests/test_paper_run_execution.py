from contextlib import closing
from dataclasses import asdict
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.learning.simulator import Order, Risk
from lab.paper_run import execution, gate, runtime, sources
from lab.phase9.ideas import current_rules
from lab.phase15 import paper, realtime
from lab.storage import utc


BOUNDARY = utc("2026-11-16T06:15:00Z")
CONFIRMATION = utc("2026-11-16T06:15:52.149797Z")
ELIGIBLE = utc("2026-11-16T06:16:00Z")
EXPIRY = utc("2026-11-16T06:25:00Z")
OBSERVED = ELIGIBLE + pd.Timedelta(milliseconds=100)
MARK = 100.1234


@pytest.fixture
def journal(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    with closing(runtime.connect()) as db:
        yield db


def lane(*, long_only=False):
    return {"book_id": "fresh-fixture", "source": sources.MARK, "symbol": "XBTUSDTM",
            "entry_model": realtime.MODE, "exit_model": realtime.MODE, "price_basis": "MARK",
            "long_only": long_only, "execution": {"funding_reserve_bps": 10.},
            "risk": asdict(Risk(10000., .0025, 1., 3., 5., .5))}


CONFIG = {"ideas": {"maximum_metadata_age_hours": 24, "modeled_liquidation_fee": .003}}
TERMS = {"as_of": BOUNDARY.isoformat(), "contract": {"status": "Open", "symbol": "XBTUSDTM", "multiplier": .001,
         "lotSize": 1., "tickSize": .1, "maxLeverage": 100., "marketMaxOrderQty": 2000.},
         "isolated": [{"level": 1, "maxRiskLimit": 250000., "maxLeverage": 100., "maintainMargin": .004}]}


def planned(db, account_lane, *, side=1, expiry=EXPIRY, hold=1):
    book = paper.initial(account_lane)
    contract = current_rules("FUTURES", TERMS, CONFIG)
    order = Order(1000., 102. if side > 0 else 98., 1000., 25., 8., 3., None, None)
    plan = {"eligible_from": ELIGIBLE, "entry_expiry": expiry,
            "entry": {"minimum": 98., "maximum": 102.}, "atr": 2., "direction": side,
            "stop": 95. if side > 0 else 105., "target_r": 2., "max_hold_minutes": hold,
            "session_flat": None, "contract": asdict(contract), "cost": {"taker": .0006, "adverse_proxy": 0.},
            "order": asdict(order)}
    analysis = {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": BOUNDARY, "plan": plan,
                "direction": "LONG" if side > 0 else "SHORT", "strategy_version": "fresh-fixture", "exit_policy": "fixed"}
    assert paper.publish(db, book, analysis, CONFIRMATION)
    return book


def observation(at, price=MARK, *, event_id="first", request_at=None, source=None):
    requested = request_at if request_at is not None else at
    receipt = requested + pd.Timedelta(milliseconds=125)
    return {"source": source or sources.MARK, "symbol": "XBTUSDTM", "price": price, "price_text": str(price),
            "source_at": at.isoformat(), "requested_at": requested.isoformat(), "received_at": receipt.isoformat(),
            "request_mono_ns": 0, "received_mono_ns": 125_000_000, "elapsed_ns": 125_000_000, "event_id": event_id}


def clock(at, *, tick=150_000_000):
    return lambda: (at + pd.Timedelta(tick, unit="ns"), tick)


def terms(book, account_lane, plan, price, now):
    return realtime._entry_terms(book, account_lane, plan, TERMS, CONFIG, price, now)


def events(db, kind):
    return [{"ledger_source_at": at, "ledger_recorded_at": decided, **json.loads(value)}
            for at, decided, value in db.execute("SELECT source_at,recorded_at,body FROM event WHERE kind=?", (kind,))]


@pytest.mark.parametrize("side,target", [(1, 110.4), (-1, 90.3)])
@pytest.mark.parametrize("action", ["stop", "target", "time_exit", "liquidation"])
def test_exact_observed_entry_and_exit_with_account_reconciliation(journal, side, target, action):
    account_lane = lane()
    book = planned(journal, account_lane, side=side)
    execution.entry(journal, book, account_lane, observation(OBSERVED), terms, read_clock=clock(OBSERVED))
    entered, = events(journal, "HYPOTHETICAL_FILL")
    assert entered["price"] == MARK and entered["reference_price_text"] == str(MARK)
    assert entered["entry_adverse_rate"] == entered["exit_adverse_rate"] == 0.
    assert entered["signal_boundary"] == BOUNDARY.isoformat()
    assert entered["confirmed_at"] == CONFIRMATION.isoformat()
    assert entered["eligible_from"] == ELIGIBLE.isoformat()
    assert entered["exchange_source_at"] == OBSERVED.isoformat()
    assert entered["source_received_at"] == (OBSERVED + pd.Timedelta(milliseconds=125)).isoformat()
    assert entered["paper_decision_at"] == entered["ledger_recorded_at"] == entered["entry_committed_at"]
    assert not entered["liquidity_verified"]
    assert entered["target"] == target
    if action == "time_exit":
        deadline = utc(entered["deadline"])
        halfway = deadline - pd.Timedelta(seconds=30)
        execution.protect(journal, book, account_lane, observation(halfway, event_id="middle"), read_clock=clock(halfway))
        price, at = MARK + side * .1234, deadline + pd.Timedelta(milliseconds=250)
    elif action == "liquidation":
        price, at = (5.1234 if side > 0 else 200.1234), OBSERVED + pd.Timedelta(seconds=5)
    else:
        sign, limit = (-1, entered["stop"]) if action == "stop" else (1, target)
        price, at = limit + sign * side * .2234, OBSERVED + pd.Timedelta(seconds=5)
    execution.protect(journal, book, account_lane, observation(at, price, event_id="exit"), read_clock=clock(at))
    closed, = events(journal, "CLOSED")
    assert not book["position"]
    assert closed["reason"] == action
    assert closed["exit_price"] == price
    assert closed["exchange_source_at"] == at.isoformat()
    assert utc(closed["exit_at"]) > at
    assert closed["adverse_cost_in_gross"] == 0.
    assert book["fees"] == pytest.approx(entered["quantity"] * (MARK + price) * .0006)
    assert closed["gross_price_pnl"] == pytest.approx(side * entered["quantity"] * (price - MARK))
    assert book["liquidation_cost"] == pytest.approx(.003 * entered["quantity"] * price if action == "liquidation" else 0.)
    assert (book["unfunded_deficit"] > 0.) == (action == "liquidation")
    assert book["cash"] == pytest.approx(10000. + book["gross"] - book["fees"] - book["liquidation_cost"]
                                         + book["unfunded_deficit"])


@pytest.mark.parametrize("commit_ms,mono_ms,expected", [
    (500, 150, "COMMIT_LOCAL_CLOCK_STEP"),
    (135, 500, "COMMIT_LOCAL_CLOCK_STEP"),
    (2300, 2300, "COMMIT_STALE_REFERENCE"),
])
def test_entry_checks_clock_and_freshness_through_commit(journal, commit_ms, mono_ms, expected):
    account_lane = lane()
    book = planned(journal, account_lane)
    ticks = iter(((OBSERVED + pd.Timedelta(milliseconds=130), 130_000_000),
                  (OBSERVED + pd.Timedelta(milliseconds=commit_ms), mono_ms * 1_000_000)))
    execution.entry(journal, book, account_lane, observation(OBSERVED), terms, read_clock=lambda: next(ticks))
    rejection, = events(journal, "ENTRY_EVIDENCE_REJECTED")
    assert rejection["reason"] == expected
    assert not book["position"] and not events(journal, "HYPOTHETICAL_FILL")


def test_commit_after_idea_expiry_cannot_use_still_fresh_data(journal):
    account_lane = lane()
    expiry = OBSERVED + pd.Timedelta(milliseconds=175)
    book = planned(journal, account_lane, expiry=expiry)
    ticks = iter(((OBSERVED + pd.Timedelta(milliseconds=150), 150_000_000), (expiry, 175_000_000)))
    execution.entry(journal, book, account_lane, observation(OBSERVED), terms, read_clock=lambda: next(ticks))
    expired, = events(journal, "EXPIRED")
    assert expired["ledger_recorded_at"] == expiry.isoformat()
    assert expired["reason"] and not book["pending"] and not book["position"]


@pytest.mark.parametrize("expiry", [EXPIRY, ELIGIBLE + pd.Timedelta(minutes=20)])
def test_delayed_historical_open_cannot_be_a_fresh_reference(journal, expiry):
    account_lane = lane()
    book = planned(journal, account_lane, side=-1, expiry=expiry)
    receipt = utc("2026-11-16T06:30:50.813842Z")
    decision = utc("2026-11-16T06:30:51.662791Z")
    candle = {"open_at": ELIGIBLE.isoformat(), "open": 64279.8, "received_at": receipt.isoformat()}
    execution.entry(journal, book, account_lane, candle, terms, read_clock=lambda: (decision, 12_000_000_000))
    assert not events(journal, "HYPOTHETICAL_FILL") and not book["position"]
    if expiry == EXPIRY:
        assert events(journal, "EXPIRED")
    else:
        assert events(journal, "ENTRY_EVIDENCE_REJECTED")[0]["reason"] == "MALFORMED_REFERENCE"


def test_long_only_and_wrong_reference_basis_fail_closed(journal):
    account_lane = lane(long_only=True)
    book = planned(journal, account_lane, side=-1)
    execution.entry(journal, book, account_lane, observation(OBSERVED, source=sources.SPOT), terms,
                    read_clock=clock(OBSERVED))
    assert events(journal, "ENTRY_EVIDENCE_REJECTED")[0]["reason"] == "WRONG_REFERENCE_SOURCE"
    execution.entry(journal, book, account_lane, observation(OBSERVED), terms, read_clock=clock(OBSERVED))
    assert events(journal, "INVALIDATED")[0]["reason"] == "DIRECTION_OUTSIDE_POLICY"
    assert not book["position"] and not book["pending"]


def test_position_protection_gap_and_pending_accounting_remain_explicit(journal):
    account_lane = lane()
    book = planned(journal, account_lane)
    execution.entry(journal, book, account_lane, observation(OBSERVED), terms, read_clock=clock(OBSERVED))
    at = OBSERVED + execution.PROTECTION_GAP + pd.Timedelta(seconds=1)
    candle = {"open_at": ELIGIBLE.isoformat(), "open": 92., "received_at": at.isoformat()}
    execution.protect(journal, book, account_lane, candle, read_clock=clock(at))
    gap, = events(journal, "PROTECTION_GAP")
    assert gap["reason"] == "MALFORMED_REFERENCE"
    assert book["position"]["unresolved_protection"]
    assert not events(journal, "EXIT_OBSERVED") and not events(journal, "CLOSED")
    forward = at + pd.Timedelta(seconds=1)
    execution.protect(journal, book, account_lane, observation(forward, 92.1234, event_id="stop"),
                      unresolved_costs=True, read_clock=clock(forward))
    assert events(journal, "PROTECTION_RESUMED") and events(journal, "EXIT_OBSERVED")
    assert book["position"]["exit_observed"]["price"] == 92.1234 and not events(journal, "CLOSED")
    execution.close(journal, book, account_lane, forward + pd.Timedelta(seconds=2))
    assert events(journal, "CLOSED")[0]["provisional"]
    assert book["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"


def test_actual_reference_revalidates_budget_and_contract_lot(journal):
    account_lane = lane()
    book = planned(journal, account_lane)
    book["cash"] = book["equity"] = 100.
    execution.entry(journal, book, account_lane, observation(OBSERVED), terms, read_clock=clock(OBSERVED))
    fill, = events(journal, "HYPOTHETICAL_FILL")
    assert fill["contracts"] == int(fill["contracts"]) < 1000
    assert 0 < fill["planned_risk"] <= .25
    assert fill["planned_risk"] == pytest.approx(fill["modeled_stop_loss"] + fill["funding_reserve"])
    assert fill["fee"] + fill["margin"] <= 100.
    assert fill["liquidation_buffer"] >= fill["required_liquidation_buffer"]


def test_runtime_rejects_duplicated_and_reordered_source_with_committed_restart_state(journal, monkeypatch):
    at = OBSERVED
    # Expose the deduplication gate independently from the real polling
    # interval so all sequenced observations can be within two seconds.
    monkeypatch.setattr(sources, "INTERVAL", pd.Timedelta(0))
    monkeypatch.setattr(runtime.shutil, "disk_usage", lambda _: SimpleNamespace(free=10 * 1024**3))
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(runtime, "_source_permission", lambda *_, **__: None)
    account_lane = lane()
    second = at + pd.Timedelta(milliseconds=450)
    older = at + pd.Timedelta(milliseconds=200)
    cases = [(observation(at, event_id="first"), at, None),
             (observation(second, event_id="first"), second, "DUPLICATE_OR_REORDERED_REFERENCE"),
             (observation(second, event_id="second"), second, None),
             (observation(older, request_at=second, event_id="reordered"), second, "DUPLICATE_OR_REORDERED_REFERENCE"),
             (observation(second + pd.Timedelta(milliseconds=350), event_id="first"),
              second + pd.Timedelta(milliseconds=350), "DUPLICATE_OR_REORDERED_REFERENCE")]
    for item, request, reason in cases:
        monkeypatch.setattr(runtime, "_observe", lambda *_, **__: item)
        monkeypatch.setattr(execution, "clock", clock(request))
        with closing(runtime.connect()) as writer:
            result = runtime.sample(writer, sources.MARK, account_lane, request, open_positions=False)
            assert result.get("status") == reason
            writer.commit()
    assert journal.execute("SELECT event_id FROM observation ORDER BY at").fetchall() == [("first",), ("second",)]
