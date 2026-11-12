from contextlib import closing
import json

import pandas as pd
import pytest

from lab.ema_paper import catalog
from lab.phase15 import paper
from lab.strategy_lab import synthetic_btc as synthetic
from lab.strategy_lab.ema_execution import BracketSetup


AT = pd.Timestamp("2027-01-03T23:45:00Z")
MINUTE = pd.Timedelta(minutes=1)


def lane(profile="day"):
    spot = profile == "position"
    return {"profile": profile, "book_id": f"synthetic fixture {profile}",
            "risk": {"starting_equity": 10_000, "risk_fraction": .0025,
                     "notional_cap": .25 if spot else 1., "liquidation_buffer_atr": 0 if spot else .5},
            "execution_clock": {"entry_seconds": {"day": 900, "swing": 14400, "position": 3600}[profile]},
            "spec": {"sides": ["long"] if spot else ["long", "short"], "execution": {"entry_limit_atr": .25},
                     "session": {"max_hold_minutes": {"day": 480, "swing": 10080, "position": 43200}[profile]}},
            "source": {"price_receipt_seconds": 1020, "decision_receipt_seconds": 120}}


def bracket(side=1, *, stop=None):
    available = AT + pd.Timedelta(seconds=30)
    return BracketSetup(AT, available, AT + 15 * MINUTE, AT + 16 * MINUTE, AT + pd.Timedelta(hours=8),
                        100., 2., stop if stop is not None else 100. - side * 2., side, None, None,
                        "ema_pullback_trend_continuation", "signal_atr_bracket_v1", 100. + side * 5., False)


def make(profile="day", side=1, *, leverage=None, scenario="primary", stop=None, funding_minutes=480):
    model = synthetic.model(profile, leverage=leverage, scenario=scenario, funding_interval_minutes=funding_minutes)
    plan, reason = synthetic.make_plan(lane(profile), bracket(side, stop=stop), 10_000, AT + MINUTE,
                                       {"source_mode": "KRAKEN_NATIVE"}, model)
    return model, plan, reason


def bar(at, prices=(100., 100.2, 99.8, 100.1), *, delay=1):
    received = at + MINUTE + pd.Timedelta(seconds=delay)
    return {**dict(zip(paper.PRICE, prices)), "open_at": at.isoformat(), "close_at": (at + MINUTE).isoformat(),
            "received_at": received.isoformat(), "observed_at": received.isoformat()}


def publish(db, plan, profile="day"):
    book = paper.initial(lane(profile))
    analysis = {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": AT, "plan": plan,
                "direction": "long" if plan["direction"] == 1 else "short", "strategy_version": "fixture",
                "exit_policy": "signal_atr_bracket_v1"}
    assert paper.publish(db, book, analysis, AT + MINUTE)
    return book


def events(db, kind):
    return [json.loads(row[0]) for row in db.execute("SELECT body FROM event WHERE kind=? ORDER BY rowid", (kind,))]


@pytest.mark.parametrize("leverage", (1, 2, 3, 5))
@pytest.mark.parametrize("side", (-1, 1))
def test_same_notional_price_pnl_has_no_second_leverage_multiplier(leverage, side):
    policy = synthetic.model("day", leverage=leverage, scenario="counterfactual")
    state = synthetic.margin_state(.1, 100_000., 101_000., side, 10_000., policy)
    assert state["initial_margin"] == 10_000. / leverage
    assert state["free_collateral"] == 10_000. - 10_000. / leverage
    assert state["unrealized_pnl"] == side * 100.
    assert state["marked_equity"] == 10_000. + side * 100.
    assert state["maintenance_margin"] == pytest.approx(101.)
    assert synthetic.load_model(policy.body(), policy.id) == policy


def test_sizing_constrains_risk_margin_and_funding_without_maximizing_leverage():
    _, three, reason = make()
    assert reason is None
    _, five, reason = make(leverage=5, scenario="counterfactual")
    assert reason is None
    a, b = three["sizing"], five["sizing"]
    assert a["quantity_btc"] == b["quantity_btc"]
    assert 0 < a["planned_risk"] <= a["risk_budget"] == 25
    assert a["quantity_btc"] * a["risk_per_btc"] == a["planned_risk"]
    assert a["maximum_initial_margin"] / b["maximum_initial_margin"] == pytest.approx(5 / 3)
    assert b["gross_leverage_capacity"] == 50_000 and b["maximum_position_notional"] == 10_000
    assert a["projected_liquidation"] < b["projected_liquidation"] < five["stop"]
    assert b["liquidation_buffer"] < a["liquidation_buffer"]
    assert 0 < a["maximum_financing_reserve"] and a["funding_periods_reserved"] == 1
    assert make(leverage=5, scenario="counterfactual", stop=80.)[2] == "STOP_TO_LIQUIDATION_BUFFER"
    assert make("position", -1)[2] == "INCOMPATIBLE_PROFILE_SIDE"
    position, position_plan, _ = make("position")
    assert not position.perpetual and position_plan["sizing"]["projected_liquidation"] is None
    assert position_plan["sizing"]["maximum_financing_reserve"] == 0
    with pytest.raises(ValueError, match="Unsupported synthetic"):
        synthetic.model("position", leverage=5)


def test_frozen_sensitivity_changes_only_counterfactual_risk_and_modeled_basis():
    primary, normal, _ = make("swing")
    worst, stressed, _ = make("swing", scenario="combined_adverse")
    assert worst.id != primary.id and worst.taker_fee == 2 * primary.taker_fee
    assert worst.adverse_execution == 2 * primary.adverse_execution
    assert worst.funding_cost_rate == pytest.approx(3 * primary.funding_cost_rate)
    assert stressed["sizing"]["quantity_btc"] < normal["sizing"]["quantity_btc"]
    assert stressed["sizing"]["funding_periods_reserved"] == 21
    result = synthetic.counterfactual_plans(lane(), bracket(), 10_000, AT + MINUTE, {})
    assert set(result) == {"3", "5"} and all(item["counterfactual_only"] for item in result.values())
    assert result["5"]["sizing"]["liquidation_buffer"] < result["3"]["sizing"]["liquidation_buffer"]
    margins = synthetic.counterfactual_margins("day", quantity_btc=10., entry=100., reference=101.,
                                             direction=1, wallet=10_000.)
    assert margins["5"]["unrealized_pnl"] == margins["3"]["unrealized_pnl"] == 10.
    assert margins["5"]["liquidation_buffer"] < margins["3"]["liquidation_buffer"]
    basis = synthetic.currency_basis_sensitivity(1., 1, 100., 100., exit_ratio=.97)
    assert basis["baseline_gross"] == 0. and basis["stressed_gross"] == -3.
    depeg, converted, _ = make(scenario="usd_usdt_plus_3pct")
    assert depeg.usd_to_nominal_usdt == 1.03 and converted["entry"]["reference"] == 103.
    assert normal["synthetic_model"]["actual_funding"] == normal["synthetic_model"]["actual_futures_mark"] == "NOT_AVAILABLE"


@pytest.mark.parametrize("side", (-1, 1))
def test_funding_debit_target_exit_cost_once_and_restart_dedup(tmp_path, side):
    policy, plan, _ = make(side=side, funding_minutes=1)
    entry = plan["eligible_from"]
    with closing(catalog.db_open(root=tmp_path)) as db, db:
        book = publish(db, plan)
        assert not synthetic.advance(db, book, entry, entry + MINUTE, bar(entry))
        assert not synthetic.advance(db, book, entry, entry + 2 * MINUTE, None)
        assert synthetic.advance(db, book, entry, entry + 2 * MINUTE, bar(entry))
        assert not synthetic.advance(db, book, entry, entry + 2 * MINUTE, bar(entry))
        assert events(db, "FUNDING") == []
        fill, = events(db, "HYPOTHETICAL_FILL")
        assert fill["leverage"] == 3 and fill["margin"] == pytest.approx(fill["quantity_btc"] * fill["price"] / 3)
        assert fill["actual_funding"] == "NOT_AVAILABLE" and fill["liquidation_price"] > 0
        assert "contracts" not in fill
        paper.save(db, book)

    with closing(catalog.db_open(root=tmp_path)) as db, db:
        book = paper.load(db, lane())
        assert not synthetic.advance(db, book, entry, entry + 2 * MINUTE, bar(entry))
        at = entry + MINUTE
        prices = (100., 106., 99., 104.) if side == 1 else (100., 101., 94., 96.)
        assert synthetic.advance(db, book, at, at + 2 * MINUTE, bar(at, prices))
        assert not synthetic.advance(db, book, at, at + 2 * MINUTE, bar(at, prices))
        funding, = events(db, "FUNDING")
        closed, = events(db, "CLOSED")
        assert funding["cash_flow"] == pytest.approx(-fill["quantity_btc"] * 100. * policy.funding_cost_rate)
        assert funding["observed_settlement"] is False and funding["funding_basis"] == "SYNTHETIC_FINANCING_DEBIT"
        assert closed["exit_kind"] == "target" and closed["raw_exit"] == plan["target"]
        assert closed["gross_before_adverse"] > closed["gross_price_pnl"]
        assert closed["net"] == pytest.approx(closed["gross_price_pnl"] - closed["fees"] + closed["funding"])
        assert book["cash"] == pytest.approx(10_000 + closed["net"])
        assert closed["trigger_bar"]["price_source"] == synthetic.PRICE_SOURCE


def test_gap_liquidation_is_isolated_and_same_minute_stop_has_priority(tmp_path):
    policy, plan, _ = make(leverage=5)
    at = plan["eligible_from"]
    with closing(catalog.db_open(root=tmp_path)) as db, db:
        book = publish(db, plan)
        assert synthetic.advance(db, book, at, at + 2 * MINUTE, bar(at))
        fill, = events(db, "HYPOTHETICAL_FILL")
        gap = at + MINUTE
        assert synthetic.advance(db, book, gap, gap + 2 * MINUTE, bar(gap, (70., 72., 69., 71.)))
        closed, = events(db, "CLOSED")
        assert closed["exit_kind"] == "liquidation" and closed["raw_exit"] == 70.
        assert closed["trigger_bar"]["competing_triggers"] == ["liquidation", "stop"]
        assert closed["liquidation_cost"] > 0 and closed["unfunded_deficit"] > 0
        assert closed["net"] == pytest.approx(-fill["initial_margin"] - fill["fee"])
        assert book["cash"] == pytest.approx(10_000 + closed["net"])

    priority = synthetic.minute_exit((100., 106., 97., 104.), direction=1, stop=98., target=105., liquidation=80.)
    assert (priority.kind, priority.raw_price, priority.competing_triggers) == ("stop", 98., ("stop", "target"))
    worst = synthetic.minute_exit((100., 106., 78., 104.), direction=1, stop=98., target=105., liquidation=80.)
    assert worst.kind == "liquidation" and worst.raw_price == 78. and not worst.at_open


def test_counterfactual_plan_cannot_advance_primary_ledger(tmp_path):
    _, plan, _ = make(leverage=5, scenario="counterfactual")
    at = plan["eligible_from"]
    with closing(catalog.db_open(root=tmp_path)) as db, db:
        book = publish(db, plan)
        with pytest.raises(PermissionError, match="cannot advance the primary ledger"):
            synthetic.advance(db, book, at, at + 2 * MINUTE, bar(at))
        assert book["position"] is None and not events(db, "HYPOTHETICAL_FILL")
