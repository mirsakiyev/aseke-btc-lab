from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys

import pandas as pd
import pytest

from lab.config import ROOT
from lab.health import state

sys.path.insert(0, str(ROOT / "tests"))
from test_fresh_paper_health import NOW, add_book, alerts, event, save_book, world

artifact = Path(__file__).parents[1] / "lab/health/fresh_paper_health.py"
if artifact.is_file():
    spec = importlib.util.spec_from_file_location("legacy_funding_health_artifact", artifact)
    monitor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(monitor)
else:
    from lab.health import fresh_paper_health as monitor


def scan(world, *, at=NOW, consistent=True):
    with state.connect() as db, db:
        return monitor.collect(db, at, view=world.view, paper_db=world.source, registration=world.record,
            consistent=consistent, current_gates={"clock": True, "entries_enabled": True,
                "books": {lane["book_id"]: True for lane in world.record["body"]["lanes"].values()}})


def waiting(world, slot="ema/day"):
    book = world.view["books"][slot]
    book.update(state="BLOCKED", reason="WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION",
        problem=None, historical_problem=None, prospective_entry_permission=False,
        financial_reconciliation_complete=False, cash=10000., starting_equity=10000., gross=0., funding=0., fees=0.,
        liquidation_cost=0., unfunded_deficit=0., position={"id": "open-"+slot,
            "mark_received_at": NOW.isoformat(), "unresolved_costs": "WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION",
            "settled_funding": []})
    save_book(world, slot)
    return book


def settled(world, slot="ema/day", *, receipt=True, event_position=None, event_at=NOW):
    book = world.view["books"][slot]
    book.update(state="ACTIVE", reason=None, financial_reconciliation_complete=True,
                funding=.1, cash=10000.1)
    book["position"].update(unresolved_costs=None, settled_funding=[NOW.isoformat()])
    save_book(world, slot)
    if receipt:
        number = world.source.execute("SELECT count(*) FROM event").fetchone()[0]+1
        world.source.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)", (
            "funding-"+str(number), book["book_id"], "FUNDING", event_position or book["position"]["id"],
            NOW.isoformat(), event_at.isoformat(), json.dumps({"settlement_at": NOW.isoformat(),
                "source_received_at": NOW.isoformat(), "rate": .001, "valuation_reference": 100., "cash_flow": .1})))
        world.source.commit()
    return book


def test_same_open_position_new_funding_receipt_reconciles_and_queues_one_truthful_clearance(world):
    waiting(world)
    scan(world)
    original = deepcopy(world.view["books"]["ema/day"])
    settled(world)
    result = scan(world)
    scan(world, at=NOW+pd.Timedelta(seconds=5))
    rows = alerts()
    assert len(rows) == 2
    assert "FUNDING RESTRICTION CLEARED" in rows[-1]["text"]
    assert "Position remains OPEN; entry is OCCUPIED" in rows[-1]["text"]
    assert "Accepted-price protection continues" in rows[-1]["text"]
    assert not result["books"]["ema/day"]["entry_available"]
    assert all("ENTRY ELIGIBILITY RESTORED" not in row["text"] for row in rows)
    assert world.view["books"]["ema/day"]["position"]["id"] == original["position"]["id"]


@pytest.mark.parametrize("guard", ["no_receipt", "wrong_position", "cost_pending", "financial_flag", "cash_mismatch", "inconsistent", "future_receipt", "unapplied_slot"])
def test_clearance_requires_real_same_position_receipt_and_consistent_reconciled_cost_evidence(world, guard):
    waiting(world)
    scan(world)
    book = settled(world, receipt=guard!="no_receipt", event_position="unrelated" if guard=="wrong_position" else None,
                   event_at=NOW+pd.Timedelta(seconds=5) if guard=="future_receipt" else NOW)
    if guard=="cost_pending":book["position"]["unresolved_costs"]="WAITING_FOR_FUNDING_RATE_OR_FORWARD_VALUATION"
    if guard=="financial_flag":book["financial_reconciliation_complete"]=False
    if guard=="cash_mismatch":book["cash"]+=1.
    if guard=="unapplied_slot":book["position"]["settled_funding"]=[]
    save_book(world,"ema/day")
    scan(world,consistent=guard!="inconsistent")
    assert not any("FUNDING RESTRICTION CLEARED" in row["text"] for row in alerts())


def test_source_recovery_alone_and_an_old_settlement_cannot_clear_current_wait(world):
    waiting(world)
    settled(world)
    waiting(world)
    scan(world)
    settled(world, receipt=False)
    scan(world)
    assert not any("FUNDING RESTRICTION CLEARED" in row["text"] for row in alerts())


def test_each_book_gets_its_own_receipt_bound_clearance_without_a_test_message(world):
    add_book(world,"unrelated/funding")
    for slot in world.view["books"]:waiting(world,slot)
    scan(world)
    for slot in world.view["books"]:settled(world,slot)
    scan(world)
    resolved=[row for row in alerts() if "FUNDING RESTRICTION CLEARED" in row["text"]]
    assert len(resolved)==2 and len({row["id"] for row in resolved})==2
    assert all(row["category"]=="health" and row["idea_id"] is None for row in resolved)


def test_versioned_funding_projection_does_not_duplicate_future_canonical_notifier(world):
    waiting(world)
    scan(world)
    book=settled(world)
    book["position"]["funding_state"]={"schema":"FUNDING_SETTLEMENT_RECONCILIATION_V1","state":"SETTLED"}
    save_book(world,"ema/day")
    scan(world)
    assert not any("FUNDING RESTRICTION CLEARED" in row["text"] for row in alerts())
