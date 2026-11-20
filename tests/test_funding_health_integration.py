from copy import deepcopy
import json

import pandas as pd

from lab.ema_paper.private_notifications import ROUTE_META
from lab.health import fresh_paper_health as monitor, state
from lab.paper_run import gate, operations
from lab.phase15 import paper
from test_paper_funding_lifecycle import SLOT, finance


def test_actual_settlement_transitions_ops_status_and_health_share_one_obligation(finance, tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path / "health")
    monkeypatch.setattr(gate, "ROOT", tmp_path / "paper")
    record = {"id": "generation", "body": {"lanes": {"unrelated/funding-model": finance.lane}}}
    with state.connect() as health, health:
        state.set_meta(health, ROUTE_META, json.dumps({"route_id": "f"*64, "enabled_at": SLOT.isoformat()}))

    def capture():
        operations.capture(finance.db, record, finance.lane, finance.book, finance.now[0],
                           source_health={"available": True}, clock_ok=True, entries_enabled=True)
        paper.save(finance.db, finance.book)
        finance.db.commit()

    def scan():
        funding = finance.book["position"]["funding_state"]
        view = {"generation_id": record["id"], "books": {"unrelated/funding-model": {
            **deepcopy(finance.book), "state": "BLOCKED" if funding["actionable"] else "ACTIVE",
            "reason": funding["state"] if funding["actionable"] else None,
            "prospective_entry_permission": False, "qualification": {"state": "QUALIFIED"},
            "operational": {"entry_permission": {"available": False,
                "reason_code": funding["state"] if funding["actionable"] else "POSITION_OPEN"}}}}}
        with state.connect() as health, health:
            return monitor.collect(health, finance.now[0], view=view, paper_db=finance.db,
                registration=record, current_gates={"clock": True, "entries_enabled": True,
                    "books": {finance.book["book_id"]: True}})

    finance.settle(finance.point())
    capture()
    scan()
    with state.connect() as health:
        assert health.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0

    finance.now[0] = SLOT+pd.Timedelta(hours=2)
    finance.state.update(state="MISSING", reason_code="RATE_MISSING_AFTER_EXISTING_COLLECTION")
    finance.settle()
    capture()
    scan()
    with state.connect() as health:
        rows = health.execute("SELECT * FROM outbox").fetchall()
        assert len(rows) == 1 and "FUNDING EVIDENCE REQUIRED" in rows[0]["text"]
        context = json.loads(rows[0]["event_json"])["operational_event"]
        assert context["settlement_due_at"] == SLOT.isoformat()
        exact_obligation = context["obligation_id"]

    finance.available()
    finance.settle()
    capture()
    scan()
    scan()
    with state.connect() as health:
        rows = health.execute("SELECT * FROM outbox ORDER BY rowid").fetchall()
        assert len(rows) == 2
        assert "FUNDING EVIDENCE RESOLVED" in rows[-1]["text"]
        assert "Position remains open; entry is occupied" in rows[-1]["text"]
        assert json.loads(rows[-1]["event_json"])["operational_event"]["obligation_id"] == exact_obligation
        assert all("ENTRY ELIGIBILITY RESTORED" not in row["text"] for row in rows)
    assert finance.book["position"]["funding_provisional"]
    report = operations.status(finance.db, record, finance.lane, finance.book, finance.now[0],
        source_health={"available": True}, clock_ok=True, entries_enabled=True, research={"research_state": "VALIDATED"})
    assert report["research_funding_state"] == "RESEARCH_PROVISIONAL"
    assert report["financial_state"] == "RECONCILED"
    assert report["entry_state"] == "ENTRY_OCCUPIED"
    assert not finance.db.execute("SELECT 1 FROM event WHERE kind='PROTECTION_INCIDENT_OPENED'").fetchone()
