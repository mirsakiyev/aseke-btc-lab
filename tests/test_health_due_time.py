from types import SimpleNamespace

import pandas as pd
import pytest

from lab.health import delivery, fresh_paper_health as monitor, state
from test_fresh_paper_health import NOW, ROUTE, alerts, world


def test_new_private_operational_notice_is_route_verified_and_sent_in_the_same_full_check(world, monkeypatch):
    from lab.health import __main__ as operator, fresh_paper, monitor as components, private_telegram
    from lab.paper_run import gate, runtime

    clock = [NOW + pd.Timedelta(milliseconds=500)]
    monkeypatch.setattr(operator, "ROOT", state.ROOT)
    monkeypatch.setattr(operator, "utc_now", lambda: clock[0])
    monkeypatch.setattr(operator.time, "time", lambda: NOW.timestamp() + 1.)
    monkeypatch.setattr(gate, "retired", lambda: {"id": "retired"})
    monkeypatch.setattr(operator, "task_evidence", lambda: {name: {"state": "READY"} for name in operator.TASKS})
    monkeypatch.setattr(components, "collect", lambda *_: ({}, {}))
    monkeypatch.setattr(runtime, "status", lambda: world.view)
    monkeypatch.setattr(fresh_paper, "collect", lambda *_a, **_k: {"state": "FIXTURE_NO_LIFECYCLE_EVENTS"})

    def scan_operations(db, now):
        return monitor.collect(db, now, registration=world.record, paper_db=world.source, view=world.view,
                               current_gates={"clock": True, "entries_enabled": True,
                                              "books": {b["book_id"]: True for b in world.record["body"]["lanes"].values()}})

    monkeypatch.setattr(operator, "_scan_fresh_operations", scan_operations)
    verified, sent = [], []
    monkeypatch.setattr(private_telegram, "destination", lambda: verified.append(True) or SimpleNamespace(identifier=ROUTE))

    def send(route, text):
        sent.append(text)
        clock[0] = NOW + pd.Timedelta(milliseconds=1100)
        return delivery.Delivery("DELIVERED", "accepted", message_id=42,
                                 request_started=NOW.timestamp() + .75, request_completed=NOW.timestamp() + 1.,
                                 api_accepted_at=NOW.timestamp() + 1.)

    monkeypatch.setattr(private_telegram, "send", send)
    monkeypatch.setattr(operator, "send", lambda *_: pytest.fail("Operational notice used the unverified legacy route"))
    result = operator._check_health({}, None, before=operator.time.monotonic(), cpu=operator.time.process_time(),
                                    deliver=True, mode="operator")
    assert result["fresh_paper_operational_health"]["state"] == "CHECKED"
    assert verified == [True] and len(sent) == 1
    assert alerts()[0]["status"] == "DELIVERED" and alerts()[0]["message_id"] == 42
    assert alerts()[0]["created"] == NOW.timestamp() + .5
