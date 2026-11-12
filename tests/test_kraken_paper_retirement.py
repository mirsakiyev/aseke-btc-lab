from contextlib import closing
import json
import sqlite3

import pytest

from lab.health import __main__ as watchdog
from lab.kraken_paper import catalog, managed, notifications, presentation, retirement, source, worker
from lab.phase15 import paper
from test_kraken_paper_catalog import NOW, PAIR, _review


def test_retirement_preserves_evidence_and_disables_every_kraken_paper_path(tmp_path):
    root = tmp_path / "kraken_paper"
    review = _review(tmp_path)
    registered = catalog.register_books(review, PAIR, NOW, now=NOW, root=root)
    seed = root / "warmup" / "retained.json"
    seed.parent.mkdir()
    seed.write_bytes(b"warmup evidence")
    raw = root / "source" / "raw.json"
    raw.parent.mkdir()
    raw.write_bytes(b"raw market evidence")
    tables = ("authorization", "source_grant", "registration", "book", "event", "readiness",
              "eligibility", "equity_sample", "warmup")
    with closing(catalog.db_open(True, root=root)) as db:
        before = {name: db.execute(f"SELECT * FROM {name}").fetchall() for name in tables}

    history = retirement.retire(root=root, now=NOW)
    assert set(history) == {"day", "swing", "position"}
    assert retirement.history(root=root) == history == retirement.retire(root=root)
    assert catalog.registrations(root=root) == registered
    for profile, record in history.items():
        assert record["state"] == "RETIRED" and record["registration_id"] == registered[profile]["id"]
        assert record["starting_equity"] == record["final_account"]["equity"] == 10000
        assert record["event_count"] == 0
    with closing(catalog.db_open(root=root)) as db:
        assert {name: db.execute(f"SELECT * FROM {name}").fetchall() for name in tables} == before
        assert len(catalog.grants(db)) == 9
        with pytest.raises(sqlite3.IntegrityError, match="immutable Kraken paper retirement"):
            db.execute("DELETE FROM retirement")
        with pytest.raises(sqlite3.IntegrityError, match="immutable Kraken paper retirement"):
            db.execute("INSERT OR REPLACE INTO retirement VALUES (?,?,?)", ("unrelated", "changed", "{}"))
        with pytest.raises(sqlite3.IntegrityError, match="retired Kraken paper book"):
            paper.event(db, registered["day"]["body"], "PUBLISHED", "future", NOW, NOW, {})
        with pytest.raises(sqlite3.IntegrityError, match="retired Kraken paper book"):
            paper.save(db, paper.initial(registered["day"]["body"]))
        with pytest.raises(sqlite3.IntegrityError, match="retired Kraken paper book"):
            db.execute("UPDATE book SET id='changed'")
        with pytest.raises(PermissionError, match="retired"):
            catalog.approved_authorization(db, at=NOW)
    assert seed.read_bytes() == b"warmup evidence" and raw.read_bytes() == b"raw market evidence"
    assert worker.status(root=root) == {"state": "RETIRED", "books": history}
    assert worker.cycle(root=root) == {"state": "RETIRED", "books": {}}
    assert managed.ensure(root=root) == {"state": "RETIRED"}
    assert notifications.collect(None, NOW, {}, root=root) == {}
    assert presentation.report(root=root) == {}
    assert watchdog._check_kraken_paper(None, {}, root) == {
        "kraken_source_producer": {"state": "RETIRED"}, "kraken_paper": {"state": "RETIRED"},
        "kraken_paper_notifications": {"state": "RETIRED"},
    }
    with pytest.raises(PermissionError, match="retired"):
        source.collect_registered(root=root)
    with pytest.raises(PermissionError, match="retired"):
        catalog.register_books(review, PAIR, NOW, now=NOW, root=root)


@pytest.mark.parametrize("activity", ["position", "pending", "problem", "PUBLISHED", "HYPOTHETICAL_FILL", "UNRESOLVED"])
def test_active_or_unresolved_evidence_aborts_entire_retirement(tmp_path, activity):
    root = tmp_path / "kraken_paper"
    registered = catalog.register_books(_review(tmp_path), PAIR, NOW, now=NOW, root=root)
    lane = registered["swing"]["body"]
    with closing(catalog.db_open(root=root)) as db, db:
        if activity.islower():
            book = paper.initial(lane)
            book[activity] = {"id": "test evidence"}
            db.execute("UPDATE book SET state=? WHERE id=?", (json.dumps(book), lane["book_id"]))
        else:
            paper.event(db, lane, activity, "fixture-idea", NOW, NOW, {})
    with pytest.raises(ValueError, match="position, fill or unresolved lifecycle"):
        retirement.retire(root=root, now=NOW)
    assert retirement.history(root=root) == {}
    with closing(catalog.db_open(True, root=root)) as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE name='retirement'").fetchone() is None


def test_resolved_unfilled_lifecycle_can_retire_with_its_history_intact(tmp_path):
    root = tmp_path / "kraken_paper"
    registered = catalog.register_books(_review(tmp_path), PAIR, NOW, now=NOW, root=root)
    lane = registered["swing"]["body"]
    with closing(catalog.db_open(root=root)) as db, db:
        for kind in ("PUBLISHED", "UNRESOLVED", "EVIDENCE_RECOVERED", "EXPIRED"):
            paper.event(db, lane, kind, "fixture-idea", NOW, NOW, {})

    assert retirement.retire(root=root, now=NOW)["swing"]["event_count"] == 4
    with closing(catalog.db_open(True, root=root)) as db:
        assert [kind for (kind,) in db.execute("SELECT kind FROM event ORDER BY rowid")] == [
            "PUBLISHED", "UNRESOLVED", "EVIDENCE_RECOVERED", "EXPIRED",
        ]
