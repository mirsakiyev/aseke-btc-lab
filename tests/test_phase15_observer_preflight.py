from contextlib import closing
import json
import sqlite3

import pytest

from lab.config import fingerprint
from lab.phase15 import catalog, paper, restart_preflight


@pytest.fixture
def phase15_ledger(tmp_path, monkeypatch):
    root = tmp_path / "runs" / "phase15"
    records = root / "registrations"
    records.mkdir(parents=True)
    start = "2026-11-09T21:45:00+00:00"
    lanes = {}
    for name in restart_preflight.LANES:
        lane = {"profile": name.split("/")[0], "risk": {"starting_equity": 10_000}}
        lanes[name] = {**lane, "book_id": fingerprint([catalog.POLICY, name, lane, start])}
    body = {"policy": catalog.POLICY, "start": start, "lanes": lanes}
    ident = fingerprint(body)
    (root / "registration.json").write_text(json.dumps({"id": ident}), encoding="utf-8")
    (records / f"{ident}.json").write_text(json.dumps({"id": ident, "body": body}), encoding="utf-8")
    monkeypatch.setattr(catalog, "ROOT", root)
    with closing(catalog.db_open()) as db, db:
        for lane in lanes.values():
            paper.save(db, paper.initial(lane))
    return tmp_path, ident, root / "paper.sqlite3", lanes["day/fixed"]["book_id"]


def test_verified_original_accounts_are_read_only(phase15_ledger):
    root, ident, path, _ = phase15_ledger
    before = path.read_bytes()
    restart_preflight.verify(root, ident)
    assert path.read_bytes() == before
    assert sorted(p.name for p in path.parent.iterdir()) == ["paper.sqlite3", "registration.json", "registrations"]


@pytest.mark.parametrize("issue", [
    "position", "pending", "missing-row", "unexpected-id", "duplicate-row", "missing-db", "invalid-db",
    "malformed-json", "missing-key", "wrong-state-id", "duplicate-field", "unresolved-event", "missing-table",
    "unresolved-state", "malformed-balance", "wrong-registration",
])
def test_preflight_blocks_uncertain_or_active_account(phase15_ledger, issue):
    root, ident, path, book_id = phase15_ledger
    if issue == "missing-db":
        path.unlink()
    elif issue == "invalid-db":
        path.write_text("this is not a database", encoding="utf-8")
    elif issue == "wrong-registration":
        (path.parent / "registration.json").write_text(json.dumps({"id": "0" * 64}), encoding="utf-8")
    else:
        with closing(sqlite3.connect(path)) as db, db:
            if issue == "missing-row":
                db.execute("DELETE FROM book WHERE id=?", (book_id,))
            elif issue == "unexpected-id":
                db.execute("UPDATE book SET id=? WHERE id=?", ("unexpected", book_id))
            elif issue == "duplicate-row":
                db.execute("CREATE TABLE duplicate AS SELECT * FROM book")
                db.execute("DROP TABLE book")
                db.execute("ALTER TABLE duplicate RENAME TO book")
                db.execute("INSERT INTO book SELECT * FROM book WHERE id=?", (book_id,))
            elif issue == "missing-table":
                db.execute("DROP TABLE event")
            elif issue == "unresolved-event":
                db.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)",
                           ("key", book_id, "PUBLISHED", "idea", "source", "recorded", "{}"))
            else:
                payload = db.execute("SELECT state FROM book WHERE id=?", (book_id,)).fetchone()[0]
                account = json.loads(payload)
                if issue in {"position", "pending"}:
                    account[issue] = {"id": "open-work"}
                elif issue == "missing-key":
                    del account["pending"]
                elif issue == "wrong-state-id":
                    account["book_id"] = "another-book"
                elif issue == "unresolved-state":
                    account["cursor"] = "2026-11-13T00:00:00+00:00"
                elif issue == "malformed-balance":
                    account["equity"] = float("nan")
                modified = json.dumps(account)
                if issue == "malformed-json":
                    modified = "{invalid"
                elif issue == "duplicate-field":
                    modified = modified[:-1] + ', "pending": null}'
                db.execute("UPDATE book SET state=? WHERE id=?", (modified, book_id))
    before = path.read_bytes() if path.exists() else None
    with pytest.raises((ValueError, KeyError, sqlite3.Error)):
        restart_preflight.verify(root, ident)
    assert (path.read_bytes() if path.exists() else None) == before


def test_preflight_aborts_on_database_access_failure(phase15_ledger, monkeypatch):
    root, ident, path, _ = phase15_ledger

    def inaccessible(database, **options):
        assert database == path.resolve().as_uri() + "?mode=ro"
        assert options["uri"] is True
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(restart_preflight.sqlite3, "connect", inaccessible)
    with pytest.raises(sqlite3.OperationalError):
        restart_preflight.verify(root, ident)
