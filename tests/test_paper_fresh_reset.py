from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.health import fresh_paper, state
from lab.paper_run import __main__ as cli
from lab.paper_run import consumer_spool, fresh_reset, gate, runtime, source_amendment
from lab.phase15 import paper
from lab.storage import file_hash, read_json, utc, write_json, writer_lock
from test_paper_run_runtime import NOW, REGISTERED, documents


@pytest.fixture
def generation(tmp_path, monkeypatch):
    root = tmp_path / "paper-run"
    monkeypatch.setattr(gate, "ROOT", root)
    monkeypatch.setattr(state, "ROOT", tmp_path / "health")
    monkeypatch.setattr(fresh_reset, "utc_now", lambda: NOW)
    monkeypatch.setattr(fresh_reset.archive, "_private", lambda _: None)
    archived, definition = documents(root)
    for slot, lane in definition["lanes"].items():
        lane["ledger_unit"] = "NOMINAL_USDT" if slot.startswith("ema/") else "USDT"
    write_json(root / "legacy-retired.json", archived)
    old = runtime.initialize(archived, {}, now=REGISTERED, prepared=definition)
    source_amendment.install(old, {"state": "QUALIFIED"}, now=REGISTERED)
    write_json(root / "entries-paused", {"generation": old["id"], "paused_at": REGISTERED.isoformat()})
    write_json(root / "entries-controls" / "old.json", {"generation": old["id"]})
    write_json(root / "consumer-losses.json", {"generation": old["id"]})
    write_json(root / "last-cycle.json.snap" / "old.json", {"generation": old["id"]})
    with state.connect() as db, db:
        for key, identifier in (("old-event", old["id"]), ("other-event", "different-generation")):
            state.enqueue(db, key, "ideas", "test event", NOW.timestamp(),
                          event_json=json.dumps({"generation_id": identifier}))

    def successor(_archive, _qualifications, now):
        next_boundary = now.floor("15min") + pd.Timedelta(minutes=15)
        return {**deepcopy(definition), "registered_at": now.isoformat(), "start": next_boundary.isoformat()}

    monkeypatch.setattr(runtime, "definitions", successor)
    return root, old, tmp_path / "archives"


def qualified():
    return {}, {"state": "QUALIFIED"}


def test_reset_preserves_raw_wal_and_leaves_only_new_financial_state(generation):
    root, old, archive_parent = generation
    slot = "ema/day"
    old_lane = old["body"]["lanes"][slot]
    old_acct = paper.initial(old_lane)
    old_acct.update(position={"id": "abandoned"}, cash=9250, fees=100, funding=40,
                    problem="OPEN_OLD_INCIDENT", permanent_block=True)
    # An abrupt owner exit leaves committed exposure in the WAL, separate from the main file.
    code = (
        "import sqlite3, sys, os; "
        "db = sqlite3.connect(sys.argv[1]); db.execute('PRAGMA journal_mode=WAL'); "
        "db.execute('UPDATE book SET state=? WHERE id=?',(sys.argv[2],sys.argv[3])); "
        "db.commit(); os._exit(0)"
    )
    ledger = root / "paper.sqlite3"
    subprocess.run([sys.executable, "-c", code, str(ledger), json.dumps(old_acct), old_lane["book_id"]],
                   check=True, timeout=20, creationflags=subprocess.CREATE_NO_WINDOW)
    wal = root / "paper.sqlite3-wal"
    raw_hashes = {path.name: file_hash(path) for path in (ledger, wal)}
    fresh_reset.request_stop(old["id"])
    result = fresh_reset.reset(old["id"], archive_parent, qualified)
    new = runtime.current()
    assert new["id"] == result["generation_id"] != old["id"]
    assert new["body"]["retired_generation"] == old["id"] and gate.legacy_entries_closed()
    assert utc(new["body"]["start"]) > utc(new["body"]["cutoff"])
    destination = Path(result["archive"])
    assert read_json(destination / "manifest.json")["body"]["books"][slot]["position_archived"]
    assert {name: file_hash(destination / name) for name in raw_hashes} == raw_hashes
    assert read_json(root / "entries-paused")["generation"] == new["id"]
    assert source_amendment.active(new)["body"]["generation"] == new["id"]
    assert consumer_spool.pending(root, new["id"]) == []
    assert not any((root / name).exists() for name in ("runner-stop.json", "consumer-losses.json", wal.name))
    assert not (root / "entries-controls" / "old.json").exists()
    with closing(runtime.connect(readonly=True)) as db, state.connect() as watchdog:
        assert db.execute("SELECT kind,count(*) FROM event GROUP BY kind").fetchall() == [("OPENING_BALANCE", 7)]
        assert db.execute("SELECT count(*) FROM source_state").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM observation").fetchone()[0] == 0
        assert {row[0]: row[1] for row in watchdog.execute("SELECT id,status FROM outbox")} == {
            "old-event": "SUPERSEDED", "other-event": "PENDING"
        }
        for name, lane in new["body"]["lanes"].items():
            identifier = lane["book_id"]
            assert identifier != old["body"]["lanes"][name]["book_id"]
            assert paper.load(db, lane) == paper.initial(lane)
            key = fresh_paper.CURSOR_PREFIX + fingerprint([new["id"], name, identifier])
            assert state.get_meta(watchdog, key) == "0"
    with pytest.raises(ValueError, match="generation"):
        cli.entries_on(old["id"], approve=True)
    with pytest.raises(ValueError, match="Activate the current generation's protection policy"):
        cli.entries_on(new["id"], approve=True)
    assert (root / "entries-paused").is_file()
    with pytest.raises(ValueError, match="MISMATCH"):
        consumer_spool.pending(root, old["id"])
    with pytest.raises(ValueError, match="no longer current"):
        source_amendment.install(old, {"state": "QUALIFIED"}, now=NOW)


def test_reset_retries_after_archive_and_after_ledger_commit(generation, monkeypatch):
    root, old, archive_parent = generation
    original_clear = fresh_reset._clear_archived
    original_write = runtime.write_json

    def interrupt_clear(*args):
        original_clear(*args)
        raise OSError("interruption after clearing archived files")

    monkeypatch.setattr(fresh_reset, "_clear_archived", interrupt_clear)
    with pytest.raises(OSError, match="clearing archived"):
        fresh_reset.reset(old["id"], archive_parent, qualified)
    expected = gate.reset_intent()["successor"]
    assert gate.reset_pending() and runtime.current() is None
    assert runtime.ensure_runner() == {"state": "GENERATION_RESET_PENDING"}

    def interrupt_publication(path, value):
        if path.name == "current.json":
            raise OSError("interruption after ledger commit")
        original_write(path, value)

    monkeypatch.setattr(fresh_reset, "_clear_archived", original_clear)
    monkeypatch.setattr(runtime, "write_json", interrupt_publication)
    no_requalify = lambda: pytest.fail("An interrupted reset must reuse its sealed plan")
    with pytest.raises(OSError, match="ledger commit"):
        fresh_reset.reset(old["id"], archive_parent, no_requalify)
    assert read_json(root / "entries-paused")["generation"] == expected
    assert gate.reset_pending() and runtime.current() is None
    monkeypatch.setattr(runtime, "write_json", original_write)
    result = fresh_reset.reset(old["id"], archive_parent, no_requalify)
    assert result["generation_id"] == expected
    assert fresh_reset.reset(old["id"], archive_parent, no_requalify) == result
    assert not gate.reset_pending()
    with closing(sqlite3.connect(root / "paper.sqlite3")) as db:
        assert db.execute("SELECT count(*) FROM event").fetchone()[0] == 7


def test_stop_and_reset_are_bound_to_generation_and_old_runner_lease(generation):
    root, old, archive_parent = generation
    with pytest.raises(ValueError, match="exact active"):
        fresh_reset.request_stop("0" * 64)
    first = fresh_reset.request_stop(old["id"])
    assert fresh_reset.request_stop(old["id"]) == first
    assert gate.stop_requested(old["id"]) and not gate.stop_requested("0" * 64)
    with writer_lock(root / ".runner.lock"), pytest.raises(RuntimeError, match=r"Another writer owns .*\.runner\.lock"):
        fresh_reset.reset(old["id"], archive_parent, qualified)
    assert runtime.current() == old
    assert not archive_parent.exists() and not gate.reset_pending()
