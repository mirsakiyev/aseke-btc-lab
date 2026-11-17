import json
import sys

import pandas as pd
import pytest

from lab.paper_run import __main__ as operator
from lab.paper_run import gate, runtime
from lab.storage import utc, write_json, writer_lock


GENERATION = "current-generation"
NOW = utc("2026-11-17T18:00:00Z")


@pytest.fixture
def controls(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, "ROOT", tmp_path / "paper-run")
    monkeypatch.setattr(runtime, "current", lambda: {"id": GENERATION})
    monkeypatch.setattr(operator, "utc_now", lambda: NOW)
    monkeypatch.setattr(runtime, "ensure_runner", lambda: pytest.fail("Entries on must not restart or launch the runner"))
    path = gate.ROOT / "entries-paused"
    write_json(path, {"generation": GENERATION, "paused_at": (NOW - pd.Timedelta(minutes=1)).isoformat()})
    return path


def test_preview_preserves_pause_without_creating_authorization(controls):
    before = controls.read_bytes()
    result = operator.entries_on(GENERATION)
    assert result["state"] == "ENTRY_RESUME_PREVIEW" and result["paused"]
    assert result["requires_explicit_operator_approval"]
    assert controls.read_bytes() == before
    assert not (gate.ROOT / "entries-controls").exists()
    assert operator.entries_on(GENERATION, approve="false")["state"] == "ENTRY_RESUME_PREVIEW"
    assert controls.exists()


def test_approved_release_records_authority_and_is_safe_to_repeat(controls):
    original = controls.read_bytes()
    result = operator.entries_on(GENERATION, approve=True)
    assert result["state"] == "CURRENT_ENTRIES_ENABLED" and not controls.exists()
    records = list((gate.ROOT / "entries-controls").glob("*.json"))
    assert len(records) == 1
    document = json.loads(records[0].read_text())
    assert document["generation"] == GENERATION and document["operator"]
    assert document["authorized_at"] == NOW.isoformat()
    assert document["pause_marker"] == json.loads(original)
    assert document["action"] == "ENTRIES_ON"
    assert operator.entries_on(GENERATION, approve=True)["state"] == "CURRENT_ENTRIES_ALREADY_ENABLED"
    assert len(list((gate.ROOT / "entries-controls").glob("*.json"))) == 1


@pytest.mark.parametrize("generation", [None, "different-generation"])
def test_generation_mismatch_never_releases_the_pause(controls, generation):
    with pytest.raises(ValueError, match="generation"):
        operator.entries_on(generation, approve=True)
    assert controls.exists()


@pytest.mark.parametrize("document", [
    {}, [], {"generation": "different-generation", "paused_at": NOW.isoformat()},
    {"generation": GENERATION, "paused_at": "not-a-date"},
    {"generation": GENERATION, "paused_at": "2026-11-17T18:00:01Z"},
    {"generation": GENERATION, "paused_at": "2026-11-17T17:59:00"},
])
def test_malformed_pause_fails_closed(controls, document):
    controls.write_text(json.dumps(document))
    with pytest.raises((ValueError, TypeError)):
        operator.entries_on(GENERATION, approve=True)
    assert controls.exists()


def test_corrupt_json_and_busy_writer_do_not_release_pause(controls):
    original = controls.read_bytes()
    controls.write_bytes(b"\x00\x00")
    with pytest.raises(ValueError):
        operator.entries_on(GENERATION, approve=True)
    assert controls.exists()
    controls.write_bytes(original)
    with writer_lock(gate.ROOT / ".writer.lock"), pytest.raises(RuntimeError, match="Another writer"):
        operator.entries_on(GENERATION, approve=True)
    assert controls.exists()


def test_generation_is_rechecked_under_the_writer_lease(controls, monkeypatch):
    reads = iter(({"id": GENERATION}, {"id": "new-generation"}))
    monkeypatch.setattr(runtime, "current", lambda: next(reads))
    with pytest.raises(ValueError, match="generation"):
        operator.entries_on(GENERATION, approve=True)
    assert controls.exists()


def test_audit_write_failure_keeps_pause_in_place(controls, monkeypatch):
    def fail_sync(_):
        raise OSError("simulated durability failure")

    monkeypatch.setattr(operator.os, "fsync", fail_sync)
    with pytest.raises(OSError, match="durability"):
        operator.entries_on(GENERATION, approve=True)
    assert controls.exists()


def test_existing_entries_off_callers_keep_their_behavior(controls, monkeypatch):
    controls.unlink()
    monkeypatch.setattr(runtime, "ensure_runner", lambda: {"state": "RUNNING"})
    result = operator.entries_off()
    assert result["state"] == "CURRENT_ENTRIES_PAUSED"
    assert result["runner"]["state"] == "RUNNING"
    assert json.loads(controls.read_text())["generation"] == GENERATION


def test_entries_on_cli_is_preview_without_approval(controls, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["paper-run", "entries", "on", "--generation", GENERATION])
    operator.main()
    assert json.loads(capsys.readouterr().out)["state"] == "ENTRY_RESUME_PREVIEW"
    assert controls.exists()
