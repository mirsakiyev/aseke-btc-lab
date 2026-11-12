from pathlib import Path
from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

from lab.ema_paper import kraken_reference as reference
from lab.kraken_paper import source
from lab.storage import write_json
from lab.strategy_lab.market_data import Purpose
from test_kraken_paper_source import Clock, MINUTE, SEAM, _handler, _scope


@pytest.fixture
def environment(monkeypatch, tmp_path):
    scope = _scope()
    clock = Clock(SEAM + pd.Timedelta(seconds=5))
    calls = []
    ref = reference._Collection(scope.source, scope.grants[0].approved_at, None)
    terms = {"price_tick": .1, "quantity_step": 1e-8, "min_quantity": .00006, "min_quote": .5}
    monkeypatch.setattr(reference, "_permission", lambda *args: ref)
    monkeypatch.setattr(reference, "_instrument_terms", lambda: terms)
    monkeypatch.setattr(reference, "utc_now", clock)
    monkeypatch.setattr(reference, "sleep", clock.pause)
    monkeypatch.setattr(reference, "_client", lambda: httpx.Client(transport=httpx.MockTransport(_handler(clock, calls))))
    return tmp_path / "independent", clock, calls, ref


def test_full_bounded_history_has_causal_grant_bound_snapshot_and_no_producer(environment):
    root, clock, calls, scope = environment
    report = reference.qualify(root=root)
    assert report == reference.qualification(root=root) == reference.qualify(root=root)
    assert reference.scope_identity(root=root) == scope.source
    assert reference.status(root=root)["state"] == "NOT_STARTED"
    assert len(calls) == 5
    assert all(second[0] - first[0] >= source.MIN_REQUEST_SPACING for first, second in zip(calls, calls[1:]))
    for minutes, window in report["body"]["intervals"].items():
        assert window["raw_rows"] == 721 and window["excluded_unfinished"] == 1
        assert window["complete_rows"] == window["tail_contiguous"] == 720
        assert window["missing_rows"] == 0
        step = pd.Timedelta(minutes=int(minutes))
        grant = scope.grant(Purpose.WARMUP, SEAM - 720 * step, SEAM)
        data = reference.market_data([grant], root=root)
        snapshot = data.read(grant.id, grant.book_id, Purpose.WARMUP, source.TRADE,
                             grant.start, grant.end, clock.at, step)
        assert snapshot.complete and len(snapshot.observations) == 720
        assert snapshot.observations[-1].end == SEAM
        assert snapshot.observations[-1].raw_sha256 == window["raw_sha256"]
        with pytest.raises(PermissionError, match="outside"):
            data.read(grant.id, "different paper book", Purpose.WARMUP, source.TRADE,
                      grant.start, grant.end, clock.at, step)


def test_one_shared_live_cycle_catches_up_missed_minutes_without_rewriting_receipts(environment):
    root, clock, calls, scope = environment
    reference.qualify(root=root)
    clock.at = SEAM + 5 * MINUTE + pd.Timedelta(seconds=6)
    caught_up = reference.collect_once(root=root)
    assert set(caught_up) == {"1"}
    assert caught_up["1"]["new_rows"] == 5 and caught_up["1"]["missing_rows"] == 0
    assert reference.collect_once(root=root) == {}
    assert len(calls) == 6
    grant = scope.grant(Purpose.EXECUTION_EVIDENCE, SEAM, SEAM + 5 * MINUTE)
    data = reference.market_data([grant], root=root)
    before = clock.at - pd.Timedelta(seconds=1)
    unavailable = data.read(grant.id, grant.book_id, grant.purpose, source.TRADE, grant.start, grant.end, before, MINUTE)
    assert unavailable.missing_opens and not unavailable.observations
    current = data.read(grant.id, grant.book_id, grant.purpose, source.TRADE, grant.start, grant.end, clock.at, MINUTE)
    assert current.complete and len(current.observations) == 5
    assert current.observations[0].available_at > current.observations[0].end + 2 * MINUTE
    assert current.observations[-1].available_at < current.observations[-1].end + 2 * MINUTE


def test_gap_retry_keeps_actual_gap_and_following_receipts(environment, monkeypatch):
    root, clock, calls, _ = environment
    reference.qualify(root=root)
    clock.at = SEAM + MINUTE + pd.Timedelta(seconds=5)
    monkeypatch.setattr(reference, "_client",
                        lambda: httpx.Client(transport=httpx.MockTransport(_handler(clock, calls, gap=SEAM))))
    assert reference.collect_once(root=root)["1"]["state"] == "GAP"
    assert reference.collect_once(root=root) == {}
    clock.pause(31)
    monkeypatch.setattr(reference, "_client", lambda: httpx.Client(transport=httpx.MockTransport(_handler(clock, calls))))
    recovered = reference.collect_once(root=root)["1"]
    assert recovered["state"] == "CAPTURED" and recovered["new_rows"] == 1
    assert pd.Timestamp(recovered["available_at"]) >= SEAM + MINUTE + pd.Timedelta(seconds=36)


def test_qualification_and_forward_producer_revalidate_consent_before_http(environment, monkeypatch):
    root, _, calls, _ = environment
    reference.qualify(root=root)

    def blocked(*args):
        raise PermissionError("private consent unavailable")

    monkeypatch.setattr(reference, "_permission", blocked)
    with pytest.raises(PermissionError):
        reference.ensure(root=root)
    with pytest.raises(PermissionError):
        reference.collect_once(root=root)
    assert len(calls) == 5
    assert not (root / reference.PROCESS).exists()
    report = reference.qualification(root=root)
    write_json(root / reference.QUALIFICATION, {**report, "body": {**report["body"], "source_id": "other"}})
    with pytest.raises(ValueError, match="checksum"):
        reference.qualification(root=root)


def test_managed_launch_deduplicates_and_checks_exact_command(environment, monkeypatch):
    root, _, _, _ = environment
    report = reference.qualify(root=root)
    commands = []
    fake = SimpleNamespace(pid=12_345, create_time=lambda: 175.1)

    def launch(command, **kwargs):
        commands.append((command, kwargs))
        return fake

    monkeypatch.setattr(reference.managed, "_resources", lambda *args: None)
    monkeypatch.setattr(reference.subprocess, "Popen", launch)
    monkeypatch.setattr(reference.psutil, "Process", lambda pid: fake)
    monkeypatch.setattr(reference, "process_identity", lambda pid, created: fake if pid == fake.pid and created == 175.1 else None)
    fake.cmdline = lambda: commands[-1][0]
    assert reference.ensure(root=root) == {"state": "STARTED", "pid": fake.pid}
    assert reference.ensure(root=root)["state"] == "RUNNING"
    fake.cmdline = lambda: ["unrelated.exe"]
    assert reference.ensure(root=root)["state"] == "PROCESS_IDENTITY_MISMATCH"
    assert len(commands) == 1
    command, options = commands[0]
    assert command[-2:] == ["--qualification", report["id"]]
    assert Path(command[-3]) == root
    assert options["creationflags"] & reference.subprocess.CREATE_NO_WINDOW


def test_retired_root_cannot_receive_reference_data():
    with pytest.raises(ValueError, match="separate store"):
        reference.qualification(root=reference.catalog.ROOT / "source")
