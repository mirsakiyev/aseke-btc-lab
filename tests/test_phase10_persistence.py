import json
from hashlib import sha256
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.phase10 import worker
from lab.storage import utc, writer_lock


NOW = utc("2026-11-19T01:20:29.008539Z")


def snapshot_bytes():
    directory = worker.RUNTIME.with_name(worker.RUNTIME.name + ".snap")
    return max(directory.glob("*.json")).read_bytes()


def windows_error(number=5):
    error = PermissionError(13, "Isolated Windows access test")
    error.winerror = number
    return error


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(worker, "STATE", tmp_path)
    monkeypatch.setattr(worker, "RUNTIME", tmp_path / "runtime.json")
    monkeypatch.setattr(worker, "LOCK", tmp_path / ".worker.lock")
    monkeypatch.setattr(worker, "OUTPUT", tmp_path / "output")
    monkeypatch.setattr(worker, "utc_now", lambda: NOW)
    monkeypatch.setattr(worker, "controls", lambda: {"paused": False, "monitor": True})
    return tmp_path


def minimal_worker(rt=None):
    item = worker.Worker.__new__(worker.Worker)
    item.rt = {"heartbeat": "2026-11-19T01:20:00+00:00", **(rt or {})}
    item.used = 0
    item.remaining = 3
    item.budget = {"maximum_response_bytes": 1000, "minimum_spacing_seconds": 0}
    item.raw = Path("unused")
    item.seen = set()
    return item


@pytest.mark.parametrize("number", [5, 32, 33])
def test_transient_windows_replace_failure_recovers_atomically(isolated, monkeypatch, number):
    path = worker.RUNTIME
    path.write_bytes(b'{"pid":7}')
    replace, calls, sleeps = Path.replace, [], []

    def contend(source, target):
        calls.append(source.read_bytes())
        assert json.loads(source.read_bytes()) == {"pid": 8}
        assert path.read_bytes() == b'{"pid":7}'
        if len(calls) == 1:
            raise windows_error(number)
        return replace(source, target)

    monkeypatch.setattr(Path, "replace", contend)
    monkeypatch.setattr(worker.time, "sleep", sleeps.append)
    worker.write_legacy_runtime({"pid": 8})
    assert worker.read_runtime() == {"pid": 8}
    assert len(calls) == 2 and sleeps == [.05]
    assert not list(isolated.glob("*.tmp"))


def test_persistent_access_denial_is_bounded_and_preserves_old_file(isolated, monkeypatch):
    worker.RUNTIME.write_bytes(b'{"pid":7}')
    attempts, sleeps = [], []

    def denied(source, target):
        attempts.append(source)
        raise windows_error()

    monkeypatch.setattr(Path, "replace", denied)
    monkeypatch.setattr(worker.time, "sleep", sleeps.append)
    with pytest.raises(PermissionError) as error:
        worker.write_legacy_runtime({"pid": 8})
    assert error.value.winerror == 5
    assert len(attempts) == 3 and sleeps == [.05, .1]
    assert worker.RUNTIME.read_bytes() == b'{"pid":7}'
    assert not list(isolated.glob("*.tmp"))


def test_nonwindows_storage_failure_does_not_retry_or_mask_primary_error(isolated, monkeypatch, caplog):
    worker.RUNTIME.write_bytes(b'{"pid":7}')
    original = OSError(28, "disk full")
    monkeypatch.setattr(worker.os, "fsync", lambda _: (_ for _ in ()).throw(original))
    monkeypatch.setattr(Path, "unlink", lambda *_a, **_k: (_ for _ in ()).throw(PermissionError("cleanup denied")))
    monkeypatch.setattr(worker.time, "sleep", lambda _: pytest.fail("No retry for fsync/disk-full failure"))
    with pytest.raises(OSError) as result:
        worker.write_legacy_runtime({"pid": 8})
    assert result.value is original
    assert worker.RUNTIME.read_bytes() == b'{"pid":7}'
    assert "temporary runtime cleanup failed" in caplog.text


def test_windows_readers_do_not_block_new_immutable_runtime_checkpoints(isolated):
    worker.write_legacy_runtime({"pid": 7})
    with worker.RUNTIME.open("rb") as legacy_reader:
        with pytest.raises(PermissionError) as error:
            worker.write_legacy_runtime({"pid": 8})
        assert error.value.winerror == 5
        worker.write_runtime({"pid": 8})
        assert worker.read_runtime() == {"pid": 8}
        assert json.loads(legacy_reader.read()) == {"pid": 7}

    directory = worker.RUNTIME.with_name(worker.RUNTIME.name + ".snap")
    first = next(directory.glob("*.json"))
    with first.open("rb") as checkpoint_reader:
        for pid in range(9, 15):
            worker.write_runtime({"pid": pid})
            worker.runtime_snapshot.trim(worker.RUNTIME, limit=8)
            assert worker.read_runtime() == {"pid": pid}
        assert first.exists() and json.loads(checkpoint_reader.read()) == {"pid": 8}
        assert len(list(directory.glob("*.json"))) == 4
    worker.write_runtime({"pid": 15})
    worker.runtime_snapshot.trim(worker.RUNTIME, limit=8)
    assert not first.exists() and len(list(directory.glob("*.json"))) == 3


def test_failed_save_never_advances_successful_heartbeat(isolated, monkeypatch):
    item = minimal_worker()
    previous = item.rt["heartbeat"]
    worker.write_runtime(item.rt)
    old_bytes = snapshot_bytes()
    monkeypatch.setattr(worker, "write_runtime", lambda _: (_ for _ in ()).throw(windows_error()))
    with pytest.raises(worker.RuntimePersistenceError) as error:
        item.save()
    assert error.value.original.winerror == 5
    assert item.rt["heartbeat"] == previous
    assert snapshot_bytes() == old_bytes


def test_pre_request_metadata_failure_sends_no_http_and_has_local_scope(isolated, monkeypatch):
    item = minimal_worker()
    worker.write_runtime({**item.rt, "active_request": None})
    previous = snapshot_bytes()
    monkeypatch.setattr(worker, "write_runtime", lambda _: (_ for _ in ()).throw(windows_error()))
    monkeypatch.setattr(worker, "send", lambda *_a, **_k: pytest.fail("Request must not run before durable metadata"))
    with pytest.raises(worker.RuntimePersistenceError) as caught:
        item.fetch(None, {"key": "perp-mark"})
    worker.record_cycle_failure(item.rt, caught.value)
    assert item.rt["active_request"] is None
    assert item.rt["backoff"]["scope"] == "RESEARCH_WORKER"
    assert item.rt["backoff"]["cause"] == "LOCAL_RUNTIME_PERSISTENCE"
    assert item.rt["backoff"]["origin"] == "lab.phase10.worker"
    assert item.rt["backoff"]["retry_at"] == item.rt["retry_at"]
    assert item.rt["last_error"]["winerror"] == 5
    assert "source_backoff" not in item.rt
    assert snapshot_bytes() == previous
    with worker.source_outcome_lock(wait_seconds=0):
        pass


def test_local_persistence_failure_never_erases_source_breaker_and_restart_retains_it(isolated, monkeypatch, caplog):
    item = minimal_worker()
    worker.set_backoff(item.rt, "RATE_LIMITED", 90, scope="SHARED_KUCOIN_SOURCE", at=NOW)
    original = dict(item.rt["source_backoff"])
    actual = worker.write_runtime
    monkeypatch.setattr(worker, "write_runtime", lambda _: (_ for _ in ()).throw(windows_error()))
    assert item.save_or_defer() is False
    assert item.save_or_defer() is False
    assert item.rt["source_backoff"] == original
    assert item.rt["retry_at"] == original["retry_at"]
    assert item.rt["heartbeat"] == "2026-11-19T01:20:00+00:00"
    assert "runtime remains unpersisted" in caplog.text
    monkeypatch.setattr(worker, "write_runtime", actual)
    assert item.save_or_defer() is True
    restarted = minimal_worker(worker.read_runtime())
    assert restarted.rt["source_backoff"] == original
    assert restarted.rt["backoff"]["cause"] == "LOCAL_RUNTIME_PERSISTENCE"
    assert restarted.rt["heartbeat"] == NOW.isoformat()


@pytest.mark.parametrize("status", ["RATE_LIMITED", "ACCESS_RESTRICTED", "INVALID_RESPONSE", "TRANSPORT_ERROR"])
def test_genuine_send_failures_keep_source_scope_durable(isolated, monkeypatch, status):
    item = minimal_worker()
    monkeypatch.setattr(worker, "send", lambda *_a: {"status": status, "received_at": NOW.isoformat()})
    assert item.fetch(None, {"key": "perp-mark"}) is None
    restarted = worker.read_runtime()
    assert restarted["source_backoff"]["scope"] == "SHARED_KUCOIN_SOURCE"
    assert restarted["source_backoff"]["cause"] == status
    assert restarted["source_backoff"]["retry_at"] == restarted["retry_at"]
    assert restarted["last_error"]["status"] == status


def test_unknown_worker_failure_is_conservative_not_local(isolated):
    row = {}
    worker.record_cycle_failure(row, ValueError("unclassified input failure"))
    assert row["backoff"]["scope"] == "SHARED_KUCOIN_SOURCE"
    assert row["backoff"]["cause"] == "UNCLASSIFIED_WORKER_ERROR"
    assert row["source_backoff"] == row["backoff"]


def test_research_raw_capacity_has_local_scope_without_erasing_remote_restriction(isolated, monkeypatch):
    item = minimal_worker()
    item.used = worker.MAX_RAW_BYTES
    monkeypatch.setattr(worker, "send", lambda *_a: pytest.fail("Capacity gate must not issue HTTP"))
    assert item.fetch(None, {"key": "perp-mark"}) is None
    assert item.rt["backoff"]["scope"] == "RESEARCH_WORKER"
    assert item.rt["backoff"]["cause"] == "RESEARCH_RAW_CAPACITY"
    assert "source_backoff" not in item.rt


def test_worker_cycle_retry_prevents_research_collection_and_records_no_decision(isolated, monkeypatch):
    item = minimal_worker()
    worker.set_backoff(item.rt, "LOCAL_RUNTIME_PERSISTENCE", 30, scope="RESEARCH_WORKER")
    calls = []
    item.manage_active = lambda quotes, now: calls.append((quotes, now))
    item.prices = lambda *_a: pytest.fail("No research request during retry")
    item.record = lambda *_a: pytest.fail("No synthetic delayed decision during retry")
    item.cycle(None)
    assert calls == [({}, NOW)]


def test_persisted_corruption_still_requires_explicit_ownership_checked_repair(isolated):
    original = b"\0" * 27063
    worker.RUNTIME.write_bytes(original)
    with pytest.raises(worker.CorruptRuntime, match="Preserve it"):
        worker.read_runtime()
    assert worker.RUNTIME.read_bytes() == original


def test_successful_http_outcome_commits_clear_intent_before_return(isolated, monkeypatch):
    item = minimal_worker()
    monkeypatch.setattr(worker, "send", lambda *_a: {"status": "RECEIVED", "received_at": NOW.isoformat()})
    row = item.fetch(None, {"key": "perp-mark"})
    restarted = worker.read_runtime()
    assert row["status"] == "RECEIVED"
    assert restarted["active_request"] is None
    assert restarted["last_request_result"]["status"] == "RECEIVED"
    worker.require_completed_request(restarted)


@pytest.mark.parametrize("status", ["RECEIVED", "RATE_LIMITED", "ACCESS_RESTRICTED"])
def test_result_publication_failure_keeps_durable_unknown_outcome_and_refuses_restart(isolated, monkeypatch, status):
    item = minimal_worker()
    actual, calls = worker.write_runtime, []

    def result_failure(row):
        calls.append(row)
        if len(calls) == 2:
            raise windows_error()
        return actual(row)

    monkeypatch.setattr(worker, "write_runtime", result_failure)
    monkeypatch.setattr(worker, "send", lambda *_a: {"status": status, "received_at": NOW.isoformat()})
    with pytest.raises(worker.RuntimePersistenceError):
        item.fetch(None, {"key": "perp-mark"})
    assert worker.read_runtime()["active_request"]["key"] == "perp-mark"
    original = snapshot_bytes()
    with pytest.raises(RuntimeError, match="Unresolved Phase 10 source-request outcome"):
        worker.run(managed=True)
    assert snapshot_bytes() == original
    # The surviving owner knows the actual result. A later successful durable
    # publication may clear intent, but must retain genuine source restrictions.
    assert item.save_or_defer()
    if status != "RECEIVED":
        assert worker.read_runtime()["source_backoff"]["cause"] == status
    assert worker.read_runtime()["active_request"] is None


def test_unknown_exception_inside_send_preserves_intent_even_after_idle_save(isolated, monkeypatch):
    item = minimal_worker()
    monkeypatch.setattr(worker, "send", lambda *_a: (_ for _ in ()).throw(PermissionError("raw evidence denied after dispatch")))
    with pytest.raises(PermissionError) as error:
        item.fetch(None, {"key": "perp-mark"})
    worker.record_cycle_failure(item.rt, error.value)
    assert item.rt["backoff"]["scope"] == "SHARED_KUCOIN_SOURCE"
    assert item.save_or_defer()
    with pytest.raises(worker.SourceOutcomePending), worker.source_outcome_lock(wait_seconds=0):
        pytest.fail("Unknown venue outcome must retain its publication fence")
    assert worker.read_runtime()["active_request"]["key"] == "perp-mark"
    with pytest.raises(RuntimeError, match="Unresolved"):
        worker.require_completed_request(worker.read_runtime())
    with pytest.raises(RuntimeError, match="Unresolved"):
        item.fetch(None, {"key": "next-request"})
    item.release_source_outcome()


def test_cache_replace_failure_is_proven_local_storage_and_preserves_cache(isolated, monkeypatch):
    folder = isolated / "candles"
    folder.mkdir()
    cache = folder / "day.parquet"
    cache.write_bytes(b"previous preserved research cache")
    original = cache.read_bytes()
    monkeypatch.setattr(Path, "replace", lambda *_a: (_ for _ in ()).throw(windows_error()))
    with pytest.raises(worker.ResearchPersistenceError) as error:
        worker.cache_save("day", pd.DataFrame({"value": [1]}))
    row = {}
    worker.record_cycle_failure(row, error.value)
    assert row["backoff"]["cause"] == "LOCAL_RESEARCH_STORAGE"
    assert row["backoff"]["scope"] == "RESEARCH_WORKER"
    assert cache.read_bytes() == original
    assert not list(folder.glob("*.tmp"))


def test_failed_result_publish_retains_fence_until_known_outcome_commits(isolated, monkeypatch):
    item = minimal_worker()
    actual, calls = worker.write_runtime, []

    def publish(row):
        calls.append(row)
        if len(calls) == 2:
            raise windows_error()
        return actual(row)

    monkeypatch.setattr(worker, "write_runtime", publish)
    monkeypatch.setattr(worker, "send", lambda *_a: {"status": "RATE_LIMITED", "received_at": NOW.isoformat()})
    with pytest.raises(worker.RuntimePersistenceError):
        item.fetch(None, {"key": "perp-mark"})
    with pytest.raises(worker.SourceOutcomePending), worker.source_outcome_lock(wait_seconds=0):
        pytest.fail("Failed critical result cannot admit an independent request")
    assert item.save_or_defer()
    with worker.source_outcome_lock(wait_seconds=0):
        assert worker.read_runtime()["source_backoff"]["cause"] == "RATE_LIMITED"


def owner_state():
    return {"pid": os.getpid(), "created": worker.psutil.Process().create_time(), "launch_id": "isolated-owned-launch",
            "worker_stage": "COLLECTING", "stopped_at": None,
            "worker_source_sha256": worker.LOADED_WORKER_SOURCE_SHA256}


def owned_intent(state):
    started = worker.time.monotonic()
    return {"schema": worker.SOURCE_REQUEST_SCHEMA, "key": "mark", "started_at": NOW.isoformat(),
            "permission_until": (NOW+pd.Timedelta(seconds=5)).isoformat(), "started_monotonic": started,
            "permission_monotonic_until": started+5,
            **{key: state[key] for key in ("pid", "created", "launch_id")}}


def test_twenty_healthy_inflight_requests_never_hold_result_fence_or_alias_protection(isolated, monkeypatch):
    item = minimal_worker(owner_state())
    item.remaining = 20
    calls = []

    def source(*_):
        with worker.source_outcome_lock(wait_seconds=0):
            durable = worker.read_runtime()
            assert worker.live_request(durable, NOW)
            calls.append(durable["active_request"]["key"])
        return {"status": "RECEIVED", "received_at": NOW.isoformat()}

    monkeypatch.setattr(worker, "send", source)
    with writer_lock(worker.LOCK):
        for number in range(20):
            assert item.fetch(None, {"key": f"research-{number}"})["status"] == "RECEIVED"
    assert len(calls) == 20
    assert worker.read_runtime()["active_request"] is None


@pytest.mark.parametrize("mutation", [
    lambda state: state["active_request"].update(schema="OLD"),
    lambda state: state["active_request"].update(launch_id="another-launch"),
    lambda state: state["active_request"].update(started_at="2026-11-19T01:20:30Z"),
    lambda state: state["active_request"].update(permission_until="2026-11-19T01:22:00Z"),
    lambda state: state.update(worker_source_sha256="0" * 64),
    lambda state: state.update(worker_stage="STOPPED"),
    lambda state: state.update(created=-1),
    lambda state: state["active_request"].update(started_monotonic=float("nan")),
    lambda state: state["active_request"].update(permission_monotonic_until=0),
])
def test_only_exact_owned_timed_inflight_intent_can_be_nonblocking(isolated, mutation):
    state = owner_state()
    state["active_request"] = owned_intent(state)
    with writer_lock(worker.LOCK):
        assert worker.live_request(state, NOW)
        mutation(state)
        assert not worker.live_request(state, NOW)


def test_unknown_intent_after_owner_release_or_deadline_is_not_healthy(isolated):
    state = owner_state()
    state["active_request"] = owned_intent(state)
    with writer_lock(worker.LOCK):
        assert not worker.live_request(state, NOW+pd.Timedelta(seconds=6))
    assert not worker.live_request(state, NOW)


def test_result_fence_acquire_timeout_expires_monotonic_permission_before_return(isolated, monkeypatch):
    item = minimal_worker(owner_state())
    clock, calls, readers = [1000.], [], []
    monkeypatch.setattr(worker.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(worker.time, "sleep", lambda delay: clock.__setitem__(0, clock[0]+delay))

    def slow_reader_at_result(*_):
        reader = worker.source_outcome_lock(wait_seconds=0)
        reader.__enter__()
        readers.append(reader)
        calls.append("HTTP")
        assert worker.live_request(worker.read_runtime(), NOW)
        return {"status": "RATE_LIMITED", "received_at": NOW.isoformat()}

    monkeypatch.setattr(worker, "send", slow_reader_at_result)
    with writer_lock(worker.LOCK):
        try:
            with pytest.raises(worker.SourceOutcomePending, match="monotonic request permission expired") as error:
                item.fetch(None, {"key": "perp-mark"})
            durable = worker.read_runtime()
            assert clock[0] > durable["active_request"]["permission_monotonic_until"]
            assert clock[0] <= 1005.02
            worker.record_cycle_failure(item.rt, error.value)
            monkeypatch.setattr(worker, "write_runtime", lambda _: (_ for _ in ()).throw(windows_error()))
            assert not item.save_or_defer()
        finally:
            for reader in readers:
                reader.__exit__(None, None, None)
        # Wall time is still inside the original five-second permission window
        # (equivalent to a rollback). Neither a free fence nor a healthy owner
        # can revive the expired monotonic request admission.
        with worker.source_outcome_lock(wait_seconds=0):
            assert not worker.live_request(worker.read_runtime(), NOW)
    assert calls == ["HTTP"]


def test_result_fence_slow_reader_releases_before_deadline_then_outcome_commits(isolated, monkeypatch):
    item = minimal_worker(owner_state())
    clock, readers = [1000.], []
    monkeypatch.setattr(worker.time, "monotonic", lambda: clock[0])

    def sleep(delay):
        clock[0] += delay
        if clock[0] >= 1000.3 and readers:
            readers.pop().__exit__(None, None, None)

    monkeypatch.setattr(worker.time, "sleep", sleep)

    def response(*_):
        reader = worker.source_outcome_lock(wait_seconds=0)
        reader.__enter__()
        readers.append(reader)
        return {"status": "RATE_LIMITED", "received_at": NOW.isoformat()}

    monkeypatch.setattr(worker, "send", response)
    try:
        assert item.fetch(None, {"key": "perp-mark"}) is None
        assert 1000.3 <= clock[0] < 1001
        assert worker.read_runtime()["active_request"] is None
        assert worker.read_runtime()["source_backoff"]["cause"] == "RATE_LIMITED"
        with worker.source_outcome_lock(wait_seconds=0):
            pass
    finally:
        for reader in readers:
            reader.__exit__(None, None, None)


def test_local_storage_error_classification_never_relabels_parser_or_source_fault(isolated):
    def denied():
        raise PermissionError("metadata file denied")

    with pytest.raises(worker.ResearchPersistenceError) as error:
        worker.research_io(denied)
    row = {}
    worker.record_cycle_failure(row, error.value)
    assert row["backoff"]["cause"] == "LOCAL_RESEARCH_STORAGE"

    def invalid_payload():
        raise ValueError("bad source payload")

    with pytest.raises(ValueError) as error:
        worker.research_io(invalid_payload)
    worker.record_cycle_failure(row, error.value)
    assert row["backoff"]["scope"] == "SHARED_KUCOIN_SOURCE"


@pytest.mark.parametrize("persistent_metadata_failure", [False, True])
def test_managed_start_records_loaded_worker_source_fingerprint_and_keeps_owner(isolated, monkeypatch, persistent_metadata_failure):
    flags = {"paused": False, "monitor": True}
    monkeypatch.setattr(worker, "controls", lambda: flags)
    monkeypatch.setattr(worker, "reference", lambda: {})
    monkeypatch.setattr(worker, "parent_design", lambda: {})
    real = worker.Worker
    writes, observations = [], []
    actual = worker.write_runtime

    def publish(row):
        writes.append(row["worker_stage"])
        if persistent_metadata_failure and row["worker_stage"] in ("COLLECTING", "IDLE"):
            raise windows_error()
        return actual(row)

    monkeypatch.setattr(worker, "write_runtime", publish)
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)

    class IdleWorker:
        save = real.save
        save_or_defer = real.save_or_defer
        release_source_outcome = real.release_source_outcome

        def __init__(self, rt, *_):
            self.rt, self.used = rt, 0
            self.raw = isolated / "raw"
            self.phase13_observer = SimpleNamespace(drain=lambda: True, status={})

        def recover_publications(self):
            pass

        def seed(self, *_):
            pass

        def cycle(self, *_):
            self.save()

        def admit_research(self):
            pass

        def observe_registered_research(self):
            observations.append(self.rt["pid"])
            flags["paused"] = len(observations) >= (2 if persistent_metadata_failure else 1)

    monkeypatch.setattr(worker, "Worker", IdleWorker)
    worker.run(managed=True)
    row = worker.read_runtime()
    assert row["worker_stage"] == "STOPPED"
    assert row["worker_source_sha256"] == worker.LOADED_WORKER_SOURCE_SHA256
    assert row["worker_source_sha256"] == sha256(Path(worker.__file__).read_bytes()).hexdigest()
    assert len(set(observations)) == 1
    if persistent_metadata_failure:
        assert observations == [row["pid"], row["pid"]]
        assert writes.count("IDLE") == 2
        assert row["backoff"]["cause"] == "LOCAL_RUNTIME_PERSISTENCE"


@pytest.mark.parametrize("change", [
    lambda row: row.update(retry_at=None),
    lambda row: row.update(retry_at="NaT"),
    lambda row: row["source_backoff"].update(schema="UNKNOWN"),
    lambda row: row["source_backoff"].update(at="2026-11-19T01:20:30Z"),
    lambda row: row["source_backoff"].update(scope="RESEARCH_WORKER"),
    lambda row: row.update(source_backoff={}),
])
def test_inconsistent_source_breaker_refuses_start_and_fetch_without_overwrite(isolated, monkeypatch, change):
    row = {}
    worker.set_backoff(row, "RATE_LIMITED", 60, scope="SHARED_KUCOIN_SOURCE")
    change(row)
    worker.write_runtime(row)
    original = snapshot_bytes()
    item = minimal_worker(worker.read_runtime())
    before = json.dumps(item.rt, sort_keys=True)
    monkeypatch.setattr(worker, "send", lambda *_a: pytest.fail("Malformed breaker cannot be discharged by a fresh request"))
    with pytest.raises(worker.CorruptRuntime, match="Inconsistent"):
        item.fetch(None, {"key": "perp-mark"})
    assert json.dumps(item.rt, sort_keys=True) == before
    with pytest.raises(worker.CorruptRuntime, match="Inconsistent"):
        worker.run(managed=True)
    assert snapshot_bytes() == original


def test_valid_legacy_retry_is_retained_and_unknown_scope_does_not_get_repaired(isolated, monkeypatch):
    item = minimal_worker({"retry_at": (NOW+pd.Timedelta(seconds=30)).isoformat()})
    monkeypatch.setattr(worker, "send", lambda *_a: pytest.fail("Legacy retry is still enforced"))
    assert item.fetch(None, {"key": "perp-mark"}) is None
    error = worker.CorruptRuntime("Unreviewed scope")
    row = {"source_backoff": {"unreviewed": True}}
    with pytest.raises(worker.CorruptRuntime):
        worker.record_cycle_failure(row, error)
    assert row == {"source_backoff": {"unreviewed": True}}


@pytest.mark.parametrize("operation", ["seed", "record", "manage_active", "recover_publications"])
def test_legacy_research_read_and_idea_journal_failures_are_local(isolated, monkeypatch, operation):
    item = minimal_worker()
    item.policy = {}

    def denied(*_a, **_k):
        raise PermissionError("isolated local research evidence denied")

    if operation == "seed":
        monkeypatch.setattr(worker, "latest", denied)
        invoke = lambda: item.seed(NOW)
    elif operation == "record":
        path = isolated / "existing-decision.json"
        path.write_text("{}", encoding="utf-8")
        item.existing_decision = lambda *_: path
        monkeypatch.setattr(worker, "read_json", denied)
        invoke = lambda: item.record("day", NOW, {}, {})
    elif operation == "recover_publications":
        item.rt["profiles"] = {name: {} for name in worker.NAMES}
        folder = worker.OUTPUT / "decisions" / worker.NAMES[0]
        folder.mkdir(parents=True)
        (folder / "existing.json").write_text("{}", encoding="utf-8")
        monkeypatch.setattr(worker, "read_json", denied)
        invoke = item.recover_publications
    else:
        item.rt["active"] = {"idea": {"market": "FUTURES", "profile": "swing"}}
        monkeypatch.setattr(worker, "observe_idea", denied)
        invoke = lambda: item.manage_active({}, NOW)
    with pytest.raises(worker.ResearchPersistenceError) as error:
        invoke()
    worker.record_cycle_failure(item.rt, error.value)
    assert item.rt["backoff"]["scope"] == "RESEARCH_WORKER"
    assert item.rt["backoff"]["cause"] == "LOCAL_RESEARCH_STORAGE"
