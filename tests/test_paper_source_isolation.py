"""Actual source admission/delivery keeps local research faults out of protection."""

from contextlib import closing
from contextlib import contextmanager
from copy import deepcopy
import json
from threading import Event
from time import sleep
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.paper_run import capabilities, ema, execution, gate, membership, operations, protection_incidents as incidents, runtime, sources
from lab.phase15 import paper
from lab.storage import writer_lock
from test_paper_run_runtime import (
    MARK_BOOK, MARK_PENDING, MARK_PENDING_2, NOW, _idea, _terms, initialize,
    observation, setup as runtime_setup,
)


def backoff(scope="RESEARCH_WORKER", cause="LOCAL_RUNTIME_PERSISTENCE", *, until=None):
    return {"schema": "PHASE10_BACKOFF_V1", "origin": "lab.phase10.worker", "scope": scope,
            "cause": cause, "at": (NOW-pd.Timedelta(seconds=1)).isoformat(),
            "retry_at": (NOW+pd.Timedelta(seconds=60) if until is None else until).isoformat()}


def worker_state(*, cause="LOCAL_RUNTIME_PERSISTENCE", scope="RESEARCH_WORKER"):
    row = backoff(scope, cause)
    return {"retry_at": row["retry_at"], "backoff": row}


@pytest.fixture
def metadata(monkeypatch, tmp_path):
    state = worker_state()
    monkeypatch.setattr(runtime.monitoring, "STATE", tmp_path/"research-state")
    monkeypatch.setattr(runtime.monitoring, "RUNTIME", tmp_path/"research-state"/"runtime.json")
    monkeypatch.setattr(runtime.monitoring, "controls", lambda: {
        "monitor": True, "paused": False, "ideas": dict.fromkeys(("day", "swing", "position"), True)})
    monkeypatch.setattr(runtime.protocol, "control", lambda: {"enabled": True})
    optional = runtime.monitoring.optional
    monkeypatch.setattr(runtime.monitoring, "optional", lambda path, fallback:
                        deepcopy(state) if path == runtime.monitoring.RUNTIME else optional(path, fallback))
    return state


def healthy_request(state, at):
    state.clear()
    monotonic = runtime.monitoring.time.monotonic()
    state.update(pid=321, created=1234.5, launch_id="isolated-source-worker", worker_stage="COLLECTING",
                 worker_source_sha256=runtime.monitoring.LOADED_WORKER_SOURCE_SHA256,
                 active_request={"schema": "PHASE10_SOURCE_REQUEST_V1", "key": "reviewed-native-request",
                                 "pid": 321, "created": 1234.5, "launch_id": "isolated-source-worker",
                                 "started_at": (at-pd.Timedelta(milliseconds=100)).isoformat(),
                                 "permission_until": (at-pd.Timedelta(milliseconds=100)+pd.Timedelta(seconds=5)).isoformat(),
                                 "started_monotonic": monotonic,
                                 "permission_monotonic_until": monotonic+5},
                 retry_at=None)


def verify_live_owner(monkeypatch):
    monkeypatch.setattr(runtime.monitoring.psutil, "Process", lambda pid: SimpleNamespace(create_time=lambda: 1234.5))
    monkeypatch.setattr(runtime.monitoring, "worker_lock_held", lambda: True)


@pytest.mark.parametrize("cause", ["LOCAL_RUNTIME_PERSISTENCE", "LOCAL_RESEARCH_STORAGE", "RESEARCH_RAW_CAPACITY"])
@pytest.mark.parametrize("source", [sources.MARK, sources.SPOT])
def test_exact_local_backoff_allows_independent_protection_but_never_entries(metadata, cause, source):
    metadata.update(worker_state(cause=cause))
    assert runtime._source_permission(source, NOW, entry_only=False) is None
    assert runtime._source_permission(source, NOW, entry_only=True) == "SHARED_SOURCE_BACKOFF"
    assert runtime._source_permission(source, NOW+pd.Timedelta(seconds=61), entry_only=True) is None


@pytest.mark.parametrize("cause", ["RATE_LIMITED", "ACCESS_RESTRICTED", "TRANSPORT_ERROR", "INVALID_EXCHANGE_RESPONSE",
                                   "UNCLASSIFIED_WORKER_ERROR"])
def test_genuine_or_ambiguous_source_backoff_still_denies_every_kucoin_consumer(metadata, cause):
    metadata.update(worker_state(scope="SHARED_KUCOIN_SOURCE", cause=cause))
    for source in sources.KUCOIN:
        for entry_only in (False, True):
            assert runtime._source_permission(source, NOW, entry_only=entry_only) == "SHARED_SOURCE_BACKOFF"
    assert runtime._source_permission(ema.SOURCE, NOW, entry_only=False) is None


def test_later_local_error_cannot_overwrite_an_independent_source_circuit_breaker(metadata):
    metadata["source_backoff"] = backoff("SHARED_KUCOIN_SOURCE", "RATE_LIMITED",
                                         until=NOW+pd.Timedelta(minutes=5))
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_BACKOFF"
    assert runtime._source_permission(sources.MARK, NOW+pd.Timedelta(seconds=61), entry_only=False) == "SHARED_SOURCE_BACKOFF"
    assert runtime._source_permission(sources.MARK, NOW+pd.Timedelta(minutes=6), entry_only=False) is None


def test_untyped_retry_and_unproven_research_error_keep_the_conservative_legacy_barrier(metadata):
    metadata.pop("backoff")
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_BACKOFF"
    metadata.update(worker_state(cause="LOCAL_RESEARCH_ERROR"))
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_BACKOFF"


def test_uncommitted_http_outcome_cannot_be_bypassed_as_a_local_persistence_error(metadata):
    metadata["active_request"] = {"key": "reviewed-mark-request", "started_at": (NOW-pd.Timedelta(seconds=1)).isoformat()}
    for entry_only in (False, True):
        assert runtime._source_permission(sources.MARK, NOW, entry_only=entry_only) == "SOURCE_REQUEST_OUTCOME_UNRESOLVED"
    assert runtime._source_permission(ema.SOURCE, NOW, entry_only=False) is None
    metadata["active_request"] = None
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) is None


def test_ordinary_verified_bounded_http_inflight_preserves_baseline_concurrent_admission(metadata, monkeypatch):
    healthy_request(metadata, NOW)
    verify_live_owner(monkeypatch)
    for entry_only in (False, True):
        assert runtime._source_permission(sources.MARK, NOW, entry_only=entry_only) is None
    assert runtime._source_permission(sources.MARK, NOW+pd.Timedelta(seconds=6), entry_only=False) == "SOURCE_REQUEST_OUTCOME_UNRESOLVED"
    monkeypatch.setattr(runtime.monitoring, "worker_lock_held", lambda: False)
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SOURCE_REQUEST_OUTCOME_UNRESOLVED"


def test_completed_but_unpublished_http_outcome_excludes_even_a_live_current_intent(metadata, monkeypatch):
    healthy_request(metadata, NOW)
    verify_live_owner(monkeypatch)
    waits = []
    @contextmanager
    def outcome_pending(*, wait_seconds):
        waits.append(wait_seconds)
        raise runtime.monitoring.SourceOutcomePending("known HTTP result not yet durable")
        yield
    monkeypatch.setattr(runtime.monitoring, "source_outcome_lock", outcome_pending)
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SOURCE_OUTCOME_PUBLICATION_PENDING"
    assert runtime._source_permission(sources.MARK, NOW, entry_only=True, wait_seconds=0) == "SOURCE_OUTCOME_PUBLICATION_PENDING"
    assert waits == [.15, 0]
    assert runtime._source_permission(ema.SOURCE, NOW, entry_only=False) is None


def test_admission_validation_advances_only_by_actual_bounded_wait(metadata, monkeypatch):
    verify_live_owner(monkeypatch)
    @contextmanager
    def waited(*, wait_seconds):
        assert wait_seconds == .15
        yield .1
    monkeypatch.setattr(runtime.monitoring, "source_outcome_lock", waited)
    # The new request genuinely started during the bounded admission-read wait.
    healthy_request(metadata, NOW+pd.Timedelta(milliseconds=150))
    original = deepcopy(metadata["active_request"])
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) is None
    assert metadata["active_request"] == original
    # A timestamp beyond elapsed monotonic time remains future-dated/rejected.
    healthy_request(metadata, NOW+pd.Timedelta(seconds=1))
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_STATE_UNAVAILABLE"


@pytest.mark.parametrize("after_http", [False, True])
def test_actual_worker_publication_failure_distinguishes_pre_send_metadata_from_known_429(
        metadata, monkeypatch, after_http):
    from test_phase10_persistence import minimal_worker, windows_error
    worker = runtime.monitoring
    monkeypatch.setattr(worker, "RUNTIME", worker.STATE/"runtime.json")
    monkeypatch.setattr(worker, "utc_now", lambda: NOW)
    from lab.storage import read_json
    monkeypatch.setattr(worker, "optional", lambda path, fallback: read_json(path) if path.exists() else fallback)
    item = minimal_worker({"active_request": None, "retry_at": None})
    item.save()
    actual = worker.write_runtime
    calls = []
    def publish(row):
        calls.append(row)
        if len(calls) == (2 if after_http else 1):
            raise windows_error()
        return actual(row)
    monkeypatch.setattr(worker, "write_runtime", publish)
    sent = []
    monkeypatch.setattr(worker, "send", lambda *_args:
                        sent.append("RATE_LIMITED") or {"status": "RATE_LIMITED", "received_at": NOW.isoformat()})
    try:
        with pytest.raises(worker.RuntimePersistenceError) as failure:
            item.fetch(None, {"key": "native-request"})
        if after_http:
            assert sent == ["RATE_LIMITED"]
            assert runtime._source_permission(sources.MARK, NOW, entry_only=False, wait_seconds=0) == "SOURCE_OUTCOME_PUBLICATION_PENDING"
            assert item.save_or_defer()
            assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_BACKOFF"
        else:
            assert not sent
            worker.record_cycle_failure(item.rt, failure.value, at=NOW)
            assert item.save_or_defer()
            assert runtime._source_permission(sources.MARK, NOW, entry_only=False) is None
            assert runtime._source_permission(sources.MARK, NOW, entry_only=True) == "SHARED_SOURCE_BACKOFF"
    finally:
        item.release_source_outcome()


@pytest.mark.parametrize("mutate", [
    lambda row: row["backoff"].update(origin="unreviewed.producer"),
    lambda row: row["backoff"].update(schema="future-schema"),
    lambda row: row["backoff"].update(scope="IGNORE_ALL_SAFETY"),
    lambda row: row["backoff"].update(at=(NOW+pd.Timedelta(seconds=1)).isoformat()),
    lambda row: row["backoff"].update(at="2026-11-17T06:16:03"),
    lambda row: row["backoff"].update(retry_at=(NOW+pd.Timedelta(seconds=55)).isoformat()),
    lambda row: row.update(retry_at=None),
    lambda row: row.update(retry_at="NaT"),
    lambda row: row.update(source_backoff=backoff("RESEARCH_WORKER")),
    lambda row: row.update(source_backoff={}),
    lambda row: row.update(active_request={"key": "mark", "started_at": (NOW+pd.Timedelta(seconds=1)).isoformat()}),
    lambda row: row.update(active_request={"key": "mark", "started_at": "2026-11-17T06:16:03"}),
    lambda row: row.update(active_request={}),
])
def test_inconsistent_or_untrusted_retry_metadata_is_rejected_source_locally(metadata, mutate):
    mutate(metadata)
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_STATE_UNAVAILABLE"
    assert runtime._source_permission(ema.SOURCE, NOW, entry_only=False) is None


@pytest.mark.parametrize("failure", [PermissionError("runtime read denied"), json.JSONDecodeError("NUL", "\0", 0)])
def test_corrupt_or_inaccessible_worker_runtime_cannot_abort_unrelated_source_admission(metadata, monkeypatch, failure):
    def unavailable(*_args):
        raise failure
    monkeypatch.setattr(runtime.monitoring, "optional", unavailable)
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_STATE_UNAVAILABLE"
    assert runtime._source_permission(ema.SOURCE, NOW, entry_only=False) is None


@pytest.mark.parametrize("content", [None, {}, [], {"unrelated": "not a runtime checkpoint"}])
def test_missing_empty_or_unknown_runtime_never_becomes_implicit_source_permission(metadata, monkeypatch, content):
    monkeypatch.setattr(runtime.monitoring, "optional", lambda *_args: content)
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_STATE_UNAVAILABLE"
    assert runtime._source_permission(ema.SOURCE, NOW, entry_only=False) is None


def test_runtime_file_absence_is_distinct_from_an_explicit_clear_checkpoint(metadata, monkeypatch, tmp_path):
    path = tmp_path/"runtime.json"
    monkeypatch.setattr(runtime.monitoring, "RUNTIME", path)
    from lab.storage import read_json
    monkeypatch.setattr(runtime.monitoring, "optional", lambda candidate, fallback:
                        read_json(candidate) if candidate.exists() else fallback)
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "SHARED_SOURCE_STATE_UNAVAILABLE"
    path.write_text(json.dumps({"active_request": None, "retry_at": None}), encoding="utf-8")
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) is None


@pytest.mark.parametrize("flags,enabled", [({"monitor": False, "paused": False}, True),
                                           ({"monitor": True, "paused": False}, False)])
def test_local_exception_never_waives_global_source_permissions(metadata, monkeypatch, flags, enabled):
    monkeypatch.setattr(runtime.monitoring, "controls", lambda: flags)
    monkeypatch.setattr(runtime.protocol, "control", lambda: {"enabled": enabled})
    assert runtime._source_permission(sources.MARK, NOW, entry_only=False) == "KUCOIN_MONITOR_OR_PROTECTION_PERMISSION_CLOSED"


def prepare_cycle(runtime_setup, monkeypatch):
    record, lanes = initialize(runtime_setup)
    # A supported future consumer inherits the source exception and circuit
    # breakers through registration; no strategy-specific runtime branch exists.
    orbital = deepcopy(lanes[MARK_BOOK])
    orbital.update(book_id=fingerprint([record["id"], "orbital/mark"]), family="ORBITAL_REFERENCE",
                   label="Isolated future KuCoin mark consumer", paper_only=True,
                   execution_mode="MANUAL_OUTSIDE_LAB", approval="No validated model",
                   entry_model="REALTIME_MARK_REFERENCE_PAPER", exit_model="REALTIME_MARK_REFERENCE_PAPER")
    orbital["capabilities"] = capabilities.declare(orbital, signal_adapter="breakout_v1",
                                                  entry_adapter="breakout_reference_v1",
                                                  generation=record["id"], slot="orbital/mark")
    with closing(runtime.connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        membership.register(db, record, {"generation": record["id"], "slot": "orbital/mark", "lane": orbital,
                                       "request_id": "isolated-source-consumer", "reason": "Isolated test only"},
                            at=NOW, approved=True)
    lanes = {**lanes, "orbital/mark": orbital}
    clock = [NOW]
    monkeypatch.setattr(runtime, "utc_now", lambda: clock[0])
    monkeypatch.setattr(execution, "utc_now", lambda: clock[0])
    monkeypatch.setattr(execution, "time", SimpleNamespace(monotonic_ns=lambda: 1_300_000_000))
    monkeypatch.setattr(execution, "clock", lambda: (clock[0], 1_300_000_000))
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(runtime.protocol, "current_metadata", lambda: {})
    monkeypatch.setattr(runtime, "_settle", lambda *_args, **_kw: False)
    monkeypatch.setattr(runtime, "evaluate", lambda *_args: None)
    monkeypatch.setattr(runtime.breakout, "trail", lambda *_args: None)
    monkeypatch.setattr(runtime.breakout, "entry_terms", lambda b,l,p,m,c,price,at: _terms(b,l,p,price,at))
    previous = NOW-pd.Timedelta(seconds=30)
    with closing(runtime.connect()) as db, db:
        incidents.append(db, {"book_id": lanes[MARK_BOOK]["book_id"]}, "PROTECTION_POLICY_ACTIVATED", record["id"], previous,
                         {"generation": record["id"]}, identity="source-isolation-policy")
        for slot in (MARK_BOOK, MARK_PENDING_2, "orbital/mark"):
            lane = lanes[slot]
            book = paper.load(db, lane)
            book["pending"] = _idea(book, 63000.)
            execution.entry(db, book, lane, observation(previous, event="opened-"+slot, price=64800.), _terms,
                            read_clock=lambda: (previous, 1_300_000_000))
            assert book["position"]
            paper.save(db, book)
        pending = paper.load(db, lanes[MARK_PENDING])
        pending["pending"] = _idea(pending, 63000.)
        paper.save(db, pending)
    return record, lanes, clock


@pytest.mark.parametrize("cause", ["LOCAL_RUNTIME_PERSISTENCE", "LOCAL_RESEARCH_STORAGE"])
def test_local_persistence_failure_does_not_interrupt_actual_shared_protection_or_allow_sibling_entry(
        runtime_setup, metadata, monkeypatch, cause):
    metadata.update(worker_state(cause=cause))
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)
    calls = []
    monkeypatch.setattr(runtime, "_observe", lambda source,previous:
                        calls.append((source, clock[0])) or observation(clock[0], event=clock[0].isoformat()))
    for delay in (0, 16, 32):
        clock[0] = NOW+pd.Timedelta(seconds=delay)
        runtime.cycle()
    with closing(runtime.connect(readonly=True)) as db:
        assert len(calls) == 3
        for slot in (MARK_BOOK, MARK_PENDING_2, "orbital/mark"):
            book = paper.load(db, lanes[slot])
            assert book["position"]["mark_event_id"] == clock[0].isoformat()
            assert not book["position"].get("unresolved_protection")
            assert not incidents.project(db, book["book_id"])
        pending = paper.load(db, lanes[MARK_PENDING])
        assert pending["position"] is None and pending["pending"] is not None
        assert not db.execute("SELECT 1 FROM event WHERE book=? AND kind='HYPOTHETICAL_FILL'", (pending["book_id"],)).fetchone()
        assert any(json.loads(raw)["reason"] == "SHARED_SOURCE_BACKOFF"
                   for raw, in db.execute("SELECT body FROM event WHERE book=? AND kind='ENTRY_EVIDENCE_REJECTED'", (pending["book_id"],)))


def test_denied_status_publication_does_not_erase_financially_accepted_mark(runtime_setup, metadata, monkeypatch):
    metadata.update(worker_state())
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)

    def observe(*_):
        with writer_lock(gate.ROOT / ".writer.lock"):
            return observation(clock[0], event="acquired-with-separate-writer-lease")

    def deny(*_):
        raise PermissionError("isolated status storage denial")

    monkeypatch.setattr(runtime, "_observe", observe)
    monkeypatch.setattr(runtime.runtime_snapshot, "publish", deny)
    result = runtime.cycle()
    assert result["status_publication"]["state"] == "FAILED"
    assert runtime.read_status_snapshot("last-cycle.json", None) is None
    with closing(runtime.connect(readonly=True)) as db:
        position = paper.load(db, lanes[MARK_BOOK])["position"]
        assert position["mark_event_id"] == "acquired-with-separate-writer-lease"


def test_blocked_managed_status_publication_does_not_delay_later_protection(runtime_setup, metadata, monkeypatch):
    metadata.update(worker_state())
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)
    entered, release, finished = Event(), Event(), Event()
    actual = runtime.runtime_snapshot.publish

    def wait(path, document):
        entered.set()
        try:
            assert release.wait(5)
            actual(path, document)
        finally:
            finished.set()

    monkeypatch.setattr(runtime, "_observe", lambda *_: observation(clock[0], event=clock[0].isoformat()))
    monkeypatch.setattr(runtime.runtime_snapshot, "publish", wait)
    with runtime.status_publications():
        try:
            assert runtime.cycle()["status_publication"]["state"] == "QUEUED"
            assert entered.wait(5)
            clock[0] += pd.Timedelta(seconds=16)
            assert runtime.cycle()["status_publication"]["state"] == "QUEUED"
            with closing(runtime.connect(readonly=True)) as db:
                assert paper.load(db, lanes[MARK_BOOK])["position"]["mark_event_id"] == clock[0].isoformat()
        finally:
            release.set()
            assert finished.wait(5)


def test_blocked_trail_research_keeps_protection_and_does_not_backdate_stop(runtime_setup, metadata, monkeypatch):
    metadata.update(worker_state())
    runtime_setup[2]["lanes"][MARK_PENDING_2]["exit"] = "atr_trail_v1"
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)
    lane = lanes[MARK_PENDING_2]
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        book["position"]["plan"]["trailing_atr"] = 2
        pos_id = book["position"]["id"]
        paper.save(db, book)

    entered, release = Event(), Event()

    def wait_for_candles(*_):
        entered.set()
        assert release.wait(5)
        return ({"close_at": (NOW - pd.Timedelta(seconds=5)).isoformat(),
                 "available_at": (NOW - pd.Timedelta(seconds=1)).isoformat(),
                 "close": 64000., "atr": 10.},)

    monkeypatch.setattr(runtime.breakout, "trail_inputs", wait_for_candles)
    monkeypatch.setattr(runtime, "_observe", lambda *_: observation(clock[0], event=clock[0].isoformat()))
    with runtime._trail_preparer() as preparer:
        try:
            assert runtime.cycle(trail_researcher=preparer)["trail_research"]["unready"] == 1
            assert entered.wait(5)
            clock[0] += pd.Timedelta(seconds=16)
            runtime.cycle(trail_researcher=preparer)
            with closing(runtime.connect(readonly=True)) as db:
                pos = paper.load(db, lane)["position"]
                assert pos["id"] == pos_id and pos["mark_event_id"] == clock[0].isoformat()
                assert not db.execute("SELECT 1 FROM event WHERE book=? AND kind='TRAIL_UPDATED'", (book["book_id"],)).fetchone()
        finally:
            release.set()
        with preparer.condition:
            assert preparer.condition.wait_for(lambda: preparer.results, 5)
        clock[0] += pd.Timedelta(seconds=1)
        runtime.cycle(trail_researcher=preparer)
        with closing(runtime.connect(readonly=True)) as db:
            pos = paper.load(db, lane)["position"]
            assert pos["stop_effective_at"] == clock[0].isoformat()
            assert pos["mark_event_id"] != clock[0].isoformat()


def test_price_result_is_invalidated_when_shared_admission_changes(runtime_setup, metadata, monkeypatch):
    metadata.update(worker_state())
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)

    def observe(*_):
        metadata.update(worker_state(cause="RATE_LIMITED", scope="SHARED_KUCOIN_SOURCE"))
        return observation(clock[0], event="late-shared-restriction")

    monkeypatch.setattr(runtime, "_observe", observe)
    runtime.cycle()
    with closing(runtime.connect(readonly=True)) as db:
        position = paper.load(db, lanes[MARK_BOOK])["position"]
        assert position.get("mark_event_id") != "late-shared-restriction"
        assert not db.execute("SELECT 1 FROM observation WHERE event_id='late-shared-restriction'").fetchone()


def test_late_market_result_cannot_reuse_a_superseded_source_reservation(runtime_setup, metadata, monkeypatch):
    metadata.update(worker_state())
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)

    def observe(*_):
        with writer_lock(gate.ROOT / ".writer.lock"), closing(runtime.connect()) as db, db:
            lane = lanes[MARK_BOOK]
            state = runtime._lane_source_state(db, lane)
            state["last_request"] = (clock[0] + sources.INTERVAL).isoformat()
            runtime._save_lane_source(db, lane, state)
        return observation(clock[0], event="superseded-market-result")

    monkeypatch.setattr(runtime, "_observe", observe)
    runtime.cycle()
    with closing(runtime.connect(readonly=True)) as db:
        assert not db.execute("SELECT 1 FROM observation WHERE event_id='superseded-market-result'").fetchone()
        assert paper.load(db, lanes[MARK_BOOK])["position"].get("mark_event_id") != "superseded-market-result"


def test_lease_elapsed_breach_is_unhealthy_and_preserves_accepted_progress(runtime_setup, metadata, monkeypatch):
    metadata.update(worker_state())
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)
    actual = runtime.membership.validate_accounts

    def slow_check(*args):
        sleep(1.05)
        return actual(*args)

    monkeypatch.setattr(runtime.membership, "validate_accounts", slow_check)
    monkeypatch.setattr(runtime, "_observe", lambda *_: observation(clock[0], event="after-slow-lease"))
    result = runtime.cycle()
    assert result["state"] == "DEGRADED"
    assert result["writer_lease"]["max_measured_ns"] > 1_000_000_000
    assert result["writer_lease"]["over_one_second"] is True
    assert runtime.read_status_snapshot("last-cycle.json", {})["state"] == "DEGRADED"
    with closing(runtime.connect(readonly=True)) as db:
        assert paper.load(db, lanes[MARK_BOOK])["position"]["mark_event_id"] == "after-slow-lease"


def test_repeated_healthy_research_http_overlap_cannot_alias_the_protection_sampling_loop(
        runtime_setup, metadata, monkeypatch):
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)
    verify_live_owner(monkeypatch)
    calls = []
    monkeypatch.setattr(runtime, "_observe", lambda source,previous:
                        calls.append((source, clock[0])) or observation(clock[0], event=clock[0].isoformat()))
    # Every paper cycle deliberately sees a currently active research request.
    # Overlap lasts beyond45s overall, but no individual request loses coverage.
    for delay in range(0, 16*20, 16):
        clock[0] = NOW+pd.Timedelta(seconds=delay)
        healthy_request(metadata, clock[0])
        runtime.cycle()
    with closing(runtime.connect(readonly=True)) as db:
        assert len(calls) == 20
        for slot in (MARK_BOOK, MARK_PENDING, MARK_PENDING_2, "orbital/mark"):
            book = paper.load(db, lanes[slot])
            assert book["position"]["mark_event_id"] == clock[0].isoformat()
            assert not book["position"].get("unresolved_protection")
            assert not incidents.project(db, book["book_id"])
        assert paper.load(db, lanes[MARK_PENDING])["pending"] is None


def test_advisory_health_never_adds_repeated_fence_wait_to_unrelated_protection(runtime_setup, metadata, monkeypatch):
    _, lanes = initialize(runtime_setup)
    waits = []
    @contextmanager
    def outcome_pending(*, wait_seconds):
        waits.append(wait_seconds)
        raise runtime.monitoring.SourceOutcomePending("source result publication unavailable")
        yield
    monkeypatch.setattr(runtime.monitoring, "source_outcome_lock", outcome_pending)
    with closing(runtime.connect()) as db:
        for lane in lanes.values():
            if lane["source"] in sources.KUCOIN:
                health = runtime._source_health(db, lane, NOW)
                assert health["reason_code"] == "SOURCE_OUTCOME_PUBLICATION_PENDING"
    assert waits and set(waits) == {0}


def test_real_shared_source_failure_still_records_incidents_for_every_exposed_dependent_consumer(
        runtime_setup, metadata, monkeypatch):
    _, lanes, clock = prepare_cycle(runtime_setup, monkeypatch)
    metadata.update(worker_state(scope="SHARED_KUCOIN_SOURCE", cause="RATE_LIMITED"))
    clock[0] = NOW+pd.Timedelta(seconds=16)
    monkeypatch.setattr(runtime, "_observe", lambda *_args: pytest.fail("Genuine shared circuit breaker must not poll"))
    runtime.cycle()
    with closing(runtime.connect(readonly=True)) as db:
        correlated = []
        for slot in (MARK_BOOK, MARK_PENDING_2, "orbital/mark"):
            book = paper.load(db, lanes[slot])
            assert book["position"]["unresolved_protection"]
            incident, = incidents.project(db, book["book_id"])
            correlated.append(incident["source_correlation_id"])
            assert incident["source_scope_id"]
        assert len(set(correlated)) == 1
        assert not incidents.project(db, lanes["ema/day"]["book_id"])
        assert db.execute("SELECT count(*) FROM event WHERE kind=?", (operations.OPENED,)).fetchone()[0] >= 2
    # Genuine current recovery is accepted, but does not erase historical loss.
    metadata.clear()
    metadata.update(active_request=None, retry_at=None)
    clock[0] += pd.Timedelta(seconds=16)
    monkeypatch.setattr(runtime, "_observe", lambda *_args: observation(clock[0], event="recovered"))
    runtime.cycle()
    with closing(runtime.connect(readonly=True)) as db:
        for slot in (MARK_BOOK, MARK_PENDING_2, "orbital/mark"):
            book = paper.load(db, lanes[slot])
            assert book["position"]["mark_event_id"] == "recovered"
            assert book["position"]["unresolved_protection"]
            incident, = incidents.project(db, book["book_id"])
            assert incident["historical_coverage"] == "UNRESOLVED" and incident.get("outcome") is None


@pytest.mark.parametrize("guard,expected", [("clock", "WORKSTATION_CLOCK_SYNC_UNVERIFIED"),
                                           ("budget", "SOURCE_REQUEST_OR_DISK_BUDGET"),
                                           ("restricted", "SOURCE_ACCESS_RESTRICTED")])
def test_independent_paper_safety_gates_remain_effective_during_local_worker_failure(
        runtime_setup, metadata, monkeypatch, guard, expected):
    _, lanes = initialize(runtime_setup)
    lane = lanes[MARK_BOOK]
    monkeypatch.setattr(runtime, "synchronized", lambda _: guard != "clock")
    monkeypatch.setattr(runtime, "_observe", lambda *_args: pytest.fail("Guard must prevent observation request"))
    with closing(runtime.connect()) as db, db:
        if guard == "budget":
            runtime._save_lane_source(db, lane, {"utc_day": str(NOW.date()), "requests": runtime.REQUEST_LIMIT})
        if guard == "restricted":
            runtime._save_lane_source(db, lane, {"restricted": True, "reason": expected})
        assert runtime.sample(db, sources.MARK, lane, NOW, open_positions=True)["status"] == expected


def test_local_exception_does_not_change_two_second_freshness_or_restart_replay_rules(runtime_setup, metadata, monkeypatch):
    _, lanes = initialize(runtime_setup)
    lane = lanes[MARK_BOOK]
    now = [NOW]
    point = [{**observation(NOW, event="stale"), "source_at": (NOW-pd.Timedelta(seconds=3)).isoformat()}]
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(execution, "clock", lambda: (now[0], 1_300_000_000))
    monkeypatch.setattr(runtime, "_observe", lambda *_args: point[0])
    with closing(runtime.connect()) as db, db:
        assert runtime.sample(db, sources.MARK, lane, now[0], open_positions=True)["status"] == "STALE_REFERENCE"
        assert not db.execute("SELECT 1 FROM observation").fetchone()
    now[0] += pd.Timedelta(seconds=16)
    point[0] = observation(now[0], event="genuine")
    with closing(runtime.connect()) as db, db:
        assert runtime.sample(db, sources.MARK, lane, now[0], open_positions=True) == point[0]
    now[0] += pd.Timedelta(seconds=16)
    point[0] = observation(now[0], event="genuine")
    with closing(runtime.connect()) as db, db:
        assert runtime.sample(db, sources.MARK, lane, now[0], open_positions=True)["status"] == "DUPLICATE_OR_REORDERED_REFERENCE"
        assert db.execute("SELECT count(*) FROM observation").fetchone()[0] == 1


def test_paper_source_specific_breaker_does_not_expand_into_unrelated_kucoin_source(runtime_setup, metadata, monkeypatch):
    _, lanes = initialize(runtime_setup)
    mark, spot = lanes[MARK_BOOK], lanes["breakout/position/fixed"]
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(execution, "clock", lambda: (NOW, 1_300_000_000))
    calls = []
    point = {**observation(NOW, source=sources.SPOT), "symbol": "BTC-USDT"}
    monkeypatch.setattr(runtime, "_observe", lambda source,previous: calls.append(source) or point)
    with closing(runtime.connect()) as db, db:
        runtime._save_lane_source(db, mark, {"reason": "RATE_LIMITED", "retry_at": (NOW+pd.Timedelta(minutes=1)).isoformat()})
        assert runtime.sample(db, sources.MARK, mark, NOW, open_positions=True)["status"] == "RATE_LIMITED"
        assert runtime.sample(db, sources.SPOT, spot, NOW, open_positions=True) == point
        assert calls == [sources.SPOT]
