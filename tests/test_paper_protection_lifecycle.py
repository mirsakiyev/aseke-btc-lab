from contextlib import closing
from copy import deepcopy
from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.paper_run import protection_commands as commands, protection_incidents as incidents
from lab.paper_run import runtime, source_amendment
from lab.phase15 import paper
from test_paper_run_live_amendment import (
    APPROVED, DAY, QUALIFICATION, TICK, generation, history, intent, live_point, saved_book,
)


@pytest.fixture
def lifecycle(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    publish = paper.publish

    def with_stream(db, book, analysis, at):
        analysis = deepcopy(analysis)
        analysis["plan"]["evidence"] = {"source_id": "test-stream"}
        return publish(db, book, analysis, at)

    monkeypatch.setattr(paper, "publish", with_stream)
    monkeypatch.setattr(commands, "utc_now", lambda: now[0])
    with closing(runtime.connect()) as db, db:
        incidents.append(db, paper.load(db, lane), "PROTECTION_POLICY_ACTIVATED", record["id"], APPROVED,
                         {"generation": record["id"], "operator": "isolated-test"}, identity="test-approval")
    intent(lane, amendment, side=-1)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    return record, now, amendment, lane


def interruption(context, *, recover=False, close=False):
    record, now, amendment, lane = context
    now[0] = TICK + pd.Timedelta(milliseconds=350)
    runtime.live_prices(record, amendment, [], {"reason": "ConnectionClosedError"}, detect_gap=True)
    if recover or close:
        observed = TICK + pd.Timedelta(seconds=2)
        now[0] = observed + pd.Timedelta(milliseconds=125)
        runtime.live_prices(record, amendment, [live_point(92, at=observed, epoch=2,
                                                          price="105.1" if close else "100.")], {})


def records(lane):
    with closing(runtime.connect(readonly=True)) as db:
        return incidents.project(db, lane["book_id"])


def request(record, lane, action, **extra):
    report = commands.report(record, DAY)
    return {"generation": record["id"], "slot": DAY, "request_id": action,
            "reason": "Isolated operator review", "review_token": report["review_token"], **extra}


def insufficient_note(tmp_path, record, position_id, now):
    artifact = tmp_path / "review.json"
    artifact.write_text(json.dumps({"generation": record["id"], "book_id": record["body"]["lanes"][DAY]["book_id"],
                                    "position_id": position_id, "recorded_at": now.isoformat(),
                                    "limitations": "Historical continuity could not be established"}))
    return {"outcome": "INSUFFICIENT_EVIDENCE", "path": str(artifact), "sha256": sha256(artifact.read_bytes()).hexdigest(),
            "provenance": {"operator": "test-reviewer", "origin": "EXPLICIT_UNRESOLVED_REVIEW",
                           "acquired_at": now.isoformat(), "source": "KRAKEN_BTC_USD_LAST_TRADE",
                           "symbol": "BTC/USD", "trigger_basis": "KRAKEN_BTC_USD_LAST_TRADE", "stream_id": "test-stream"}}


def adjudicate(context, tmp_path):
    record, now, _, lane = context
    incident, = records(lane)
    manifest = insufficient_note(tmp_path, record, incident["position_id"], now[0])
    command = request(record, lane, "adjudication-1", incident_id=incident["incident_id"], evidence=manifest)
    return commands.execute("adjudicate", command, apply=True, authorized=True), command


def test_short_disconnect_and_duplicate_detection_share_one_persistent_incident(lifecycle):
    record, now, amendment, lane = lifecycle
    interruption(lifecycle)
    runtime.live_prices(record, amendment, [], {"reason": "ConnectionClosedError"}, detect_gap=True)
    incident, = records(lane)
    assert incident["historical_coverage"] == "UNRESOLVED"
    assert incident["current_feed_health"] == "UNAVAILABLE"
    assert incident["first_accepted_reference"] is None
    assert saved_book(lane)["position"]["protection_gap"]["incident_id"] == incident["incident_id"]

    # An acknowledgement without a valid new protector observation proves no recovery.
    witness = {"id": "ack-only", "kind": "SUBSCRIPTION_ACKNOWLEDGED", "at": now[0].isoformat()}
    assert runtime.live_prices(record, amendment, [], {}, witnesses=[witness]) == ["ack-only"]
    assert records(lane)[0]["current_feed_health"] == "UNAVAILABLE"
    at = TICK + pd.Timedelta(seconds=2)
    now[0] = at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(92, at=at, epoch=2)], {})
    recovered, = records(lane)
    assert recovered["current_feed_health"] == "RECOVERED"
    assert recovered["first_accepted_reference"]["event_id"] == "92"
    assert recovered["historical_coverage"] == "UNRESOLVED"
    assert saved_book(lane)["position"]["unresolved_protection"]


def test_prolonged_missing_reference_is_unified_with_the_disconnect_path(lifecycle):
    record, now, amendment, lane = lifecycle
    now[0] = TICK + pd.Timedelta(seconds=46)
    runtime.live_prices(record, amendment, [], {})
    first, = records(lane)
    runtime.live_prices(record, amendment, [], {}, detect_gap=True)
    assert records(lane)[0]["incident_id"] == first["incident_id"]
    assert len(records(lane)) == 1
    at = TICK + pd.Timedelta(seconds=47)
    now[0] = at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(92, at=at, epoch=2)], {})
    assert records(lane)[0]["current_feed_health"] == "RECOVERED"


def test_incident_recording_failure_rolls_back_state_and_retries_once(lifecycle, monkeypatch):
    _, _, _, lane = lifecycle
    original = incidents.append

    def fail_after_insert(db, book, kind, position_id, at, body, **kwargs):
        result = original(db, book, kind, position_id, at, body, **kwargs)
        if kind == "PROTECTION_INCIDENT_OPENED":
            raise RuntimeError("Simulated crash after INSERT")
        return result

    monkeypatch.setattr(incidents, "append", fail_after_insert)
    with pytest.raises(RuntimeError, match="Simulated crash"):
        interruption(lifecycle)
    assert not records(lane)
    assert not saved_book(lane)["position"].get("unresolved_protection")
    monkeypatch.setattr(incidents, "append", original)
    interruption(lifecycle)
    assert len(records(lane)) == 1


def test_adjudication_is_explicit_idempotent_and_does_not_clear_open_history(lifecycle, tmp_path):
    record, _, _, lane = lifecycle
    interruption(lifecycle, recover=True)
    incident, = records(lane)
    bad = request(record, lane, "unapproved", incident_id=incident["incident_id"], evidence={})
    with pytest.raises(PermissionError, match="Explicit operator"):
        commands.execute("adjudicate", bad, apply=True)
    assert records(lane)[0].get("outcome") is None
    result, command = adjudicate(lifecycle, tmp_path)
    assert result["state"] == "APPLIED"
    assert commands.execute("adjudicate", command, apply=True, authorized=True)["state"] == "ALREADY_APPLIED"
    with pytest.raises(ValueError, match="reused"):
        commands.execute("resume", command, apply=True, authorized=True)
    assert records(lane)[0]["outcome"] == "INSUFFICIENT_EVIDENCE"
    assert saved_book(lane)["position"]["unresolved_protection"]
    resume = request(record, lane, "resume-open")
    with pytest.raises(ValueError, match="flat"):
        commands.execute("resume", resume, apply=True, authorized=True)


def test_review_token_blocks_a_concurrent_exit_and_resume_preserves_economics(lifecycle, tmp_path, monkeypatch):
    record, now, amendment, lane = lifecycle
    interruption(lifecycle, recover=True)
    adjudicate(lifecycle, tmp_path)
    stale = request(record, lane, "stale-resume")
    at = TICK + pd.Timedelta(seconds=4)
    now[0] = at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(93, price="105.1", at=at, epoch=2)], {})
    assert history(lane, "CLOSED")[0]["provisional"]
    with pytest.raises(ValueError, match="changed"):
        commands.execute("resume", stale, apply=True, authorized=True)
    before = saved_book(lane)
    monkeypatch.setattr(commands, "_healthy", lambda *_: {"clock_and_reference_checks": "TEST_QUALIFIED"})
    command = request(record, lane, "resume-flat")
    with pytest.raises(PermissionError):
        commands.execute("resume", command, apply=True)
    result = commands.execute("resume", command, apply=True, authorized=True)
    after = saved_book(lane)
    for key in ("cash", "equity", "fees", "gross", "funding", "problem"):
        assert after[key] == before[key]
    assert after["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert result["record"]["capital_dependency"]["incident_ids"]
    assert not incidents.admission_allowed(after, now[0])
    assert incidents.admission_allowed(after, pd.Timestamp(after["prospective_admission"]["effective_at"]))
    assert commands.execute("resume", command, apply=True, authorized=True)["state"] == "ALREADY_APPLIED"
    with closing(runtime.connect(readonly=True)) as db:
        reloaded = runtime._load_book(db, lane)
    assert reloaded["prospective_admission"] == after["prospective_admission"]


def test_adjudication_rejects_shortened_interval(lifecycle, tmp_path):
    record, now, _, lane = lifecycle
    interruption(lifecycle, recover=True)
    incident, = records(lane)
    manifest = insufficient_note(tmp_path, record, incident["position_id"], now[0])
    command = request(record, lane, "short-review", incident_id=incident["incident_id"], evidence=manifest,
                      review_interval_end=(TICK + pd.Timedelta(milliseconds=400)).isoformat())
    with pytest.raises(ValueError, match="excludes"):
        commands.execute("adjudicate", command)


def test_legacy_import_preserves_unknown_first_recovery_and_prior_events(lifecycle, tmp_path):
    record, now, amendment, lane = lifecycle
    at = TICK + pd.Timedelta(milliseconds=300)
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        paper.event(db, book, "PROTECTION_GAP", book["position"]["id"], at, at,
                    {"reason": "KRAKEN_STREAM_DISCONTINUITY", "last_reference": book["position"]["mark_received_at"]})
        book["position"]["unresolved_protection"] = True
        paper.save(db, book)
    now[0] = TICK + pd.Timedelta(seconds=2, milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(92, price="105.1", at=TICK + pd.Timedelta(seconds=2))], {})
    with closing(runtime.connect(readonly=True)) as db:
        original = db.execute("SELECT key,body FROM event WHERE kind='PROTECTION_GAP'").fetchone()
    review = commands.report(record, DAY)
    incident, = review["incidents"]
    assert incident["first_accepted_reference"] is None
    manifest = insufficient_note(tmp_path, record, incident["position_id"], now[0])
    command = request(record, lane, "legacy-review", incident_id=incident["incident_id"], evidence=manifest)
    commands.execute("adjudicate", command, apply=True, authorized=True)
    imported, = records(lane)
    assert imported["first_accepted_reference"] is None
    assert imported["current_feed_health"] == "UNKNOWN_HISTORICAL"
    assert imported["interval"]["ends_at"] is None
    with closing(runtime.connect(readonly=True)) as db:
        assert db.execute("SELECT key,body FROM event WHERE kind='PROTECTION_GAP'").fetchone() == original


def test_protection_command_rejects_generation_changed_before_writer_lease(lifecycle, monkeypatch):
    record, _, _, lane = lifecycle
    command = request(record, lane, "activation-racing-reset")
    generations = iter((record, {**record, "id": "a" * 64}))
    monkeypatch.setattr(runtime, "current", lambda: next(generations))
    monkeypatch.setattr(runtime, "connect", lambda: pytest.fail("Obsolete command opened the financial DB"))

    with pytest.raises(ValueError, match="generation changed"):
        commands.execute("activate", command, apply=True, authorized=True)


def test_activation_and_resume_fail_closed_on_exposure_or_unqualified_health(lifecycle, monkeypatch):
    record, now, _, lane = lifecycle
    command = request(record, lane, "activate-open")
    with pytest.raises(PermissionError):
        commands.execute("activate", command, apply=True)
    with closing(runtime.connect()) as db:
        book = paper.load(db, lane)
        with pytest.raises(ValueError, match="already active"):
            commands._activate(db, record, command, book, now[0], "test")
    monkeypatch.setattr(runtime, "synchronized", lambda _: False)
    with pytest.raises(ValueError, match="clock"):
        commands._healthy(record, lane)
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(runtime, "read_status_snapshot", lambda name, _: {"state": "BACKOFF"})
    with pytest.raises(ValueError, match="not streaming"):
        commands._healthy(record, lane)


def test_nan_reconciliation_and_unreviewed_incidents_prevent_resume(lifecycle):
    record, now, _, lane = lifecycle
    interruption(lifecycle, close=True)
    with closing(runtime.connect(readonly=True)) as db:
        book = paper.load(db, lane)
        book["cash"] = float("nan")
        with pytest.raises(ValueError, match="invalid amounts"):
            commands._resume(db, record, lane, book, records(lane), now[0])
    command = request(record, lane, "unreviewed-resume")
    with pytest.raises(ValueError, match="disposition"):
        commands.execute("resume", command, apply=True, authorized=True)


def test_resumed_book_publishes_only_a_future_signal_and_records_capital_dependency(lifecycle, tmp_path, monkeypatch):
    from lab.paper_run import ema

    record, now, amendment, lane = lifecycle
    interruption(lifecycle, close=True)
    adjudicate(lifecycle, tmp_path)
    monkeypatch.setattr(commands, "_healthy", lambda *_: {"clock_and_reference_checks": "TEST_QUALIFIED"})
    commands.execute("resume", request(record, lane, "prospective-only"), apply=True, authorized=True)
    book = saved_book(lane)
    boundary = pd.Timestamp(book["prospective_admission"]["effective_at"])
    original = history(lane, "PUBLISHED")[0]
    plan = deepcopy(original["plan"])
    plan.update(eligible_from=boundary + pd.Timedelta(minutes=1), entry_expiry=boundary + pd.Timedelta(minutes=3))
    analysis = {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": boundary, "direction": "SHORT",
                "strategy_version": "prospective-test", "exit_policy": "signal_atr_bracket_v1", "plan": plan}
    monkeypatch.setattr(ema, "confirmation", lambda *_args, **_kwargs: {"state": "EVALUATED", "analysis": analysis})
    now[0] = boundary - pd.Timedelta(seconds=1)
    with closing(runtime.connect()) as db, db:
        book = runtime._load_book(db, lane)
        runtime.evaluate(db, record, {DAY: book}, {DAY: lane}, now[0], {})
        assert book["pending"] is None
        now[0] = boundary + pd.Timedelta(seconds=1)
        runtime.evaluate(db, record, {DAY: book}, {DAY: lane}, now[0], {})
        assert book["pending"]
        assert book["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    point_at = boundary + pd.Timedelta(minutes=1, milliseconds=50)
    now[0] = point_at + pd.Timedelta(milliseconds=125)
    runtime.live_prices(record, amendment, [live_point(93, at=point_at, epoch=2)], {})
    position = saved_book(lane)["position"]
    assert position and position["research_provenance"]["incident_ids"]
    assert history(lane, "HYPOTHETICAL_FILL")[-1]["research_provenance"] == position["research_provenance"]


def test_resumed_book_reaches_real_ema_confirmation_without_clearing_history(lifecycle, tmp_path, monkeypatch):
    from lab.paper_run import ema, protection_reporting

    record, now, _, lane = lifecycle
    interruption(lifecycle, close=True)
    adjudicate(lifecycle, tmp_path)
    monkeypatch.setattr(commands, "_healthy", lambda *_: {"clock_and_reference_checks": "TEST_QUALIFIED"})
    commands.execute("resume", request(record, lane, "real-confirmation"), apply=True, authorized=True)
    before = saved_book(lane)
    boundary = pd.Timestamp(before["prospective_admission"]["effective_at"])
    incidents_before = records(lane)
    lane = {**lane, "execution_clock": {**lane["execution_clock"],
            "decision_seconds": int(pd.Timedelta(lane["timeframe"]).total_seconds()), "protection_seconds": 60},
            "spec": {**lane["spec"], "execution": {**lane["spec"]["execution"], "processing_seconds": 30}},
            "feature_scope_end": lane["publication_end_exclusive"]}
    calls = []

    def chart(*_args):
        calls.append(True)
        return (pd.DataFrame(), boundary, "isolated-chart", pd.Timedelta(seconds=0)), None

    # Keep the actual confirmation function, especially its account-state guard.
    monkeypatch.setattr(ema, "_feature_history", chart)
    monkeypatch.setattr(ema, "proposal", lambda *_args, **_kwargs: {"setup": None, "decision": "NO_TRADE"})
    now[0] = boundary - pd.Timedelta(seconds=1)
    with closing(runtime.connect()) as db, db:
        book = runtime._load_book(db, lane)
        previous = deepcopy(book.get("last_evaluation"))
        runtime.evaluate(db, record, {DAY: book}, {DAY: lane}, now[0], {})
        assert not calls and book.get("last_evaluation") == previous
        now[0] = boundary + pd.Timedelta(seconds=1)
        runtime.evaluate(db, record, {DAY: book}, {DAY: lane}, now[0], {})
        assert calls == [True]
        assert book["last_evaluation"]["state"] == "EVALUATED"
        assert book["last_evaluation"]["signal"]["decision"] == "NO_TRADE"
        assert book["last_evaluation"]["boundary"] == boundary.isoformat()
    after = saved_book(lane)
    for key in ("cash", "equity", "fees", "funding", "gross", "problem", "prospective_admission"):
        assert after[key] == before[key]
    assert after["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert records(lane) == incidents_before
    assert after["position"] is None and after["pending"] is None
    with closing(runtime.connect(readonly=True)) as db:
        research = protection_reporting.summarize(db, lane, after)
    assert research["research_state"] == "QUARANTINED"
    assert research["excluded_closed_count"] == 1


@pytest.mark.parametrize("revocation", ["tampered_authorization", "new_incident"])
def test_revoked_resume_never_reaches_ema_chart_assessment(lifecycle, tmp_path, monkeypatch, revocation):
    from lab.paper_run import ema

    record, now, _, lane = lifecycle
    interruption(lifecycle, close=True)
    adjudicate(lifecycle, tmp_path)
    monkeypatch.setattr(commands, "_healthy", lambda *_: {"clock_and_reference_checks": "TEST_QUALIFIED"})
    commands.execute("resume", request(record, lane, "revoked-confirmation"), apply=True, authorized=True)
    book = saved_book(lane)
    boundary = pd.Timestamp(book["prospective_admission"]["effective_at"])
    now[0] = boundary + pd.Timedelta(seconds=1)
    with closing(runtime.connect()) as db, db:
        if revocation == "tampered_authorization":
            book["prospective_admission"]["event_id"] = "missing-immutable-authorization"
        else:
            # Isolate admission revocation from occupancy: the normal incident
            # recorder removes the permit; a subsequently flat book stays blocked.
            book["position"] = deepcopy(records(lane)[0]["frozen_position"])
            book["position"]["id"] = "later-incident-position"
            incidents.open_incident(db, book, now[0], "NEW_REFERENCE_INTERRUPTION", witness_id="new-incident")
            book["position"] = None
        paper.save(db, book)
    monkeypatch.setattr(ema, "_feature_history", lambda *_: pytest.fail("Revoked admission reached chart assessment"))
    with closing(runtime.connect()) as db, db:
        book = runtime._load_book(db, lane)
        assert not book.get("prospective_admission")
        before = deepcopy(book.get("last_evaluation"))
        runtime.evaluate(db, record, {DAY: book}, {DAY: lane}, now[0], {})
        assert book.get("last_evaluation") == before
        assert book["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"


def test_policy_activation_is_reviewed_explicit_and_idempotent(generation, monkeypatch):
    record, now = generation
    source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = record["body"]["lanes"][DAY]
    monkeypatch.setattr(commands, "utc_now", lambda: now[0])
    monkeypatch.setattr(runtime, "status", lambda: {"last_cycle": {
        "generation": record["id"], "runner_build_id": runtime.RUNNER_BUILD, "at": now[0].isoformat(),
    }})
    command = request(record, lane, "activate-policy")
    assert commands.execute("activate", command)["state"] == "PREVIEW"
    with closing(runtime.connect(readonly=True)) as db:
        assert incidents.active(db) is None
    with pytest.raises(PermissionError):
        commands.execute("activate", command, apply=True)
    assert commands.execute("activate", command, apply=True, authorized=True)["state"] == "APPLIED"
    assert commands.execute("activate", command, apply=True, authorized=True)["state"] == "ALREADY_APPLIED"
    with closing(runtime.connect(readonly=True)) as db:
        assert incidents.active(db)["generation"] == record["id"]


def test_incident_journal_rejects_updates_and_deletes(lifecycle):
    interruption(lifecycle)
    with closing(runtime.connect()) as db:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute("UPDATE event SET body='{}' WHERE kind='PROTECTION_INCIDENT_OPENED'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            db.execute("DELETE FROM event WHERE kind='PROTECTION_INCIDENT_OPENED'")
