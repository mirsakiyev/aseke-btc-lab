from contextlib import closing
from copy import deepcopy
from hashlib import sha256
import json
import sqlite3

import pandas as pd
import pytest

from lab.paper_run import capabilities as caps, membership, recovery
from lab.paper_run import protection_commands as commands, protection_evidence as evidence, protection_incidents as incidents
from lab.phase15 import paper
from test_paper_capabilities_membership import AT, GENERATION, apply, eighth, registered, seven
from test_paper_protection_evidence import BASE, chart, save_chart, save_document
from test_paper_protection_lifecycle import generation, lifecycle, live_point


@pytest.mark.parametrize("slot", list(seven()))
def test_every_registered_source_can_record_honest_insufficiency(chart, slot):
    manifest, context, document = chart
    lane = seven()[slot]
    capability = caps.contract(lane, generation=GENERATION, slot=slot)
    source = capability["source"]
    context.update(source=source["id"], symbol=source["instrument"], trigger_basis=source["id"],
                   stream_id=source["version"], source_identity=source,
                   evidence_adapter=capability["adapters"]["evidence"])
    manifest.update(outcome="INSUFFICIENT_EVIDENCE")
    manifest["provenance"].update(source=source["id"], symbol=source["instrument"], trigger_basis=source["id"],
                                  stream_id=source["version"], source_identity=source)
    document = {key: document[key] for key in ("generation", "book_id", "position_id", "recorded_at")}
    document.update(schema="OPERATOR_EVIDENCE_NOTE_V1", reason="Historical coverage cannot be established")
    save_document(manifest, document)
    result = evidence.validate_evidence(manifest, context)
    assert result["outcome"] == "INSUFFICIENT_EVIDENCE"
    assert result["historical_coverage"] == result["exit_impact"] == "UNRESOLVED"
    assert result["evidence"][0]["source_identity"] == source


@pytest.mark.parametrize("slot", ["breakout/day/fixed", "breakout/position/fixed"])
def test_kraken_trade_candles_cannot_adjudicate_kucoin_references(chart, slot):
    manifest, context, document = chart
    save_chart(*chart)
    capability = caps.contract(seven()[slot])
    source = capability["source"]
    context.update(source=source["id"], symbol=source["instrument"], trigger_basis=source["id"],
                   source_identity=source, evidence_adapter=capability["adapters"]["evidence"])
    manifest["provenance"].update(source=source["id"], symbol=source["instrument"], trigger_basis=source["id"],
                                  source_identity=source)
    with pytest.raises(ValueError, match="approved favorable evidence adapter"):
        evidence.validate_evidence(manifest, context)


def test_stop_only_native_review_does_not_invent_a_target(chart):
    manifest, context, document = chart
    context["position"]["target"] = document["model"]["target"] = None
    save_chart(*chart)
    result = evidence.validate_evidence(manifest, context)
    assert result["checks"]["frozen_model"]["target"] is None
    assert not result["checks"]["threshold_crossing_possible"]


def test_cash_spot_model_does_not_require_futures_liquidation_or_funding(chart):
    _, context, _ = chart
    context["position"]["plan"]["contract"] = {**context["position"]["plan"]["contract"], "family": "spot"}
    context["position"]["position"].pop("funding")
    assert evidence._model(context["position"])["liquidation_recomputed"] is None


def journal():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE event(key TEXT PRIMARY KEY,book TEXT,kind TEXT,idea TEXT,source_at TEXT,recorded_at TEXT,body TEXT)")
    return db


def append_raw(db, kind, at, body, *, book_id="book", position_id="position"):
    index = db.execute("SELECT count(*) FROM event").fetchone()[0]
    key = f"event-{index}"
    db.execute("INSERT INTO event VALUES(?,?,?,?,?,?,?)",
               (key, book_id, kind, position_id, at.isoformat(), at.isoformat(), json.dumps(body)))
    return key


def initial_model_events(db, position):
    plan = position["plan"]
    append_raw(db, "PUBLISHED", BASE, {"plan": {**plan, "direction": -1}})
    append_raw(db, "HYPOTHETICAL_FILL", BASE,
               {"quantity": 1., "price": 100., "entry_equity": 10000., "fee": .1,
                "margin": 33., "notional": 100., "entry_at": BASE.isoformat(),
                "stop": position["stop"], "target": position["target"], "deadline": position["deadline"]})


def test_effective_trail_and_funding_segments_use_actual_journal_times(chart):
    manifest, context, document = chart
    with closing(journal()) as db:
        initial_model_events(db, context["position"])
        changed = BASE + pd.Timedelta(seconds=90)
        stop_event = append_raw(db, "TRAIL_UPDATED", changed,
                                {"effective_stop": 100.5, "close_at": (BASE + pd.Timedelta(minutes=1)).isoformat()})
        funding_event = append_raw(db, "FUNDING", changed, {"settlement_at": changed.isoformat(), "cash_flow": -.3})
        book = {"book_id": "book", "position": None}
        with pytest.raises(ValueError, match="segmented"):
            commands._position(db, book, "position", through=context["interval_end"])
        assert commands._position(db, book, "position", through=changed)["stop"] == 110.
        segments = commands._position_segments(db, book, "position", context["interval_start"], context["interval_end"])
    assert len(segments) == 2
    assert segments[0]["position"]["stop"] == 110.
    assert segments[0]["end"] == changed.isoformat()
    assert segments[1]["position"]["stop"] == 100.5
    assert segments[1]["position"]["position"]["reserved"] == 32.7
    assert segments[1]["position"]["position"]["funding"] == -.3
    assert segments[1]["effective_event_ids"] == [stop_event, funding_event]
    save_chart(*chart)
    context.update(terms_segments=segments, segmented_terms_verified=True)
    document["schema"] = evidence.SEGMENTED_SCHEMA
    document["segments"] = [
        {"interval_start": item["start"], "interval_end": item["end"],
         "effective_event_ids": item["effective_event_ids"], "model": evidence._model(item["position"])}
        for item in segments]
    save_document(manifest, document)
    with pytest.raises(ValueError, match="touches or crosses"):
        evidence.validate_evidence(manifest, context)
    manifest["outcome"] = "EXIT_IMPACT_POSSIBLE"
    result = evidence.validate_evidence(manifest, context)
    assert len(result["checks"]["effective_segments"]) == 2
    document["segments"][0]["model"]["stop"] = 100.5
    save_document(manifest, document)
    with pytest.raises(ValueError, match="frozen position model"):
        evidence.validate_evidence(manifest, context)


def test_static_candle_claim_cannot_hide_effective_term_changes(chart):
    manifest, context, document = chart
    save_chart(*chart)
    context["terms_segments"] = [{"start": context["interval_start"]}, {"end": context["interval_end"]}]
    with pytest.raises(ValueError, match="segmented"):
        evidence.validate_evidence(manifest, context)


def permit(db, book, at=AT):
    incidents.append(db, book, "PROTECTION_POLICY_ACTIVATED", "generation", at,
                     {"generation": GENERATION}, identity="activation")
    dependency = {"incident_ids": [], "position_ids": [], "segment_id": "segment"}
    key = incidents.append(db, book, "BOOK_PROSPECTIVE_RESUME", "prior-position", at,
                           {"generation": GENERATION, "effective_at": at.isoformat(),
                            "capital_dependency": dependency}, identity="resume")
    book["prospective_admission"] = {"policy": incidents.POLICY, "event_id": key,
                                     "effective_at": at.isoformat(), "capital_dependency": dependency}


@pytest.mark.parametrize("kind", ["PROTECTION_GAP", "UNMODELED_TIER", "PROTECTION_INCIDENT_IMPORTED",
                                  "PROTECTION_INCIDENT_ADJUDICATED", "OPERATIONAL_BLOCK_OPENED"])
def test_a_later_historical_cause_revokes_old_permission_even_if_legacy_flag_path_missed_it(kind):
    book = {"book_id": "book", "problem": "UNRESOLVED_PROTECTION_INTERVAL"}
    with closing(journal()) as db:
        permit(db, book)
        assert incidents.admission_allowed(book, AT)
        append_raw(db, kind, AT + pd.Timedelta(seconds=1), {"requires_incident_review": True})
        incidents.restore_admission(db, book)
        assert not book.get("prospective_admission")
        assert not incidents.admission_allowed(book, AT + pd.Timedelta(seconds=2))


def test_temporary_current_health_cause_keeps_historical_permission_but_permanent_block_does_not_admit():
    book = {"book_id": "book", "problem": "UNRESOLVED_PROTECTION_INTERVAL"}
    with closing(journal()) as db:
        permit(db, book)
        append_raw(db, "OPERATIONAL_BLOCK_OPENED", AT + pd.Timedelta(seconds=1),
                   {"requires_incident_review": False, "reason": "CLOCK_UNVERIFIED"})
        incidents.restore_admission(db, book)
        assert book.get("prospective_admission")
        book["permanent_block"] = True
        assert not incidents.admission_allowed(book, AT)


def test_nonstandard_eighth_inherits_durable_recovery_without_name_dispatch(registered):
    db, record = registered
    request = eighth(record)
    apply(db, record, request)
    lane = membership.lanes(record, db=db)[request["slot"]]
    book = paper.load(db, lane)
    assert lane["family"] not in ("EMA", "BREAKOUT")
    item = recovery.reassess(db, record, lane, book, [], AT,
                             health={"qualified": False, "episode_id": "source-episode"})
    assert item["stage"] == "WAIT_CURRENT_HEALTH"
    assert item["needs_current_health"] and recovery.acquisition_due(db, book["book_id"], AT)
    assert not recovery.automatic_resume_assessment(item)["automatic_action_authorized"]
    # A repeated scan reuses exactly one work item and audit identity.
    count = db.execute("SELECT count(*) FROM recovery_work_event").fetchone()[0]
    assert recovery.reassess(db, record, lane, book, [], AT,
                             health={"qualified": False, "episode_id": "source-episode"}) == item
    assert db.execute("SELECT count(*) FROM recovery_work_event").fetchone()[0] == count


def test_acquisition_retries_are_bounded_persistent_and_do_not_approve_recovery(registered):
    db, record = registered
    lane = record["body"]["lanes"]["breakout/position/fixed"]
    book = paper.load(db, lane)
    work = recovery.reassess(db, record, lane, book, [], AT, health={"qualified": False})
    for _ in range(recovery.MAX_ATTEMPTS):
        at = pd.Timestamp(work["next_attempt_at"])
        assert recovery.acquisition_due(db, book["book_id"], at)
        work, = recovery.record_acquisition(db, book["book_id"], at, success=False, reason="STALE_REFERENCE")
        assert not recovery.record_acquisition(db, book["book_id"], at, success=False)
    assert work["attempts"] == recovery.MAX_ATTEMPTS
    assert work["stage"] == "RETRY_LIMIT_REVIEW_REQUIRED"
    assert not recovery.acquisition_due(db, book["book_id"], pd.Timestamp(work["next_attempt_at"]))
    assert recovery.items(db, book["book_id"])[0] == work
    assert not book.get("prospective_admission")
    with pytest.raises(ValueError, match="No reviewed"):
        recovery.automatic_resume_assessment(work, policy={"version": recovery.AUTO_POLICY,
                                                          "automatic_actions_enabled": True})


def test_technical_source_recovery_finishes_durable_work_and_stops_flat_acquisition(registered):
    db, record = registered
    lane = record["body"]["lanes"]["breakout/position/fixed"]
    book = paper.load(db, lane)
    original = deepcopy(book)
    work = recovery.reassess(db, record, lane, book, [], AT, health={"qualified": False})
    assert recovery.acquisition_due(db, book["book_id"], AT)
    recovery.record_acquisition(db, book["book_id"], AT, success=True)
    recovered_at = AT + pd.Timedelta(seconds=1)
    recovered = recovery.reassess(db, record, lane, book, [], recovered_at, health={"qualified": True})
    assert recovered["id"] == work["id"] and recovered["stage"] == "RESOLVED_TECHNICAL"
    assert recovered["attempts"] == 1 and not recovered["needs_current_health"]
    assert not recovery.acquisition_due(db, book["book_id"], AT+pd.Timedelta(days=1))
    assert recovery.record_acquisition(db, book["book_id"], AT+pd.Timedelta(days=1), success=True) == []
    count = db.execute("SELECT count(*) FROM recovery_work_event").fetchone()[0]
    assert recovery.reassess(db, record, lane, book, [], recovered_at, health={"qualified": True}) is None
    assert db.execute("SELECT count(*) FROM recovery_work_event").fetchone()[0] == count
    assert book == original and not book.get("prospective_admission")


def test_evidence_acquisition_assessment_disposition_and_permission_remain_separate(registered):
    db, record = registered
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book["problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    records = [{"incident_id": "one", "current_feed_health": "RECOVERED", "historical_coverage": "UNRESOLVED"}]
    work = recovery.reassess(db, record, lane, book, records, AT)
    assert work["stage"] == "AWAIT_EVIDENCE_ACQUISITION"
    manifest = {"path": "immutable-review.json", "sha256": "a" * 64, "provenance": {"source": "reviewed"}}
    recovery.attach_evidence(db, work["id"], AT, manifest)
    assert recovery.reassess(db, record, lane, book, records, AT)["stage"] == "AWAIT_EVIDENCE_ASSESSMENT"
    recovery.attach_evidence(db, work["id"], AT, manifest, assessment={"valid": True, "outcome": recovery.FAVORABLE})
    assert recovery.reassess(db, record, lane, book, records, AT)["stage"] == "AWAIT_INCIDENT_DISPOSITION"
    records[0]["outcome"] = "INSUFFICIENT_EVIDENCE"
    assert recovery.reassess(db, record, lane, book, records, AT)["stage"] == "OPERATOR_REVIEW_REQUIRED"
    records[0]["outcome"] = recovery.FAVORABLE
    assert recovery.reassess(db, record, lane, book, records, AT,
                             health={"qualified": True})["stage"] == "READY_FOR_OPERATOR_RESUME"
    assert not book.get("prospective_admission")


def test_other_exposed_books_do_not_prevent_flat_book_assessment_and_pauses_are_preserved(registered):
    db, record = registered
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book["problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    records = [{"incident_id": "reviewed", "outcome": recovery.FAVORABLE, "historical_coverage": "UNRESOLVED"}]
    other_lane = record["body"]["lanes"]["breakout/swing/trail"]
    other = paper.load(db, other_lane)
    other["position"] = {"id": "unrelated-open"}
    paper.save(db, other)
    assert recovery.reassess(db, record, lane, book, records, AT,
                             health={"qualified": True})["stage"] == "READY_FOR_OPERATOR_RESUME"
    assert recovery.reassess(db, record, lane, book, records, AT,
                             health={"qualified": True}, controls={"paused": True})["stage"] == "INTENTIONAL_RESTRICTION"
    assert paper.load(db, other_lane)["position"]["id"] == "unrelated-open"


def test_recovery_init_and_interruption_never_commit_a_partial_financial_transaction(registered):
    db, record = registered
    lane = record["body"]["lanes"]["ema/day"]
    db.execute("BEGIN IMMEDIATE")
    append_raw(db, "UNCOMMITTED_FINANCIAL_MARKER", AT, {})
    recovery.reassess(db, record, lane, paper.load(db, lane), [], AT, health={"qualified": False})
    db.rollback()
    assert db.execute("SELECT count(*) FROM event WHERE kind='UNCOMMITTED_FINANCIAL_MARKER'").fetchone()[0] == 0
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_work'").fetchone()
    assert not exists or db.execute("SELECT count(*) FROM recovery_work").fetchone()[0] == 0


@pytest.mark.parametrize("slot", ["breakout/day/fixed", "breakout/position/fixed", "orbital/reversion"])
def test_source_correct_operator_disposition_and_resume_work_while_another_book_is_open(registered, chart, tmp_path, monkeypatch, slot):
    db, record = registered
    _, old_context, _ = chart
    if slot == "orbital/reversion":
        future = eighth(record)
        apply(db, record, future)
    lane = membership.lanes(record, db=db)[slot]
    capability = caps.contract(lane, generation=record["id"], slot=slot)
    source = capability["source"]
    book = paper.load(db, lane)
    position = deepcopy(old_context["position"])
    position.update(id="target-position", entry_at=(AT-pd.Timedelta(minutes=3)).isoformat(),
                    deadline=(AT+pd.Timedelta(hours=1)).isoformat())
    position["plan"].update(direction=-1, evidence={"source_id": source["version"]})
    book["problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    book["position"] = None
    paper.save(db, book)
    before = AT - pd.Timedelta(minutes=3)
    append_raw(db, "PUBLISHED", before, {"plan": position["plan"]}, book_id=book["book_id"], position_id=position["id"])
    append_raw(db, "HYPOTHETICAL_FILL", before,
               {"quantity": 1., "price": 100., "entry_equity": 10000., "fee": 0., "margin": 100.,
                "notional": 100., "entry_at": position["entry_at"], "stop": 110., "target": 90.,
                "deadline": position["deadline"]}, book_id=book["book_id"], position_id=position["id"])
    append_raw(db, "EXIT_OBSERVED", AT-pd.Timedelta(minutes=1), {"price": 100.},
               book_id=book["book_id"], position_id=position["id"])
    append_raw(db, "CLOSED", AT-pd.Timedelta(minutes=1),
               {"net": 0., "fees": 0., "funding": 0., "gross_price_pnl": 0., "liquidation_cost": 0.,
                "unfunded_deficit": 0., "net_r": 0., "holding_minutes": 2., "provisional": True},
               book_id=book["book_id"], position_id=position["id"])
    incidents.append(db, book, "PROTECTION_POLICY_ACTIVATED", record["id"], before,
                     {"generation": record["id"]}, identity="test-active-policy")
    incidents.append(db, book, "PROTECTION_INCIDENT_OPENED", position["id"], before,
                     {"generation": record["id"], "position_id": position["id"], "incident_id": "incident",
                      "detected_at": before.isoformat(), "current_feed_health": "RECOVERED", "frozen_position": position,
                      "interval": {"starts_after": before.isoformat(), "ends_at": (AT-pd.Timedelta(minutes=1)).isoformat()},
                      "historical_coverage": "UNRESOLVED"}, identity="incident")
    other_lane = membership.lanes(record, db=db)["breakout/swing/trail"]
    other = paper.load(db, other_lane)
    other["position"] = {"id": "preserved-unrelated-exposure"}
    paper.save(db, other)
    note = {"schema": "OPERATOR_EVIDENCE_NOTE_V1", "generation": record["id"], "book_id": book["book_id"],
            "position_id": position["id"], "recorded_at": AT.isoformat(), "reason": "History insufficient"}
    path = tmp_path / "source-correct-note.json"
    path.write_text(json.dumps(note), encoding="utf-8")
    manifest = {"outcome": "INSUFFICIENT_EVIDENCE", "path": str(path), "sha256": sha256(path.read_bytes()).hexdigest(),
                "provenance": {"origin": "EXPLICIT_REVIEW", "operator": "test", "acquired_at": AT.isoformat(),
                               "source": source["id"], "symbol": source["instrument"], "trigger_basis": source["id"],
                               "stream_id": source["version"], "source_identity": source}}
    monkeypatch.setattr(commands, "utc_now", lambda: AT)
    monkeypatch.setattr(commands, "_healthy", lambda *_: {"clock_and_reference_checks": "TEST_PASSED"})
    _, _, _, token = commands._state(db, record, slot)
    request = {"generation": record["id"], "slot": slot, "review_token": token, "request_id": "review-source",
               "reason": "Explicit one-time isolated review", "incident_id": "incident", "evidence": manifest}
    assert commands._execute(db, record, "adjudicate", request, False)["state"] == "PREVIEW"
    assert commands._execute(db, record, "adjudicate", request, True)["state"] == "APPLIED"
    _, _, _, token = commands._state(db, record, slot)
    request = {"generation": record["id"], "slot": slot, "review_token": token, "request_id": "new-resume",
               "reason": "Explicit isolated prospective continuation"}
    result = commands._execute(db, record, "resume", request, True)
    assert pd.Timestamp(result["record"]["effective_at"]) > AT
    restored = paper.load(db, lane)
    incidents.restore_admission(db, restored)
    assert restored["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    assert restored["prospective_admission"]["capital_dependency"]["incident_ids"] == ["incident"]
    assert paper.load(db, other_lane)["position"]["id"] == "preserved-unrelated-exposure"


def test_unknown_legacy_interval_has_an_honest_insufficiency_path_without_fabricated_times(chart):
    manifest, context, document = chart
    context.update(interval_start=None, interval_end=None, historical_model_unavailable=True, position=None)
    document = {key: document[key] for key in ("generation", "book_id", "position_id", "recorded_at")}
    document.update(schema="OPERATOR_EVIDENCE_NOTE_V1", reason="Original interval is not retained")
    manifest["outcome"] = "INSUFFICIENT_EVIDENCE"
    save_document(manifest, document)
    result = evidence.validate_evidence(manifest, context)
    assert result["reviewed_interval"] == {"start": None, "end": None}
    assert not result["historical_model_available"]
    manifest["outcome"] = "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED"
    with pytest.raises(ValueError, match="Unknown historical"):
        evidence.validate_evidence(manifest, context)


def test_unattributed_provisional_history_is_projected_without_inventing_detection_time():
    with closing(journal()) as db:
        book = {"book_id": "book", "position": None, "problem": "UNRESOLVED_PROTECTION_INTERVAL"}
        append_raw(db, "CLOSED", AT, {"provisional": True, "source_received_at": AT.isoformat()})
        records = incidents.legacy_incidents(db, book, GENERATION)
        assert len(records) == 1 and records[0]["detected_at"] is None
        assert records[0]["interval"]["starts_after"] is None
        assert records[0]["first_accepted_reference"] is None
        assert records[0]["known_terminal_recorded_at"] == AT.isoformat()


def test_a_clean_capital_dependent_close_does_not_revoke_valid_permission():
    with closing(journal()) as db:
        book = {"book_id": "book", "problem": "UNRESOLVED_PROTECTION_INTERVAL"}
        permit(db, book)
        append_raw(db, "CLOSED", AT+pd.Timedelta(seconds=1),
                   {"provisional": False, "research_provenance": {"position_ids": ["prior-position"]}})
        incidents.restore_admission(db, book)
        assert incidents.admission_allowed(book, AT+pd.Timedelta(seconds=2))


@pytest.mark.parametrize("cause", ["unresolved_flag", "provisional_close", "incident_journal"])
def test_missing_legacy_problem_projection_cannot_hide_a_historical_admission_cause(cause):
    with closing(journal()) as db:
        book = {"book_id": "book", "problem": None}
        if cause == "unresolved_flag":
            book["position"] = {"id": "legacy-open", "unresolved_protection": True}
        elif cause == "provisional_close":
            append_raw(db, "CLOSED", AT, {"provisional": True})
        else:
            incidents.append(db, book, "PROTECTION_INCIDENT_IMPORTED", "old-position", AT,
                             {"incident_id": "unreviewed", "historical_coverage": "UNRESOLVED"}, identity="unreviewed")
        before = db.total_changes
        incidents.restore_admission(db, book)
        assert book["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
        assert not incidents.admission_allowed(book, AT)
        assert db.total_changes == before


def test_recovery_rearm_is_explicit_idempotent_and_never_grants_financial_permission(registered):
    db, record = registered
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    item = recovery.reassess(db, record, lane, book, [], AT, health={"qualified": False})
    request = {"generation": record["id"], "book_id": book["book_id"], "work_id": item["id"],
               "request_id": "operator-retry", "reason": "Source access repaired; retry original safeguards"}
    assert recovery.rearm(db, request, AT)["state"] == "PREVIEW"
    result = recovery.rearm(db, request, AT, approved=True)
    assert result["state"] == "APPLIED" and not result["financial_or_resume_authorization"]
    assert recovery.rearm(db, request, AT, approved=True)["state"] == "ALREADY_APPLIED"
    with pytest.raises(ValueError, match="different arguments"):
        recovery.rearm(db, {**request, "reason": "Changed authorization"}, AT, approved=True)
    assert not book.get("prospective_admission")


def test_read_only_work_status_does_not_create_tables(registered, tmp_path):
    db, _ = registered
    path = tmp_path / "read-only-status.sqlite3"
    with closing(sqlite3.connect(path)) as target:
        db.backup(target)
    with closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True)) as saved:
        assert recovery.items(saved) == []
        assert not saved.execute("SELECT 1 FROM sqlite_master WHERE name='recovery_work'").fetchone()


def test_count_only_financial_match_cannot_hide_missing_terminal_evidence(registered):
    db, record = registered
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    append_raw(db, "HYPOTHETICAL_FILL", AT, {}, book_id=book["book_id"], position_id="one")
    append_raw(db, "CLOSED", AT, {}, book_id=book["book_id"], position_id="different")
    with pytest.raises(ValueError, match="matching fill"):
        commands.terminal_financial_checks(db, lane, book)


@pytest.mark.parametrize("history", ["none", "wrong_position", "missing_exit", "complete_provisional"])
def test_recovery_stage_uses_matching_terminal_journal_without_requiring_validated_history(registered, history):
    db, record = registered
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book.update(problem="UNRESOLVED_PROTECTION_INTERVAL", accounting_complete=False)
    records = [{"incident_id": "incident", "position_id": "affected", "outcome": recovery.FAVORABLE,
                "current_feed_health": "RECOVERED", "historical_coverage": "UNRESOLVED"}]
    if history != "none":
        position_id = "unrelated" if history == "wrong_position" else "affected"
        append_raw(db, "HYPOTHETICAL_FILL", AT, {}, book_id=book["book_id"], position_id=position_id)
        if history != "missing_exit":
            append_raw(db, "EXIT_OBSERVED", AT, {}, book_id=book["book_id"], position_id=position_id)
        append_raw(db, "CLOSED", AT,
                   {"net": 0., "fees": 0., "funding": 0., "gross_price_pnl": 0., "liquidation_cost": 0.,
                    "unfunded_deficit": 0., "provisional": True}, book_id=book["book_id"], position_id=position_id)
    work = recovery.reassess(db, record, lane, book, records, AT, health={"qualified": True})
    finance = work["financial_reconciliation"]
    assert finance["basis"] == "TERMINAL_FINANCIAL_JOURNAL" and finance["nominal_vector_complete"]
    if history == "complete_provisional":
        assert finance["complete"] and work["stage"] == "READY_FOR_OPERATOR_RESUME"
        assert not book["accounting_complete"] and not work["automatic_action_authorized"]
    else:
        assert not finance["complete"] and work["stage"] == "WAIT_FINANCIAL_RECONCILIATION"
        assert not work["needs_current_health"] and finance["detail"]


def test_actual_resume_refuses_an_incident_position_without_any_terminal_receipt(registered):
    db, record = registered
    lane = record["body"]["lanes"]["ema/day"]
    book = paper.load(db, lane)
    book["problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    records = [{"incident_id": "unexplained", "position_id": "missing", "outcome": "INSUFFICIENT_EVIDENCE"}]
    with pytest.raises(ValueError, match="genuine matching terminal CLOSED"):
        commands._resume(db, record, lane, book, records, AT)
    assert not book.get("prospective_admission")


@pytest.mark.parametrize("exposed", [False, True])
def test_unmodeled_tier_has_an_explicit_financial_review_path_and_never_favorable_automatic_resume(registered, exposed):
    db, record = registered
    lane = record["body"]["lanes"]["breakout/day/fixed"]
    book = paper.load(db, lane)
    if exposed:
        book["position"] = {"id": "protected-open", "unresolved_costs": "UNMODELED_TIER"}
    append_raw(db, "UNMODELED_TIER", AT, {"reason": "No reviewed instrument cost tier"}, book_id=book["book_id"])
    work = recovery.reassess(db, record, lane, book, [], AT, health={"qualified": True})
    assert work["stage"] == ("WAIT_TERMINAL_EXPOSURE" if exposed else "UNSUPPORTED_FINANCIAL_MODEL_REVIEW_REQUIRED")
    assert work["financial_reconciliation"]["reason_code"] == "UNMODELED_TIER"
    assert not work["financial_reconciliation"]["complete"] and not work["needs_current_health"]
    assert not recovery.automatic_resume_assessment(work)["automatic_action_authorized"]
    assert not book.get("prospective_admission")
    if exposed:
        assert book["position"]["id"] == "protected-open"


def test_unknown_legacy_model_can_be_disposed_but_missing_financial_evidence_still_blocks_resume(registered, tmp_path):
    db, record = registered
    slot = "ema/day"
    lane = record["body"]["lanes"][slot]
    book = paper.load(db, lane)
    book["problem"] = "UNRESOLVED_PROTECTION_INTERVAL"
    paper.save(db, book)
    incidents.append(db, book, "PROTECTION_POLICY_ACTIVATED", record["id"], AT,
                     {"generation": record["id"]}, identity="policy")
    append_raw(db, "CLOSED", AT, {"provisional": True, "source_received_at": AT.isoformat(),
                                  "net": 0., "fees": 0., "funding": 0., "net_r": 0., "holding_minutes": 1.},
               book_id=book["book_id"], position_id="legacy-no-fill")
    records = incidents.legacy_incidents(db, book, record["id"])
    assert len(records) == 1
    note = {"generation": record["id"], "book_id": book["book_id"], "position_id": "legacy-no-fill",
            "recorded_at": AT.isoformat(), "schema": "OPERATOR_EVIDENCE_NOTE_V1",
            "reason": "Original interval and frozen model were not retained"}
    path = tmp_path / "unknown-model.json"
    path.write_text(json.dumps(note), encoding="utf-8")
    request = {"slot": slot, "incident_id": records[0]["incident_id"], "evidence": {
        "outcome": "INSUFFICIENT_EVIDENCE", "path": str(path), "sha256": sha256(path.read_bytes()).hexdigest(),
        "provenance": {"origin": "EXPLICIT_UNKNOWN_REVIEW", "operator": "test", "acquired_at": AT.isoformat(),
                       "source": caps.KRAKEN, "symbol": "BTC/USD", "trigger_basis": caps.KRAKEN, "stream_id": None}}}
    result = commands._adjudicate(db, record, lane, book, records, request)
    assert result["outcome"] == "INSUFFICIENT_EVIDENCE"
    assert result["reviewed_interval"]["start"] is None
    assert not result["historical_model_available"]
    with pytest.raises(ValueError, match="matching fill"):
        commands.terminal_financial_checks(db, lane, book)


def test_resume_health_rejects_undrained_own_delivery_without_blocking_on_another_book(lifecycle, monkeypatch):
    from lab.paper_run import runtime, delivery

    record, now, amendment, lane = lifecycle
    monkeypatch.setattr(runtime.kraken_live, "permission", lambda *_: None)
    point = live_point(91)
    snapshots = {
        "ema-live-status.json": {"state": "STREAMING", "at": now[0].isoformat(),
                                 "last_usable_observation": point},
        "last-cycle.json": {"state": "RUNNING", "generation": record["id"], "runner_build_id": runtime.RUNNER_BUILD,
                            "at": now[0].isoformat(), "pid": 123},
    }
    monkeypatch.setattr(runtime, "read_status_snapshot", lambda name, fallback: snapshots.get(name, fallback))
    monkeypatch.setattr(delivery, "pending_books", lambda _: [{"book_id": lane["book_id"], "pending_count": 1, "loss_count": 0}])
    with pytest.raises(ValueError, match="Unprocessed source delivery"):
        commands._healthy(record, lane)
    monkeypatch.setattr(delivery, "pending_books", lambda _: [{"book_id": "other-book", "pending_count": 1, "loss_count": 1}])
    assert commands._healthy(record, lane)["clock_and_reference_checks"] == "PASSED"
