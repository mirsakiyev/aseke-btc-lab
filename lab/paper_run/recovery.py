"""Bounded recovery assessment; financial approval stays in the reviewed operator API.

These work items make transient retries and historical review visible. They do
not infer missing prices, clear protection history, adjudicate an incident or
grant entry permission. Automatic historical resume remains a disabled proposal.
"""

from copy import deepcopy
from contextlib import closing
import argparse
import json
from math import isfinite
from pathlib import Path
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.paper_run import protection_incidents as incidents
from lab.storage import utc


VERSION = "PAPER_RECOVERY_WORK_V1"
AUTO_POLICY = "AUTO_RESUME_POLICY_V1_DRAFT"
MAX_ATTEMPTS = 20
BASE_RETRY_SECONDS = 15
MAX_RETRY_SECONDS = 300
MAX_EVIDENCE_PER_WORK = 32
FAVORABLE = "NO_EXIT_THRESHOLD_CROSSING_SUPPORTED"
TERMINAL = {"RESUMED", "RESOLVED_TECHNICAL", "SUPERSEDED"}


def init(db):
    # Do not use executescript: this function may run inside a financial caller's
    # transaction and must never commit its partially processed book.
    db.execute("CREATE TABLE IF NOT EXISTS recovery_work (id TEXT PRIMARY KEY,book TEXT NOT NULL,state TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS recovery_work_event (id TEXT PRIMARY KEY,work TEXT NOT NULL,"
               "book TEXT NOT NULL,at TEXT NOT NULL,body TEXT NOT NULL)")
    for operation in ("UPDATE", "DELETE"):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS recovery_work_{operation.lower()} BEFORE {operation} "
                   "ON recovery_work_event BEGIN SELECT RAISE(ABORT,'Recovery audit is append-only'); END")


def policy_proposal():
    return {"version": AUTO_POLICY, "state": "DRAFT_DISABLED", "automatic_actions_enabled": False,
            "permitted_outcomes": [FAVORABLE], "requires_separate_review_and_activation": True,
            "required": ["registered_supported_capabilities", "flat_no_pending", "terminal_nominal_reconciliation",
                         "every_relevant_incident_disposed", "approved_no_exit_impact_evidence", "current_clock",
                         "qualified_source", "fresh_reference", "matching_managed_runner", "no_intentional_pause",
                         "no_independent_risk_restriction", "strictly_future_boundary"],
            "never_automatic": ["INSUFFICIENT_EVIDENCE", "EXIT_IMPACT_POSSIBLE", "UNSUPPORTED_CAPABILITY",
                                "ACTIVE_PROTECTION_LOSS", "UNRECONCILED_FINANCES"]}


def _existing(db, identifier):
    row = db.execute("SELECT state FROM recovery_work WHERE id=?", (identifier,)).fetchone()
    return json.loads(row[0]) if row else None


def items(db, book_id=None):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_work'").fetchone():
        return []
    rows = db.execute("SELECT state FROM recovery_work" + (" WHERE book=?" if book_id else "") + " ORDER BY rowid",
                      (book_id,) if book_id else ()).fetchall()
    return [json.loads(row[0]) for row in rows]


def _save(db, value, at, action):
    previous = _existing(db, value["id"])
    if previous == value:
        return value
    serial = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    key = fingerprint([VERSION, value["id"], action, serial])
    payload = {"schema": VERSION, "action": action, "at": at.isoformat(), "work": value}
    db.execute("INSERT OR IGNORE INTO recovery_work_event VALUES (?,?,?,?,?)",
               (key, value["id"], value["book_id"], at.isoformat(), json.dumps(payload, sort_keys=True, allow_nan=False)))
    db.execute("INSERT INTO recovery_work VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state",
               (value["id"], value["book_id"], serial))
    return value


def _finances(book):
    fields = ("cash", "starting_equity", "gross", "fees", "funding", "liquidation_cost", "unfunded_deficit")
    values = [book.get(key) for key in fields]
    if not all(type(value) in (int, float) and isfinite(value) for value in values):
        return False
    expected = book["starting_equity"] + book["gross"] + book["funding"] - book["fees"] - book["liquidation_cost"] + book["unfunded_deficit"]
    return abs(book["cash"] - expected) <= 1e-8 and not (book.get("position") or {}).get("unresolved_costs")


def _terminal_finances(db, lane, book, records, *, unmodeled=False):
    """Assess nominal receipts, without demanding validated historical research."""
    nominal = _finances(book)
    if unmodeled:
        return {"complete": False, "nominal_vector_complete": nominal,
                "basis": "UNSUPPORTED_INSTRUMENT_MODEL", "reason_code": "UNMODELED_TIER",
                "detail": "Preserve the financial restriction; a separately reviewed instrument/cost model and genuine receipts are required"}
    if not nominal:
        return {"complete": False, "nominal_vector_complete": False, "basis": "NOMINAL_VECTOR",
                "reason_code": "NOMINAL_FINANCIAL_RECONCILIATION_REQUIRED"}
    if book.get("position") or book.get("pending"):
        return {"complete": False, "nominal_vector_complete": True, "basis": "TERMINAL_OBLIGATION_PENDING",
                "reason_code": "EXPOSURE_NOT_TERMINAL"}
    present = db.execute("SELECT 1 FROM event WHERE book=? AND kind IN "
                         "('HYPOTHETICAL_FILL','EXIT_OBSERVED','CLOSED','FUNDING') LIMIT 1",
                         (book["book_id"],)).fetchone()
    if not (present or records or book.get("problem")):
        return {"complete": True, "nominal_vector_complete": True, "basis": "NOMINAL_VECTOR_NO_TERMINAL_HISTORY"}
    from lab.paper_run.protection_commands import terminal_financial_checks

    try:
        result = terminal_financial_checks(db, lane, book,
                    expected_position_ids={item["position_id"] for item in records if item.get("position_id")})
    except (ValueError, KeyError, TypeError, sqlite3.Error) as error:
        return {"complete": False, "nominal_vector_complete": True, "basis": "TERMINAL_FINANCIAL_JOURNAL",
                "reason_code": "TERMINAL_FINANCIAL_EVIDENCE_REQUIRED", "detail": str(error)[:400]}
    return {"complete": True, "nominal_vector_complete": True, "basis": "TERMINAL_FINANCIAL_JOURNAL",
            "terminal_checks": result}


def _decision(book, records, at, health, controls, acquired, assessed, financial):
    if book.get("permanent_block") or controls.get("paused") or controls.get("intentional_pause") or controls.get("independent_risk_block"):
        return "INTENTIONAL_RESTRICTION", "Preserve the intentional/risk restriction; operator review is required", False
    if book.get("position"):
        return "WAIT_TERMINAL_EXPOSURE", "Continue accepted-price protection and legitimate exits; do not force flatness", False
    if book.get("pending"):
        return "WAIT_PENDING_COMPLETION", "Preserve ordinary pending-entry expiry/cancellation policy", False
    if financial.get("reason_code") == "UNMODELED_TIER":
        return "UNSUPPORTED_FINANCIAL_MODEL_REVIEW_REQUIRED", "Preserve the unmodeled-tier restriction; obtain a separately reviewed instrument/cost model and genuine financial receipts", False
    if not financial["complete"]:
        return "WAIT_FINANCIAL_RECONCILIATION", "Obtain genuine terminal costs and nominal reconciliation", False
    if any(item.get("current_feed_health") == "UNAVAILABLE" for item in records):
        return "ACTIVE_PROTECTION_REVIEW_REQUIRED", "Review the unresolved active protection obligation with source evidence", False
    permit = book.get("prospective_admission")
    if permit:
        if not health.get("qualified"):
            return "WAIT_CURRENT_HEALTH", "Reassess current source/clock/reference checks without erasing the historical permit", True
        if at < utc(permit["effective_at"]):
            return "ADMISSION_AUTHORIZED_FUTURE_BOUNDARY", "Wait for the authorized strictly future strategy boundary", False
        return "RESUMED", "Continue normal prospective strategy gates; retain historical research exclusions", False
    missing = [item for item in records if not item.get("outcome")]
    if missing:
        if not acquired:
            return "AWAIT_EVIDENCE_ACQUISITION", "Acquire source-correct evidence or an explicit limitation note", False
        if not assessed:
            return "AWAIT_EVIDENCE_ASSESSMENT", "Validate acquired evidence with the declared approved adapter", False
        return "AWAIT_INCIDENT_DISPOSITION", "Preview and authorize a truthful disposition through the supported per-book command", False
    if records and any(item.get("outcome") != FAVORABLE for item in records):
        return "OPERATOR_REVIEW_REQUIRED", "Ambiguous/insufficient exit impact requires explicitly reviewed prospective permission", not health.get("qualified", False)
    if not health.get("qualified"):
        return "WAIT_CURRENT_HEALTH", "Acquire/reassess fresh authorized source evidence and original clock/runner checks", True
    if book.get("problem"):
        return "READY_FOR_OPERATOR_RESUME", "Preview the supported new per-book resume request; automatic historical approval is disabled", False
    return "RESOLVED_TECHNICAL", "Current technical checks recovered; retain ordinary strategy/risk gates", False


def reassess(db, record, lane, book, records, at, *, health=None, controls=None):
    """Persist bounded assessment state, without performing a financial transition."""
    from lab.paper_run import capabilities

    init(db)
    at = utc(at)
    health, controls = health or {}, controls or {}
    incidents.restore_admission(db, book)
    position = book.get("position") or {}
    unmodeled = bool(db.execute("SELECT 1 FROM event WHERE book=? AND kind='UNMODELED_TIER' LIMIT 1",
                                (book["book_id"],)).fetchone())
    financial = _terminal_finances(db, lane, book, records, unmodeled=unmodeled)
    restricted = (book.get("problem") or book.get("permanent_block") or records or position.get("unresolved_costs")
                  or unmodeled or health.get("qualified") is False or controls.get("paused") or controls.get("independent_risk_block")
                  or (not position and not book.get("pending") and not financial["complete"]))
    if not restricted:
        resolved = []
        for old in items(db, book["book_id"]):
            if old["generation"] != record["id"] or old["stage"] in TERMINAL:
                continue
            if old.get("incident_ids"):
                # A vanished historical scope is not technical source recovery.
                old.update(stage="RECOVERY_SCOPE_INSPECTION_REQUIRED", needs_current_health=False,
                           next_action="Preserve the historical work scope and inspect missing incident evidence")
                resolved.append(_save(db, old, at, "SCOPE_INSPECTION_REQUIRED"))
            elif health.get("qualified") is True:
                old.update(stage="RESOLVED_TECHNICAL", needs_current_health=False,
                           resolved_at=at.isoformat(), financial_reconciliation=financial,
                           next_action="Technical checks recovered; retain normal prospective strategy and risk gates")
                resolved.append(_save(db, old, at, "TECHNICAL_RECOVERY"))
        return resolved[-1] if resolved else None
    contract = capabilities.contract(lane, generation=record["id"])
    scope = {"generation": record["id"], "book_id": book["book_id"],
             "incident_ids": sorted(item["incident_id"] for item in records),
             "episode_id": health.get("episode_id"), "capability_id": fingerprint(contract)}
    identifier = fingerprint([VERSION, scope])
    value = _existing(db, identifier) or {
        "schema": VERSION, "id": identifier, **scope, "created_at": at.isoformat(), "attempts": 0,
        "max_attempts": MAX_ATTEMPTS, "next_attempt_at": at.isoformat(), "acquired_evidence": [],
        "assessments": [], "policy": AUTO_POLICY, "automatic_action_authorized": False,
    }
    stage, action, needs = _decision(book, records, at, health, controls,
                                      value["acquired_evidence"], value["assessments"], financial)
    if needs and value["attempts"] >= value["max_attempts"]:
        stage, action, needs = "RETRY_LIMIT_REVIEW_REQUIRED", "Bounded acquisition retries exhausted; inspect source and explicitly rearm if appropriate", False
    value.update(stage=stage, next_action=action, needs_current_health=needs,
                 financial_reconciliation=financial,
                 historical_coverage="UNRESOLVED" if any(item.get("historical_coverage") != "VERIFIED" for item in records) else "NO_UNRESOLVED_INCIDENT_RECORDED")
    for old in items(db, book["book_id"]):
        if old["id"] != identifier and old["stage"] not in TERMINAL:
            old.update(stage="SUPERSEDED", needs_current_health=False, next_action="A newer incident/capability scope requires a new assessment")
            _save(db, old, at, "SUPERSEDED")
    return _save(db, value, at, "ASSESSED")


def acquisition_due(db, book_id, at):
    at = utc(at)
    return any(item.get("needs_current_health") and item["attempts"] < item["max_attempts"]
               and utc(item["next_attempt_at"]) <= at for item in items(db, book_id))


def record_acquisition(db, book_id, at, *, success, reason=None):
    """Record one actually attempted source acquisition; never relax its checks."""
    at = utc(at)
    changed = []
    for value in items(db, book_id):
        if not value.get("needs_current_health") or utc(value["next_attempt_at"]) > at:
            continue
        if value["attempts"] >= value["max_attempts"]:
            continue
        value["attempts"] += 1
        value["last_attempt_at"] = at.isoformat()
        value["last_acquisition"] = {"success": success is True, "reason": reason}
        delay = min(MAX_RETRY_SECONDS, BASE_RETRY_SECONDS * 2 ** min(value["attempts"] - 1, 8))
        value["next_attempt_at"] = (at + pd.Timedelta(seconds=delay)).isoformat()
        if value["attempts"] >= value["max_attempts"] and success is not True:
            value.update(stage="RETRY_LIMIT_REVIEW_REQUIRED", needs_current_health=False,
                         next_action="Inspect source failure and explicitly rearm the bounded work item")
        changed.append(_save(db, value, at, "SOURCE_ACQUISITION"))
    return changed


def attach_evidence(db, work_id, at, manifest, *, assessment=None):
    """Stage evidence metadata; only the separate audited command can adjudicate it."""
    init(db)
    value = _existing(db, work_id)
    if value is None:
        raise ValueError("Unknown registered recovery work item")
    if not isinstance(manifest, dict) or not all(manifest.get(key) for key in ("path", "sha256", "provenance")):
        raise ValueError("An immutable source evidence manifest is required")
    reference = deepcopy(manifest)
    if reference not in value["acquired_evidence"]:
        if len(value["acquired_evidence"]) >= MAX_EVIDENCE_PER_WORK:
            raise ValueError("Recovery work evidence limit requires a scoped operator review")
        value["acquired_evidence"].append(reference)
    if assessment is not None:
        if not isinstance(assessment, dict) or not isinstance(assessment.get("valid"), bool):
            raise ValueError("An explicit evidence adapter assessment is required")
        item = {"manifest_sha256": manifest["sha256"], "result": deepcopy(assessment)}
        if item not in value["assessments"]:
            if len(value["assessments"]) >= MAX_EVIDENCE_PER_WORK:
                raise ValueError("Recovery work assessment limit requires a scoped operator review")
            value["assessments"].append(item)
    return _save(db, value, utc(at), "EVIDENCE_STAGED")


def automatic_resume_assessment(work, *, policy=None):
    """Return a review proposal, never an implicit blanket approval."""
    proposal = policy_proposal() if policy is None else policy
    if proposal.get("version") != AUTO_POLICY or proposal.get("automatic_actions_enabled") is not False:
        raise ValueError("No reviewed automatic-resume policy is commissioned by this release")
    return {"policy": AUTO_POLICY, "policy_state": "DRAFT_DISABLED", "automatic_action_authorized": False,
            "work_id": work["id"], "generation": work["generation"], "book_id": work["book_id"],
            "incident_ids": list(work["incident_ids"]), "capability_id": work["capability_id"],
            "stage": work["stage"], "next_action": work["next_action"]}


def rearm(db, request, at, *, approved=False):
    """Explicitly permit another bounded technical retry series, never a trade/resume."""
    if not isinstance(request, dict) or set(request) != {"generation", "book_id", "work_id", "request_id", "reason"}:
        raise ValueError("Recovery rearm requires exact generation, book, work, request and reason fields")
    if not all(isinstance(value, str) and value.strip() and len(value) <= 2048 for value in request.values()):
        raise ValueError("Recovery rearm fields must be bounded nonempty strings")
    if approved is True:
        if not db.in_transaction:
            raise ValueError("Recovery rearm requires the existing writer transaction")
        init(db)
    elif not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_work'").fetchone():
        raise ValueError("Unknown registered recovery work item")
    identity = fingerprint([VERSION, "REARM", request["request_id"]])
    prior = db.execute("SELECT body FROM recovery_work_event WHERE id=?", (identity,)).fetchone()
    if prior:
        body = json.loads(prior[0])
        if body.get("request_sha256") != fingerprint(request):
            raise ValueError("Recovery rearm request ID was reused with different arguments")
        return {"state": "ALREADY_APPLIED", "event_id": identity, "work": _existing(db, request["work_id"])}
    value = _existing(db, request["work_id"])
    if not value or value["book_id"] != request["book_id"] or value["generation"] != request["generation"]:
        raise ValueError("Recovery rearm identities do not match the immutable work scope")
    if value["stage"] not in {"RETRY_LIMIT_REVIEW_REQUIRED", "WAIT_CURRENT_HEALTH", "OPERATOR_REVIEW_REQUIRED"}:
        raise ValueError("This work item is not awaiting current-health acquisition; preserve its actual restriction")
    if approved is not True:
        return {"state": "PREVIEW", "request": request, "work": value,
                "requires_explicit_operator_approval": True, "financial_or_resume_authorization": False}
    at = utc(at)
    value.update(attempts=0, next_attempt_at=at.isoformat(), needs_current_health=True,
                 stage="WAIT_CURRENT_HEALTH", next_action="Retry source acquisition under unchanged permissions and freshness limits")
    body = {"schema": VERSION, "action": "REARM", "request": request,
            "request_sha256": fingerprint(request), "at": at.isoformat(), "work": value}
    db.execute("INSERT INTO recovery_work_event VALUES(?,?,?,?,?)",
               (identity, value["id"], value["book_id"], at.isoformat(), json.dumps(body, sort_keys=True)))
    db.execute("UPDATE recovery_work SET state=? WHERE id=?",
               (json.dumps(value, sort_keys=True, separators=(",", ":")), value["id"]))
    return {"state": "APPLIED", "event_id": identity, "work": value,
            "financial_or_resume_authorization": False}


def main():
    from lab.paper_run import runtime, membership
    from lab.storage import read_json, utc_now, writer_lock

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    commands.add_parser("policy")
    commands.add_parser("status").add_argument("--book")
    action = commands.add_parser("rearm")
    action.add_argument("--request", type=Path, required=True)
    action.add_argument("--apply", action="store_true")
    action.add_argument("--approve", action="store_true")
    args = parser.parse_args()
    if args.operation == "policy":
        result = policy_proposal()
    elif args.operation == "status":
        with closing(runtime.connect(readonly=True)) as db:
            result = {"policy": policy_proposal(), "items": items(db, args.book)}
    else:
        request = read_json(args.request)
        record = runtime.current()
        if not record or request.get("generation") != record["id"]:
            raise ValueError("Recovery rearm generation does not match the current registration")
        if args.apply and args.approve is not True:
            raise PermissionError("Explicit approval is required to rearm recovery acquisition")
        if args.apply:
            with writer_lock(runtime.gate.ROOT / ".writer.lock"), closing(runtime.connect()) as db, db:
                db.execute("BEGIN IMMEDIATE")
                record = runtime.current()
                if not record or request.get("generation") != record["id"]:
                    raise ValueError("Recovery generation changed before the writer transaction")
                if request.get("book_id") not in membership.expected_book_ids(record, db=db):
                    raise ValueError("Recovery rearm book is not registered")
                result = rearm(db, request, utc_now(), approved=True)
        else:
            with closing(runtime.connect(readonly=True)) as db:
                # Preview does not initialize or mutate the recovery tables.
                if not items(db, request.get("book_id")):
                    raise ValueError("No current recovery work exists for this registered book")
                if request.get("book_id") not in membership.expected_book_ids(record, db=db):
                    raise ValueError("Recovery rearm book is not registered")
                result = rearm(db, request, utc_now())
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
