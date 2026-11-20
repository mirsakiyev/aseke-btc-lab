"""Preview-first recovery of a missing projection of preserved forward evidence.

No mark candle/current quote substitution, applied-rate correction, closed-history
restatement, entry permission, runner supervision or source polling is supported.
"""
import argparse
from contextlib import closing
from copy import deepcopy
import getpass
import json

import pandas as pd

from lab.config import fingerprint
from lab.paper_run import capabilities, funding, funding_sources, gate, membership
from lab.phase13 import protocol
from lab.phase15 import paper
from lab.storage import read_json, utc, utc_now, writer_lock


VERSION = "PRESERVED_FORWARD_FUNDING_RECONCILIATION_V1"
KIND = "FUNDING_RECONCILIATION_APPROVED"
FIELDS = {"protocol", "generation", "slot", "book_id", "position_id", "settlement_at",
          "observation_event_id", "observation_sha256", "observation_body_sha256",
          "review_token", "request_id", "reason"}


def _token(db, record, lane, book):
    position = book.get("position") or {}
    economics = {name: position.get(name) for name in (
        "id", "entry_at", "deadline", "stop", "target", "position", "funding_references",
        "settled_funding", "exit_observed", "funding_reconciliation")}
    receipts = db.execute("SELECT key FROM event WHERE book=? AND kind IN ('FUNDING',?, 'CLOSED','EXIT_OBSERVED') ORDER BY rowid",
                          (book["book_id"], KIND)).fetchall()
    return fingerprint([VERSION, record["id"], lane["book_id"], economics,
                        {k: book.get(k) for k in ("cash", "funding", "fees", "gross", "liquidation_cost", "unfunded_deficit")}, receipts])


def _context(db, record, slot):
    from lab.paper_run import runtime

    lanes = membership.validate_accounts(db, record)
    if slot not in lanes:
        raise ValueError("FUNDING_UNREGISTERED_BOOK")
    lane = lanes[slot]
    if capabilities.contract(lane)["funding"] != "KUCOIN_SETTLED":
        raise ValueError("FUNDING_RECOVERY_ADAPTER_UNSUPPORTED")
    book = runtime._load_book(db, lane)
    if not book.get("position"):
        raise ValueError("FUNDING_RECOVERY_REQUIRES_CURRENT_POSITION_OR_GENUINE_EXIT_ACCOUNTING_OBLIGATION")
    return lane, book


def inspect_book(db, record, slot, at, metadata):
    lane, book = _context(db, record, slot)
    assessment = funding.assess(db, book, lane, record["body"], at, metadata, generation=record["id"])
    candidates = {}
    for item in assessment.get("obligations", []):
        due = item.get("slot") or item.get("settlement_at") or item.get("due_at")
        if due:
            candidates[due] = funding_sources.archived_forward(db, lane, book["position"], utc(due), at)
    return {"mode": "READ_ONLY_FUNDING_REVIEW", "protocol": VERSION, "generation": record["id"],
            "slot": slot, "book_id": book["book_id"], "position_id": book["position"]["id"],
            "review_token": _token(db, record, lane, book), "funding": assessment,
            "preserved_forward_candidates": candidates,
            "unsupported": ["NATIVE_CANDLE_OR_CURRENT_MARK_SUBSTITUTION", "APPLIED_RATE_DELTA_OR_CLOSED_HISTORY_RESTATEMENT"],
            "note": "A later projection recovery is labelled financial reconciliation, never a newly timely market observation."}


def _adopted(record, at):
    from lab.paper_run import runtime

    heartbeat = runtime.read_status_snapshot("last-cycle.json", {})
    if (heartbeat.get("state") != "RUNNING" or heartbeat.get("generation") != record["id"]
            or heartbeat.get("runner_build_id") != runtime.RUNNER_BUILD
            or not pd.Timedelta(0) <= at-utc(heartbeat["at"]) < pd.Timedelta(seconds=45)):
        raise ValueError("FUNDING_REVIEWED_WRITER_BUILD_NOT_ADOPTED")
    if not runtime.synchronized(at):
        raise ValueError("FUNDING_RECONCILIATION_CLOCK_UNVERIFIED")


def restore(db, record, request, at, metadata, *, apply=False, approved=False):
    if not isinstance(request, dict) or set(request) != FIELDS or request.get("protocol") != VERSION:
        raise ValueError("FUNDING_RECONCILIATION_REQUEST_SCHEMA")
    if request["generation"] != record["id"]:
        raise ValueError("FUNDING_RECONCILIATION_GENERATION_MISMATCH")
    for key in FIELDS:
        if not isinstance(request[key], str) or not request[key].strip() or len(request[key]) > 1000:
            raise ValueError("FUNDING_RECONCILIATION_REQUEST_IDENTITY")
    digest = fingerprint(request)
    for key, raw in db.execute("SELECT key,body FROM event WHERE kind=?", (KIND,)):
        body = json.loads(raw)
        if body.get("request_id") == request["request_id"]:
            if body.get("request_sha256") != digest:
                raise ValueError("FUNDING_RECONCILIATION_REQUEST_REUSED")
            return {"state": "ALREADY_APPLIED", "event_id": key, "record": body}
    lane, book = _context(db, record, request["slot"])
    position = book["position"]
    if book["book_id"] != request["book_id"] or position["id"] != request["position_id"]:
        raise ValueError("FUNDING_RECONCILIATION_POSITION_CHANGED")
    if request["review_token"] != _token(db, record, lane, book):
        raise ValueError("FUNDING_RECONCILIATION_REVIEW_TOKEN_CHANGED")
    slot = execution_stamp(request["settlement_at"])
    assessment = funding.assess(db, book, lane, record["body"], at, metadata, generation=record["id"])
    if (assessment.get("schedule") or {}).get("state") != "COMPATIBLE":
        raise ValueError("FUNDING_RECONCILIATION_SCHEDULE_REVIEW_REQUIRED")
    obligations = {item.get("slot") or item.get("settlement_at") or item.get("due_at") for item in assessment.get("obligations", [])}
    if slot.isoformat() not in obligations:
        raise ValueError("FUNDING_RECONCILIATION_NOT_AN_EXPECTED_OBLIGATION")
    if db.execute("SELECT 1 FROM event WHERE book=? AND idea=? AND kind='FUNDING' AND source_at=?",
                  (book["book_id"], position["id"], slot.isoformat())).fetchone():
        raise ValueError("FUNDING_ALREADY_APPLIED_NO_RECONSTRUCTION_ALLOWED")
    if slot.isoformat() in position.get("funding_references", {}):
        raise ValueError("FUNDING_FORWARD_REFERENCE_ALREADY_PRESENT")
    candidates = funding_sources.archived_forward(db, lane, position, slot, at)
    matching = [item for item in candidates if item["observation_identity"]["event_id"] == request["observation_event_id"]
                and item["observation_sha256"] == request["observation_sha256"]
                and item["observation_body_sha256"] == request["observation_body_sha256"]]
    if len(matching) != 1:
        raise ValueError("FUNDING_PRESERVED_FORWARD_EVIDENCE_NOT_VERIFIED")
    candidate = matching[0]
    detail = {"protocol": VERSION, "generation": record["id"], "book_id": book["book_id"],
              "position_id": position["id"], "settlement_at": slot.isoformat(),
              "request_id": request["request_id"], "request_sha256": digest, "reason": request["reason"],
              "operator": getpass.getuser(), "reviewed_at": at.isoformat(),
              "recovery_basis": "PRESERVED_ACCEPTED_FORWARD_EVIDENCE", "evidence": candidate,
              "historical_liquidation_path": "NOT_RETROACTIVELY_VERIFIED", "research_provisional": True}
    if not apply:
        return {"state": "FUNDING_RECONCILIATION_PREVIEW", "would_restore": detail,
                "financial_effect": "Only normal writer settlement with admitted native rate; no historical fill or exit is generated."}
    if approved is not True:
        raise PermissionError("FUNDING_RECONCILIATION_EXPLICIT_APPROVAL_REQUIRED")
    if not db.in_transaction:
        raise ValueError("FUNDING_RECONCILIATION_REQUIRES_AUTHORITATIVE_WRITER_TRANSACTION")
    _adopted(record, at)
    event_id = fingerprint([VERSION, record["id"], request["request_id"]])
    db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)", (event_id, book["book_id"], KIND, position["id"],
               at.isoformat(), at.isoformat(), json.dumps(detail, sort_keys=True, allow_nan=False)))
    obs = candidate["observation"]
    position.setdefault("funding_references", {})[slot.isoformat()] = {
        "price": obs["price"], "source_at": obs["source_at"], "received_at": obs["received_at"],
        "origin": "PRESERVED_ACCEPTED_FORWARD_EVIDENCE", "restored_at": at.isoformat(),
        "recovery_label": "PRESERVED_ACCEPTED_FORWARD_EVIDENCE", "historical_reconciliation": True,
        "reconciliation_event_id": event_id, "evidence": deepcopy(candidate)}
    position["funding_provisional"] = True
    position.setdefault("funding_reconciliation", []).append({"event_id": event_id, "settlement_at": slot.isoformat(),
                                                             "basis": detail["recovery_basis"]})
    funding.settle(db, book, lane, record["body"], at, metadata, generation=record["id"])
    paper.save(db, book)
    return {"state": "FUNDING_RECONCILIATION_APPLIED", "event_id": event_id, "record": detail,
            "funding": (book.get("position") or {}).get("funding_state"), "nominal_cash": book["cash"],
            "historical_results_validated": False}


def execution_stamp(value):
    from lab.paper_run.execution import stamp

    return stamp(value)


def main():
    from lab.paper_run import runtime

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    inspect_parser = commands.add_parser("inspect")
    inspect_parser.add_argument("--slot", required=True)
    restoration = commands.add_parser("restore-forward-valuation")
    restoration.add_argument("--request", required=True)
    restoration.add_argument("--apply", action="store_true")
    restoration.add_argument("--approve", action="store_true")
    args = parser.parse_args()
    record = runtime.current()
    if not record:
        raise ValueError("No current registered paper generation")
    if args.action == "inspect":
        with closing(runtime.connect(readonly=True)) as db:
            result = inspect_book(db, record, args.slot, utc_now(), protocol.current_metadata())
    elif not args.apply:
        with closing(runtime.connect(readonly=True)) as db:
            result = restore(db, record, read_json(args.request), utc_now(), protocol.current_metadata())
    else:
        with writer_lock(gate.ROOT / ".writer.lock"), closing(runtime.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            result = restore(db, record, read_json(args.request), utc_now(), protocol.current_metadata(), apply=True, approved=args.approve)
    print(json.dumps(result, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
