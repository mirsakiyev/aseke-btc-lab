"""Versioned future membership projected over the untouched sealed registration."""

from contextlib import closing
from copy import deepcopy
import argparse
import getpass
import json
from pathlib import Path
import re
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.paper_run import capabilities, gate, source_amendment
from lab.phase15 import paper
from lab.storage import read_json, utc, utc_now, writer_lock


SCHEMA = "PAPER_MEMBERSHIP_AMENDMENT_V1"
KIND = "PAPER_BOOK_REGISTERED_V1"
MAX_MEMBERS = 64


def _amendments(db, record):
    for key, book_id, idea, raw in db.execute("SELECT key,book,idea,body FROM event WHERE kind=? ORDER BY rowid", (KIND,)):
        body = json.loads(raw)
        if body.get("schema") != SCHEMA or body.get("generation") != record["id"]:
            raise ValueError("MEMBERSHIP_AMENDMENT_GENERATION_OR_SCHEMA_MISMATCH")
        if body.get("amendment_id") != fingerprint(body["amendment"]):
            raise ValueError("MEMBERSHIP_AMENDMENT_HASH_MISMATCH")
        value = body["amendment"]
        request = {"generation": value.get("generation"), "slot": value.get("slot"), "lane": value.get("lane"),
                   "request_id": value.get("request_id"), "reason": body.get("reason")}
        if (value.get("generation") != record["id"] or book_id != (value.get("lane") or {}).get("book_id")
                or idea != record["id"] or key != fingerprint([SCHEMA, record["id"], value.get("request_id")])
                or body.get("request_sha256") != fingerprint(request)):
            raise ValueError("MEMBERSHIP_AMENDMENT_IDENTITY_MISMATCH")
        yield key, body


def lanes(record, amendment=None, *, db=None):
    """Registered expected membership, including subsequently admitted books."""
    if db is None:
        from lab.paper_run import runtime
        with closing(runtime.connect(readonly=True)) as connection:
            connection.execute("BEGIN")
            return lanes(record, amendment, db=connection)
    if amendment is None:
        amendment = source_amendment.active(record)
    result = deepcopy(source_amendment.lanes(record, amendment))
    identifiers = {lane["book_id"] for lane in result.values()}
    if len(identifiers) != len(result):
        raise ValueError("REGISTERED_BOOK_IDENTITIES_NOT_UNIQUE")
    for _, event in _amendments(db, record):
        value = event["amendment"]
        slot, lane = value["slot"], value["lane"]
        if slot in result or lane["book_id"] in identifiers:
            raise ValueError("MEMBERSHIP_REPLACES_AN_EXISTING_BOOK")
        # Registration validated capabilities before the immutable append. Keep
        # admitted membership authoritative even if a later implementation no
        # longer supports a member; per-book status can then report that member
        # without suppressing every other registered account.
        result[slot] = deepcopy(lane)
        identifiers.add(lane["book_id"])
    return result


def expected_book_ids(record, amendment=None, *, db=None):
    return {value["book_id"] for value in lanes(record, amendment, db=db).values()}


def validate_accounts(db, record, amendment=None):
    registered = lanes(record, amendment, db=db)
    actual = {row[0] for row in db.execute("SELECT id FROM book")}
    expected = {value["book_id"] for value in registered.values()}
    if actual != expected:
        raise ValueError("REGISTERED_BOOK_MEMBERSHIP_MISMATCH: missing=" + repr(sorted(expected-actual)) +
                         "; unexpected=" + repr(sorted(actual-expected)))
    return registered


def _validate_request(db, record, request, at):
    if not isinstance(request, dict) or set(request) != {"generation", "slot", "lane", "request_id", "reason"}:
        raise ValueError("Membership request requires generation, slot, lane, request_id and reason")
    if request["generation"] != record["id"]:
        raise ValueError("MEMBERSHIP_GENERATION_MISMATCH")
    if not all(isinstance(request.get(k), str) and request[k].strip() and len(request[k]) <= 500
               for k in ("slot", "request_id", "reason")) or not re.fullmatch(r"[a-z0-9][a-z0-9_/-]{0,127}", request["slot"]):
        raise ValueError("INVALID_MEMBERSHIP_REQUEST_IDENTITY")
    lane = request["lane"]
    if not isinstance(lane, dict):
        raise ValueError("INVALID_MEMBERSHIP_LANE")
    capabilities.validate_registration(lane, generation=record["id"], slot=request["slot"])
    registered = lanes(record, db=db)
    if len(registered) >= MAX_MEMBERS:
        raise ValueError("MEMBERSHIP_RESOURCE_LIMIT")
    if request["slot"] in registered or lane["book_id"] in {value["book_id"] for value in registered.values()}:
        raise ValueError("MEMBERSHIP_REPLACES_AN_EXISTING_BOOK")
    start = utc(record["body"]["start"])
    end, follow = (utc(lane[key]) for key in ("publication_end_exclusive", "follow_end_exclusive"))
    step = pd.Timedelta(lane["timeframe"])
    if step <= pd.Timedelta(0) or not start <= at < end <= follow:
        raise ValueError("INVALID_MEMBERSHIP_STUDY_WINDOW")
    # Future registration is admission to existing reference capabilities, never
    # permission for a new endpoint or a new source subscription.
    scope = capabilities.source_scope_id(lane)
    existing_scopes = {capabilities.source_scope_id(item) for item in registered.values()}
    if scope not in existing_scopes:
        raise ValueError("SOURCE_SCOPE_REQUIRES_SEPARATE_REVIEW")
    return registered


def register(db, record, request, *, at=None, operator=None, approved=False):
    """Apply one approved amendment inside the caller's existing writer transaction."""
    if approved is not True:
        raise PermissionError("Explicit membership approval is required")
    if not db.in_transaction:
        raise ValueError("MEMBERSHIP_REQUIRES_WRITER_TRANSACTION")
    if not isinstance(request, dict):
        raise ValueError("INVALID_MEMBERSHIP_REQUEST")
    at = utc_now() if at is None else utc(at)
    identity = fingerprint([SCHEMA, record["id"], request.get("request_id")])
    prior = db.execute("SELECT body FROM event WHERE key=? AND kind=?", (identity, KIND)).fetchone()
    if prior:
        value = json.loads(prior[0])
        if value.get("request_sha256") != fingerprint(request):
            raise ValueError("Membership request ID was reused with different arguments")
        return {"state": "ALREADY_APPLIED", "event_id": identity, "record": value}
    _validate_request(db, record, request, at)
    lane = deepcopy(request["lane"])
    amendment = {"generation": record["id"], "slot": request["slot"], "lane": lane,
                 "registered_at": at.isoformat(), "request_id": request["request_id"]}
    payload = {"schema": SCHEMA, "generation": record["id"], "amendment": amendment,
               "amendment_id": fingerprint(amendment), "request_sha256": fingerprint(request),
               "operator": operator or getpass.getuser(), "reason": request["reason"]}
    for operation in ("UPDATE", "DELETE"):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS membership_journal_{operation.lower()} BEFORE {operation} ON event "
                   f"WHEN OLD.kind='{KIND}' BEGIN SELECT RAISE(ABORT,'Membership journal is append-only'); END")
    db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)", (
        identity, lane["book_id"], KIND, record["id"], at.isoformat(), at.isoformat(),
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)))
    book = paper.initial(lane)
    paper.event(db, book, "OPENING_BALANCE", "", at, at,
                {"generation": record["id"], "slot": request["slot"], "book_id": lane["book_id"],
                 "opening_balance": book["cash"], "ledger_unit": lane.get("ledger_unit", "NOMINAL_USDT"),
                 "basis": "EXPLICIT_FUTURE_MEMBERSHIP_ENDOWMENT", "membership_event": identity})
    paper.save(db, book)
    _inherit_source_restriction(db, record, lane, book, at)
    paper.save(db, book)
    return {"state": "APPLIED", "event_id": identity, "record": payload}


def _inherit_source_restriction(db, record, lane, book, at):
    from lab.paper_run import operations

    return operations.inherit_source_restriction(db, record, lane, book, at)


def execute(request, *, apply=False, authorized=False):
    """Preview by default; the supported operator mutation owns existing leases."""
    from lab.paper_run import runtime
    record = runtime.current()
    if not record:
        raise ValueError("No current paper generation")
    if apply is not True:
        with closing(runtime.connect(readonly=True)) as db:
            db.execute("BEGIN")
            _validate_request(db, record, request, utc_now())
            return {"state": "PREVIEW", "generation": record["id"], "slot": request["slot"],
                    "book_id": request["lane"]["book_id"], "source_scope_id": capabilities.source_scope_id(request["lane"]),
                    "requires_explicit_operator_approval": True}
    if authorized is not True:
        raise PermissionError("Explicit membership approval is required")
    with writer_lock(gate.ROOT / ".writer.lock"), closing(runtime.connect()) as db, db:
        db.execute("BEGIN IMMEDIATE")
        latest = runtime.current()
        if latest["id"] != record["id"]:
            raise ValueError("MEMBERSHIP_GENERATION_CHANGED")
        return register(db, latest, request, approved=True)


def describe(db, record, amendment=None):
    registered = lanes(record, amendment, db=db)
    books = {}
    for slot, lane in registered.items():
        try:
            books[slot] = {"book_id": lane["book_id"], "state": "SUPPORTED",
                           "source_scope_id": capabilities.source_scope_id(lane),
                           "capabilities": capabilities.contract(lane, generation=record["id"], slot=slot)}
        except (ValueError, TypeError, KeyError) as error:
            books[slot] = {"book_id": lane.get("book_id"), "state": "UNSUPPORTED_CAPABILITIES", "reason": str(error)}
    return {"schema": SCHEMA, "generation": record["id"], "original_registration": record["id"],
            "state": "SUPPORTED" if all(value["state"] == "SUPPORTED" for value in books.values()) else "PARTIAL",
            "books": books}


def status():
    from lab.paper_run import runtime

    record = runtime.current()
    if not record:
        return {"schema": SCHEMA, "state": "UNREGISTERED", "books": {}}
    with closing(runtime.connect(readonly=True)) as db:
        db.execute("BEGIN")
        return describe(db, record)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    commands.add_parser("status")
    command = commands.add_parser("register")
    command.add_argument("--request", type=Path, required=True)
    command.add_argument("--apply", action="store_true")
    command.add_argument("--approve", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = status() if args.operation == "status" else execute(
            read_json(args.request), apply=args.apply, authorized=args.approve)
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        parser.exit(1, f"Paper membership: {error}\n")
    print(json.dumps(result, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
