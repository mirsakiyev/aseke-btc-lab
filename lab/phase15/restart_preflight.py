"""Read-only safety gate for restarting the existing Phase 15 observer."""

from contextlib import closing
from datetime import datetime
from hashlib import sha256
import json
from math import isfinite
from pathlib import Path
import sqlite3
import sys


ORIGINAL_REGISTRATION_ID = "00850dad14451b34635ece9adba09b1d29263ff63cb01eeab68d5bb4e4d16ae4"
LANES = {"day/fixed", "swing/fixed", "swing/trail", "position/fixed"}
BALANCE_FIELDS = (
    "starting_equity", "cash", "equity", "peak_equity", "drawdown", "funding", "fees",
    "liquidation_cost", "turnover", "gross", "raw_gross", "unfunded_deficit",
)


class PreflightError(ValueError):
    """A failure with a fixed, operator-safe explanation."""


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PreflightError("Duplicate JSON field")
        result[key] = value
    return result


def _object(payload):
    result = json.loads(payload, object_pairs_hook=_unique_object)
    if not isinstance(result, dict):
        raise PreflightError("Expected a JSON object")
    return result


def _digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _registered_books(root, expected_id):
    pointer = _object((root / "registration.json").read_text(encoding="utf-8"))
    if pointer != {"id": expected_id}:
        raise PreflightError("The original Phase 15 registration is missing or different")
    record = _object((root / "registrations" / f"{expected_id}.json").read_text(encoding="utf-8"))
    body = record["body"]
    if record["id"] != expected_id or _digest(body) != expected_id:
        raise PreflightError("The original Phase 15 registration checksum differs")
    lanes = body["lanes"]
    if set(lanes) != LANES or body["policy"] != "prospective-native-minute-paper-v1":
        raise PreflightError("Expected the four original Phase 15 lanes")
    books = {}
    for name, lane in lanes.items():
        definition = dict(lane)
        book_id = definition.pop("book_id")
        if book_id != _digest([body["policy"], name, definition, body["start"]]) or book_id in books:
            raise PreflightError("An original Phase 15 book identity differs")
        books[book_id] = lane["risk"]["starting_equity"]
    return books


def _verify_state(payload, book_id, starting_equity):
    state = _object(payload)
    if state["book_id"] != book_id:
        raise PreflightError("Account state does not match its registered book")
    if any(state[field] is not None for field in ("position", "pending", "cursor", "problem")):
        raise PreflightError("A Phase 15 book has exposure, pending work, or unresolved state")
    if state["permanent_block"] is not False:
        raise PreflightError("A Phase 15 book has a permanent block or invalid block state")
    if any(type(state[field]) not in (int, float) or not isfinite(state[field]) for field in BALANCE_FIELDS):
        raise PreflightError("A Phase 15 book has malformed accounting state")
    if state["starting_equity"] != starting_equity or state["cash"] != state["equity"]:
        raise PreflightError("A Phase 15 book has inconsistent flat accounting")
    for field in ("last_source_close", "last_source_received_at"):
        value = state[field] if field == "last_source_close" else state.get(field)
        if value is not None and datetime.fromisoformat(value).utcoffset() is None:
            raise PreflightError("A Phase 15 book has an invalid source clock")


def verify(project_root, expected_registration_id=ORIGINAL_REGISTRATION_ID):
    """Validate the sealed book identities and committed ledger in one read transaction."""
    root = Path(project_root).resolve() / "runs" / "phase15"
    books = _registered_books(root, expected_registration_id)
    path = root / "paper.sqlite3"
    if not path.is_file() or path.resolve() != path:
        raise PreflightError("Expected the canonical Phase 15 state database")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
        db.execute("BEGIN")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"book", "event"} <= tables:
            raise PreflightError("The Phase 15 account or event table is missing")
        rows = db.execute("SELECT id,state FROM book LIMIT 5").fetchall()
        if len(rows) != len(books) or {row[0] for row in rows} != books.keys():
            raise PreflightError("A Phase 15 account is missing, unexpected, or duplicated")
        for book_id, payload in rows:
            _verify_state(payload, book_id, books[book_id])
        if db.execute("SELECT 1 FROM event WHERE book IS NULL OR book NOT IN (?,?,?,?) LIMIT 1", tuple(books)).fetchone():
            raise PreflightError("The ledger contains an unregistered paper book")
        unfinished = db.execute(
            "SELECT 1 FROM event WHERE kind IN ('PUBLISHED','HYPOTHETICAL_FILL','CLOSED','EXPIRED','INVALIDATED') "
            "GROUP BY book,idea HAVING idea IS NULL OR idea='' OR SUM(kind='PUBLISHED')!=1 "
            "OR SUM(kind IN ('CLOSED','EXPIRED','INVALIDATED'))!=1 OR SUM(kind='HYPOTHETICAL_FILL')!=SUM(kind='CLOSED') LIMIT 1"
        ).fetchone()
        if unfinished:
            raise PreflightError("The Phase 15 event journal has an unfinished or inconsistent paper lifecycle")


def main():
    try:
        if len(sys.argv) != 1:
            raise PreflightError("The Phase 15 restart preflight accepts no arguments")
        verify(Path(__file__).resolve().parents[2])
    except (OSError, sqlite3.Error, KeyError, TypeError, ValueError, OverflowError) as exc:
        # JSON/parser and filesystem exceptions may include private input.
        detail = str(exc) if isinstance(exc, PreflightError) else type(exc).__name__
        print(f"Phase 15 observer preflight blocked ({detail}).", file=sys.stderr)
        return 1
    print("Phase 15 observer preflight passed for the four original flat books.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
