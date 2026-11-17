"""Serialized, explicitly approved administrative exits for the EMA Day paper book.

The existing managed collector remains the sole market-data owner. An operator
request waits for a new complete trade observation committed by that collector;
it never starts a subscription or interprets a status ``last_mark`` as a quote.
"""

from contextlib import closing
import getpass
import json
import time

from lab.config import fingerprint
from lab.paper_run import execution, gate, runtime, source_amendment
from lab.phase15 import paper
from lab.storage import read_json, utc, writer_lock


SCHEMA = "OPERATOR_PAPER_EXIT_V1"
REQUEST_EVENT = "OPERATOR_PAPER_EXIT_REQUESTED"
EXIT_REASON = "OPERATOR_REQUESTED_PAPER_EXIT"
WAIT_SECONDS = 15.
RETRY_SECONDS = .25
FIELDS = {"generation", "slot", "book_id", "position_id", "request_id", "reason"}


class EvidenceUnavailable(ValueError):
    """A bounded attempt may retry this failure while normal protection runs."""


def _request(request):
    if not isinstance(request, dict) or set(request) != FIELDS:
        raise ValueError("Operator-close request requires exactly the documented identity and reason fields")
    if not all(isinstance(request[key], str) and request[key].strip() for key in FIELDS):
        raise ValueError("Operator-close identities and reason must be nonempty strings")
    if request["slot"] != "ema/day":
        raise ValueError("This operator-close operation supports only the explicitly selected EMA Day book")
    if len(request["request_id"]) > 128 or len(request["reason"]) > 2048:
        raise ValueError("Operator-close request ID or reason exceeds its bound")


def _context(db, request):
    record = runtime.current()
    if not record or record["id"] != request["generation"]:
        raise ValueError("Operator-close generation does not match the current generation")
    amendment = source_amendment.active(record)
    if not amendment:
        raise ValueError("The approved managed Kraken trade amendment is unavailable")
    lanes = source_amendment.lanes(record, amendment)
    lane = lanes.get(request["slot"])
    if not lane or lane["book_id"] != request["book_id"]:
        raise ValueError("Operator-close book does not match the selected slot")
    if (lane.get("source") != "KRAKEN_BTC_USD_LAST_TRADE" or lane.get("symbol") != "BTC/USD"
            or lane.get("observation_kind") != execution.WS_TRADE):
        raise ValueError("Operator close requires the approved Kraken BTC/USD trade reference")
    book = runtime._load_book(db, lane)
    position = book.get("position")
    if position and position["id"] != request["position_id"]:
        raise ValueError("A different position is open; the requested position must never close its successor")
    return record, amendment, lane, book


def _prior(db, request):
    key = fingerprint([SCHEMA, request["request_id"]])
    row = db.execute("SELECT kind,body FROM event WHERE key=?", (key,)).fetchone()
    if row:
        body = json.loads(row[1])
        if row[0] != REQUEST_EVENT or body.get("request_sha256") != fingerprint(request):
            raise ValueError("Operator-close request ID was reused with different arguments")
        return key, body
    return key, None


def _terminal(db, request):
    rows = db.execute("SELECT key,kind,body FROM event WHERE book=? AND idea=? "
                      "AND kind IN ('EXIT_OBSERVED','CLOSED') ORDER BY rowid",
                      (request["book_id"], request["position_id"])).fetchall()
    if not rows:
        return None
    values = {}
    for key, kind, raw in rows:
        if kind in values:
            raise ValueError("The target has duplicate terminal events; reconciliation is required")
        values[kind] = {"event_id": key, "body": json.loads(raw)}
    result = values.get("CLOSED") or values["EXIT_OBSERVED"]
    body = result["body"]
    return {"state": "TARGET_ALREADY_CLOSED" if "CLOSED" in values else "EXIT_ALREADY_OBSERVED",
            "generation": request["generation"], "book_id": request["book_id"],
            "position_id": request["position_id"], "request_id": request["request_id"],
            "operator_request_applied": body.get("operator_request_id") == request["request_id"],
            "financially_closed": "CLOSED" in values, "events": values}


def _healthy(record, amendment, lane):
    at, _ = execution.clock()
    marker = gate.ROOT / "entries-paused"
    if marker.is_symlink() or not marker.is_file():
        raise ValueError("A matching temporary entry pause is required")
    pause = read_json(marker)
    try:
        paused_at = execution.stamp(pause["paused_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("The entry-pause evidence is malformed") from exc
    if pause.get("generation") != record["id"] or paused_at > at:
        raise ValueError("The entry pause generation or timestamp is invalid")
    if lane.get("qualification", {}).get("state") != "QUALIFIED":
        raise ValueError("The current source is not qualified")
    if not runtime.synchronized(at):
        raise EvidenceUnavailable("WORKSTATION_CLOCK_SYNC_UNVERIFIED")
    heartbeat = read_json(gate.ROOT / "last-cycle.json")
    if (heartbeat.get("state") != "RUNNING" or heartbeat.get("generation") != record["id"]
            or heartbeat.get("source_amendment_id") != amendment["id"]
            or type(heartbeat.get("pid")) is not int or heartbeat["pid"] <= 0
            or not heartbeat.get("runner_build_id")
            or not 0 <= (at - execution.stamp(heartbeat["at"])).total_seconds() < 45):
        raise EvidenceUnavailable("A fresh managed collector heartbeat is required")
    stream = read_json(gate.ROOT / "ema-live-status.json")
    if (stream.get("state") != "STREAMING" or stream.get("amendment_id") != amendment["id"]
            or not 0 <= (at - execution.stamp(stream["at"])).total_seconds() < 45):
        raise EvidenceUnavailable("The authorized Kraken transport is not currently streaming")
    runtime.kraken_live.permission(at)
    return heartbeat


def _append_request(db, request, book, at, baseline, heartbeat):
    key, prior = _prior(db, request)
    if prior:
        return key, prior
    body = {"schema": SCHEMA, "request": request, "request_sha256": fingerprint(request),
            "request_id": request["request_id"], "operator": getpass.getuser(),
            "requested_at": at.isoformat(), "baseline_source_event_id": (baseline or {}).get("event_id"),
            "baseline_source_at": (baseline or {}).get("source_at"),
            "generation": request["generation"], "book_id": request["book_id"],
            "position_id": request["position_id"], "runner_pid": heartbeat["pid"],
            "runner_build": heartbeat["runner_build_id"]}
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
    db.execute("INSERT INTO event VALUES (?,?,?,?,?,?,?)", (
        key, book["book_id"], REQUEST_EVENT, request["position_id"], at.isoformat(), at.isoformat(), encoded))
    return key, body


def _new_observation(db, request, approval, book, lane):
    obs = runtime._source_state(db, lane["source"]).get("last_observation")
    at, mono = execution.clock()
    if reason := execution.valid_snapshot(obs, lane, at, mono):
        raise EvidenceUnavailable(reason)
    requested = execution.stamp(approval["requested_at"])
    if min(execution.stamp(obs["source_at"]), execution.stamp(obs["received_at"])) <= requested:
        raise EvidenceUnavailable("WAITING_FOR_NEW_POST_REQUEST_TRADE")
    baseline = approval.get("baseline_source_event_id")
    if baseline is not None and (not str(baseline).isdigit() or int(obs["event_id"]) <= int(baseline)):
        raise EvidenceUnavailable("REFERENCE_REPLAY_OR_PRE_REQUEST_WATERMARK")
    position = book["position"]
    if execution.stamp(obs["source_at"]) <= execution.stamp(position["entry_at"]):
        raise EvidenceUnavailable("REFERENCE_PRECEDES_ENTRY")
    previous_id = str(position.get("mark_event_id", ""))
    if not previous_id.isdigit() or int(obs["event_id"]) < int(previous_id):
        raise EvidenceUnavailable("REFERENCE_PRECEDES_CURRENT_PROTECTION")
    if (execution.stamp(obs["source_at"]) < execution.stamp(position["mark_source_at"])
            or execution.stamp(obs["received_at"]) < execution.stamp(position["mark_received_at"])):
        raise EvidenceUnavailable("REFERENCE_PRECEDES_CURRENT_PROTECTION")
    same = int(obs["event_id"]) == int(previous_id)
    if same and any(obs[observed] != position[accepted] for observed, accepted in (
            ("price", "last_mark"), ("source_at", "mark_source_at"), ("received_at", "mark_received_at"))):
        raise EvidenceUnavailable("CURRENT_PROTECTION_REFERENCE_MISMATCH")
    return obs, same


def _attempt(request):
    with writer_lock(gate.ROOT / ".writer.lock"), closing(runtime.connect()) as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            record, amendment, lane, book = _context(db, request)
            _, approval = _prior(db, request)
            if not approval:
                raise ValueError("A durable approved operator-close request is required")
            if terminal := _terminal(db, request):
                db.rollback()
                return terminal
            if not book.get("position"):
                raise ValueError("The exact target is not open and has no terminal evidence")
            _healthy(record, amendment, lane)
            obs, already_protected = _new_observation(db, request, approval, book, lane)
            decision, decision_mono = execution.clock()
            if reason := execution.valid_snapshot(obs, lane, decision, decision_mono):
                raise EvidenceUnavailable(reason)
            unresolved = runtime._settle(db, book, lane, record["body"], decision, {}, obs)
            # A newer point may still need normal protection processing. Its
            # genuine stop/target/time outcome wins over administrative intent.
            if not already_protected and book.get("position"):
                execution.protect(db, book, lane, obs, unresolved_costs=unresolved, read_clock=execution.clock)
            natural = _terminal(db, request)
            if not natural:
                committed, mono = execution.clock()
                if committed < decision or (reason := execution.valid_snapshot(obs, lane, committed, mono)):
                    raise EvidenceUnavailable(reason or "LOCAL_CLOCK_STEP")
                position = book["position"]
                quantity = position["position"]["remaining"]
                detail = {**execution.evidence(obs, decision, committed, decision_mono, mono),
                          "reason": EXIT_REASON, "exit_at": committed.isoformat(),
                          "exit_committed_at": committed.isoformat(), "operator_intervened": True,
                          "operator_request_id": request["request_id"],
                          "operator_request_sha256": approval["request_sha256"],
                          "operator_requested_at": approval["requested_at"], "operator": approval["operator"],
                          "operator_reason": request["reason"], "operator_request": request,
                          "quantity_closed": quantity, "effective_stop": position["stop"],
                          "target": position["target"], "original_deadline": position["deadline"],
                          "unresolved_protection": bool(position.get("unresolved_protection")),
                          "source_amendment_id": amendment["id"],
                          "evidence_provenance": "NEW_MANAGED_KRAKEN_TRADE_AFTER_OPERATOR_REQUEST"}
                position["exit_observed"] = detail
                paper.event(db, book, "EXIT_OBSERVED", position["id"], committed, committed, detail)
                # The normal accounting path decides whether financing evidence
                # suffices. An observed exit is not necessarily a closed account.
                runtime._settle(db, book, lane, record["body"], committed, {}, obs)
            paper.save(db, book)
            _healthy(record, amendment, lane)
            final_at, final_mono = execution.clock()
            if reason := execution.valid_snapshot(obs, lane, final_at, final_mono):
                raise EvidenceUnavailable("LEDGER_COMMIT_" + reason)
            result = _terminal(db, request)
            db.commit()
            result["state"] = ("OPERATOR_PAPER_EXIT_CLOSED" if result["financially_closed"] else
                               "OPERATOR_PAPER_EXIT_OBSERVED_AWAITING_ACCOUNTING") if not natural else "NATURAL_EXIT_WON_RACE"
            result["cash"] = book["cash"]
            result["entry_control_paused"] = True
            return result
        except BaseException:
            db.rollback()
            raise


def execute(request, *, apply=False, authorized=False):
    """Preview, or durably request and attempt one bounded serialized paper exit."""
    _request(request)
    if apply is not True:
        with closing(runtime.connect(readonly=True)) as db:
            db.execute("BEGIN")
            record, amendment, lane, book = _context(db, request)
            _prior(db, request)
            if terminal := _terminal(db, request):
                return terminal
            if not book.get("position"):
                raise ValueError("The exact target position is not open")
            heartbeat = _healthy(record, amendment, lane)
            return {"state": "OPERATOR_PAPER_EXIT_PREVIEW", "request": request,
                    "request_sha256": fingerprint(request), "runner": heartbeat,
                    "remaining_quantity": book["position"]["position"]["remaining"],
                    "new_post_request_observation_required": True, "requires_explicit_approval": True}
    if authorized is not True:
        raise ValueError("Applying an operator paper exit requires explicit approval")
    with writer_lock(gate.ROOT / ".writer.lock"), closing(runtime.connect()) as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            record, amendment, lane, book = _context(db, request)
            _prior(db, request)
            if terminal := _terminal(db, request):
                db.rollback()
                return terminal
            if not book.get("position"):
                raise ValueError("The exact target position is not open")
            heartbeat = _healthy(record, amendment, lane)
            at, _ = execution.clock()
            baseline = runtime._source_state(db, lane["source"]).get("last_observation")
            _, approval = _append_request(db, request, book, at, baseline, heartbeat)
            db.commit()
        except BaseException:
            db.rollback()
            raise
    end = time.monotonic() + WAIT_SECONDS
    last_reason = "NO_NEW_REFERENCE"
    while True:
        try:
            return _attempt(request)
        except EvidenceUnavailable as exc:
            last_reason = str(exc)
        except RuntimeError as exc:
            if not str(exc).startswith("Another writer owns "):
                raise
            last_reason = "WRITER_CONTENTION"
        if time.monotonic() >= end:
            return {"state": "OPERATOR_PAPER_EXIT_NOT_EXECUTED", "request_id": request["request_id"],
                    "requested_at": approval["requested_at"], "reason": last_reason,
                    "normal_protection_continues": True, "entry_control_paused": True}
        time.sleep(min(RETRY_SECONDS, max(0., end - time.monotonic())))


def command(args):
    return execute(read_json(args.request), apply=args.apply, authorized=args.approve)
