"""Operator commands for the current seven-slot paper generation."""

import argparse
from contextlib import closing
import getpass
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
from uuid import uuid4

import pandas as pd

from lab.config import fingerprint
from lab.paper_run import archive, ema, fresh_reset, gate, kraken_live, runtime, source_amendment
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


ARCHIVE_PARENT = Path.home() / "ASEKE Paper Archives"
PREFLIGHT_AGE = pd.Timedelta(minutes=15)


def _source_code():
    return {name: file_hash(runtime.PROJECT / "lab/paper_run" / name)
            for name in ("runtime.py", "execution.py", "sources.py", "ema.py")}


def probe(save=None):
    qualifications = runtime.probe()
    if save is not None:
        body = {"at": utc_now().isoformat(), "protocol": runtime.VERSION,
                "project": str(runtime.PROJECT), "qualifications": qualifications, "code": _source_code()}
        write_json(save, {"id": fingerprint(body), "body": body})
    return qualifications


def _load_sealed(path):
    record = read_json(path)
    if fingerprint(record["body"]) != record["id"]:
        raise ValueError(f"Saved paper cutover checksum differs: {path}")
    return record["body"]


def _preflight(path):
    if path is None:
        return runtime.probe()
    body = _load_sealed(path)
    if (body["protocol"] != runtime.VERSION or body["project"] != str(runtime.PROJECT)
            or body["code"] != _source_code()
            or not pd.Timedelta(0) <= utc_now()-utc(body["at"]) <= PREFLIGHT_AGE):
        raise ValueError("Saved paper preflight is stale or outside this project and protocol")
    return body["qualifications"]


def _prepared():
    path = gate.ROOT / "prepared-cutover.json"
    return _load_sealed(path) if path.exists() else None


def _book_rows():
    if not (gate.ROOT / "paper.sqlite3").is_file():
        return False
    with closing(runtime.connect(readonly=True)) as db:
        return bool(db.execute("SELECT 1 FROM book LIMIT 1").fetchone())


def _definition(archived, qualification):
    previous = _prepared()
    now = utc_now()
    if previous and previous["archive_id"] == archived["id"]:
        body = previous["generation"]
        if _book_rows() or (utc(body["start"]) > now and qualification is None):
            return body, previous["qualifications"]
    qualifications = qualification or (previous or {}).get("qualifications")
    if not qualifications:
        raise ValueError("No live source qualification for the proposed generation")
    body = runtime.definitions(archived, qualifications, now)
    preparation = {"archive_id": archived["id"], "qualifications": qualifications, "generation": body}
    write_json(gate.ROOT / "prepared-cutover.json", {"id": fingerprint(preparation), "body": preparation})
    return body, qualifications


def _notifications(archived, body):
    from lab.health import fresh_paper, state

    generation = fingerprint(body)
    key = f"fresh_paper_cutover:{generation}"
    with state.connect() as db, db:
        prior = state.get_meta(db, key)
        archived_body = archived["body"]
        queued = fresh_paper.supersede_legacy(
            db, archived_body["legacy_books"], archived_body["legacy_ideas"], generation, utc(archived_body["cutoff"]))
        identifiers = sorted(set(json.loads(prior) if prior else []) | set(queued))
        state.set_meta(db, key, json.dumps(identifiers))
        for slot, lane in body["lanes"].items():
            cursor = fresh_paper.CURSOR_PREFIX + fingerprint([generation, slot, lane["book_id"]])
            if state.get_meta(db, cursor) is None:
                state.set_meta(db, cursor, 0)
    return identifiers


def cutover(archive_parent=ARCHIVE_PARENT, preflight=None):
    """Prepare, retire, and initialize under the old owners' writer locks."""
    qualification = None
    previous = _prepared()
    if not runtime.current() and (preflight is not None or not previous or (
            not gate.retired() and utc_now()-utc(previous["generation"]["registered_at"]) > PREFLIGHT_AGE)):
        qualification = _preflight(preflight)
    with archive.cutover_locks():
        if gate.reset_pending():
            raise ValueError("Resume the pending generation reset before cutover")
        archived = archive.create_locked(archive_parent)
        current = runtime.current()
        if not current:
            body, qualifications = _definition(archived, qualification)
        archive.retire_locked(archived)
        superseded = []

        def before_publish(record):
            superseded.extend(_notifications(archived, record["body"]))

        if current:
            before_publish(current)
        else:
            current = runtime.initialize(archived, qualifications, prepared=body, before_publish=before_publish)
        body = current["body"]
        cleanup = archive.apply_cleanup_locked(archived)
        preservation = archive.protected_hashes() == archived["body"]["protected_hashes"]
    return {"state": "CURRENT", "generation_id": current["id"], "archive_id": archived["id"],
            "archive": archived["body"]["path"], "cutoff": body["cutoff"], "start": body["start"],
            "books": {slot: lane["book_id"] for slot, lane in body["lanes"].items()},
            "legacy_outbox_superseded": len(superseded), "cleanup": cleanup,
            "protected_registration_hashes_match": preservation, "runner": runtime.ensure_runner()}


def entries_off():
    """Pause current entries while keeping the protection runner active."""
    with writer_lock(gate.ROOT / ".writer.lock"):
        record = runtime.current()
        if not record or gate.reset_pending():
            raise ValueError("No current paper generation to pause")
        path = gate.ROOT / "entries-paused"
        if not path.exists():
            write_json(path, {"paused_at": utc_now().isoformat(), "generation": record["id"]})
    return {"state": "CURRENT_ENTRIES_PAUSED", "generation": record["id"], "runner": runtime.ensure_runner()}


def _entry_pause(generation, now):
    record = runtime.current()
    if not record or record["id"] != generation or gate.reset_pending():
        raise ValueError("The requested generation does not match the current paper generation")
    path = gate.ROOT / "entries-paused"
    if path.is_symlink():
        raise ValueError("The entry pause marker must be a regular file")
    if not path.exists():
        return path, None, None
    raw = path.read_bytes()
    marker = json.loads(raw)
    if not isinstance(marker, dict) or marker.get("generation") != generation:
        raise ValueError("The entry pause marker does not match the current generation")
    paused = pd.Timestamp(marker.get("paused_at"))
    if pd.isna(paused) or paused.tzinfo is None or utc(paused) > now:
        raise ValueError("The entry pause timestamp is invalid or future dated")
    return path, raw, marker


def _expected_entry_pause(raw, expected_pause_sha256):
    digest = sha256(raw).hexdigest() if raw is not None else None
    if expected_pause_sha256 is not None and expected_pause_sha256 != digest:
        raise ValueError("The current entry pause does not match the approved SHA-256")
    return digest


def entries_on(generation, *, approve=False, expected_pause_sha256=None):
    """Preview release of the current entry pause; approval records authority before release."""
    now = utc_now()
    path, raw, marker = _entry_pause(generation, now)
    digest = _expected_entry_pause(raw, expected_pause_sha256)
    if approve is not True:
        return {"state": "ENTRY_RESUME_PREVIEW", "generation": generation,
                "paused": raw is not None, "requires_explicit_operator_approval": True,
                "pause_marker": marker, "pause_sha256": digest}
    with writer_lock(gate.ROOT / ".writer.lock"):
        now = utc_now()
        path, raw, marker = _entry_pause(generation, now)
        digest = _expected_entry_pause(raw, expected_pause_sha256)
        if raw is None:
            return {"state": "CURRENT_ENTRIES_ALREADY_ENABLED", "generation": generation}
        reset_intent = gate.reset_intent()
        if reset_intent and reset_intent["successor"] == generation:
            from lab.paper_run import protection_incidents

            with closing(runtime.connect(readonly=True)) as db:
                policy = protection_incidents.active(db)
            if not policy or policy["generation"] != generation:
                raise ValueError("Activate the current generation's protection policy before enabling entries")
        identifier = uuid4().hex
        authorization = {"schema": "CURRENT_ENTRY_CONTROL_AUTHORIZATION_V1", "id": identifier,
                         "generation": generation, "action": "ENTRIES_ON", "operator": getpass.getuser(),
                         "authorized_at": now.isoformat(), "pause_sha256": digest,
                         "pause_marker": marker}
        folder = gate.ROOT / "entries-controls"
        folder.mkdir(parents=True, exist_ok=True)
        saved = folder / f"{identifier}.json"
        encoded = json.dumps(authorization, indent=2, sort_keys=True, allow_nan=False).encode("utf-8")
        with saved.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if saved.read_bytes() != encoded or path.is_symlink() or path.read_bytes() != raw:
            raise ValueError("Entry-control evidence changed before the pause could be released")
        path.unlink()
        return {"state": "CURRENT_ENTRIES_ENABLED", "generation": generation,
                "authorization_path": str(saved), "authorization_sha256": sha256(encoded).hexdigest()}


def _kraken_qualification(path, *, max_age=None):
    if path is None:
        raise ValueError("Supply a current Kraken measurement to prepare a new generation")
    digest = file_hash(path)
    report = read_json(path)
    if file_hash(path) != digest:
        raise ValueError("Kraken measurement changed while it was being read")
    now = utc_now()
    feed = report["feed"]
    counts = feed["counts"]
    ready = report.get("age_at_decision_readiness_seconds") or {}
    if (report["endpoint"] != source_amendment.ENDPOINT or report["source"] != ema.SOURCE or report["symbol"] != ema.SYMBOL
            or not utc(report["started_at"]) < utc(report["ended_at"]) <= now
            or (max_age is not None and now - utc(report["ended_at"]) > max_age)
            or not 120 <= report["duration_seconds"] <= 301 or not report["forced_reconnect_requested"]
            or counts.get("subscriptions_acknowledged", 0) < 2 or counts.get("queue_dropped", 0)
            or not ready.get("count") or not 0 <= ready["max"] <= 2
            or feed["state"] != "STREAMING" or utc(feed["last_trade_received_at"]) < utc(feed["subscription_ack_at"])):
        raise ValueError("Measurement did not demonstrate bounded fresh BTC/USD trades and reconnect recovery")
    if not runtime.synchronized(now):
        raise ValueError("Workstation clock synchronization could not be verified")
    kraken_live.permission(now)
    return {"state": "QUALIFIED", "reason": None, "qualified_at": now.isoformat(),
            "measurement_sha256": digest, "measurement": report}


def amend_ema_source(path):
    record = runtime.current()
    if not record:
        raise ValueError("No current generation to amend")
    if prior := source_amendment.active(record):
        return prior
    qualification = _kraken_qualification(path)
    now = utc(qualification["qualified_at"])
    return source_amendment.install(record, qualification, now=now)


def reset(generation, archive_parent, measurement):
    def qualify():
        qualifications = runtime.probe()
        return qualifications, _kraken_qualification(measurement, max_age=PREFLIGHT_AGE)

    return fresh_reset.reset(generation, archive_parent, qualify)


def main():
    parser = argparse.ArgumentParser(prog="python -m lab.paper_run")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    commands.add_parser("run")
    entries = commands.add_parser("entries")
    entries.add_argument("state", choices=("off", "on"))
    entries.add_argument("--generation", help="Exact current generation required for entries on")
    entries.add_argument("--expected-pause-sha256", help="Refuse to release a different pause marker")
    entries.add_argument("--approve", action="store_true", help="Explicitly approve releasing the current entry pause")
    commands.add_parser("probe").add_argument("--save", type=Path)
    operator_close = commands.add_parser("operator-close", help="Preview a targeted administrative paper exit")
    operator_close.add_argument("--request", type=Path, required=True)
    operator_close.add_argument("--apply", action="store_true")
    operator_close.add_argument("--approve", action="store_true", help="Approve only the exact requested paper position")
    commands.add_parser("amend-ema-source").add_argument("--measurement", required=True, type=Path)
    commands.add_parser("verify-archive").add_argument("path", type=Path)
    transition = commands.add_parser("cutover")
    transition.add_argument("--archive-parent", type=Path, default=ARCHIVE_PARENT)
    transition.add_argument("--preflight", type=Path)
    commands.add_parser("stop", help="Stop and fence the exact current generation's runner").add_argument(
        "--generation", required=True)
    fresh = commands.add_parser("reset", help="Archive the stopped generation and initialize a paused successor")
    fresh.add_argument("--generation", required=True)
    fresh.add_argument("--archive-parent", type=Path, default=ARCHIVE_PARENT)
    fresh.add_argument("--kraken-measurement", type=Path, help="A fresh reconnect measurement, required for the first attempt")
    protection = commands.add_parser("protection")
    operations = protection.add_subparsers(dest="operation", required=True)
    operations.add_parser("report").add_argument("--slot", required=True)
    for operation in ("activate", "adjudicate", "resume"):
        action = operations.add_parser(operation)
        action.add_argument("--request", type=Path, required=True)
        action.add_argument("--apply", action="store_true")
        action.add_argument("--approve", action="store_true", help="Explicitly authorize the reviewed state change")
    args = parser.parse_args()
    if args.command == "run":
        try:
            runtime.run()
        except KeyboardInterrupt:
            pass
        return
    actions = {
        "status": runtime.status,
        "operator-close": lambda: operator_close_action(args),
        "probe": lambda: probe(args.save),
        "amend-ema-source": lambda: amend_ema_source(args.measurement),
        "entries": lambda: entries_off() if args.state == "off" else entries_on(
            args.generation, approve=args.approve, expected_pause_sha256=args.expected_pause_sha256),
        "verify-archive": lambda: archive.verify(args.path),
        "cutover": lambda: cutover(args.archive_parent, args.preflight),
        "stop": lambda: fresh_reset.request_stop(args.generation),
        "reset": lambda: reset(args.generation, args.archive_parent, args.kraken_measurement),
        "protection": lambda: protection_action(args),
    }
    try:
        print(json.dumps(actions[args.command](), indent=2, default=str, allow_nan=False))
    except (ValueError, OSError, RuntimeError, sqlite3.Error) as error:
        parser.exit(1, f"Paper run: {error}\n")


def protection_action(args):
    from lab.paper_run.protection_commands import command

    return command(args)


def operator_close_action(args):
    from lab.paper_run.operator_close import command

    return command(args)


if __name__ == "__main__":
    main()
