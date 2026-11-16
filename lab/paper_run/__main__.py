"""Operator commands for the current seven-slot paper generation."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.paper_run import archive, ema, gate, kraken_live, runtime, source_amendment
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
        if not record:
            raise ValueError("No current paper generation to pause")
        path = gate.ROOT / "entries-paused"
        if not path.exists():
            write_json(path, {"paused_at": utc_now().isoformat(), "generation": record["id"]})
    return {"state": "CURRENT_ENTRIES_PAUSED", "generation": record["id"], "runner": runtime.ensure_runner()}


def amend_ema_source(path):
    record = runtime.current()
    if not record:
        raise ValueError("No current generation to amend")
    if prior := source_amendment.active(record):
        return prior
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
            or not 120 <= report["duration_seconds"] <= 301 or not report["forced_reconnect_requested"]
            or counts.get("subscriptions_acknowledged", 0) < 2 or counts.get("queue_dropped", 0)
            or not ready.get("count") or not 0 <= ready["max"] <= 2
            or feed["state"] != "STREAMING" or utc(feed["last_trade_received_at"]) < utc(feed["subscription_ack_at"])):
        raise ValueError("Measurement did not demonstrate bounded fresh BTC/USD trades and reconnect recovery")
    if not runtime.synchronized(now):
        raise ValueError("Workstation clock synchronization could not be verified")
    kraken_live.permission(now)
    qualification = {"state": "QUALIFIED", "reason": None, "qualified_at": now.isoformat(),
                     "measurement_sha256": digest, "measurement": report}
    return source_amendment.install(record, qualification, now=now)


def main():
    parser = argparse.ArgumentParser(prog="python -m lab.paper_run")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "run"):
        commands.add_parser(name)
    commands.add_parser("entries").add_argument("state", choices=("off",))
    commands.add_parser("probe").add_argument("--save", type=Path)
    commands.add_parser("amend-ema-source").add_argument("--measurement", required=True, type=Path)
    commands.add_parser("verify-archive").add_argument("path", type=Path)
    transition = commands.add_parser("cutover")
    transition.add_argument("--archive-parent", type=Path, default=ARCHIVE_PARENT)
    transition.add_argument("--preflight", type=Path)
    args = parser.parse_args()
    if args.command == "run":
        try:
            runtime.run()
        except KeyboardInterrupt:
            pass
        return
    actions = {
        "status": runtime.status,
        "probe": lambda: probe(args.save),
        "amend-ema-source": lambda: amend_ema_source(args.measurement),
        "entries": entries_off,
        "verify-archive": lambda: archive.verify(args.path),
        "cutover": lambda: cutover(args.archive_parent, args.preflight),
    }
    try:
        print(json.dumps(actions[args.command](), indent=2, default=str, allow_nan=False))
    except (ValueError, OSError, RuntimeError, sqlite3.Error) as error:
        parser.exit(1, f"Paper run: {error}\n")


if __name__ == "__main__":
    main()
