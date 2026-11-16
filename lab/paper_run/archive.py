"""Consistent backups and immutable disposition of the seven delayed-price books."""

import csv
from contextlib import ExitStack, closing, contextmanager
from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import time
from uuid import uuid4

from lab.config import ROOT as PROJECT, fingerprint
from lab.paper_run import gate
from lab.storage import file_hash, put_bytes, read_json, utc, utc_now, write_json, writer_lock


LEGACY = "LEGACY_DELAYED_PRICE_SIMULATION"
EXCLUSION = "EXCLUDED_FROM_CURRENT_FRESH_PRICE_PERFORMANCE"
REASON = "Operator-authorized clean prospective reference-price paper generation"
PHASE15_DATABASE = "runs/phase15/paper.sqlite3"
EMA_DATABASE = "runs/ema_paper/paper.sqlite3"
HEALTH_DATABASE = "runs/health/watchdog.sqlite3"
KRAKEN_REFERENCE = "runs/ema_kraken_reference/source/source.sqlite3"
STATE_DIRECTORY = "runs/paper-run"
BREAKOUT = {"day/fixed", "swing/fixed", "swing/trail", "position/fixed"}
EMA = {"day", "swing", "position"}
REQUIRED_DATABASES = (PHASE15_DATABASE, EMA_DATABASE, HEALTH_DATABASE)
REFERENCE_DATABASES = (
    "runs/ema_paper/proxy/protocol.sqlite3",
    "runs/ema_paper/synthetic/protocol.sqlite3",
    "runs/ema_paper/synthetic/chart-recovery.sqlite3",
    "runs/ema_paper/synthetic/position-seam.sqlite3",
    KRAKEN_REFERENCE,
)
REFERENCE_FILES = (
    "runs/phase15/controls.json", "runs/phase15/last-managed.json",
    "runs/phase13/registration.json", "runs/phase14/amendment.json",
    "runs/ema_kraken_reference/qualification.json", "runs/ema_kraken_reference/reference-process.json",
    "runs/ema_kraken_reference/reference-heartbeat.json", "data/phase10/metadata.json",
    "kucoin-reference.yaml",
)
PROTECTED_FILES = (
    "data/holdout.lock.json", "runs/phase13/registration.json",
    "runs/phase14/amendment.json", "runs/phase14/seed.json",
    "runs/phase16/initial.json", "runs/phase16/controls.json",
    "runs/phase17/registration.json", "runs/phase17/pilot/registration.json",
    "runs/ema_kraken_reference/qualification.json", "runs/kraken_paper/paper.sqlite3",
)
CLEANUP = {
    "runs/phase15/first-managed.json": ("DELETE_REDUNDANT", "Commissioning-cycle export superseded by the cutoff snapshot"),
    "runs/phase15/commissioning-verification.json": ("DELETE_REDUNDANT", "Generated commissioning evidence retained in the archive"),
    "runs/ema_paper/activation-report.json": ("DELETE_REDUNDANT", "Generated legacy account activation export"),
    "runs/phase15/REPORT.md": ("NEEDS_REVIEW", "Historical reports can be linked by private notes"),
    "EMA_PAPER_SOURCE_APPROVAL_DRAFT.json": ("KEEP_SHARED", "Referenced by the independent source eligibility audit"),
    "EMA_SPOT_WARM_START_APPROVAL_DRAFT.json": ("KEEP_SHARED", "Referenced by Spot source qualification"),
    PHASE15_DATABASE: ("ARCHIVE", "Legacy journal stays in place behind retired-writer guards for compatibility readers"),
    EMA_DATABASE: ("ARCHIVE", "Legacy journal stays in place behind retired-writer guards for compatibility readers"),
    KRAKEN_REFERENCE: ("KEEP_SHARED", "Live Kraken historical warmup and original receipt clocks"),
    HEALTH_DATABASE: ("KEEP_ACTIVE", "Preserve prior deliveries, unrelated health incidents and current outbox"),
}
SHA = re.compile(r"[a-f0-9]{64}")


def _path(base, relative):
    candidate = base / relative
    path = candidate.resolve()
    if path == base or not path.is_relative_to(base) or path != candidate:
        raise ValueError(f"Path escapes its expected root: {relative}")
    return path


@contextmanager
def cutover_locks(*, project=PROJECT, wait_seconds=20):
    """Take the same writer locks as the watchdog and paper processors."""
    project = Path(project).resolve()
    deadline = time.monotonic() + wait_seconds
    locks = (f"{STATE_DIRECTORY}/.migration.lock", "runs/health/.check.lock",
             "runs/phase15/.writer.lock", "runs/ema_paper/.cycle.lock")
    while True:
        with ExitStack() as stack:
            try:
                for lock in locks:
                    stack.enter_context(writer_lock(project / lock))
            except RuntimeError as error:
                if not str(error).startswith("Another writer owns ") or time.monotonic() >= deadline:
                    raise
            else:
                yield
                return
        time.sleep(.25)


def _readonly(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)


def _checks(db):
    if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
        raise ValueError("Paper archive database integrity check failed")
    tables = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    counts = {}
    for table in tables:
        quoted = table.replace('"', '""')
        counts[table] = db.execute(f'SELECT count(*) FROM "{quoted}"').fetchone()[0]
    return counts


def _backup(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with closing(_readonly(source)) as live, closing(sqlite3.connect(destination)) as saved:
        live.backup(saved)
        saved.execute("PRAGMA journal_mode=DELETE")
        return _checks(saved)


def _private(directory):
    """Use only the current Windows user and SYSTEM for the new archive tree."""
    who = subprocess.check_output(["whoami.exe", "/user", "/fo", "csv", "/nh"], text=True).strip()
    account, sid = next(csv.reader([who]))
    if not account or not re.fullmatch(r"S-1-(?:\d+-)+\d+", sid):
        raise RuntimeError("Could not verify the archive owner's Windows SID")
    subprocess.run(["icacls.exe", str(directory), "/inheritance:r", "/grant:r",
                    f"*{sid}:(OI)(CI)F", "*S-1-5-18:(OI)(CI)F"],
                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _sealed(path, expected=None):
    record = read_json(path)
    if record["id"] != (expected or record["id"]) or fingerprint(record["body"]) != record["id"]:
        raise ValueError(f"Legacy registration checksum differs: {path}")
    return record


def _record_files(project, phase):
    references = {"runs/phase15/registration.json", f"runs/phase15/registrations/{phase['id']}.json"}
    for key, directory, folder in (("source_protocol", "phase13", "protocols"),
                                   ("phase14_amendment", "phase14", "amendment")):
        if identifier := phase["body"].get(key):
            if not SHA.fullmatch(identifier):
                raise ValueError("Invalid Phase 15 source binding identifier")
            relative = f"runs/{directory}/{folder}/{identifier}.json"
            _sealed(_path(project, relative), identifier)
            references.add(relative)
    return references


def _rows(db, expected):
    accounts = {key: json.loads(raw) for key, raw in db.execute("SELECT id,state FROM book")}
    if set(accounts).difference(expected):
        raise ValueError("Unregistered books in the legacy paper ledger")
    return accounts


def _summary(db, profile, identifier, lane, accounts, cutoff):
    key = lane["book_id"]
    events = db.execute("SELECT key,kind,idea,source_at,recorded_at,body FROM event WHERE book=? ORDER BY rowid",
                        (key,)).fetchall()
    state = accounts.get(key)
    if not state:
        if events or not profile.startswith("breakout/"):
            raise ValueError(f"Legacy financial state is missing for {profile}")
        state = {"book_id": key, "cash": lane["risk"]["starting_equity"], "equity": lane["risk"]["starting_equity"],
                 "position": None, "pending": None}
    if state["book_id"] != key:
        raise ValueError("Legacy account identity differs from the registration")
    disposition = {"kind": "ADMINISTRATIVE_RUN_TERMINATION", "at": cutoff,
                   "pending": "CANCELLED_AT_CUTOVER" if state.get("pending") else None,
                   "exposure": "ADMINISTRATIVELY_ARCHIVED_UNRESOLVED" if state.get("position") else None,
                   "strategy_result": None, "execution_price": None, "reason": REASON}
    return {"book_id": key, "registration_id": identifier, "account": state, "event_count": len(events),
            "event_hash": fingerprint(events), "disposition": disposition}, {row[2] for row in events if row[2]}


def _inventory(root, registrations, cutoff):
    phase, ema = registrations
    lanes = phase["body"]["lanes"]
    if set(lanes) != BREAKOUT or set(ema) != EMA:
        raise ValueError("The four Breakout and three EMA legacy slots are required")
    values, ideas = {}, set()
    entries = (
        (PHASE15_DATABASE, {f"breakout/{name}": (phase["id"], lane) for name, lane in lanes.items()}),
        (EMA_DATABASE, {f"ema/{name}": (record["id"], record["body"]) for name, record in ema.items()}),
    )
    for relative, by_profile in entries:
        with closing(_readonly(_path(root, relative))) as db:
            known = {lane["book_id"] for _, lane in by_profile.values()}
            accounts = _rows(db, known)
            for profile, (identifier, lane) in by_profile.items():
                values[profile], ids = _summary(db, profile, identifier, lane, accounts, cutoff)
                ideas.update(ids)
            if any(row[0] not in known for row in db.execute("SELECT DISTINCT book FROM event")):
                raise ValueError("Legacy event has no registered account")
    return values, sorted(ideas)


def _registrations(project):
    from lab.ema_paper import catalog

    pointer = read_json(_path(project, "runs/phase15/registration.json"))
    identifier = pointer["id"]
    if not SHA.fullmatch(identifier):
        raise ValueError("Invalid Phase 15 registration ID")
    phase = _sealed(_path(project, f"runs/phase15/registrations/{identifier}.json"), identifier)
    with closing(_readonly(_path(project, EMA_DATABASE))) as db:
        ema = catalog.registrations(db)
    return phase, ema


def _hashes(value):
    if isinstance(value, dict):
        for field, data in value.items():
            if field in {"raw_sha256", "raw_sha", "candle_raw_sha256"}:
                yield from (item for item in (data if isinstance(data, list) else [data])
                            if isinstance(item, str) and SHA.fullmatch(item))
            yield from _hashes(data)
    elif isinstance(value, list):
        for item in value:
            yield from _hashes(item)


def _referenced_raw(project, archive):
    wanted = set()
    for relative in (PHASE15_DATABASE, EMA_DATABASE):
        with closing(_readonly(_path(archive, relative))) as db:
            for payload, in db.execute("SELECT body FROM event UNION ALL SELECT state FROM book"):
                wanted.update(_hashes(json.loads(payload)))
    found = {}
    for directory in ("phase9", "phase10", "phase13", "phase14", "phase15", "phase17"):
        for folder in (project / "runs" / directory).rglob("raw"):
            if not folder.is_dir():
                continue
            for path in folder.glob("*.json"):
                if path.stem in wanted and file_hash(path) == path.stem:
                    found[path.stem] = path.resolve().relative_to(project).as_posix()
    kraken = set()
    if _path(archive, KRAKEN_REFERENCE).exists():
        with closing(_readonly(_path(archive, KRAKEN_REFERENCE))) as db:
            for digest, payload in db.execute("SELECT sha,body FROM raw"):
                if digest in wanted and sha256(payload).hexdigest() == digest:
                    kraken.add(digest)
    missing = wanted - found.keys() - kraken
    if missing:
        raise ValueError(f"Legacy financial evidence missing {len(missing)} referenced raw receipts")
    return {"files": found, "kraken_sha256": sorted(kraken)}


def _copy(project, archive, relatives):
    rows = []
    for relative in sorted(relatives):
        source, destination = _path(project, relative), _path(archive, relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        digest = file_hash(destination)
        if digest != file_hash(source):
            raise ValueError(f"Legacy source file changed during preservation: {relative}")
        rows.append({"path": relative, "sha256": digest, "bytes": destination.stat().st_size})
    return rows


def _git(project, *args):
    return subprocess.run(["git", "-C", str(project), *args], check=True, capture_output=True, text=True).stdout


def _reference_check(project, relative):
    name = Path(relative).name
    result = subprocess.run(["git", "-C", str(project), "grep", "-l", "-F", "-e", name, "--", "lab", "tests", "*.md", "*.ps1"],
                            capture_output=True, text=True)
    if result.returncode not in (0, 1):
        raise RuntimeError("Could not check versioned references for legacy cleanup")
    files = set(result.stdout.splitlines())
    files.difference_update({"lab/paper_run/archive.py", "tests/test_paper_run_archive.py"})
    for path in project.glob("*.md"):
        if not path.is_symlink() and name in path.read_text(encoding="utf-8"):
            files.add(path.name)
    return sorted(files)


def _cleanup_manifest(project, archive):
    result = []
    for relative, (status, reason) in CLEANUP.items():
        source = _path(project, relative)
        if not source.is_file():
            continue
        tracked = bool(_git(project, "ls-files", "--", relative).strip())
        refs = _reference_check(project, relative) if status == "DELETE_REDUNDANT" else []
        if status == "DELETE_REDUNDANT" and (refs or tracked):
            status = "NEEDS_REVIEW"
        target = _path(archive, relative)
        row = {"path": relative, "classification": status, "reason": reason, "tracked": tracked,
               "reference_check": "Git-indexed code, tests, docs, operator scripts and local top-level Markdown by exact filename",
               "references_found": refs, "private": not tracked, "generated": relative.startswith("runs/"),
               "archive_path": str(target) if target.is_file() else None,
               "archive_sha256": file_hash(target) if target.is_file() else None}
        result.append(row)
    return result


def _health_hash(path):
    with closing(_readonly(path)) as db:
        return fingerprint([
            db.execute("SELECT * FROM outbox ORDER BY id").fetchall(),
            db.execute("SELECT * FROM outbox_attempt ORDER BY rowid").fetchall(),
            db.execute("SELECT key,value FROM meta WHERE key LIKE 'paper_lifecycle%' "
                       "OR key LIKE 'ema_paper_lifecycle%' OR key LIKE 'ema_private%' ORDER BY key").fetchall(),
        ])


def protected_hashes(project=PROJECT):
    project = Path(project).resolve()
    return {relative: file_hash(_path(project, relative)) for relative in PROTECTED_FILES
            if _path(project, relative).is_file()}


def create_locked(archive_parent, *, project=PROJECT, cutoff=None):
    """Seal and restore-check the archive while all legacy writers are fenced."""
    project = Path(project).resolve()
    legacy_marker = project / STATE_DIRECTORY
    if prior := gate.retired(root=legacy_marker):
        return verify(Path(prior["body"]["path"]), expected=prior["id"])
    parent = Path(archive_parent).expanduser().resolve()
    if (parent == project or parent.is_relative_to(project)
            or any((p / ".git").exists() for p in (parent, *parent.parents))):
        raise ValueError("The legacy archive must be outside the project and Git worktree")
    parent.mkdir(parents=True, exist_ok=True)
    archive_ready = legacy_marker / "archive-ready.json"
    phase, ema = _registrations(project)
    protected = protected_hashes(project)
    if archive_ready.exists():
        pending = read_json(archive_ready)
        previous = verify(pending["path"], expected=pending["id"])
        previous_body = previous["body"]
        current, ideas = _inventory(project, (phase, ema), previous_body["cutoff"])
        if (current == previous_body["slots"] and ideas == previous_body["legacy_ideas"]
                and _health_hash(_path(project, HEALTH_DATABASE)) == previous_body["notification_cutoff_hash"]
                and protected == previous_body["protected_hashes"]):
            return previous
    databases = set(REQUIRED_DATABASES) | {name for name in REFERENCE_DATABASES if _path(project, name).is_file()}
    sizes = sum(_path(project, name).stat().st_size for name in databases)
    if shutil.disk_usage(parent).free < 2 * 1024**3 + 4 * sizes:
        raise RuntimeError("Insufficient disk space for legacy archive and isolated restore")
    now = utc_now() if cutoff is None else utc(cutoff)
    stamp = now.isoformat()
    archive = parent / f"paper-legacy-{now.strftime('%Y%m%dT%H%M%S%fZ')}"
    archive.mkdir()
    _private(archive)
    slots, ideas = _inventory(project, (phase, ema), stamp)
    tables = {}
    entries = []
    for relative in sorted(databases):
        destination = _path(archive, relative)
        tables[relative] = _backup(_path(project, relative), destination)
        entries.append({"path": relative, "sha256": file_hash(destination), "bytes": destination.stat().st_size})
    proof, archived_ideas = _inventory(archive, (phase, ema), stamp)
    if proof != slots or archived_ideas != ideas:
        raise ValueError("Legacy financial state changed during the coordinated archive")
    evidence = _referenced_raw(project, archive)
    names = _record_files(project, phase) | set(evidence["files"].values())
    names.update(relative for relative in REFERENCE_FILES if _path(project, relative).is_file())
    names.update(relative for relative, (classification, _) in CLEANUP.items()
                 if classification in {"DELETE_REDUNDANT", "NEEDS_REVIEW"} and _path(project, relative).is_file())
    entries.extend(_copy(project, archive, names))
    pinned = {path for record in ema.values() for path in record["body"].get("code_pins", {})}
    codes = {path: file_hash(_path(project, path)) for path in sorted(pinned)}
    body = {"path": str(archive), "cutoff": stamp, "classifications": [LEGACY, EXCLUSION], "reason": REASON,
            "slots": slots, "legacy_books": sorted(item["book_id"] for item in slots.values()), "legacy_ideas": ideas,
            "files": entries, "tables": tables, "raw_receipts": evidence, "cleanup": _cleanup_manifest(project, archive),
            "notification_cutoff_hash": _health_hash(_path(archive, HEALTH_DATABASE)),
            "protected_hashes": protected,
            "code": {"head": _git(project, "rev-parse", "HEAD").strip(), "ema_registered_code_files": codes},
            "restore_instruction": "Verify in an isolated directory. Never replace current paper or watchdog databases."}
    record = {"id": fingerprint(body), "body": body}
    write_json(archive / "manifest.json", record)
    verify(archive, expected=record["id"])
    write_json(archive_ready, {"id": record["id"], "path": str(archive)})
    return record


def verify(archive, *, expected=None):
    """Validate each hash, then read restored database copies in a private scratch directory."""
    archive = Path(archive).resolve()
    record = _sealed(archive / "manifest.json", expected)
    body = record["body"]
    if archive != Path(body["path"]).resolve():
        raise ValueError("Archive manifest is bound to a different path")
    if not all(relative in body["tables"] for relative in REQUIRED_DATABASES):
        raise ValueError("Archive is missing one or more required database snapshots")
    files = {row["path"]: row for row in body["files"]}
    if len(files) != len(body["files"]) or not set(body["tables"]).issubset(files):
        raise ValueError("Archive file and database inventory differ")
    for relative, row in files.items():
        path = _path(archive, relative)
        if path.stat().st_size != row["bytes"] or file_hash(path) != row["sha256"]:
            raise ValueError(f"Archive content checksum differs: {relative}")
    scratch = archive / f".restore-{uuid4().hex}"
    scratch.mkdir()
    restored = []
    try:
        for number, (relative, expected_counts) in enumerate(body["tables"].items()):
            path = scratch / f"{number}.sqlite3"
            restored.append(path)
            shutil.copyfile(_path(archive, relative), path)
            with closing(_readonly(path)) as db:
                if _checks(db) != expected_counts:
                    raise ValueError(f"Restored database inventory differs: {relative}")
    finally:
        for path in restored:
            path.unlink(missing_ok=True)
        scratch.rmdir()
    return record


def _freeze_financial_database(path, tables):
    # Defense in depth for old manual entry points and an already-imported worker.
    with closing(sqlite3.connect(path, timeout=3)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        present = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in tables:
            if table not in present:
                continue
            for operation in ("INSERT", "UPDATE", "DELETE"):
                db.execute(f"CREATE TRIGGER IF NOT EXISTS paper_reset_{table}_{operation.lower()} "
                           f"BEFORE {operation} ON {table} "
                           "BEGIN SELECT RAISE(ABORT,'legacy paper generation retired'); END")


def retire_locked(record, *, project=PROJECT):
    """Persist retirement after verification. Reentry completes any interrupted guards."""
    project = Path(project).resolve()
    body = record["body"]
    if verify(body["path"], expected=record["id"]) != record:
        raise ValueError("Retirement record differs from the verified archive")
    current, ideas = _inventory(project, _registrations(project), body["cutoff"])
    if current != body["slots"] or ideas != body["legacy_ideas"]:
        raise ValueError("Old ledgers advanced after archival. Preserve the actual cutoff before retiring")
    marker = gate.retirement_path(project / STATE_DIRECTORY)
    previous = gate.retired(root=marker.parent)
    if previous and previous != record:
        raise ValueError("Legacy paper already has a different immutable retirement")
    if not previous:
        if _health_hash(_path(project, HEALTH_DATABASE)) != body["notification_cutoff_hash"]:
            raise ValueError("Notification records advanced after archival. Preserve the actual cutoff before retiring")
        if protected_hashes(project) != body["protected_hashes"]:
            raise ValueError("Protected registrations or holdout controls changed during archival")
        write_json(marker, record)
    _freeze_financial_database(_path(project, PHASE15_DATABASE), ("book", "event", "spot_minute", "spot_request", "spot_correction"))
    _freeze_financial_database(_path(project, EMA_DATABASE),
                               ("book", "event", "registration", "readiness", "eligibility", "equity_sample",
                                "synthetic_seed", "chart_recovery_event"))
    return record


def apply_cleanup_locked(record, *, project=PROJECT):
    """Remove only verified, unchanged and unreferenced exact generated paths."""
    project = Path(project).resolve()
    body = record["body"]
    if verify(body["path"], expected=record["id"]) != gate.retired(root=project / STATE_DIRECTORY):
        raise ValueError("Cleanup requires the verified legacy retirement")
    archive = Path(body["path"]).resolve()
    journal = archive / "cleanup-results.json"
    prior = read_json(journal) if journal.exists() else {}
    for candidate in body["cleanup"]:
        relative = candidate["path"]
        approved = CLEANUP.get(relative)
        if candidate["classification"] != "DELETE_REDUNDANT" or not approved or approved[0] != "DELETE_REDUNDANT":
            continue
        source = _path(project, relative)
        backup = _path(archive, relative)
        if not source.exists():
            if prior.get(relative, {}).get("state") == "VERIFIED_BEFORE_REMOVAL":
                prior[relative]["state"] = "REMOVED"
                write_json(journal, prior)
            continue
        if (_git(project, "ls-files", "--", relative).strip() or _reference_check(project, relative)
                or file_hash(source) != candidate["archive_sha256"] or file_hash(backup) != candidate["archive_sha256"]):
            prior[relative] = {**candidate, "state": "NEEDS_REVIEW", "at": utc_now().isoformat(),
                               "reason": "Source bytes, reference checks or tracking state changed after archival"}
            write_json(journal, prior)
            continue
        prior[relative] = {**candidate, "state": "VERIFIED_BEFORE_REMOVAL", "at": utc_now().isoformat()}
        write_json(journal, prior)
        source.unlink()
        prior[relative] = {**prior[relative], "state": "REMOVED"}
        write_json(journal, prior)
    return prior


def main():
    import argparse

    parser = argparse.ArgumentParser(prog="python -m lab.paper_run.archive")
    parser.add_argument("action", choices=("verify",))
    parser.add_argument("archive", type=Path)
    arguments = parser.parse_args()
    record = verify(arguments.archive)
    print(json.dumps({"id": record["id"], "cutoff": record["body"]["cutoff"],
                      "database_counts": record["body"]["tables"], "isolated_restore": "VERIFIED"}, indent=2))


if __name__ == "__main__":
    main()
