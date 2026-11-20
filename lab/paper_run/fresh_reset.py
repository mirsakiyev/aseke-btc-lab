"""Discontinue one stopped paper generation and open its paused successor."""

from contextlib import closing
import json
from pathlib import Path
import re
import shutil
import sqlite3
from tempfile import TemporaryDirectory

from lab.config import fingerprint
from lab.health import fresh_paper, state
from lab.paper_run import archive, gate, runtime, source_amendment
from lab.phase13.registry import sealed
from lab.storage import file_hash, read_json, utc_now, write_json, writer_lock


DATABASE = ("paper.sqlite3", "paper.sqlite3-wal", "paper.sqlite3-journal", "paper.sqlite3-shm")
FILES = set(DATABASE) | {
    "current.json", "initializing.json", "prepared-cutover.json", "ema-live-source.json",
    "ema-live-status.json", "last-cycle.json", "last-launch.json", "entries-paused",
    "runner-stop.json", "runner.log", "consumer-losses.json",
}
FOLDERS = {"registrations", "source-amendments", "entries-controls", "ema-live-status.json.snap", "last-cycle.json.snap"}
RETAINED = {
    "legacy-retired.json", "archive-ready.json", gate.RESET_FILE,
    ".migration.lock", ".runner.lock", ".launch.lock", ".writer.lock", ".consumer-loss.lock",
}


def request_stop(generation):
    """Tell the exact current runner to exit and prevent its supervised relaunch."""
    with writer_lock(gate.ROOT / ".launch.lock"):
        current = runtime.current()
        if not current or current["id"] != generation or gate.reset_pending():
            raise ValueError("Select the exact active paper generation")
        if not gate.stop_requested(generation):
            write_json(gate.ROOT / "runner-stop.json", {"generation": generation, "requested_at": utc_now().isoformat()})
    return {"state": "RUNNER_STOP_REQUESTED", "generation_id": generation}


def _generation_files(root):
    files = set()
    for path in root.iterdir():
        if path.name in RETAINED:
            continue
        if path.name in FILES or re.fullmatch(r"\.[a-f0-9]{32}\.tmp", path.name):
            files.add(archive._path(root, path.name).relative_to(root).as_posix())
            continue
        if path.name not in FOLDERS or not archive._path(root, path.name).is_dir():
            raise ValueError(f"Unclassified paper generation path: {path.name}")
        for child in path.rglob("*"):
            relative = child.relative_to(root).as_posix()
            if archive._path(root, relative).is_file():
                files.add(relative)
    return sorted(files)


def _inspect_copy(destination, names, generation):
    # Recover any hot SQLite journal on a disposable copy. The raw files stay sealed.
    with TemporaryDirectory(dir=destination) as restored:
        for name in DATABASE[:3]:
            if name in names:
                shutil.copyfile(destination / name, Path(restored) / name)
        with closing(sqlite3.connect(Path(restored) / DATABASE[0])) as db:
            if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise ValueError("Retired paper database failed integrity check on a copy")
            books = {identifier: json.loads(raw) for identifier, raw in db.execute("SELECT id,state FROM book")}
    if set(books) != {lane["book_id"] for lane in generation["body"]["lanes"].values()}:
        raise ValueError("Retired account identities differ from the active registration")
    return {
        slot: {"book_id": lane["book_id"], "position_archived": bool(books[lane["book_id"]]["position"]),
               "pending_archived": bool(books[lane["book_id"]]["pending"])}
        for slot, lane in generation["body"]["lanes"].items()
    }


def _preserve(root, destination, current, cutoff):
    manifest = destination / "manifest.json"
    if manifest.is_file():
        return sealed(manifest)
    names = _generation_files(root)
    if not {"current.json", DATABASE[0]}.issubset(names):
        raise ValueError("The reviewed generation's pointer and financial database must exist")
    destination.mkdir(parents=True, exist_ok=True)
    archive._private(destination)
    files = archive._copy(root, destination, names)
    books = _inspect_copy(destination, names, current)
    body = {"generation": current["id"], "path": str(destination), "cutoff": cutoff.isoformat(),
            "disposition": "ADMINISTRATIVELY_DISCONTINUED", "books": books, "files": files}
    record = {"id": fingerprint(body), "body": body}
    write_json(manifest, record)
    return record


def _clear_archived(root, destination, saved):
    files = {row["path"]: row for row in saved["body"]["files"]}
    live = set(_generation_files(root))
    if live - files.keys():
        raise ValueError("Paper generation paths changed after archival")
    for name, row in files.items():
        if file_hash(archive._path(destination, name)) != row["sha256"]:
            raise ValueError(f"Retirement archive content differs: {name}")
        if name in live and file_hash(archive._path(root, name)) != row["sha256"]:
            raise ValueError(f"Active generation content changed after archival: {name}")
    # Withdraw the old registration first so readers cannot see a half-reset ledger.
    for name in sorted(live, key=lambda name: (name != "current.json", name)):
        archive._path(root, name).unlink()


def _notifications(old, new, cutoff):
    with state.connect() as db, db:
        db.execute(
            "UPDATE outbox SET status='SUPERSEDED', detail=? WHERE status IN ('PENDING','RETRY') "
            "AND CASE WHEN json_valid(event_json) THEN json_extract(event_json,'$.generation_id') END=?",
            (f"Paper generation administratively discontinued at {cutoff}; successor {new['id']}", old),
        )
        for slot, lane in new["body"]["lanes"].items():
            key = fresh_paper.CURSOR_PREFIX + fingerprint([new["id"], slot, lane["book_id"]])
            if state.get_meta(db, key) is None:
                state.set_meta(db, key, 0)


def _save_intent(root, body, phase):
    body = {**body, "phase": phase}
    write_json(root / gate.RESET_FILE, {"id": fingerprint(body), "body": body})
    return body


def reset(generation, archive_parent, qualify):
    """Run or resume the exact reset after the old launchers and owner have exited."""
    if not re.fullmatch(r"[a-f0-9]{64}", generation):
        raise ValueError("An exact retired paper generation identity is required")
    root = gate.ROOT.resolve()
    parent = Path(archive_parent).expanduser().resolve()
    if parent.is_relative_to(runtime.PROJECT) or any((p / ".git").exists() for p in (parent, *parent.parents)):
        raise ValueError("The paper archive must be outside Git worktrees")
    destination = archive._path(parent, f"paper-retired-{generation[:12]}")
    with (writer_lock(root / ".migration.lock"), writer_lock(state.ROOT / ".check.lock"),
          writer_lock(root / ".launch.lock"), writer_lock(root / ".runner.lock"),
          writer_lock(root / ".writer.lock"), writer_lock(root / ".consumer-loss.lock")):
        return _reset_locked(root, destination, generation, qualify)


def _reset_locked(root, destination, generation, qualify):
    current = runtime.current()
    intent = gate.reset_intent()
    if intent and intent["generation"] == generation:
        if intent["archive_path"] != str(destination):
            raise ValueError("Retry the reset with its original archive path")
        saved = sealed(destination / "manifest.json", intent["archive_id"])
    else:
        if gate.reset_pending() or not current or current["id"] != generation:
            raise ValueError("The requested paper generation is no longer the reset target")
        qualifications, stream = qualify()
        now = utc_now()
        prepared = runtime.definitions(gate.retired(), qualifications, now)
        blocked = [slot for slot, lane in prepared["lanes"].items()
                   if lane["source"] != runtime.ema.SOURCE and lane["qualification"]["state"] != "QUALIFIED"]
        if blocked or stream.get("state") != "QUALIFIED":
            raise ValueError(f"Current market sources failed qualification for {', '.join(blocked) or 'EMA'}")
        saved = _preserve(root, destination, current, now)
        prepared.update(cutoff=saved["body"]["cutoff"], retired_generation=generation,
                        retired_generation_archive=str(destination))
        for slot, lane in prepared["lanes"].items():
            lane["retired_book_id"] = current["body"]["lanes"][slot]["book_id"]
            lane["book_id"] = fingerprint([runtime.VERSION, generation, slot, prepared["start"]])
        if saved["body"]["generation"] != generation or saved["body"]["path"] != str(destination):
            raise ValueError("Existing archive is bound to a different generation or path")
        plan = {"generation": generation, "archive_id": saved["id"], "archive_path": str(destination),
                "successor": fingerprint(prepared), "prepared": prepared, "stream": stream}
        intent = _save_intent(root, plan, "ARCHIVED")
    if intent["phase"] == "ARCHIVED":
        _clear_archived(root, destination, saved)
        intent = _save_intent(root, intent, "INITIALIZING")
        current = None
    if intent["phase"] == "INITIALIZING":
        if current and current["id"] != intent["successor"]:
            raise ValueError("A different generation became current during the reset")

        def before_publish(record):
            write_json(root / "entries-paused", {"generation": record["id"], "paused_at": utc_now().isoformat()})
            source_amendment._install_locked(record, intent["stream"], now=utc_now())
            _notifications(generation, record, saved["body"]["cutoff"])

        current = runtime._initialize_locked(gate.retired(), {}, utc_now(), intent["prepared"],
                                             before_publish, refresh_start=False)
        if current["id"] != intent["successor"]:
            raise ValueError("Initialized registration differs from reset intent")
        intent = _save_intent(root, intent, "COMPLETE")
    if intent["phase"] != "COMPLETE" or not current or current["id"] != intent["successor"]:
        raise ValueError("Completed reset registration is no longer current")
    return {"state": "CURRENT", "generation_id": current["id"], "retired_generation": generation,
            "entries_paused": (root / "entries-paused").is_file(), "archive": str(destination),
            "database": str(root / DATABASE[0]), "start": current["body"]["start"],
            "books": {slot: lane["book_id"] for slot, lane in current["body"]["lanes"].items()}}
