from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd

from lab.config import ROOT, load_profiles
from lab.data import feed
from lab.model import check_model, windows
from lab.research import code_hash
from lab.storage import DATA, MODELS, RUNS, file_hash, put_bytes, read_json, utc, write_json

AUDITS = RUNS / "phase2"
CHECKPOINT = AUDITS / "checkpoint"
RUN_IDS = {
    "position_spot": "604a8b968f88efe7cfa0",
    "swing": "f72cb13d044440711f26",
    "day": "af1b89dbecba1e44b489",
}
MUTABLE_REPORTS = (RUNS / "data-validation.json", RUNS / "summary.md")


def frozen_runs() -> dict:
    result = {}
    for key, run_id in RUN_IDS.items():
        folder = RUNS / key / run_id
        result[key] = (folder, read_json(folder / "manifest.json"))
    return result


def frozen_prices(record: dict) -> pd.DataFrame:
    path = feed(record["seconds"]) / "snapshots" / f"{record['sha256']}.parquet"
    if file_hash(path) != record["sha256"]:
        raise ValueError(f"Frozen source checksum mismatch: {path}")
    frame = pd.read_parquet(path)
    frame["open_at"] = frame.open_at.dt.as_unit("ns")
    frame["close_at"] = frame.close_at.dt.as_unit("ns")
    return frame.set_index("open_at").sort_index()


def preserve() -> dict:
    """Pin originals, verify the executable checkpoint, and archive its small source files."""
    pointer = CHECKPOINT / "evidence.json"
    if pointer.exists():
        record = read_json(pointer)
        if file_hash(CHECKPOINT / "source.zip") != record["source_archive_sha256"]:
            raise ValueError("Checkpoint source archive hash mismatch")
        for path, digest in record["files"].items():
            original = ROOT / path
            if original in MUTABLE_REPORTS:
                archived = CHECKPOINT / original.name
                if not archived.exists() and file_hash(original) == digest:
                    put_bytes(archived, original.read_bytes())
                original = archived
            if file_hash(original) != digest:
                raise ValueError(f"Original checkpoint file changed: {path}")
        return record
    CHECKPOINT.mkdir(parents=True, exist_ok=True)
    checks = {}
    originals = [DATA / "holdout.lock.json", *MUTABLE_REPORTS]
    profiles = load_profiles()
    for key, (folder, manifest) in frozen_runs().items():
        assert manifest["code_sha256"] == code_hash()
        assert manifest["windows"] == windows().model_dump()
        assert not manifest["holdout_used"] and not manifest["paper_approved"]
        model = manifest["model"]
        check_model(model, profiles[key], profiles[key].costs)
        registry = MODELS / key / model["model_version"]
        assert read_json(registry / "model.json") == model
        assert file_hash(registry / "ridge.joblib") == model["artifact_sha256"]
        for filename, digest in manifest["files"].items():
            assert file_hash(folder / filename) == digest
        source = manifest["source_snapshot"]
        parent = feed(source["seconds"]) / "snapshots"
        parquet = parent / f"{source['sha256']}.parquet"
        assert file_hash(parquet) == source["sha256"]
        originals.extend([folder / "manifest.json", *[folder / name for name in manifest["files"]],
                          registry / "model.json", registry / "ridge.joblib", parquet,
                          parent / f"{source['checkpoint_hash']}.json"])
        checks[key] = {
            "run": str(folder), "model": model, "source_snapshot": source, "windows": manifest["windows"],
            "code_sha256": manifest["code_sha256"], "profile": manifest["profile"],
        }
    source_files = list((ROOT / "lab").glob("*.py")) + list((ROOT / "tests").glob("*.py"))
    source_files += [ROOT / name for name in (
        "profiles.yaml", "research.yaml", "README.md", "DECISIONS_NEEDED.md", "DATA_DICTIONARY.md",
        "ARCHITECTURE.md", "DEPENDENCIES.md", "requirements.lock.txt", "dashboard.py", "lab.ps1", "dashboard.ps1",
    )]
    archive = CHECKPOINT / "source.zip"
    with ZipFile(archive, "w", compression=ZIP_DEFLATED) as zipped:
        for path in source_files:
            zipped.write(path, path.relative_to(ROOT))
    for path in MUTABLE_REPORTS:
        put_bytes(CHECKPOINT / path.name, path.read_bytes())
    record = {
        "profiles": checks, "source_archive_sha256": file_hash(archive),
        "files": {str(path.relative_to(ROOT)): file_hash(path) for path in originals},
    }
    write_json(pointer, record)
    return record


def holdout_end() -> pd.Timestamp:
    rules = windows()
    for _, manifest in frozen_runs().values():
        if manifest["windows"] != rules.model_dump():
            raise ValueError("Registered holdout differs from the persistent research lock")
    return utc(rules.data_cutoff)
