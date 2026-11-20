"""Durable boundary for the legacy paper writers."""

from pathlib import Path

from lab.config import fingerprint
from lab.storage import RUNS, read_json


ROOT = RUNS / "paper-run"
RESET_FILE = "generation-reset.json"


def reset_intent():
    path = ROOT / RESET_FILE
    if not path.exists():
        return None
    record = read_json(path)
    if fingerprint(record["body"]) != record["id"]:
        raise ValueError("Paper generation reset checksum differs")
    return record["body"]


def reset_pending():
    intent = reset_intent()
    return bool(intent and intent["phase"] != "COMPLETE")


def stop_requested(generation):
    path = ROOT / "runner-stop.json"
    return path.exists() and read_json(path)["generation"] == generation


def retirement_path(root=None):
    return (ROOT if root is None else Path(root)) / "legacy-retired.json"


def legacy_entries_closed(*, root=None):
    # A corrupt marker also closes entries. Operators must review the marker.
    return retirement_path(root).exists()


def retired(*, root=None):
    marker = retirement_path(root)
    if not marker.exists():
        return None
    record = read_json(marker)
    if fingerprint(record["body"]) != record["id"]:
        raise ValueError("Legacy paper retirement marker checksum differs")
    return record
