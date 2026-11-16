"""Durable boundary for the legacy paper writers."""

from pathlib import Path

from lab.config import fingerprint
from lab.storage import RUNS, read_json


ROOT = RUNS / "paper-run"


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
