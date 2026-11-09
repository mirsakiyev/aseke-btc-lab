from pathlib import Path

import yaml

from lab.config import ROOT, fingerprint
from lab.learning.design import read_manifest
from lab.storage import DATA, RUNS, file_hash, read_json, write_json, writer_lock


CONFIG = ROOT / "kucoin-reference.yaml"
ROOT_DATA = DATA / "phase9"
OUTPUT = RUNS / "phase9"
NAMES = ("position", "swing", "day")
APPROVAL = "No validated model"
MODE = "MANUAL_OUTSIDE_LAB"


def reference():
    value = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    if value["execution_mode"] != MODE or value["validation"] != APPROVAL or tuple(value["profiles"]) != NAMES:
        raise ValueError("Invalid manual reference or research profiles")
    for fee in value["fees"].values():
        if not all(0 <= fee[field] < .01 for field in ("maker", "taker", "adverse_proxy")):
            raise ValueError("Expected decimal rates and separate adverse execution")
    return value


def parent_design():
    return read_manifest(reference()["research_parent_manifest"], check_code=False)


def code_id():
    return fingerprint({path.name: file_hash(path) for path in sorted(Path(__file__).parent.glob("*.py"))})


def freeze_reference(config):
    parent = parent_design()
    data = {"reference": config, "config_sha256": file_hash(CONFIG), "source_code_sha256": code_id(),
            "research_parent": parent["id"], "holdout": parent["design"]["holdout"]}
    digest = fingerprint(data)
    path = OUTPUT / "references" / f"{digest}.json"
    with writer_lock(OUTPUT / ".reference.lock"):
        if not path.exists():
            write_json(path, {"id": digest, **data})
    return digest


def read_frozen_reference(ref_id):
    if len(ref_id) != 64 or any(char not in "0123456789abcdef" for char in ref_id):
        raise ValueError("Invalid frozen reference ID")
    raw = read_json(OUTPUT / "references" / f"{ref_id}.json")
    if raw["id"] != ref_id or fingerprint({key: value for key, value in raw.items() if key != "id"}) != ref_id:
        raise ValueError("Frozen reference checksum mismatch")
    return raw


def controls():
    path = ROOT_DATA / "controls.json"
    return read_json(path) if path.exists() else {"monitor": False, "training": False, "ideas": dict.fromkeys(NAMES, False)}


def set_control(name, value, profile=None):
    if name not in ("monitor", "training", "ideas") or type(value) is not bool:
        raise ValueError("Unknown Phase 9 control")
    with writer_lock(ROOT_DATA / ".controls.lock"):
        current = controls()
        if name == "ideas":
            if profile not in NAMES:
                raise ValueError("Select a known idea profile")
            current["ideas"][profile] = value
        else:
            current[name] = value
        write_json(ROOT_DATA / "controls.json", current)
        return current
