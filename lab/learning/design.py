from itertools import product
from pathlib import Path

import psutil
import yaml

from lab.config import ROOT, fingerprint
from lab.data import feed
from lab.model import dependency_versions
from lab.phase2.checkpoint import frozen_runs, holdout_end
from lab.storage import DATA, RUNS, file_hash, read_json, utc, utc_now, writer_lock, write_json


CONFIG = ROOT / "learning.yaml"
OUTPUT = RUNS / "learning"
STATE = DATA / "learning"
MIB = 1024 ** 2
FEATURE_VERSION = "causal-ohlcv-registry-v1"
LABEL_VERSION = "fixed-account-closed-path-net-r-v1"
SPOT = "position_spot_research_v1"
PROFILE_IDS = (SPOT, "swing_perp_research_v1", "day_perp_research_v1")


def config():
    data = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    if (tuple(data["profiles"]) != PROFILE_IDS or len(data["strategies"]) != 3 or len(data["exits"]) != 2
            or len(data["models"]) != 3 or not 1 <= data["budget"]["max_threads"] <= 2
            or not 1 <= data["budget"]["wall_seconds"] <= 7200):
        raise ValueError("Unexpected research catalog or resource limits. Freeze a reviewed configuration.")
    return data


def code_hashes():
    files = sorted(Path(__file__).parent.glob("*.py"))
    files += [ROOT / "lab" / name for name in ("config.py", "data.py", "features.py", "execution.py", "storage.py")]
    return {path.relative_to(ROOT).as_posix(): file_hash(path) for path in files}


def inventory():
    ram, disk = psutil.virtual_memory(), psutil.disk_usage(str(ROOT))
    return {"logical_cpus": psutil.cpu_count(), "physical_cpus": psutil.cpu_count(logical=False),
            "ram_total_mib": ram.total // MIB, "ram_available_mib": ram.available // MIB,
            "disk_free_mib": disk.free // MIB}


def catalog(settings):
    for profile, entry, exit_policy, model in product(
        settings["profiles"], settings["strategies"], settings["exits"], settings["models"]
    ):
        yield {"id": "/".join((profile, entry, exit_policy, model)), "profile": profile, "entry": entry,
               "exit_policy": exit_policy, "model": model, "role": "primary",
               "cost": settings["profiles"][profile]["primary_cost"]}
    for profile in settings["profiles"]:
        for entry, exit_policy in product(settings["strategies"], settings["exits"]):
            yield {"id": "/".join((profile, entry, exit_policy, "mean_control_v1")), "profile": profile,
                   "entry": entry, "exit_policy": exit_policy, "model": "mean_control_v1", "role": "control",
                   "cost": settings["profiles"][profile]["primary_cost"]}
        for control in ("cash", "passive_25pct"):
            yield {"id": f"{profile}/{control}", "profile": profile, "model": control, "role": "control",
                   "cost": settings["profiles"][profile]["primary_cost"]}
    for entry, exit_policy in product(settings["strategies"], settings["exits"]):
        yield {"id": "/".join((SPOT, entry, exit_policy, "spot_lower_fee_v1")), "profile": SPOT,
               "entry": entry, "exit_policy": exit_policy, "model": "rules_v1", "role": "cost_diagnostic",
               "cost": "spot_lower_fee_v1"}


def sources(settings, pinned):
    original = pinned["source_snapshot"]
    spot = feed(original["seconds"]) / "snapshots" / f"{original['sha256']}.parquet"
    if file_hash(spot) != original["sha256"]:
        raise ValueError("Pinned Coinbase snapshot checksum mismatch")
    result = {SPOT: {"status": "AVAILABLE", "prices": str(spot), "sha256": original["sha256"],
                     "source": original["source"], "published_at": original["published_at"],
                     "historical_availability": original["historical_availability"]}}
    if descriptor := settings.get("spot_source"):
        required = {"prices", "source", "published_at", "historical_availability"}
        if not required <= set(descriptor) or descriptor["source"] != original["source"]:
            raise ValueError("A reviewed spot revision must preserve Coinbase BTC-USD source and provenance")
        path = (ROOT / descriptor["prices"]).resolve()
        result[SPOT] = {**descriptor, "status": "AVAILABLE", "prices": str(path), "sha256": file_hash(path)}
    for key in PROFILE_IDS[1:]:
        descriptor = settings.get("perp_sources", {}).get(key)
        if not descriptor:
            result[key] = {"status": "BLOCKED_DATA", "reason": "No permitted aligned trade, mark, funding and rule archive"}
            continue
        required = {"source", "permission_evidence", "retrieved_at", "timestamp_units", "funding_interval_minutes",
                    "funding_anchor_utc", "rule_evidence", "rule_effective_from", "rule_effective_through",
                    "trade", "mark", "funding", "rules", "hypothetical_current_rules"}
        if not required <= set(descriptor) or not descriptor["permission_evidence"]:
            raise ValueError(f"Incomplete source provenance for {key}")
        if descriptor["source"] in (original["source"], settings["instruments"]["linear_usdt_perp_v1"]["source"]):
            raise ValueError("A futures source must be actual, separate perpetual data")
        assets = {}
        for kind in ("trade", "mark", "funding"):
            path = (ROOT / descriptor[kind]).resolve()
            if not path.is_file() or path.stat().st_size > settings["budget"]["output_cap_mib"] * MIB:
                raise ValueError(f"Missing or oversized {kind} source for {key}")
            assets[kind] = {"file": str(path), "sha256": file_hash(path)}
        result[key] = {**descriptor, "assets": assets, "status": "HYPOTHETICAL_REPLAY"}
    return result


def freeze(*, reviewed_new=False):
    with writer_lock(OUTPUT / ".design.lock"):
        pointer = OUTPUT / "first-study.json"
        if pointer.exists() and not reviewed_new:
            return read_manifest(read_json(pointer)["id"])
        previous_design = read_manifest(read_json(pointer)["id"], check_code=False) if pointer.exists() else None
        settings = config()
        pinned = frozen_runs()["position_spot"][1]
        lock = read_json(DATA / "holdout.lock.json")
        if pinned["windows"] != lock or holdout_end() != utc(lock["data_cutoff"]):
            raise ValueError("Existing holdout boundary changed")
        left, right = utc(lock["holdout_start"]), utc(lock["data_cutoff"])
        if left >= right:
            raise ValueError("Invalid existing holdout")
        folds = [{**fold, "outer_end": lock["holdout_start"] if fold["outer_end"] == "holdout_start" else fold["outer_end"]}
                 for fold in settings["evaluation"]["folds"]]
        previous = None
        for fold in folds:
            inner, start, end = (utc(fold[k]) for k in ("inner_start", "outer_start", "outer_end"))
            if not utc(settings["evaluation"]["train_start"]) < inner < start < end <= left or (
                previous is not None and previous != start
            ):
                raise ValueError("Learning fold is out of order or touches the sealed period")
            previous = end
        body = {
            "settings": settings, "code_sha256": code_hashes(), "config_sha256": file_hash(CONFIG),
            "dependencies": dependency_versions(), "seed": settings["seed"], "hardware_at_freeze": inventory(),
            "feature_version": FEATURE_VERSION, "label_version": LABEL_VERSION,
            "holdout": {"start": left.isoformat(), "end_exclusive": right.isoformat(),
                        "lock_sha256": file_hash(DATA / "holdout.lock.json"), "used": False},
            "available_at_cutoff_exclusive": left.isoformat(), "folds": folds,
            "data": sources(settings, pinned), "trials": list(catalog(settings)),
            "diagnostics": {"scope": "SIMULATOR_DIAGNOSTIC", "ordinary": ["long", "short"],
                            "leverage_cases": settings["execution"]["leverage_stress"],
                            "stress_cases": settings["execution"]["stresses"],
                            "risk_grid_promotion_eligible": False},
            "status_labels": ["BETTER_ON_DEVELOPMENT", "NO_DEMONSTRATED_IMPROVEMENT", "INSUFFICIENT_EVIDENCE", "BLOCKED_DATA"],
            "scope": "Previously inspected development. Train and inner/outer labels require maturity strictly before their right boundary. "
                     "No after-holdout fitting or live-observer data consumption in this first campaign.",
            "selection": "No nominations from this consumed first study. No observer or approval changes.",
        }
        if previous_design:
            old = previous_design["design"]
            comparable = ("config_sha256", "code_sha256", "data", "folds", "holdout")
            if all(body[key] == old[key] for key in comparable):
                return read_manifest(previous_design["id"])
            body["previous_manifest_id"] = previous_design["id"]
        manifest_id = fingerprint(body)
        record = {"id": manifest_id, "frozen_at_utc": utc_now().isoformat(), "design": body}
        write_json(OUTPUT / manifest_id / "manifest.json", record)
        write_json(pointer, {"id": manifest_id})
        return record


def read_manifest(manifest_id, *, check_code=True):
    record = read_json(OUTPUT / manifest_id / "manifest.json")
    if fingerprint(record["design"]) != manifest_id or record["id"] != manifest_id:
        raise ValueError("Frozen learning manifest checksum mismatch")
    if file_hash(DATA / "holdout.lock.json") != record["design"]["holdout"]["lock_sha256"]:
        raise ValueError("Existing holdout lock changed")
    if check_code and (record["design"]["code_sha256"] != code_hashes()
                       or record["design"]["config_sha256"] != file_hash(CONFIG)):
        raise ValueError("Learning code or configuration changed. A reviewed new study manifest is required.")
    return record


def window(left, right, design):
    if not utc(left) < utc(right) <= utc(design["holdout"]["start"]):
        raise ValueError("First learning study accepts only pre-holdout BTC data")
    return utc(left), utc(right)
