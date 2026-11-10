import pandas as pd

from lab.config import fingerprint
from lab.phase13.registry import sealed
from lab.phase16.plan import freeze_initial
from lab.phase17.registry import ROOT, registered
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


PILOT = ROOT / "pilot"
SCOPE = "FEATURE_INITIALIZATION_ONLY"


def save(kind, body):
    key = fingerprint(body)
    path = PILOT / kind / f"{key}.json"
    if not path.exists():
        write_json(path, {"id": key, "body": body})
    return sealed(path, key)


def find(kind, key):
    if len(key) != 64 or any(char not in "0123456789abcdef" for char in key):
        raise ValueError("Invalid pilot artifact identity")
    return sealed(PILOT / kind / f"{key}.json", key)


def current():
    path = PILOT / "registration.json"
    return find("registrations", read_json(path)["id"]) if path.exists() else None


def register(now=None):
    with writer_lock(PILOT / ".registration.lock"):
        if active := current():
            return active
        parent = registered()
        if not parent:
            raise ValueError("The original Phase 17 registration is required")
        previous = parent["body"]
        campaign = freeze_initial()
        now = utc_now() if now is None else utc(now)
        activation = now.floor("1D")+pd.Timedelta(days=1)
        profiles = {}
        for name, original in previous["profiles"].items():
            profiles[name] = {"label_start": max(activation, utc(original["start"])).isoformat(),
                "source_end_exclusive": original["end_exclusive"], "minimum_train": original["minimum_train"],
                "minimum_inner": original["minimum_inner"], "minimum_new_mature": original["minimum_new_mature"],
                "minimum_outer": original["minimum_outer"], "interval_days": original["interval_days"],
                "queue": original["queue"], "profile_rules": campaign["body"]["profiles"][name]["profile"],
                "recipe": original["recipe"], "features": original["features"], "market": original["market"],
                "instrument": original["instrument"], "context_bar": {"position":"1D", "day":"15min", "swing":"4h"}[name],
                "evaluation_days": {"position":120, "day":90, "swing":None}[name]}
        policy = dict(campaign["body"]["policy"])
        policy.update(max_fit_attempts=2, max_attempts_per_fit=2, max_account_attempts=24)
        document = {"version": "phase17.1-prospective-fit-pilot-v1", "parent_phase17": parent["id"],
                    "parent_phase16": campaign["id"], "registered_at": now.isoformat(), "activation": activation.isoformat(),
                    "profiles": profiles, "policy": policy, "budget": previous["budget"], "holdout": previous["holdout"],
                    "protected_futures": previous["phase14_evaluation"], "spot_calendar_overlap": previous["phase15_spot_overlap"],
                    "scope": "RESEARCH_ONLY", "feature_source": "Independent Phase 9/10 monitoring cache, checked public trade receipts only",
                    "feature_permission": SCOPE, "excluded": ["Phase 14 seed and futures outcome sample", "sealed holdout artifacts and outcomes", "Phase 15 paper and user outcomes"],
                    "training_split": "First supervised closed UTC day with 35 mature training labels freezes the inner boundary. Inner decisions must follow that boundary. No hyperparameter search or model selection from feature-only bars.",
                    "fit_gate": "One pinned Ridge after the train, inner, new-maturity and existing interval floors. Preserve the training split and use the inner sample only to assess the frozen fit.",
                    "evaluation": "Separate forward registration after fit and last consumed receipt. Fixed model, rules, training mean and cash with fixed stresses. Manual evaluation after the horizon. No refit or promotion."}
        record = save("registrations", document)
        write_json(PILOT / "registration.json", {"id": record["id"]})
        return record


def input_record(pilot):
    parent = registered()
    if parent["id"] != pilot["body"]["parent_phase17"]:
        raise ValueError("Pilot parent differs from the original managed source")
    body = parent["body"]
    if any(utc(pilot["body"]["profiles"][name]["label_start"]) < utc(row["start"]) for name,row in body["profiles"].items()):
        raise ValueError("Pilot label start overlaps the original protected source boundary")
    profiles = {name: {**row, "start": pilot["body"]["profiles"][name]["label_start"]} for name,row in body["profiles"].items()}
    return {"id": parent["id"], "body": {**body, "profiles": profiles}}
