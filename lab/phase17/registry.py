from pathlib import Path

import pandas as pd

from lab.config import fingerprint
from lab.phase13.registry import sealed
from lab.storage import RUNS, read_json, utc, utc_now, write_json, writer_lock


ROOT = RUNS / "phase17"
DAY = pd.Timedelta(days=1)
PROFILE = {
    "day": {"context_days": 10, "train_days": 30, "inner_days": 14, "outer_days": 14, "maximum_days": 180, "queue": True},
    "position": {"context_days": 147, "train_days": 75, "inner_days": 45, "outer_days": 60, "maximum_days": 420, "queue": True},
    "swing": {"context_days": 201, "train_days": 0, "inner_days": 0, "outer_days": 0, "maximum_days": 360, "queue": False},
}
WORK = {"child_rss_mib": 640, "max_threads": 2, "wall_seconds": 7200, "min_available_ram_mib": 1250,
        "min_free_disk_mib": 2048, "output_cap_mib": 256}


def save(kind, body):
    identifier = fingerprint(body)
    path = ROOT / kind / (identifier + ".json")
    if not path.exists():
        write_json(path, {"id": identifier, "body": body})
    return sealed(path, identifier)


def registered():
    path = ROOT / "registration.json"
    if path.exists():
        key = read_json(path)["id"]
        return sealed(ROOT / "registrations" / (key + ".json"), key)


def register(now=None):
    from lab.phase13 import protocol
    from lab.phase14.context import pointer
    from lab.phase15.catalog import registered as paper
    from lab.phase16.plan import freeze_initial
    from lab.phase17.discovery import DOCS

    with writer_lock(ROOT / ".registration.lock"):
        if existing := registered():
            return existing
        original, amended, phase15, initial = protocol.active(), pointer("amendment"), paper(), freeze_initial()
        seed = pointer("seed")
        if not all((original, amended, seed, phase15, initial)):
            raise ValueError("The frozen Phase 13–16 dependencies and feature-context seed must exist")
        now = utc_now() if now is None else utc(now)
        holdout = initial["body"]["holdout"]
        activation = max(now.floor("min") + pd.Timedelta(minutes=1), utc(holdout["end_exclusive"]))
        futures_start = max(activation, utc(amended["body"]["end_exclusive"]) + DAY,
                            utc(phase15["body"]["follow_end_exclusive"])).ceil("1D")
        spot_start = activation.ceil("1h")
        profiles = {}
        for name, plan in PROFILE.items():
            parent = initial["body"]["profiles"][name]
            spot = name == "position"
            start = spot_start if spot else futures_start
            # Higher-timeframe UTC buckets must start on a complete day.
            warm = start.ceil("1D") + plan["context_days"] * DAY
            inner = warm + plan["train_days"] * DAY
            outer = inner + plan["inner_days"] * DAY
            profiles[name] = {**plan, "recipe": parent["trial"], "features": parent["features"], "risk": parent["risk"],
                              "costs": parent["costs"], "decision": parent["profile"]["decision"],
                              "target": initial["body"]["policy"]["target"], "minimum_train": parent["minimum_train"],
                              "minimum_inner": parent["minimum_inner"], "minimum_outer": parent["minimum_outer"],
                              "minimum_new_mature": parent["minimum_new_mature"], "interval_days": parent["interval_days"],
                              "market": "SPOT" if spot else "FUTURES", "instrument": "BTC-USDT" if spot else "XBTUSDTM",
                              "start": start.isoformat(), "context_lower_bound": warm.isoformat(), "inner_start": inner.isoformat(),
                              "outer_start": outer.isoformat(), "first_cutoff": (outer + plan["outer_days"] * DAY).isoformat(),
                              "end_exclusive": (start + plan["maximum_days"] * DAY).isoformat()}
        body = {"version": "phase17-native-data-policy-v1", "registered_at": now.isoformat(), "activation": activation.isoformat(),
                "profiles": profiles, "scope": "RESEARCH_ONLY", "docs": DOCS,
                "holdout": holdout, "parent_protocols": {"phase13": original["id"], "phase14": amended["id"],
                "phase14_seed": seed["id"], "phase15": phase15["id"], "phase16": initial["id"]},
                "phase14_evaluation": {k: amended["body"][k] for k in ("start", "end_exclusive")},
                "phase15_spot_overlap": {k: phase15["body"][k] for k in ("start", "publication_end_exclusive", "follow_end_exclusive")},
                "rights": {"venue": "KuCoin", "source": "Official unauthenticated public GET; local research only",
                           "feature_context": True, "development_labels": True, "model_fitting": True,
                           "prospective_evaluation": False, "live_use": False, "redistribution": False},
                "overlap": "Phase 13/14 raw futures through the protected horizon supply coverage counts only. Phase 14 seed is never loaded. Phase 15 Spot market times may overlap independent public hourly observations; paper events, outcomes and user data are not inputs. Existing protocol roles are unchanged.",
                "revisions": "Keep first received observations and corrections. Quarantine a conflicting source instant for new qualification; never rewrite a sealed bundle, label ledger or prior publication.",
                "maturity": "Strictly before each cut. Require completed causal feature inputs and a fully resolved IOC simulation with actual source and receipt evidence. No imputation, zero-label substitution, or economic score of future reserved data.",
                "evaluation": "The fixed inner and outer cuts and seven-day eligible end extensions are development for the resulting candidate. Any later prospective evaluation requires a separately frozen protocol starting after its last consumed source/receipt and freezing its version, updates and stop rules.",
                "budget": {**WORK, "spot_requests": 10500, "spot_response_bytes": 128 * 1024, "spot_retained_bytes": 96 * 1024**2,
                           "futures_shard_days": 60, "futures_shard_requests": 22500, "futures_shard_retained_bytes": 192 * 1024**2,
                           "max_native_rows": 650000, "check_period_days": 7, "max_training_campaigns": 1},
                "swing": "Produce baseline outcome readiness only. The shelved Phase 16 filter stays shelved."}
        record = save("registrations", body)
        write_json(ROOT / "registration.json", {"id": record["id"]})
        return record


def native_bundle(identifier):
    if len(identifier) != 64 or any(c not in "0123456789abcdef" for c in identifier):
        raise ValueError("Invalid Phase 17 bundle identity")
    path = ROOT / "bundles" / (identifier + ".json")
    bundle = sealed(path, identifier)
    registration = registered()
    body = bundle["body"]
    if (not registration or body["registration"] != registration["id"] or body["scope"] != "RESEARCH_ONLY"
            or body["state"] != "ADMITTED" or body["descriptor"]["profile"] == "swing"):
        raise ValueError("Phase 17 native bundle is not admitted for this learner")
    profile = registration["body"]["profiles"][body["descriptor"]["profile"]]
    if utc(body["descriptor"]["start"]) < utc(profile["start"]):
        raise ValueError("Phase 17 bundle crosses its protected input boundary")
    return bundle, path


def data_catalog():
    from lab.phase13 import protocol
    from lab.phase14.context import pointer
    from lab.phase16.plan import freeze_initial

    stream = registered()
    initial, seed, original = freeze_initial(), pointer("seed"), protocol.active()
    if not stream or not seed or not original:
        raise ValueError("Register the protected dependencies and native stream first")
    policy = stream["body"]
    history = {}
    for profile, row in initial["body"]["profiles"].items():
        history[profile] = {"id": row["data_manifest"], "scope": row["source_scope"],
                            "start": row["train_start"], "end_exclusive": row["exclusive_data_end"],
                            "consumed_by": [initial["id"]], "phase17_uses": [],
                            "evidence": "Preserved prior development; no change to source rights"}
    return save("catalogs", {"version": "phase17-source-catalog-v1", "stream": stream["id"], "historical": history,
        "holdout": policy["holdout"] | {"phase17_uses": [], "performance_evaluation": "SEALED"},
        "phase14_seed": {"id": seed["id"], "scope": seed["body"]["scope"], "phase17_uses": [],
                          "original_use": "Frozen Phase 14 causal feature context"},
        "protected_futures": {"venue": "KuCoin", "instrument": "XBTUSDTM", "bases": ["trade", "mark", "settled_funding"],
                              "first_source_protocol": original["id"], "amendment": policy["parent_protocols"]["phase14"],
                              "window": policy["phase14_evaluation"], "phase17_uses": ["COVERAGE_ONLY"]},
        "phase15_overlap": {"id": policy["parent_protocols"]["phase15"], "window": policy["phase15_spot_overlap"],
                            "allowed": "Independent public BTC-USDT Spot hourly trade receipts",
                            "excluded": "Published ideas, account events, paper P&L and user reports"},
        "new_stream": {"rights": policy["rights"], "overlap": policy["overlap"],
                       "profiles": {k: {f: v[f] for f in ("market", "instrument", "start", "end_exclusive")} for k,v in policy["profiles"].items()}}})
