from pathlib import Path

import pandas as pd

from lab.config import ROOT, fingerprint
from lab.phase2 import store
from lab.phase2.checkpoint import frozen_runs
from lab.phase4.sensitivity import OUTPUT as PHASE4
from lab.phase5.design import LABEL, OUTPUT as PHASE5, checked_completion, sources
from lab.storage import RUNS, file_hash, read_json, utc_now, write_json, writer_lock

OUTPUT = RUNS / "phase6"
PHASE5_ID = "swing-38dc720717c2d8"
VERSION = "risk-and-observer-readiness-v1"
RECEIPT_HORIZON = pd.Timedelta("120s")


def origin():
    plan, _, phase3, _, _, phase4 = sources()
    phase5 = PHASE5 / PHASE5_ID
    checked_completion(phase5)
    previous = read_json(phase5 / "manifest.json")
    if previous["phase3_design_sha256"] != plan["design_sha256"] or (
        previous["phase4_completion_sha256"] != file_hash(phase4 / "complete.json")
    ):
        raise ValueError("Phase 5 does not match the pinned Phase 3 and 4 sources")
    return plan, phase3, phase4, phase5, previous


def freeze(root: Path = OUTPUT) -> Path:
    with writer_lock(root / ".writer.lock"):
        pointer = root / "frozen.json"
        if pointer.exists():
            directory = root / read_json(pointer)["directory"]
            read_manifest(directory)
            return directory
        plan, phase3, phase4, phase5, previous = origin()
        settings = read_json(PHASE4 / "verification.json")
        draft = Path(settings["draft_file"])
        if file_hash(draft) != settings["draft_sha256"]:
            raise ValueError("The registered MEXC draft has changed")
        with store.connect(readonly=True) as db:
            db.execute("BEGIN")
            experiment = store.get(db, "experiment")
        if not experiment or experiment["holdout_performance_revealed"] or experiment["paper_approved"]:
            raise ValueError("Unexpected experimental shadow state")
        as_of = utc_now()
        specification = {
            "label": LABEL, "version": VERSION, "observation_as_of_utc": as_of.isoformat(),
            "historical_evaluation_start_utc": previous["requested_start_utc"],
            "historical_evaluation_end_utc": previous["requested_end_utc"],
            "holdout": plan["windows"], "holdout_performance_revealed": False,
            "phase3_completion_sha256": file_hash(phase3 / "complete.json"),
            "phase4_completion_sha256": file_hash(phase4 / "complete.json"),
            "phase5_completion_sha256": file_hash(phase5 / "complete.json"),
            "phase5_design_sha256": previous["design_sha256"],
            "phase5_verification_sha256": file_hash(PHASE5 / "verification.json"),
            "price_snapshot": plan["source_snapshot"], "frozen_profile": plan["profile"],
            "frozen_profile_hash": plan["profile_hash"], "costs": previous["costs"]["lower_fee"],
            "original_execution_logic_sha256": plan["phase1_logic_sha256"],
            "dependencies": plan["dependencies"],
            "shadow_profile_manifests": {key: file_hash(folder / "manifest.json")
                                         for key, (folder, _) in frozen_runs().items()},
            "original_experiment": experiment,
            "prior_observation_code_sha256": {name: file_hash(ROOT / "lab" / "phase2" / name)
                                               for name in ("shadow.py", "shadow_ledger.py", "collector.py", "store.py")},
            "config_sha256": {name: file_hash(ROOT / name) for name in ("profiles.yaml", "research.yaml", "mexc-research.yaml")},
            "mexc_draft": {"path": str(draft), "sha256": settings["draft_sha256"], "status": "DRAFT_BLOCKED"},
            "risk_audit": {
                "account": "Preserved unfiltered Swing lower-fee execution. All 68 closed trades.",
                "budget": "0.25% of decision-time equity.",
                "planned": "Cost-inclusive loss to the original stop for the actual planned quantity at sizing reference prices.",
                "overrun": "max(0, -realized net USD - decision-time risk budget USD)",
                "reconciliation": "Relative to budget, sum planned sizing slack, raw entry and stop-to-exit price movement, "
                                  "fee change and adverse price change. No inferred intrabar path.",
                "details": "All reported overruns, first chronological within-budget loser and first chronological winner.",
            },
            "observation": {
                "historical_candle_lookback_hours": 24, "prospective_decision_lookback_hours": 24,
                "arrival_denominator": "All closes at or after activation with elapsed descriptive close+120s horizon, "
                                       "including downtime and periods with unknown receipt instrumentation.",
                "arrival_horizon_seconds": int(RECEIPT_HORIZON.total_seconds()),
                "latency_target": "No decision or candle SLA registered; the 120s receipt threshold is descriptive only.",
                "decision_deadline": "Each pinned modeled opening. Reuse the original journal and revision receipt proof.",
            },
            "replay_budget": "Zero planned. At most one separately frozen corrective lower-fee replay only for a confirmed defect.",
            "no_retraining_or_promotion": True, "no_new_cohort": True,
        }
        digest = fingerprint(specification)
        directory = root / f"audit-{digest[:14]}"
        write_json(directory / "manifest.json", {**specification, "manifest_sha256": digest, "frozen_at_utc": as_of.isoformat()})
        write_json(pointer, {"directory": directory.name, "label": LABEL})
        return directory


def read_manifest(directory: Path) -> dict:
    raw = read_json(directory / "manifest.json")
    hashed = {key: value for key, value in raw.items() if key not in ("manifest_sha256", "frozen_at_utc")}
    if raw["version"] != VERSION or fingerprint(hashed) != raw["manifest_sha256"]:
        raise ValueError("Frozen audit manifest changed")
    return raw
