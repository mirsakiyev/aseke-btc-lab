from dataclasses import asdict
from zoneinfo import ZoneInfo

import pandas as pd

from lab.config import fingerprint
from lab.phase9.data import metadata
from lab.phase9.ideas import current_rules
from lab.phase9.reference import reference
from lab.phase13.registry import ROOT, freeze, sealed
from lab.storage import utc, utc_now, read_json, write_json, writer_lock


CONTROL = ROOT / "observation-control.json"
POINTER = ROOT / "registration.json"
OBSERVATION = {"step_minutes": 15, "close_grace_seconds": 45, "decision_max_receipt_delay_seconds": 120,
               "max_requests": 22500, "max_requests_per_cycle": 3, "max_attempts_per_boundary": 3,
               "max_catchup_minutes": 180, "funding_poll_minutes": 60, "funding_overlap_hours": 24,
               "min_spacing_seconds": .35, "timeout_seconds": 5, "max_response_bytes": 2 * 1024**2,
               "max_retained_bytes": 192 * 1024**2, "min_free_disk_bytes": 2 * 1024**3}


def active():
    if POINTER.exists():
        identifier = read_json(POINTER)["id"]
        return sealed(ROOT / "protocols" / (identifier + ".json"), identifier)
    return None


def control():
    return read_json(CONTROL) if CONTROL.exists() else {"enabled": False}


def set_control(enabled):
    if type(enabled) is not bool:
        raise ValueError("Observation control requires a boolean")
    with writer_lock(ROOT / ".observation-control.lock"):
        write_json(CONTROL, {"enabled": enabled, "at": utc_now().isoformat()})
    return control()


def current_metadata():
    from lab.phase10.worker import STATE
    path = STATE / "metadata.json"
    return read_json(path) if path.exists() else metadata()


def register(now=None):
    with writer_lock(ROOT / ".protocol.lock"):
        if existing := active():
            return existing
        registry = freeze()
        cfg = reference()
        rules = current_metadata()
        now = now if now is not None else utc_now()
        if (not rules.get("as_of") or utc(rules["as_of"]) > now
                or now - utc(rules["as_of"]) > pd.Timedelta(hours=24)):
            raise ValueError("Current verified public KuCoin metadata required at registration")
        contract = asdict(current_rules("FUTURES", rules, cfg))
        start = now.floor("min") + pd.Timedelta(minutes=1)
        end = start + pd.Timedelta(days=90)
        local = ZoneInfo("America/Toronto")
        named = {}
        for lane, item in registry["body"]["candidates"].items():
            named[lane] = {key: item[key] for key in ("id", "role", "trial", "fixed_artifact", "fixed_mean_net_r",
                            "fixed_training_cutoff_exclusive", "threshold_net_r", "feature_columns")}
            if cutoff := item["fixed_training_cutoff_exclusive"]:
                named[lane]["fixed_age_days_at_registration"] = (now - utc(cutoff)).days
        body = {"registration_version": "phase13-received-future-data-v1", "registry": registry["id"],
                "registered_at": now.isoformat(), "start": start.isoformat(), "end_exclusive": end.isoformat(),
                "technical_review_due": (start + pd.Timedelta(days=7)).isoformat(), "display_timezone": str(local),
                "start_toronto": start.tz_convert(local).isoformat(), "end_toronto": end.tz_convert(local).isoformat(),
                "type": "PREREGISTERED_FUTURE_DATA_OFFLINE_REPLAY", "evidence_state": "PENDING",
                "publication": "RESEARCH_ONLY", "execution": "MANUAL_OUTSIDE_LAB", "validated": False,
                "candidates": named, "instrument": {"venue": "KuCoin", "symbol": "XBTUSDTM", "trade_type": "FUTURES",
                    "settlement": "USDT", "data_bases": ["trade", "mark", "settled_funding"], "contract": contract,
                    "metadata_at_registration": rules, "scope": "HYPOTHETICAL_REPLAY"},
                "rules": {"costs": registry["body"]["candidates"]["day/estimator"]["cost"],
                    "risk_by_profile": {profile: registry["body"]["candidates"][f"{profile}/estimator"]["risk"]
                                        for profile in ("day", "swing")},
                    "feature_history": "Exact frozen feature construction using observations after the sealed interval. "
                      "The current stored Phase 10 warm-up is not admitted: higher cadence, incomplete accounting basis, "
                      "and contains sealed-period source times. No sealed cache access or backfill.",
                    "minimum_context": {"day": "51 complete 4h bars plus frozen causal features",
                                        "swing": "201 complete 1D bars for the 200-day context and its slope"},
                    "receipt_gate": "All required closed trade feature inputs available by boundary plus 120 seconds. "
                        "Replay decision at last necessary receipt or boundary, whichever is later, plus the frozen 30 "
                        "seconds of processing. First execution opportunity is the next native minute at or after "
                        "that time, with the original Day entry and flat limits. Offline timestamps describe replay availability.",
                    "late_missing_correction": "Late receipts cannot support earlier decisions. Catch-up may repair economic "
                        "price paths with its original late receipt. An observed correction is kept separately and blocks "
                        "affected point-in-time input unless the original is valid for that decision. Unknown price or funding "
                        "while exposed leaves the outcome unresolved. Empty funding responses do not establish zero funding.",
                    "funding": "Settled native source timestamp and mark-open proxy with Phase 10 ordering. Document expected "
                        "dated funding slots from the recorded contract metadata. Distinguish zero, late receipt, and missing.",
                    "chronology": "One continuous account per lane from the registered start. Weekly reporting does not "
                        "flatten or reset positions. Preserve Day flat rules. At horizon, stop new evaluations and report "
                        "any residual exposure as unresolved at the last observed mark. Keep later settlements outside the primary window.",
                    "metadata": "Require versioned dated public contract and tier observations. Flag rule changes or stale "
                        "metadata and exclude paths whose fixed assumptions become inapplicable.",
                    "sample_floors": {"outer_forecast_mature": 10, "uncertainty_closed_trades": 25,
                                     "bootstrap": "Frozen monthly block method, 500 replicates"},
                    "fixed_horizon": "One 90-day primary window. First technical review after seven complete days. "
                        "No automatic extensions, retraining, candidate swaps, or early performance selection.",
                    "swing_readiness_warning": "With the present permissible history, Swing's context cannot warm within "
                        "90 days. Collect evidence and report a readiness shortfall rather than substituting a model or a shorter context."},
                "acquisition": OBSERVATION}
        record = {"id": fingerprint(body), "body": body}
        write_json(ROOT / "protocols" / (record["id"] + ".json"), record)
        write_json(POINTER, {"id": record["id"]})
        return record
