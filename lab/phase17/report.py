from contextlib import closing

import pandas as pd

from lab.phase13 import observation, protocol
from lab.phase16 import control
from lab.phase16.plan import folder
from lab.phase16.report import status as challenger_status
from lab.phase17.collection import GRACE, HOUR, spot_store
from lab.phase17.registry import ROOT, registered
from lab.storage import put_bytes, utc, utc_now


def paper_counts():
    from lab.phase15.catalog import ROOT as PAPER, db_open, registered as paper

    registration = paper()
    counts = {}
    if not registration or not (PAPER / "paper.sqlite3").exists():
        return counts
    with closing(db_open(readonly=True)) as db:
        for lane in registration["body"]["lanes"].values():
            row = counts.setdefault(lane["profile"], {"published_ideas": 0, "paper_hypothetical_fills": 0, "paper_closed": 0})
            kinds = dict(db.execute("SELECT kind,count(*) FROM event WHERE book=? GROUP BY kind", (lane["book_id"],)))
            for key, kind in (("published_ideas", "PUBLISHED"), ("paper_hypothetical_fills", "HYPOTHETICAL_FILL"), ("paper_closed", "CLOSED")):
                row[key] += kinds.get(kind, 0)
    return counts


def status():
    from lab.phase17.pilot_report import managed_capture_view, status as pilot_status

    record = registered()
    now = utc_now()
    if not record:
        return {"state": "UNREGISTERED", "approval": "No validated model", "execution": "MANUAL_OUTSIDE_LAB"}
    body = record["body"]
    latest = managed_capture_view(control.optional(ROOT / "last-managed.json"))
    challenger = challenger_status()
    paper = paper_counts()
    rows = {}
    for name, cfg in body["profiles"].items():
        saved = control.optional(ROOT / "readiness" / f"{name}.json") or {}
        admitted = control.optional(ROOT / "admitted" / f"{name}.json") or {}
        result = control.optional(folder(admitted["campaign"]) / "result.json") if admitted else None
        result = result["profiles"].get(name, {}) if result else {}
        phase16 = challenger["profiles"][name]
        current = latest.get("spot_coverage" if cfg["market"] == "SPOT" else "futures") if latest else None
        qualified_start = utc(cfg["start"])
        context = utc(cfg["context_lower_bound"])
        if now < qualified_start:
            state, reason = "WAITING_FOR_REGISTERED_START", f"Eligible {cfg['instrument']} collection starts {cfg['start']}"
            if cfg["market"] == "FUTURES":
                reason += ". Protected futures minutes count as shared coverage only"
        elif now < context:
            state, reason = "WAITING_FOR_FEATURE_CONTEXT", f"Need the registered {cfg['context_days']}-day context and causal complete decision bars; calendar lower bound {cfg['context_lower_bound']}"
        else:
            state, reason = saved.get("state", "WAITING_FOR_LABEL_CHECK"), saved.get("reason", "Waiting for a supervised native cutoff and receipt check")
        counts = saved.get("counts", {})
        accounts = result.get("accounts", {})
        primary = accounts.get("challenger/base", {}).get("metrics", {})
        stress = [accounts.get("challenger/" + case, {}).get("metrics", {}) for case in ("double_cost", "delay")]
        rows[name] = {"market": cfg["market"], "instrument": cfg["instrument"], "state": state, "reason": reason,
                      "capture": current, "last_label_cutoff": saved.get("cutoff"), "source_bundle": saved.get("source_bundle"),
                      "feature_ready_decisions": counts.get("feature_ready", 0), "eligible_baseline_setups": counts.get("eligible_baseline_setups", 0),
                      "mature_permitted_labels": counts.get("mature_permitted_labels", 0), "new_mature_labels": saved.get("new_mature", 0),
                      "hypothetical_executions": counts.get("hypothetical_fills", 0), "exclusions": counts.get("exclusions", {}),
                      "closed_evaluation_trades": primary.get("closed_trades", 0),
                      "stress_complete_cases": sum(bool(case.get("accounting_complete")) for case in stress),
                      "stress_closed_trades_by_case": {key: case.get("closed_trades", 0) for key, case in zip(("double_cost", "delay"), stress)},
                      "paper": paper.get(name, {}), "folds": saved.get("folds", {}),
                      "funding_slots": counts.get("observed_funding_slots") if cfg["market"] == "FUTURES" else None,
                      "funding_unresolved": counts.get("unresolved_funding_slots") if cfg["market"] == "FUTURES" else None,
                      "whole_source_coverage": counts.get("source_state"),
                      "admitted_bundle": admitted.get("bundle"), "campaign": admitted.get("campaign"),
                      "candidate_queue_enabled": cfg["queue"], "training_state": phase16["state"], "last_native_result": result.get("conclusion"),
                      "last_phase16_result": phase16["conclusion"], "next_training_check": admitted.get("not_before"),
                      "first_registered_cutoff": cfg["first_cutoff"], "feature_context_lower_bound": cfg["context_lower_bound"],
                      "minimums": {"new_mature": cfg["minimum_new_mature"], "training": cfg["minimum_train"],
                                   "inner": cfg["minimum_inner"], "outer": cfg["minimum_outer"], "closed_economic": 25,
                                   "interval_days": cfg["interval_days"]}}
    spot_db = observation.directory(spot_store(record)) / "observations.sqlite3"
    return {"state": "COLLECTING_AND_QUALIFYING", "approval": challenger["approval"], "execution": challenger["execution"],
            "registration": record["id"], "activation": body["activation"], "recurring_training": challenger["controls"]["recurring"],
            "legacy_training_off": challenger["legacy_training_off"], "last_managed": latest,
            "first_complete_managed": control.optional(ROOT / "first-complete-managed.json"),
            "next_managed_check": challenger["next_managed_check"],
            "next_spot_due": (max(utc(body["profiles"]["position"]["start"])+HOUR,
                                  (now-GRACE).floor(HOUR)+HOUR)+GRACE).isoformat(),
            "spot_store_exists": spot_db.exists(), "profiles": rows,
            "last_label_guard": control.optional(ROOT / "label-admission.json"), "managed_error": control.optional(ROOT / "managed-error.json"),
            "pilot": pilot_status(now), "last_phase16_managed_check": challenger["last_managed_check"], "new_native_estimator_fits": sum(
                (control.optional(folder(p["campaign"]) / "resources.json") or {}).get("actual_fits", 0)
                for p in rows.values() if p["campaign"]), "collection_enabled": protocol.control()["enabled"]}


def report():
    data = status()
    lines = ["# Phase 17 KuCoin-native readiness", "", "Approval **No validated model**. Execution **MANUAL_OUTSIDE_LAB**.", ""]
    if "profiles" in data:
        lines += [f"Registration `{data['registration']}` activated {data['activation']}.", "",
                  f"Recurring training {'on' if data['recurring_training'] else 'off'}. Native fits {data['new_native_estimator_fits']}.", ""]
        if latest := data["last_managed"]:
            fut = latest["futures"]
            lines += [f"Last managed capture {latest['at']}. Futures source interval {fut['start']} to {fut['end_exclusive']} exclusive, "
                      f"{fut['paired']} paired minutes, {fut['trade_gaps']} trade gaps and {fut['mark_gaps']} mark gaps.", ""]
        if data["pilot"]["state"] == "PILOT_REGISTERED":
            lines += [f"## Phase 17.1 fitting pilot `{data['pilot']['id']}`", "",
                      "| Profile | Feature evidence | Mature train | Mature inner | Fit | Separate evaluation |",
                      "| --- | --- | ---: | ---: | --- | --- |"]
            for name,row in data["pilot"]["profiles"].items():
                lines.append(f"| {name.title()} | {row['feature_readiness']} | {row['train']} / {row['train_required'] or '—'} | "
                             f"{row['inner']} / {row['inner_required'] or '—'} | {row['fit_status']} | {row['evaluation']['state']} |")
            lines += ["", f"Actual pilot fits {data['pilot']['actual_pilot_fits']}.", ""]
            if first := data["pilot"]["first_spot_managed"]:
                spot = first["spot"]
                lines += [f"First managed BTC-USDT Spot hour {spot['left']} to {spot['end_exclusive']} exclusive. "
                          f"Received {spot['receipt']['received_at']} with {first['spot_coverage']['trade_gaps']} trade gaps. "
                          "Spot mark-price pairing and funding are N/A.", ""]
            lines += ["The original Phase 17 full-fold checkpoints appear below.", ""]
        for name, row in data["profiles"].items():
            lines += [f"## {name.title()}", "", f"**{row['state']}**. {row['reason']}.", "",
                      f"Feature-ready decisions {row['feature_ready_decisions']}, eligible baseline setups {row['eligible_baseline_setups']}, "
                      f"mature labels {row['mature_permitted_labels']}, new mature labels {row['new_mature_labels']}/{row['minimums']['new_mature']}, "
                      f"counterfactual executions {row['hypothetical_executions']}, closed evaluation trades {row['closed_evaluation_trades']}, "
                      f"completed stress cases {row['stress_complete_cases']}/2.", "",
                      f"Phase 15 published ideas {row['paper'].get('published_ideas',0)}. Bundle {row['admitted_bundle'] or 'pending'}. "
                      f"Last native comparison {row['last_native_result'] or 'pending'}. First registered cut {row['first_registered_cutoff']}.", ""]
    path = (ROOT / "pilot" if data.get("pilot", {}).get("state") == "PILOT_REGISTERED" else ROOT) / "REPORT.md"
    put_bytes(path, ("\n".join(lines)+"\n").encode())
    return {"report": str(path), "status": data["state"], "registration": data.get("registration")}
