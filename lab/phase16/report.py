from math import sqrt

import pandas as pd

from lab.phase11.campaign import gates_off
from lab.phase13.observation import PULSE
from lab.phase13.registry import sealed
from lab.phase15.catalog import registered as paper_registration
from lab.phase16.control import active, optional, settings
from lab.phase16.evaluate import counts
from lab.phase16.plan import ROOT, HYPOTHESES, folder
from lab.storage import put_bytes, read_json, utc, utc_now


def pooled(folds, role):
    errors = [value["outer_forecast"][role] for value in folds.values() if value["state"] == "EVALUATED"]
    n = sum(row["count"] for row in errors)
    return {"matched_labels": n, "mae_R": sum(row["count"]*row["mae_net_r"] for row in errors)/n if n else None,
            "rmse_R": sqrt(sum(row["count"]*row["rmse_net_r"]**2 for row in errors)/n) if n else None}


def primary_comparison(profile):
    accounts = profile.get("accounts", {})
    return {case: {lane: {key: row["metrics"][key] for key in (
            "return", "closed_trades", "drawdown_observed_marks", "exposure_fraction_of_observed_marks")}
            for lane in ("challenger", "unfiltered") if (row := accounts.get(f"{lane}/{case}"))}
            for case in ("base", "double_cost", "delay")} if accounts else {}


def status():
    ctrl = settings()
    initial = optional(ROOT/"initial.json")
    manifest = sealed(folder(initial["id"])/"manifest.json", initial["id"]) if initial else None
    result = optional(folder(initial["id"])/"result.json") if initial else None
    process = active()
    live_stage = optional(folder(process["campaign"])/"stage.json") if process else None
    paper = paper_registration()
    now = utc_now()
    queue = [read_json(path) for path in (ROOT/"queue").glob("*.json")]
    profiles = {}
    for name in HYPOTHESES:
        cfg = manifest["body"]["profiles"][name] if manifest else {}
        policy = manifest["body"]["policy"] if manifest else {}
        last, last_at, progress, maturity, last_fit, fitted_at = None, None, None, None, None, None
        for path in (ROOT/"campaigns").glob("*/result.json"):
            row = read_json(path)
            candidate = row["profiles"].get(name)
            if not candidate:
                continue
            if not last_at or utc(row["completed_at"]) > utc(last_at):
                last, last_at = candidate, row["completed_at"]
                progress = optional(path.parent/"input-progress"/f"{name}.json")
                maturity = optional(path.parent/"allocation-maturity.json")
                latest = sealed(path.parent/"manifest.json", row["campaign"])
                cfg = latest["body"]["profiles"][name]
                policy = latest["body"]["policy"]
            if candidate["state"] == "COMPLETED_RESEARCH_ONLY" and (not fitted_at or utc(row["completed_at"]) > utc(fitted_at)):
                last_fit, fitted_at = candidate, row["completed_at"]
        pending = sorted((row for row in queue if row["profile"] == name and not (folder(row["id"])/"result.json").exists()),
                         key=lambda row: row["not_before"])
        next_at = (utc(fitted_at)+pd.Timedelta(days=cfg["interval_days"])).isoformat() if fitted_at and cfg else None
        queued_source = None
        if pending:
            next_at = pending[0]["not_before"]
            queued_id = pending[0]["id"]
            queued_cfg = sealed(folder(queued_id)/"manifest.json", queued_id)["body"]["profiles"][name]
            queued_source = {"campaign": queued_id, "data_manifest": queued_cfg["data_manifest"],
                             "scope": queued_cfg["source_scope"], "end_exclusive": queued_cfg["exclusive_data_end"]}
        if live_stage and live_stage.get("profile") == name:
            state, reason = live_stage["state"], "Bounded worker owns the research slot"
        elif ctrl["action"] == "pause":
            state, reason = "PAUSED", "The challenger checkpoint and original deadline are retained."
        elif not ctrl["recurring"]:
            state, reason = "TRAINING_DISABLED", "Recurring Phase 16 checks are off. Run-once is explicit."
        elif not manifest:
            state, reason = "DATA_NOT_ADMITTED", "Freeze the initial admitted source plan"
        elif pending:
            clock = optional(folder(pending[0]["id"])/"deadline.json")
            if clock and utc(clock["deadline"]) <= now:
                state, reason = "DEADLINE_ELAPSED", "The original allocated campaign deadline expired"
            else:
                state = "QUEUED" if now >= utc(next_at) else "WAITING_FOR_INTERVAL"
                reason = "Allocated native data awaits the supervised mature-label and chronological checks"
        elif last and last["state"] in {"WAITING_FOR_NEW_MATURE_LABELS", "DATA_NOT_ADMITTED", "NO_NEW_INPUTS"}:
            state, reason = last["state"], last["reason"]
        else:
            state, reason = "NO_NEW_INPUTS", "No separately admitted new development allocation"
        evaluated = [row for row in (last_fit or {}).get("folds", {}).values() if row["state"] == "EVALUATED"]
        models = [row["artifacts"]["outer"] for row in evaluated]
        baseline = {key.removesuffix("/base"): {field: account["metrics"][field]
                    for field in ("return", "closed_trades", "drawdown_observed_marks")}
                    for key, account in (last_fit or {}).get("accounts", {}).items() if key.endswith("/base")}
        if baseline:
            baseline["cash"] = {"return": 0., "closed_trades": 0, "drawdown_observed_marks": 0.}
        profiles[name] = {"state": state, "reason": reason, "source": cfg.get("source_scope"),
            "data_manifest": (progress or {}).get("data_manifest", cfg.get("data_manifest")), "recipe": cfg.get("trial"),
            "features": cfg.get("features"), "target": policy.get("target"), "comparators": policy.get("controls"),
            "budget": policy.get("budget"), "queued_source": queued_source,
            "training_cutoff": models[-1]["training_cutoff"] if models else None,
            "latest_model_train_mature": models[-1]["mature_labels"] if models else None,
            "development_cutoff": (progress or {}).get("cutoff", cfg.get("exclusive_data_end")),
            "mature_labels": (progress or {}).get("mature_labels"),
            "new_mature_labels": (last or {}).get("new_mature_labels", len((maturity or {}).get("new_decision_mature_ids", []))),
            "required_new_mature": cfg.get("minimum_new_mature"), "interval_days": cfg.get("interval_days"),
            "last_completed_at": last_at, "last_state": (last or {}).get("state"), "conclusion": (last or {}).get("conclusion"),
            "conclusion_reason": (last or {}).get("reason"), "last_model_completed_at": fitted_at,
            "next_eligible_check": next_at, "models": models, "queued": pending, "last_model_baseline": baseline,
            "primary_comparison": primary_comparison(last_fit or {}),
            "live_recipes": [row["strategy_version"]+" / "+row["exit"] for row in (paper or {}).get("body", {}).get("lanes", {}).values()
                             if row["profile"] == name],
            "permission": "RESEARCH_ONLY. Prospective book needs explicit source rights, independent qualified forward evaluation, sample floors and stress clearance."}
    return {"approval": "No validated model", "execution": "MANUAL_OUTSIDE_LAB", "legacy_training_off": gates_off(),
            "controls": ctrl, "campaign": initial["id"] if initial else None, "active": process, "stage": live_stage,
            "profiles": profiles, "initial_result": {k:v for k,v in result.items() if k != "profiles"} if result else None,
            "resource_evidence": optional(folder(initial["id"])/"resources.json") if initial else None,
            "counts": counts(folder(initial["id"])) if initial else {}, "budget": manifest["body"]["policy"]["budget"] if manifest else None,
            "next_managed_check": (optional(PULSE) or {}).get("wake"),
            "last_managed_check": optional(ROOT/"last-managed-check.json"),
            "phase15": {key:paper["body"][key] for key in ("start", "publication_end_exclusive", "follow_end_exclusive")} if paper else None}


def fmt(value, *, percentage=False):
    return "—" if value is None else f"{value:.2%}" if percentage else f"{value:,.4f}"


def report(identifier=None):
    initial = optional(ROOT/"initial.json")
    campaign = identifier or (initial or {}).get("id")
    root = folder(campaign) if campaign else None
    manifest = sealed(root/"manifest.json", campaign)["body"] if root else None
    result = optional(root/"result.json") if root else None
    ledger = counts(root) if root else {}
    lines = ["# Phase 16 controlled challenger learning", "", "Approval **No validated model**. Execution **MANUAL_OUTSIDE_LAB**.", ""]
    if manifest:
        policy = manifest["policy"]
        lines += [f"Frozen campaign `{campaign}` · {manifest['kind']} · {manifest['scope']}.", "",
                  f"Target. {policy['target']}", "",
                  "Comparators. " + ", ".join(policy["controls"]) + ".", "",
                  f"Resource budget {policy['budget']}. Attempt ledger {ledger}.", ""]
        if manifest["kind"] == "INITIAL_DEVELOPMENT":
            lines += ["All initial comparisons use consumed pre-holdout development data.", ""]
        for name, cfg in manifest["profiles"].items():
            trial = cfg["trial"]
            lines += [f"**{name.title()} input**. `{cfg['data_manifest']}` · {cfg['source_scope']} · "
                      f"{trial['entry']} / {trial['exit_policy']} · ends {cfg['exclusive_data_end']} exclusive.", ""]
    if result:
        for name, profile in result["profiles"].items():
            lines += [f"## {name.title()}", "", f"{profile['state']}. {profile.get('conclusion','—')}. {profile['reason']}.", ""]
            folds = profile.get("folds", {})
            if folds:
                lines += ["| Fold | State | Inner train / validation | Outer train / evaluation | Challenger MAE | Mean MAE |",
                          "| --- | --- | --- | --- | ---: | ---: |"]
                for fold,row in folds.items():
                    c = row["sample_counts"]
                    forecasts = row.get("outer_forecast", {})
                    lines.append(f"| {fold} | {row['state']} | {c['inner_train']} / {c['inner_validation']} | "
                                 f"{c['outer_train']} / {c['outer_mature']} | {fmt(forecasts.get('challenger',{}).get('mae_net_r'))} | "
                                 f"{fmt(forecasts.get('training_mean',{}).get('mae_net_r'))} |")
                lines += ["", f"Matched pooled outer forecast metrics in R. Challenger {pooled(folds,'challenger')}. "
                          f"Training mean {pooled(folds,'training_mean')}.", ""]
            if accounts := profile.get("accounts"):
                lines += ["### Candidate and matching unfiltered rule", "",
                          "| Assumptions | Candidate net return / closed | Rule net return / closed | Candidate / rule drawdown | Candidate / rule exposure |",
                          "| --- | ---: | ---: | ---: | ---: |"]
                for case, lanes in primary_comparison(profile).items():
                    c, r = (lanes[key] for key in ("challenger", "unfiltered"))
                    lines.append(f"| {case} | {fmt(c['return'],percentage=True)} / {c['closed_trades']} | "
                                 f"{fmt(r['return'],percentage=True)} / {r['closed_trades']} | "
                                 f"{fmt(c['drawdown_observed_marks'],percentage=True)} / {fmt(r['drawdown_observed_marks'],percentage=True)} | "
                                 f"{fmt(c['exposure_fraction_of_observed_marks'],percentage=True)} / {fmt(r['exposure_fraction_of_observed_marks'],percentage=True)} |")
                identities = [f"{fold} `{row['artifacts']['outer']['version']}`" for fold,row in folds.items() if row["state"] == "EVALUATED"]
                lines += ["", "Outer model versions. " + ", ".join(identities) + ". Evidence classification "
                          + ("consumed historical development." if manifest["kind"] == "INITIAL_DEVELOPMENT" else "new native development, research only."), ""]
                lines += ["Cash return and drawdown are 0% in all cases.", "",
                          "| Comparator / case | Net return | Drawdown | Closed | Net expectancy USDT / R | Turnover | Marked exposure | Hold minutes | Worst loss | Complete |",
                          "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
                for key,account in accounts.items():
                    m = account["metrics"]
                    lines.append(f"| {key} | {fmt(m['return'],percentage=True)} | {fmt(m['drawdown_observed_marks'],percentage=True)} | "
                                 f"{m['closed_trades']} | {fmt(m['net_expectancy'])} / {fmt(m['mean_net_r'])} | {fmt(m['turnover'])} | "
                                 f"{fmt(m['exposure_fraction_of_observed_marks'],percentage=True)} | {fmt(m['mean_hold_minutes'])} | "
                                 f"{fmt(m['worst_closed_trade'])} | {m['accounting_complete']} |")
                for key,account in accounts.items():
                    m = account["metrics"]
                    lines += ["", f"**{key}**. Modeled gross {fmt(m['gross_at_modeled_prices'])}, adverse already embedded "
                              f"{fmt(m['adverse_already_in_gross'])}, fees {fmt(m['fees'])}, net funding {fmt(m['funding'])}, "
                              f"funding paid {fmt(m['funding_paid'])}, funding received {fmt(m['funding_received'])}, "
                              f"top-five winners {fmt(m['top_five_winners'])}, losses over budget {m['planned_risk_overruns']}, "
                              f"ambiguities {m['ambiguous_closed_trades']}, unresolved {m['unresolved_reason'] or '—'}. "
                              f"25-trade floor {m['closed_sample_sufficient']}."]
            lines.append("")
    elif root:
        lines += ["Campaign result pending.", ""]
    path = (root if identifier else ROOT)/"REPORT.md"
    put_bytes(path, ("\n".join(lines)+"\n").encode())
    return {"report": str(path), "approval": "No validated model", "campaign": campaign,
            "counts": ledger, "profiles": {name: row.get("conclusion") or row["state"]
                                            for name, row in result["profiles"].items()} if result else {}}
