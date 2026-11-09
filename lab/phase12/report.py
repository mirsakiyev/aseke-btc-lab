from collections import Counter

import psutil

from lab.learning.control import admission
from lab.phase10.worker import status as worker_status
from lab.phase11 import campaign
from lab.phase11.archive import size_under
from lab.phase11.resources import freeze as freeze_resources
from lab.phase12 import view
from lab.storage import put_bytes, read_json


def optional(path):
    return read_json(path) if path.exists() else None


def current_campaign():
    paths = sorted((view.ROOT / "campaigns").glob("*/manifest.json"), key=lambda path: path.stat().st_mtime)
    return paths[-1].parent if paths else None


def active_process(folder):
    active = optional(folder / "active.json")
    if active:
        try:
            child = psutil.Process(active["pid"])
            if child.create_time() == active["created"]:
                return {**active, "rss_bytes": sum(proc.memory_info().rss for proc in [child, *child.children(recursive=True)])}
        except psutil.Error:
            pass
    return None


def status():
    pointer = optional(view.ROOT / "view.json")
    archived = optional(view.ROOT / "views" / f'{pointer["id"]}.json') if pointer else None
    folder = current_campaign()
    live = worker_status()
    result = eligibility = execution = clock = active = observation = None
    progress = {}
    if folder:
        result = optional(folder / "result.json")
        eligibility = optional(folder / "eligibility.json")
        execution = optional(folder / "fit-manifest.json")
        clock = optional(folder / "deadline.json")
        active = active_process(folder)
        history = sorted((folder / "observations").glob("*.json"), key=lambda path: path.stat().st_mtime)
        observation = read_json(history[-1]) if history else None
        progress = campaign.ledger(folder)
        progress["lanes_prepared"] = len(list(folder.glob("lanes/*/*/prepared.json")))
        progress["output_bytes"] = size_under(folder)
    blocked = None
    if folder and not clock and not active:
        resources = freeze_resources()["body"]
        blocked = ("Continuous training gates must be OFF" if not campaign.gates_off() else
                   admission(resources["unchanged_guards"]))
    state = (result["state"] if result else "FITTING" if active and eligibility else "PREPARING_ELIGIBILITY" if active
             else "PARTIAL" if clock else "WAITING_FOR_ADMISSION" if blocked else "READY" if archived else "WAITING_FOR_VIEW")
    return {"state": state, "admission_reason": blocked, "execution": "MANUAL_OUTSIDE_LAB",
            "approval": "No validated model", "campaign": folder.name if folder else None,
            "view": pointer["id"] if pointer else None, "fit_manifest": execution["id"] if execution else None,
            "clock": clock, "progress": progress, "last_supervisor_observation": observation,
            "market_coverage_folds": archived["body"]["eligible"] if archived else [],
            "profiles": eligibility["profiles"] if eligibility else None, "result": result,
            "continuous_training_off": campaign.gates_off(), "active": active,
            "kucoin": {"state": live["runtime"], "identity": live["identity"], "day": live["profiles"]["day"]}}


def percent(number):
    return f"{100 * number:.2f}%" if number is not None else "-"


def error_value(value):
    return f"{value:.6f}" if value is not None else "-"


def report():
    snapshot = status()
    folder = current_campaign()
    archive = optional(view.ROOT / "views" / f'{snapshot["view"]}.json') if snapshot["view"] else None
    lines = ["# Phase 12 complete-history futures amendment", "",
             f"Campaign **{snapshot['state']}**. Approval **No validated model**. Execution **MANUAL_OUTSIDE_LAB**.", ""]
    if snapshot["admission_reason"]:
        lines += [f"Admission condition. {snapshot['admission_reason']}.", ""]
    if archive:
        body = archive["body"]
        lines += [f"The original Phase 11 qualification {body['original']['qualification']} retains its rejection. "
                  f"The amended view {archive['id']} retains {body['source_start_inclusive']} through "
                  f"{body['source_end_exclusive']} exclusive. It contains {body['rows']['trade']:,} trade and mark "
                  f"minutes each and {body['funding']['rows']:,} original funding settlements.", "",
                  f"The earlier prefix contained {body['excluded']['trade']:,} trade rows, "
                  f"{body['excluded']['mark']:,} mark rows and {body['excluded']['funding']:,} settlements. "
                  f"{body['excluded']['regimes']}", "",
                  f"Cached acquisition remains at {body['acquisition']['requests_used']} of "
                  f"{body['acquisition']['requests_used'] + body['acquisition']['requests_remaining']} GETs. "
                  "Historical scope is **RESEARCH_ONLY / HYPOTHETICAL_REPLAY**. KuCoin reference fees and adverse pricing "
                  "apply to Binance-source data. The sealed holdout remains excluded.", ""]
    if snapshot["profiles"]:
        lines += ["## Feature readiness", "", "| Profile | First feature-ready UTC | Ready rows |",
                  "| --- | --- | ---: |"]
        lines.extend(f"| {name} | {item['first_feature_ready']} | {item['feature_ready_count']:,} |"
                     for name, item in snapshot["profiles"].items())
        lines.append("")
    elif warmup := optional(view.ROOT / "feature-audit.json"):
        lines += ["## Warm-up audit before the first inner split", "", "| Profile | First feature-ready UTC | Ready before split |",
                  "| --- | --- | ---: |"]
        lines.extend(f"| {name} | {item['first_feature_ready']} | {item['ready_before_first_inner_split']:,} |"
                     for name, item in warmup["profiles"].items())
        lines.append("")
    if snapshot["state"] == "COMPLETE" and (checked := optional(view.ROOT / "verified-results.json")):
        lines += ["## Verified result summary", "",
                  f"Verified {checked['fit_artifacts']} model artifact hashes, "
                  f"{checked['estimator_forecast_eval_count']} estimator forecast evaluations, and "
                  f"{checked['account_fold_evals']} account-fold replays. The forecast count includes "
                  f"{checked['forecast_eval_count'] - checked['estimator_forecast_eval_count']} additional mean-control evaluations.", "",
                  "| Estimator | Weighted outer MAE (R) | Matched mean MAE (R) | Lower MAE |", "| --- | ---: | ---: | ---: |"]
        for model, info in checked["forecast_comparison"].items():
            lines.append(f"| {model} | {info['weighted_mae']:.4f} | {info['weighted_mean_control']:.4f} | "
                         f"{info['lower_mae']}/{info['matches']} |")
        lines.append("")
    if folder:
        eligibility = optional(folder / "eligibility.json")
        if eligibility:
            totals = Counter((trial["profile"], key, row["status"]) for trial in eligibility["trials"].values()
                             for key, row in trial["folds"].items())
            lines += ["## Frozen variant eligibility", "",
                      "| Profile | Fold | Eligible variants | Ineligible variants |",
                      "| --- | --- | ---: | ---: |"]
            for profile in eligibility["profiles"]:
                for fold in snapshot["market_coverage_folds"]:
                    lines.append(f"| {profile} | {fold} | {totals[profile,fold,'ELIGIBLE']} | {totals[profile,fold,'INELIGIBLE']} |")
            lines += ["", "| Candidate | Fold | Inner train | Outer train | Validation | Outer outcomes | Gate |",
                      "| --- | --- | ---: | ---: | ---: | ---: | --- |"]
            for name, item in eligibility["trials"].items():
                for fold, row in item["folds"].items():
                    counts = row.get("sample_counts", {})
                    lines.append(f"| {name} | {fold} | {counts.get('inner_train','-')} | {counts.get('outer_train','-')} | "
                                 f"{counts.get('inner_validation','-')} | {counts.get('outer_mature','-')} | "
                                 f"{'. '.join(row['reasons']) or row['status']} |")
            lines.append("")
        rows = [read_json(path) for path in folder.glob("trials/*/result.json")]
        progress = snapshot["progress"]
        lines += ["## Historical execution", "",
                  f"Completed {progress['actual_estimator_fits']} historical estimator fits. "
                  f"Recorded {progress['candidate_results']} candidate and control results. "
                  f"Prepared {progress['lanes_prepared']} of 12 label lanes.", ""]
        if rows:
            ordering = read_json(folder / "manifest.json")["body"]["trial_order"]
            rank = {trial["id"]: i for i, trial in enumerate(t for trials in ordering.values() for t in trials)}
            rows.sort(key=lambda row: rank[row["trial"]["id"]])
            lines += ["| Candidate | Status | Net return | Drawdown | Closed trades | Coverage |",
                      "| --- | --- | ---: | ---: | ---: | --- |"]
            for row in rows:
                m = row.get("metrics", {})
                lines.append(f"| {row['trial']['id']} | {row['status']} | {percent(m.get('return'))} | "
                             f"{percent(m.get('drawdown_observed_marks'))} | {m.get('closed_trades','-')} | "
                             f"{m.get('coverage_scope', m.get('account_state','-'))} |")
            baseline = {(row["profile"], row["trial"].get("entry"), row["trial"].get("exit_policy")): row.get("metrics", {})
                        for row in rows if row["trial"]["model"] == "rules_v1"}
            lines += ["", "## Account comparison across full outer coverage", "",
                      "| Candidate | Net return | Matching unfiltered rule | Difference vs rule |",
                      "| --- | ---: | ---: | ---: |"]
            for row in rows:
                trial, metrics = row["trial"], row.get("metrics", {})
                if trial["model"] not in ("ridge_v1", "hist_v1", "mean_control_v1"):
                    continue
                matching = baseline.get((row["profile"], trial["entry"], trial["exit_policy"]), {})
                model_return, rule_return = metrics.get("return"), matching.get("return")
                if (metrics.get("full_window_observed") and matching.get("full_window_observed")
                        and model_return is not None and rule_return is not None):
                    lines.append(f"| {trial['id']} | {percent(model_return)} | {percent(rule_return)} | "
                                 f"{100 * (model_return-rule_return):+.2f} pp |")
            mean = {(row["profile"], row["trial"]["entry"], row["trial"]["exit_policy"], fold["fold"]):
                    fold["outer_error"]["mae_net_r"] for row in rows if row["trial"]["model"] == "mean_control_v1"
                    for fold in row.get("folds", []) if fold.get("outer_error")}
            lines += ["", "## Forecast errors", "",
                      "| Candidate | Fold | MAE inner (R) | MAE outer (R) | Mean baseline outer (R) | Outer labels |",
                      "| --- | --- | ---: | ---: | ---: | ---: |"]
            for row in rows:
                for fold in row.get("folds", []):
                    if fold.get("outer_error"):
                        inner, outer = fold["inner_error"], fold["outer_error"]
                        trial = row["trial"]
                        reference = mean.get((row["profile"], trial["entry"], trial["exit_policy"], fold["fold"]))
                        lines.append(f"| {row['trial']['id']} | {fold['fold']} | {error_value(inner['mae_net_r'])} | "
                                     f"{error_value(outer['mae_net_r'])} | {error_value(reference)} | {outer['count']} |")
            lines.append("")
        if snapshot["last_supervisor_observation"]:
            used = snapshot["last_supervisor_observation"]
            lines += [f"Sampled peak child-tree RSS was {used['peak_child_rss_bytes_sampled']/2**20:.1f} MiB. "
                      f"Research outputs occupied {used['outputs_bytes']/2**20:.2f} MiB. "
                      f"Supervisor stop reason was {used.get('stop_reason') or 'none recorded'}.", ""]
    path = view.ROOT / "REPORT.md"
    put_bytes(path, ("\n".join(lines) + "\n").encode())
    return {"report": str(path), "state": snapshot["state"], "campaign": snapshot["campaign"],
            "fit_manifest": snapshot["fit_manifest"], "progress": snapshot["progress"]}
