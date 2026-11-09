from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from lab.config import Execution, Profile, fingerprint
from lab.phase2.checkpoint import AUDITS
from lab.phase3.design import METHODS, OUTPUT, development_prices, load_plan
from lab.phase3.forecasts import build_forecasts, quality
from lab.phase3.observation import health, reconcile_day
from lab.phase3.portfolio import BENCHMARKS, run_scenario
from lab.storage import file_hash, put_bytes, read_json, utc, utc_now, write_json, writer_lock


def gaps_and_reconciliation(plan, candles, folds, predictions, scenarios):
    folder = AUDITS / plan["phase2_audit"]
    source = folder / "data-audit.json"
    if file_hash(source) != plan["phase2_reconciliation_sha256"]:
        raise ValueError("Phase 2 data audit checksum changed")
    prior = read_json(source)
    differences = pd.read_parquet(folder / "resolution-differences.parquet")
    if len(differences) != prior["reconciliation"]["different_hours"]:
        raise ValueError("Phase 2 discrepancy detail does not match the pinned audit")
    cutoff = utc(plan["windows"]["holdout_start"])
    hourly = prior["feeds"]["3600"]
    results = {}
    for fold in folds:
        start, end = (utc(fold[key]) for key in ("evaluation_start_utc", "evaluation_end_exclusive_utc"))
        observed = candles.loc[(candles.index >= start) & (candles.index < end)]
        diff = differences.loc[(differences.index >= start) & (differences.index < end)]
        results[fold["id"]] = {
            "expected_hourly_bars": int((end - start) / pd.Timedelta("1h")), "observed_hourly_bars": len(observed),
            "missing_hourly_bars": int((end - start) / pd.Timedelta("1h")) - len(observed),
            "completed_decision_bars": fold["evaluation"]["completed_decision_bars"],
            "warmed_decision_bars": fold["evaluation"]["warm_decision_bars"],
            "execution_gap_labels": int(predictions.loc[predictions.fold.eq(fold["id"]), "status"].eq("execution_gap").sum()),
            "reconciliation_classes": diff["class"].value_counts().to_dict(),
        }
    return {
        "canonical": "Coinbase Exchange BTC-USD native hourly. No substitution, backfill, or interpolation.",
        "gap_policy": "Incomplete bars and interrupted warmup are excluded causally. Active-trade gaps stop the affected "
                      "account; comparisons truncate at the common last mark.",
        "phase2_original": {"missing_hourly": hourly["missing_bars"], "missing_five_minute": prior["feeds"]["300"]["missing_bars"],
                            "comparison": prior["reconciliation"]},
        "hourly_gap_intervals_pre_holdout": [row for row in hourly["gaps"] if utc(row["start"]) < cutoff],
        "reconciliation_classes_pre_holdout": differences.loc[differences.index < cutoff, "class"].value_counts().to_dict(),
        "folds": results,
        "account_gap_stops": {name: [method for method in METHODS
                                     if scenario["methods"][method]["observed_engine_status"] == "execution_gap"]
                              for name, scenario in scenarios.items() if "methods" in scenario},
    }


def legacy_swing(plan):
    old = read_json(AUDITS / plan["phase2_audit"] / "swing.json")
    previous = old["windows"]["evaluation"]
    ledger = previous["accounts"]["baseline"]["closed_trade_attribution_usd"]
    return {
        "original_interval": [previous["start_inclusive"], previous["end_exclusive"]],
        "original_ridge_score_passes": previous["score_pass"],
        "original_trade_count": previous["accounts"]["baseline"]["trade_count"],
        "original_same_trade_accounting": ledger,
        "gross_minus_net_usd": ledger["raw_gross"] - ledger["net"],
        "included_in_new_aggregate_as_separate_result": False,
    }


def concentration(scenarios, simulations):
    ref = scenarios.get("reference")
    if ref is None or "methods" not in ref:
        return {}
    start, end = ref["account_start"], ref["matched_end"]
    result = {}
    for method, sim in simulations.items():
        increments = {key: value["methods"][method]["net_pnl_usd"] for key, value in ref["folds"].items()
                      if value["account_state"] == "complete"}
        values = sim.trades
        if len(values):
            closed = values.loc[values.exit_at.notna() & (values.exit_bar_open < end) & (values.entry_at >= start)]
            positive, negative = closed.net_usd[closed.net_usd > 0], closed.net_usd[closed.net_usd < 0]
        else:
            positive = negative = pd.Series(dtype=float)
        baseline = {key: value["methods"]["baseline"]["net_pnl_usd"] for key, value in ref["folds"].items()
                    if value["account_state"] == "complete"}
        deltas = {key: pnl - baseline[key] for key, pnl in increments.items()}
        total_absolute = sum(abs(x) for x in deltas.values())
        result[method] = {
            "positive_account_windows": sum(value > 0 for value in increments.values()),
            "completed_account_windows": len(increments), "window_pnl_usd": increments,
            "window_pnl_minus_baseline_usd": deltas,
            "largest_window_share_of_absolute_differences": max(map(abs, deltas.values())) / total_absolute
            if total_absolute else None,
            "top_three_positive_trades_share_of_closed_gains": float(positive.nlargest(3).sum() / positive.sum())
            if len(positive) else None,
            "top_three_losses_share_of_closed_losses": float(negative.nsmallest(3).sum() / negative.sum())
            if len(negative) else None,
        }
    return result


def chart(scenario, simulations, path):
    start, end = scenario["account_start"], scenario["matched_end"]
    names = {"baseline": "Baseline", "ridge": "Ridge", "mean": "Training mean"}
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=.09,
                        subplot_titles=("Marked equity in USD", "Lowest daily observed drawdown"))
    for method, sim in simulations.items():
        values = sim.equity.loc[(sim.equity.index > start) & (sim.equity.index <= end)]
        by_day = values.equity.resample("1D").last().dropna()
        draw = values.drawdown.resample("1D").min().dropna()
        fig.add_trace(go.Scatter(x=by_day.index, y=by_day, name=names[method], mode="lines"), row=1, col=1)
        fig.add_trace(go.Scatter(x=draw.index, y=draw, name=names[method], mode="lines", showlegend=False), row=2, col=1)
        if method == "baseline":
            for name in BENCHMARKS:
                values_by_day = values[name].resample("1D").last().dropna()
                fig.add_trace(go.Scatter(x=values_by_day.index, y=values_by_day, name=name.replace("_", " "),
                                         mode="lines", line={"dash": "dot"}), row=1, col=1)
    fig.update_yaxes(tickformat=".0%", row=2, col=1)
    fig.update_xaxes(title_text="UTC dates", row=2, col=1)
    fig.update_layout(template="plotly_white", height=740, title="Swing walk-forward · reference costs · matched dates")
    fig.write_html(path, include_plotlyjs=True)


def write_summary(directory: Path, result: dict) -> None:
    ref = result["scenarios"].get("reference", {})
    lines = [
        "# Phase 3 Swing walk-forward", "",
        "Exploratory development evidence on preserved native hourly Coinbase BTC-USD candles. "
        "The holdout remained sealed and the running shadow models stayed pinned.", "",
        "The first scheduled block failed the frozen training coverage rule. Each method then ran one continuous USD 10,000 "
        "hypothetical account through the remaining folds. Folds are marked changes in those accounts.", "",
        "| UTC window [start, end) | Fit labels | Paired forecast labels | Baseline | Ridge | Training mean |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for fold in result["folds"]:
        marks = ref.get("folds", {}).get(fold["id"], {})
        metrics = marks.get("methods", {})
        pnl = [f"{metrics[key]['net_return']:.2%} ({metrics[key]['positions_closed']})" if metrics.get(key) else "—"
               for key in METHODS]
        fitting = fold["training"]["fit_count"] if fold["fit_state"] == "fitted" else (
            f"Insufficient, {fold['warm_calendar_months']} warmed months"
        )
        left, right = (fold[key][:10] for key in ("evaluation_start_utc", "evaluation_end_exclusive_utc"))
        lines.append(f"| {left} – {right} | {fitting} | {fold['prediction_quality']['paired_label_count']} | "
                     + " | ".join(pnl) + " |")
    if ref.get("methods"):
        lines.extend(["", "Window cells show marked net return and closed trade count.", "",
                      "## Reference matched comparison", "",
                      f"Marks from {ref['account_start']} through {ref['matched_end']} UTC.",
                      "", "| Method | Net return | Gross USD | Fees USD | Execution drag USD | Trades closed | "
                      "Worst observed drawdown | Break-even fee per side |",
                      "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
        for name, info in ref["methods"].items():
            row = info["matched"]
            fee = row["fixed_ledger_fee"]["break_even_fee_bps_per_side"]
            limit = f"{fee:.2f} bp" if fee is not None else row["fixed_ledger_fee"]["status"].replace("_", " ")
            dd = (f"{row['drawdown_on_observed_marks']:.2%}" if info["full_period_drawdown_available"]
                  else f"{row['drawdown_on_observed_marks']:.2%} (partial marks)")
            lines.append(f"| {name} | {row['net_return']:.2%} | {row['gross_marked_same_trades_usd']:.2f} | "
                         f"{row['fees_usd']:.2f} | {row['adverse_execution_drag_usd']:.2f} | {row['positions_closed']} | "
                         f"{dd} | {limit} |")
        benchmarks = ref["methods"]["baseline"]["matched"]["benchmark_net_returns"]
        lines.append("")
        lines.append("Aligned benchmark returns were " + ", ".join(
            f"{key.replace('_', ' ')} {value:.2%}" for key, value in benchmarks.items()) + ".")
        lines.extend(["", "## Prediction error on identical mature labels", "",
                      "| Forecast | MAE | RMSE | Bias | All scored setups passing |",
                      "| --- | ---: | ---: | ---: | ---: |"])
        quality_report = result["pooled_reference_prediction_quality"]
        for name in ("ridge", "mean"):
            e = quality_report["error_same_mature_labels"][name]
            scores = quality_report["scores"][name]
            if e:
                lines.append(f"| {name} | {e['mae']:.3%} | {e['rmse']:.3%} | {e['bias_prediction_minus_target']:.3%} | "
                             f"{scores['accepted']}/{scores['count']} |")
        lines.append("")
        dependence = quality_report["dependence"].get("overlapping_outcome_pairs", 0)
        lines.append(f"The paired sample contains {quality_report['paired_label_count']} mature labels and "
                     f"{dependence} overlapping outcome pairs. Error differences are descriptive. "
                     "Forecast quality and execution outcomes are separate.")
        lines.extend(["", "## Frozen-model sensitivities", "",
                      "| Scenario | Baseline | Ridge | Training mean |",
                      "| --- | ---: | ---: | ---: |"])
        for name, detail in result["scenarios"].items():
            if name == "reference":
                continue
            if "methods" in detail:
                values = [f"{detail['methods'][key]['matched']['net_return']:.2%}" for key in METHODS]
                lines.append(f"| {name} | " + " | ".join(values) + " |")
            else:
                lines.append(f"| {name} | Failed | — | — |")
    old = result["legacy_swing"]["original_same_trade_accounting"]
    lines.extend(["", "## Earlier checkpoint kept separate", "",
                  f"The prior Swing ledger marked USD {old['raw_gross']:.2f} gross. Fees of USD {old['fees']:.2f} and "
                  f"adverse price drag of USD {old['adverse_price_drag']:.2f} explain the "
                  f"USD {result['legacy_swing']['gross_minus_net_usd']:.2f} gap to net. This holds the trades and quantities fixed.",
                  "", "## Artifacts", "",
                  "- [Frozen study design](../frozen-study.json)",
                  "- [Metrics, fold identities, and experiment ledger](study-results.json)",
                  "- [Data gaps and reconciliation](data-coverage.json)",
                  "- [Day count and session reconciliation](day-reconciliation.json)",
                  "- [Operational observation](operational-health.json)", ""])
    if (directory / "reference-equity.html").exists():
        lines.append("- [Reference equity and drawdown charts](reference-equity.html)")
    put_bytes(directory / "SUMMARY.md", "\n".join(lines).encode("utf-8"))


def run(directory: Path) -> Path:
    with writer_lock(OUTPUT / ".writer.lock"):
        plan = load_plan(directory)
        prior = sorted(directory.glob("attempt-*"))
        if prior and (prior[-1] / "complete.json").exists():
            return prior[-1]
        attempt = directory / f"attempt-{len(prior) + 1:02d}"
        attempt.mkdir()
        started = utc_now()
        write_json(attempt / "attempt-start.json", {"started_at_utc": started, "design_sha256": plan["design_sha256"],
                                                  "study_code_sha256": fingerprint({
                                                      item.name: file_hash(item) for item in sorted(Path(__file__).parent.glob("*.py"))
                                                  })})
        checks = {}
        for name, inspect in (("day-reconciliation", lambda: reconcile_day(attempt)), ("operational-health", health)):
            try:
                check = inspect()
            except (OSError, ValueError, AssertionError) as exc:
                check = {"state": "unavailable", "error": str(exc)}
            checks[name] = check
            write_json(attempt / f"{name}.json", check)
        try:
            output = evaluate(attempt, plan)
            output["ancillary_checks"] = {name: value.get("state", "completed") for name, value in checks.items()}
            write_json(attempt / "study-results.json", output)
            write_summary(attempt, output)
            write_json(attempt / "complete.json", {
                "completed_at_utc": utc_now(),
                "files": {str(file.relative_to(attempt)): file_hash(file)
                          for file in sorted(attempt.rglob("*")) if file.is_file()},
                "approved": False,
            })
            write_json(OUTPUT / "latest.json", {"directory": directory.name, "attempt": attempt.name})
        except Exception as exc:
            write_json(attempt / "failure.json", {"at": utc_now(), "error_type": type(exc).__name__, "error": str(exc)})
            raise
        return attempt


def evaluate(attempt, plan):
    profile = Profile(**plan["profile"])
    candles = development_prices(plan)
    signals, predictions, folds = build_forecasts(candles, profile, plan, attempt)
    scenarios, reference_simulations = {}, {}
    for name, raw in plan["cost_scenarios"].items():
        if not predictions.fit_eligible.any():
            scenarios[name] = {"state": "no_eligible_training_folds"}
            continue
        try:
            scenarios[name], simulations = run_scenario(candles, signals, predictions, folds, profile, Execution(**raw),
                                                        attempt / name, name)
        except (ValueError, AssertionError) as exc:
            scenarios[name] = {"state": "failed", "error": str(exc)}
            continue
        if name == "reference":
            reference_simulations = simulations
            chart(scenarios[name], simulations, attempt / "reference-equity.html")
    write_json(attempt / "data-coverage.json", gaps_and_reconciliation(plan, candles, folds, predictions, scenarios))
    return {
        "design_sha256": plan["design_sha256"], "folds": folds, "scenarios": scenarios,
        "pooled_reference_prediction_quality": quality(predictions.loc[predictions.fit_eligible], profile.min_eval),
        "reference_concentration": concentration(scenarios, reference_simulations),
        "legacy_swing": legacy_swing(plan), "holdout_performance_revealed": False, "paper_approved": False,
    }
