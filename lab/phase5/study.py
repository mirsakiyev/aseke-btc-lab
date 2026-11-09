from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from lab.config import Profile, fingerprint
from lab.features import decisions
from lab.phase3.design import development_prices
from lab.phase5.design import LABEL, OUTPUT, checked_completion, freeze, sources
from lab.phase5.metrics import comparison
from lab.phase5.observations import prospective_report
from lab.phase5.replays import identity_delta, originals, stress_case
from lab.phase5.sizing import sizing_report
from lab.storage import file_hash, put_bytes, read_json, utc, utc_now, write_json, writer_lock

DISPLAY = {
    "reference": "Original reference", "fixed_ledger": "Fixed ledger", "lower_fee": "Lower-fee replay",
    "proxy_10bp": "Wider proxy stress", "extra_hour": "One-hour delay stress",
}


def chart(accounts, results, path: Path):
    start, end = results["requested_start_utc"], results["matched_end_utc"]
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=.075,
                        subplot_titles=("Swing equity (USD)", "Swing drawdown (daily low of hourly marks)",
                                        "Aligned lower-fee benchmarks (USD)"))
    colors = ("#5a6270", "#9f6524", "#057a70", "#8e4b9b", "#2764ab")
    for (key, account), color in zip(accounts.items(), colors):
        frame = account.equity.loc[(account.equity.index > start) & (account.equity.index <= end)]
        equity = frame.equity.resample("1D").last().dropna()
        drawdown = frame.drawdown.resample("1D").min().dropna()
        fig.add_trace(go.Scatter(x=equity.index, y=equity, name=DISPLAY[key], line={"color": color}), row=1, col=1)
        fig.add_trace(go.Scatter(x=drawdown.index, y=drawdown, name=DISPLAY[key], line={"color": color},
                                 showlegend=False), row=2, col=1)
        if key == "lower_fee":
            for benchmark in ("cash_benchmark", "full_btc", "passive_25pct"):
                marks = frame[benchmark].resample("1D").last().dropna()
                fig.add_trace(go.Scatter(x=marks.index, y=marks, name=benchmark.replace("_", " ")), row=3, col=1)
    fig.update_yaxes(tickformat=".1%", row=2, col=1)
    fig.update_xaxes(title_text="UTC dates", row=3, col=1)
    fig.update_layout(template="plotly_white", height=890, title=LABEL)
    fig.write_html(path, include_plotlyjs=True)


def observation_text(report: dict) -> str:
    worker = report["worker_health"]
    continuity = report.get("continuity", {})
    heartbeat = worker.get("heartbeat") or {}
    lines = [
        f"Observation snapshot at {report['observed_at_utc']} UTC.",
        f"Worker PID {worker['worker_pid']} · {worker['state']} · experiment "
        f"{continuity.get('experiment_id', 'unavailable')}.",
        f"Original activation {continuity.get('experiment', {}).get('observation_start_utc', 'unavailable')}.",
        f"Process elapsed seconds {worker['current_worker_elapsed_seconds']}. "
        f"Heartbeat received {heartbeat.get('received_at', 'unavailable')}.",
        "Coverage is for the existing Coinbase reference-fee shadow.",
        "", "| Profile | Timely verified / assessable | Feature-ready | Late (causal / catch-up / unclear) | Missing | Unknown clock | Data blocked | No signal | Session barred | Pending |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key, row in report["profiles"].items():
        late = f"{row['late_causally_started_but_missed_cutoff']}/{row['late_catchup_reconstruction']}/{row['late_input_timing_unknown']}"
        lines.append(f"| {key} | {row['timely_verified_records']}/{row['assessable_opportunities']} | "
                     f"{row['timely_feature_ready_records']} | {row['late_total']} ({late}) | {row['missing']} | "
                     f"{row['unknown_timing_excluding_late']} | "
                     f"{row['data_blocked_records']} | {row['no_baseline_signal_records']} | "
                     f"{row['session_ineligible_assessable_opportunities']} | {row['pending_before_cutoff']} |")
    lines.extend(["", "Data blocks and session status can overlap timing classes. Scheduled time before activation "
                  "and still-pending deadlines are outside the assessable denominator. No decision latency SLA is registered.",
                  "", "| Execution bars | Stored in 24h candle window | Receipted within 120s | Last closed mark |",
                  "| --- | ---: | ---: | --- |"])
    for key, row in worker.get("feeds", {}).items():
        lines.append(f"| {key}s | {row['observed_closed_candles_24h']}/{row['expected_closed_candles_24h']} | "
                     f"{row['received_within_120_seconds_of_close']} | {row['last_observed_close']} |")
    lines.extend(["", "Stored candles include catch-up. Receipt counts and worker uptime are separate from "
                  "prospective decision coverage. Exact decision and input receipt evidence is available in JSON."])
    return "\n".join(lines)


def percent(value):
    return "—" if value is None else f"{value:.2%}"


def summarize(directory, report, sizing, obs):
    m = report["methods"]
    stress = [m[key]["aggregate"]["net_return"] for key in ("proxy_10bp", "extra_hour") if key in m]
    finding = "FRAGILE" if any(value <= 0 for value in stress) else "INCONCLUSIVE"
    full = report["full_frozen_continuous_span"]
    lines = [
        "# Phase 5 Swing robustness", "", f"**{LABEL}**", "",
        f"**{finding}** · Bounded, exploratory USD comparisons. The original holdout remains sealed.",
        "", f"Matched account start {report['requested_start_utc']}. Last common mark {report['matched_end_utc']}. "
        f"Elapsed calendar time **{report['elapsed_utc_days']:.2f} UTC days**. "
        + ("The requested span was reached." if full else "Common coverage ended early; annualization is suppressed."),
        "", "The insufficient January–June 2023 window remains excluded. Hourly drawdown includes cash and open BTC "
        "on observed marks. Missing marks are left missing, and intrahour extremes can be deeper.",
        "", "| Case | Fee / proxy per side | Total | Annualized | Net USD | Drawdown | Held time | Mean / peak BTC weight | Closed / open |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key, info in m.items():
        row, c = info["aggregate"], info["execution"]
        lines.append(f"| {DISPLAY[key]} | {c['fee_bps']} / {c['proxy_bps']} bp | {percent(row['net_return'])} | "
                     f"{percent(row['annualized_net_return'])} | {row['net_pnl_usd']:,.2f} | "
                     f"{percent(row['drawdown_on_observed_marks'])} | {percent(row['observed_time_exposure'])} | "
                     f"{percent(row['mean_btc_weight'])} / {percent(row['peak_btc_weight_on_hourly_marks'])} | "
                     f"{row['positions_closed']} / {row['positions_carried_out']} |")
    if report["failed_stress"]:
        lines.append(f"\nUnavailable stress evidence {report['failed_stress']}.")
    lines.extend(["", "Annualization is geometric over actual elapsed UTC days using 365.2425 days per year. "
                  "The per-window metrics retain gross, fees, adverse execution, turnover, entry-notional and holding distributions, "
                  "and open/unrealized values.", "", "## Gross-to-net and benchmark context", "",
                  "| Case | Gross USD | Fees USD | Adverse USD | Cash | Full BTC | Passive 25% BTC |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for key, info in m.items():
        row = info["aggregate"]
        benchmarks = " | ".join(percent(b["net_return"]) for b in row["benchmarks"].values())
        lines.append(f"| {DISPLAY[key]} | {row['gross_marked_same_trades_usd']:,.2f} | {row['fees_usd']:,.2f} | "
                     f"{row['adverse_execution_drag_usd']:,.2f} | {benchmarks} |")
    lower = m["lower_fee"]["aggregate"]
    passive = lower["benchmarks"]["passive_25pct"]
    full_btc = lower["benchmarks"]["full_btc"]
    lines.extend(["",
                  f"The lower-fee replay used {lower['mean_btc_weight']:.2%} mean marked BTC weight. "
                  f"The passive 25% allocation averaged {passive['mean_btc_weight']:.2%} as it drifted, "
                  f"and had {passive['observed_drawdown']:.2%} observed drawdown. Full BTC had "
                  f"{full_btc['observed_drawdown']:.2%} drawdown. The exposure and drawdown differences matter "
                  "alongside the higher passive returns. These benchmarks use the same dates, fees and terminal mark.",
                  "", "## Existing windows and calendar contributions", "",
                  "| Period (UTC) | " + " | ".join(DISPLAY[k] for k in m) + " |",
                  "| --- | " + " | ".join("---:" for _ in m) + " |"])
    for period in [*report["evaluation_windows"], *report["calendar_years"]]:
        label = period.get("id") or period["calendar_year_utc"]
        rows = period["methods"]
        cells = [f"{rows[k]['net_pnl_usd']:+,.2f} / {rows[k]['drawdown_on_observed_marks']:.2%}"
                 if rows.get(k) else "—" for k in m]
        lines.append(f"| {label} · {str(period['left'])[:10]} to {str(period['right'])[:10]} | " + " | ".join(cells) + " |")
    lines.extend(["", "Cells are USD net P&L and observed drawdown. Year endpoints are calendar boundaries, "
                  "with partial first and last years.", "", "## Sizing and concentration", "",
                  f"Lower fees on original quantities account for **USD {sizing['fixed_ledger_fee_savings_usd']:,.2f}**. "
                  f"The changed quantities account for **USD {sizing['additional_replay_pnl_usd_unrounded']:,.2f}** more, "
                  "using unrounded endpoints. Subtracting the displayed Phase 4 rounded P&L figures gives USD 158.57.",
                  "", "| Incremental sizing contribution | USD |", "| --- | ---: |"])
    for key, value in sizing["sizing_effect_components_usd"].items():
        lines.append(f"| {key.removesuffix('_usd').replace('_', ' ')} | {value:,.2f} |")
    top = m["lower_fee"]["closed_trade_concentration"]["top_winners"]
    median = m["lower_fee"]["closed_trade_concentration"]["trade_net_usd"]["p50"]
    mean = m["lower_fee"]["closed_trade_concentration"]["trade_net_usd"]["mean"]
    lines.extend(["",
                  f"Lower-fee closed trades averaged USD {mean:.2f}, with a median of USD {median:.2f}. "
                  f"Top 1, 3 and 5 winners contributed USD {top['1']['contribution_usd']:.2f}, "
                  f"USD {top['3']['contribution_usd']:.2f} and USD {top['5']['contribution_usd']:.2f}. "
                  f"Subtracting the five winners leaves USD {top['5']['closed_net_minus_winners_arithmetic_only_usd']:.2f} "
                  "with lower-fee replay quantities held fixed. This arithmetic does not resize or replay trades. The sample is 68 executions, "
                  "with no independence or significance claim. The 31.78 bp prior break-even fee depends on its fixed ledger and proxy.",
                  "", "## Live prospective coverage", "", observation_text(obs), "", "## Evidence", "",
                  "- [Frozen diagnostic manifest](manifest.json)",
                  "- [Metrics, stress trades and concentration](results.json)",
                  "- [Independent sizing checks and representative trades](sizing.json)",
                  "- [Prospective observation timing and health](prospective-observations.json)",
                  "- [Equity, drawdown and aligned benchmarks](equity-drawdown.html)", ""])
    put_bytes(directory / "SUMMARY.md", "\n".join(lines).encode("utf-8"))


def run() -> Path:
    folder = freeze()
    with writer_lock(OUTPUT / ".execution.lock"):
        if (folder / "complete.json").exists():
            checked_completion(folder)
            return folder
        manifest = read_json(folder / "manifest.json")
        plan, prior3, phase3, _, prior4, phase4 = sources()
        profile = Profile(**manifest["profile"])
        start = utc(manifest["requested_start_utc"])
        candles = development_prices(plan)
        signals = decisions(candles, profile)
        reference = pd.read_parquet(phase3 / "reference" / "execution-comparison.parquet")
        eligible = reference.loc[reference.fit_eligible & (reference.index >= start)]
        accounts = originals(phase3, phase4, prior3, prior4)
        failed = {}
        for name in manifest["execution_stress_budget"]["cases"]:
            try:
                accounts[name] = stress_case(folder, name, manifest, candles, signals, eligible, profile)
            except (RuntimeError, ValueError, AssertionError, OSError) as exc:
                failed[name] = str(exc)
        results = comparison(accounts, manifest, float(candles.loc[start, "open"]), profile)
        results.update(
            label=LABEL, account_currency="USD", design_sha256=manifest["design_sha256"], failed_stress=failed,
            analysis_code_sha256=fingerprint({p.name: file_hash(p) for p in sorted(Path(__file__).parent.glob("*.py"))}),
            stress_trade_identity_vs_lower_fee={name: identity_delta(accounts["lower_fee"], accounts[name],
                                                                     results["matched_end_utc"])
                                               for name in accounts if name in manifest["execution_stress_budget"]["cases"]},
        )
        sizing = {"label": LABEL, **sizing_report(accounts, manifest, candles, signals, profile)}
        obs = prospective_report()
        write_json(folder / "results.json", results)
        write_json(folder / "sizing.json", sizing)
        write_json(folder / "prospective-observations.json", obs)
        chart(accounts, results, folder / "equity-drawdown.html")
        summarize(folder, results, sizing, obs)
        write_json(folder / "complete.json", {"label": LABEL, "completed_at_utc": utc_now(), "files": {
            str(p.relative_to(folder)): file_hash(p) for p in folder.rglob("*") if p.is_file()
        }})
        write_json(OUTPUT / "latest.json", {"label": LABEL, "directory": folder.name})
    return folder
