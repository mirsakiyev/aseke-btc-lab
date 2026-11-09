from pathlib import Path

import numpy as np
import pandas as pd

from lab.backtest import Simulation, drawdown, simulate
from lab.config import Execution, Profile, fingerprint
from lab.execution import plan_setups
from lab.features import decisions
from lab.phase3.design import OUTPUT as PHASE3, development_prices, load_plan
from lab.phase3.portfolio import execution_events, period_metrics
from lab.phase4.config import LABEL, REFERENCE, MarketIdentity, ResearchConfig
from lab.storage import RUNS, file_hash, put_bytes, read_json, utc, utc_now, write_json, writer_lock

OUTPUT = RUNS / "phase4"
PHASE3_ID = "20261109T014235Z-a9572f71e8"
PHASE3_ATTEMPT = "attempt-01"
SENSITIVITY_VERSION = "coinbase-swing-published-fee-v1"


def source():
    directory = PHASE3 / PHASE3_ID
    plan = load_plan(directory)
    attempt = directory / PHASE3_ATTEMPT
    complete = read_json(attempt / "complete.json")
    for name in ("study-results.json", "reference/baseline-equity.parquet", "reference/baseline-trades.parquet",
                 "reference/baseline-execution-events.parquet", "reference/execution-comparison.parquet"):
        if file_hash(attempt / name) != complete["files"][name.replace("/", "\\")]:
            raise ValueError(f"Preserved Phase 3 artifact failed verification: {name}")
    return plan, read_json(attempt / "study-results.json"), attempt


def fixed_ledger_reprice(original: Simulation, before: Execution, after: Execution,
                         starting_cash: float, step: pd.Timedelta) -> Simulation:
    if before.model_dump(exclude={"fee_bps"}) != after.model_dump(exclude={"fee_bps"}):
        raise ValueError("Fixed-ledger repricing changes the fee only")
    equity, trades = original.equity.copy(), original.trades.copy()
    orders = execution_events(trades, before)
    savings = (orders["notional"] * (before.fee - after.fee)).groupby(orders["at"] + step).sum()
    if not savings.index.isin(equity.index).all():
        raise ValueError("A preserved fill has no mark for fee attribution")
    saved_fees = savings.reindex(equity.index, fill_value=0).cumsum()
    original_btc_value = equity.equity - equity.cash
    equity["cash"] += saved_fees
    equity["equity"] += saved_fees
    equity["btc_weight"] = original_btc_value / equity.equity
    equity["drawdown"] = drawdown(equity.equity, starting_cash)
    for name, allocation in (("full_btc", 1.0), ("passive_25pct", 0.25)):
        original_entry_notional = starting_cash * allocation / (1 + before.fee)
        equity[name] += original_entry_notional * (before.fee - after.fee)
    if len(trades):
        trades["original_fees_usd"] = trades.fees_usd
        trades["original_net_usd"] = trades.net_usd
        trades["sizing_reference_fee_bps"] = before.fee_bps
        trades["fee_bps"] = after.fee_bps
        trades["fees_usd"] = (trades.entry_notional + trades.exit_notional) * after.fee
        trades["net_usd"] = (trades.net_usd + trades.original_fees_usd - trades.fees_usd).where(trades.exit_at.notna())
    return Simulation(original.metrics, equity, trades, pd.DataFrame())


def market_identity(plan: dict, config: ResearchConfig, case: str) -> dict:
    return MarketIdentity(
        venue="coinbase_exchange", product="spot", market_type="unlevered_spot", symbol="BTC-USD",
        base_currency="BTC", quote_currency="USD", data_version=plan["source_snapshot"]["sha256"],
        strategy_version=plan["profile"]["strategy_version"],
        cost_version="phase3-reference-50bp-5bp" if case == "reference" else config.published.name,
        execution_version="phase1-closed-bar-v1",
    ).model_dump(mode="json")


def compare(simulations, costs, reference, profile) -> dict:
    start, end = (utc(reference[name]) for name in ("account_start", "requested_end"))
    last_common = min(end, *(s.equity.index[-1] for s in simulations.values()))
    orders = {name: execution_events(sim.trades, costs[name]) for name, sim in simulations.items()}
    aggregate = {}
    for name, sim in simulations.items():
        aggregate[name] = period_metrics(sim, orders[name], start, last_common, profile.starting_cash, profile.step)
        if aggregate[name] is None:
            raise ValueError("No jointly observed account marks")
        aggregate[name]["full_requested_window"] = last_common == end
        aggregate[name]["last_account_mark"] = sim.equity.index[-1]
        aggregate[name]["account_status"] = sim.metrics["status"]
    return {
        "start_utc": start, "requested_end_utc": end, "matched_end_utc": last_common, "full_match": last_common == end,
        "aggregate": aggregate, "orders": orders,
    }


def calculation(config: ResearchConfig, plan, previous, origin, directory: Path, manifest: dict) -> dict:
    profile = Profile(**plan["profile"])
    reference = previous["scenarios"]["reference"]
    start, end = (utc(reference[name]) for name in ("account_start", "requested_end"))
    if profile.id != "swing" or profile.costs != REFERENCE or end != utc(plan["windows"]["holdout_start"]):
        raise ValueError("Unexpected frozen Swing reference or holdout")
    fee = config.published.execution_sensitivity(profile.costs)
    equity = pd.read_parquet(origin / "reference" / "baseline-equity.parquet")
    trades = pd.read_parquet(origin / "reference" / "baseline-trades.parquet")
    original_summary = reference["methods"]["baseline"]
    original = Simulation({"status": original_summary["observed_engine_status"]}, equity, trades, pd.DataFrame())
    repriced = fixed_ledger_reprice(original, profile.costs, fee, profile.starting_cash, profile.step)
    candles = development_prices(plan)
    signals = decisions(candles, profile)
    planned = plan_setups(candles, signals, profile, fee, end)
    prior = pd.read_parquet(origin / "reference" / "execution-comparison.parquet")
    eligible = prior.loc[prior.fit_eligible & (prior.index >= start)]
    by_signal = {item.signal_at: item for item in planned}
    selected = []
    for ts, row in eligible.iterrows():
        setup = by_signal[ts]
        if setup.entry_at != row.opening or setup.reason != row.execution_setup_status:
            raise ValueError("Fee-only replay changed a frozen market or timing input")
        selected.append(setup)
    replay = simulate(candles, selected, profile, fee, start, end)
    if len(replay.trades):
        replay.trades["signal_fold"] = eligible.loc[replay.trades.signal_at, "fold"].to_numpy()
    sims = {"reference": original, "fixed_ledger": repriced, "execution_replay": replay}
    costs = {"reference": profile.costs, "fixed_ledger": fee, "execution_replay": fee}
    compared = compare(sims, costs, reference, profile)
    orders = compared.pop("orders")
    common = compared["matched_end_utc"]
    if compared["full_match"]:
        np.testing.assert_allclose(compared["aggregate"]["reference"]["net_pnl_usd"],
                                   original_summary["matched"]["net_pnl_usd"], atol=1e-8)
    np.testing.assert_allclose(compared["aggregate"]["reference"]["gross_marked_same_trades_usd"],
                               compared["aggregate"]["fixed_ledger"]["gross_marked_same_trades_usd"], atol=1e-8)
    windows = []
    for window in plan["folds"]:
        left = utc(window["evaluation_start_utc"])
        right = utc(window["evaluation_end_exclusive_utc"])
        row = {"id": window["id"], "start_utc": left, "end_exclusive_utc": right, "partial": window["partial"]}
        if left < start or left >= common:
            row["state"] = "insufficient_preserved_phase3_coverage" if left < start else "unavailable_after_common_mark"
        else:
            right_mark = min(right, common)
            row.update(state="complete" if right_mark == right else "truncated", methods={
                name: period_metrics(sim, orders[name], left, right_mark, profile.starting_cash, profile.step)
                for name, sim in sims.items()
            })
        windows.append(row)
    for name in ("fixed_ledger", "execution_replay"):
        sim = sims[name]
        for suffix, frame in (("trades", sim.trades), ("equity", sim.equity), ("execution-events", orders[name])):
            frame.assign(research_scope=LABEL, account_currency="USD", method=name).to_parquet(
                directory / f"{name}-{suffix}.parquet"
            )
    original_ids = set(trades.loc[trades.entry_at < common, "signal_at"])
    replay_ids = set(replay.trades.loc[replay.trades.entry_at < common, "signal_at"]) if len(replay.trades) else set()
    if len(replay.trades):
        for row in replay.trades.itertuples():
            if row.model_version is not None:
                raise ValueError("Phase 4 execution must remain unfiltered")
    return {
        "label": LABEL, "account_currency": "USD", "design_sha256": manifest["design_sha256"],
        "comparison": compared, "windows": windows,
        "reference_original_break_even": original_summary["matched"]["fixed_ledger_fee"],
        "reference_vs_replay_entries": {"shared": len(original_ids & replay_ids),
                                        "reference_only": sorted(original_ids - replay_ids),
                                        "replay_only": sorted(replay_ids - original_ids)},
        "replay_open_or_unresolved_position": replay.metrics["open_position"],
        "replay_benchmark_totals_at_own_end": replay.metrics["benchmarks"],
        "models_loaded_or_fitted": False, "holdout_performance_revealed": False,
    }


def summarize(result: dict, directory: Path) -> None:
    compare = result["comparison"]
    rows = compare["aggregate"]
    labels = {"reference": "Preserved reference", "fixed_ledger": "Fixed ledger, 5 bp", "execution_replay": "Execution replay, 5 bp"}
    lines = [
        "# Published-fee Swing sensitivity", "", f"**{LABEL}**", "",
        "All accounts and P&L are in USD. The historical schedule, strategy and holdout are preserved. "
        "The 5 bp taker fee uses present published guidance on older Coinbase prices. Every case retains "
        "the same 5 bp adverse-price proxy per side.", "",
        f"Matched marks from {compare['start_utc']} to {compare['matched_end_utc']}. "
        + ("The common mark reached the frozen development boundary." if compare["full_match"]
           else "An account ended early. Drawdowns below use the matched partial period."), "",
        "| Method | Net return | Gross USD | Fees USD | Adverse execution USD | Net USD | Drawdown | Closed | Break-even fee per side |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key, row in rows.items():
        fee = row["fixed_ledger_fee"]["break_even_fee_bps_per_side"]
        break_even = f"{fee:.2f} bp" if fee is not None else row["fixed_ledger_fee"]["status"].replace("_", " ")
        lines.append(f"| {labels[key]} | {row['net_return']:.2%} | {row['gross_marked_same_trades_usd']:.2f} | "
                     f"{row['fees_usd']:.2f} | {row['adverse_execution_drag_usd']:.2f} | {row['net_pnl_usd']:.2f} | "
                     f"{row['drawdown_on_observed_marks']:.2%} | {row['positions_closed']} | {break_even} |")
    lines.extend(["", "Fixed-ledger repricing holds the original BTC quantities and fills constant. The new replay "
                  "recomputes sizing from the preserved risk and exposure rules. Break-even fees hold each "
                  "ledger and adverse proxy fixed.", "", "## Matched continuous-account windows", "",
                  "| UTC [start, end) | Preserved reference | Fixed ledger | Execution replay | Replay open at end |",
                  "| --- | ---: | ---: | ---: | ---: |"])
    for window in result["windows"]:
        dates = f"{str(window['start_utc'])[:10]} – {str(window['end_exclusive_utc'])[:10]}"
        methods = window.get("methods")
        if not methods:
            lines.append(f"| {dates} | {window['state'].replace('_', ' ')} | — | — | — |")
        else:
            values = " | ".join(f"{methods[k]['net_return']:.2%}" for k in labels)
            lines.append(f"| {dates} | {values} | {methods['execution_replay']['positions_carried_out']} |")
    lines.extend(["", "## Aligned benchmarks", "", "| Cost and sizing treatment | Cash | Full BTC | Passive 25% BTC |",
                  "| --- | ---: | ---: | ---: |"])
    for name, row in rows.items():
        values = " | ".join(f"{v:.2%}" for v in row["benchmark_net_returns"].values())
        lines.append(f"| {labels[name]} | {values} |")
    lines.extend(["", "In the fixed-ledger column, the passive benchmarks also retain their original units. "
                  "In the execution replay, benchmarks start with the new entry fee under the original sizing policy.", "",
                  "Exposure, turnover, unrealized P&L, gross and fee breakdowns for every window are in the results. "
                  "The Phase 3 negative finding still applies under its original assumptions. The cheaper replay "
                  "does not establish a tradable MEXC edge.", "",
                  "- [Frozen sensitivity manifest](manifest.json)",
                  "- [Per-window and aggregate metrics](results.json)", ""])
    put_bytes(directory / "SUMMARY.md", "\n".join(lines).encode("utf-8"))


def run(config: ResearchConfig, root: Path = OUTPUT) -> Path:
    plan, previous, origin = source()
    case = {
        "label": LABEL, "sensitivity_version": SENSITIVITY_VERSION, "phase3_run": PHASE3_ID,
        "phase3_attempt": PHASE3_ATTEMPT, "phase3_design_sha256": plan["design_sha256"],
        "phase3_completion_sha256": file_hash(origin / "complete.json"),
        "config_sha256": config.identity, "published_fee": config.published.model_dump(mode="json"),
        "reference_execution": REFERENCE.model_dump(), "counterfactual_execution": (
            config.published.execution_sensitivity(REFERENCE).model_dump()
        ),
        "reference_market": market_identity(plan, config, "reference"),
        "counterfactual_market": market_identity(plan, config, "counterfactual"),
        "profile_hash": plan["profile_hash"], "schedule": plan["folds"], "holdout": plan["windows"],
        "account_currency": "USD", "fees_in_quote_value": True, "observed_spread_added": False,
        "scope": "Unfiltered Swing only; one fixed-quantity fee repricing and one new execution replay.",
    }
    digest = fingerprint(case)
    directory = root / f"swing-{digest[:14]}"
    with writer_lock(root / ".writer.lock"):
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            manifest = read_json(manifest_path)
            if manifest["design_sha256"] != digest or any(manifest[k] != v for k, v in case.items()):
                raise ValueError("Previously frozen sensitivity manifest changed")
        else:
            manifest = {**case, "design_sha256": digest, "frozen_at_utc": utc_now().isoformat()}
            write_json(manifest_path, manifest)
        complete = directory / "complete.json"
        if complete.exists():
            for name, checksum in read_json(complete)["files"].items():
                if file_hash(directory / name) != checksum:
                    raise ValueError("Completed sensitivity checksum mismatch")
            return directory
        try:
            result = calculation(config, plan, previous, origin, directory, manifest)
            write_json(directory / "results.json", result)
            summarize(result, directory)
        except Exception as exc:
            write_json(directory / "failure.json", {"label": LABEL, "error": str(exc), "at_utc": utc_now()})
            raise
        write_json(complete, {"label": LABEL, "files": {p.name: file_hash(p) for p in directory.iterdir()
                                                        if p.is_file()}, "completed_at_utc": utc_now()})
        write_json(root / "latest-sensitivity.json", {"label": LABEL, "directory": directory.name})
    return directory
