from collections import Counter
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd

from lab.backtest import size_order
from lab.config import ORIGIN, Profile, load_profiles
from lab.data import TARGET_START
from lab.execution import adverse, execution_open, plan_setups
from lab.features import decisions, feature_columns
from lab.model import load_model, split_labels
from lab.research import clip
from lab.storage import put_bytes, utc, utc_now, write_json, writer_lock

from lab.phase2.audit_data import gaps, integrity, reconciliation, retries
from lab.phase2.checkpoint import AUDITS, frozen_prices, frozen_runs, preserve


def counts(values) -> dict:
    return dict(Counter(values))


def distribution(values: pd.Series) -> dict:
    values = values.dropna()
    if values.empty:
        return {"count": 0}
    return {"count": len(values), "positive": int(values.gt(0).sum()), "zero": int(values.eq(0).sum()),
            "negative": int(values.lt(0).sum()), "mean": float(values.mean()),
            **{f"p{int(q * 100)}": float(values.quantile(q)) for q in (0, .1, .5, .9, 1)}}


def audit_window(candles, signals, labels, start, end, folder, profile, name, artifacts):
    sample, split = split_labels(labels, start, end)
    assigned = labels.loc[(labels.index < end) & ((labels.index >= start) if start is not None else True)]
    bars = signals.loc[(signals.index < end) & ((signals.index >= start) if start is not None else True)]
    split["with_all_features"] = len(sample.dropna(subset=feature_columns(profile)))
    split["resolved_before_cutoff"] = int((assigned.resolved_at < end).sum())
    split["execution_gap"] = int(assigned.status.eq("execution_gap").sum())
    split["boundary_censored_or_crossed"] = int((
        assigned.status.isin(["window_boundary", "entry_outside_window"]) | assigned.resolved_at.ge(end)
    ).sum())
    result = {
        "start_inclusive": start, "end_exclusive": end,
        "first_completed_close": bars.index.min() if len(bars) else None,
        "last_completed_close": bars.index.max() if len(bars) else None,
        "completed_decision_bars": len(bars),
        "quality_eligible_complete_native_inputs": len(bars),
        "incomplete_calendar_decision_buckets": (
            int((bars.index[-1] - bars.index[0]) // pd.Timedelta(profile.decision)) + 1 - len(bars)
            if len(bars) else 0
        ),
        "baseline_warmed_up": int(bars.baseline_ready.sum()),
        "model_warmed_up": int(bars.ready.sum()), "baseline_entry_signals": int(bars.enter.sum()),
        "independent_hypothetical_setups": len(assigned),
        "hypothetical_targets_resolved_any_time_in_development": int(assigned.target.notna().sum()),
        "label_sample": split, "target_distribution_decimal": distribution(sample.target),
        "all_baseline_score_distribution_decimal": distribution(assigned.predicted_net_return),
        "mature_label_score_distribution_decimal": distribution(sample.predicted_net_return),
        "score_missing": int(assigned.predicted_net_return.isna().sum()),
        "score_pass": int(assigned.predicted_net_return.gt(0).sum()),
        "score_reject": int(assigned.predicted_net_return.le(0).sum()),
        "entry_threshold_decimal": 0, "approval_gate": "research_only_no_validated_model",
    }
    if not artifacts:
        return result, None
    setups = plan_setups(clip(candles, end), bars, profile, profile.costs, end)
    trace = pd.DataFrame.from_records([{
        "signal_at": s.signal_at, "available_at": s.available_at, "modeled_entry": s.entry_at,
        "setup_status_in_account_window": s.reason, "setup_outcome_observed_through": s.observed_through,
    } for s in setups]).set_index("signal_at")
    trace = trace.join(assigned[["target", "resolved_at", "status", "predicted_net_return"]])
    trace["setup_outcome_observed_through"] = pd.to_datetime(trace.setup_outcome_observed_through, utc=True)
    trace["candidate_score_gate"] = trace.predicted_net_return.map(
        lambda x: "missing_model_features" if pd.isna(x) else ("positive" if x > 0 else "nonpositive_net_score")
    )
    result["accounts"] = {}
    journals = {}
    for account in ("baseline", "candidate"):
        journal = pd.read_parquet(folder / f"{name}-{account}-trades.parquet")
        journals[account] = journal
        if len(journal):
            assert (journal.entry_at < end).all() and (journal.entry_at > journal.available_at).all()
            assert (journal.exit_at.dropna() <= end).all()
            closed = journal[journal.exit_at.notna()]
            assert (closed.exit_at <= closed.deadline).all()
            if profile.session:
                minutes = journal.entry_at.dt.hour * 60 + journal.entry_at.dt.minute
                assert (minutes < profile.session.cutoff_minute_utc).all()
        outcomes = trace_account(setups, journal, trace, profile, account, end)
        trace[account] = outcomes
        result["accounts"][account] = accounting(journal, artifacts[account], profile)
        result["accounts"][account]["decision_outcomes"] = counts(outcomes)
        result["accounts"][account]["modeled_entry_attempts"] = sum(
            item in ("filled", "filled_unresolved", "missing_entry_bar", "cash_rejected") for item in outcomes
        )
    result["account_comparison"] = compare_journals(journals, artifacts)
    return result, trace


def trace_account(setups, journal, trace, profile, account, end) -> list[str]:
    fills = {row.signal_at: row for row in journal.itertuples()}
    result, seen = [], set()
    cash = profile.starting_cash
    pending = None
    halted = False
    for setup in setups:
        ts = setup.signal_at
        gate = trace.loc[ts, "candidate_score_gate"]
        if setup.entry_at >= end:
            reason = "window_ended_before_entry"
        elif halted:
            reason = "account_stopped_on_unresolved_position"
        elif pending is not None and setup.available_at <= pending:
            reason = "position_occupied_or_order_pending"
        elif account == "candidate" and gate != "positive":
            reason = gate
        elif setup.entry_raw is None:
            reason = setup.reason
            if reason == "missing_entry_bar":
                pending = setup.entry_at
        elif ts in fills:
            row = fills[ts]
            seen.add(ts)
            halted = pd.isna(row.exit_at)
            pending = row.exit_at
            reason = "filled_unresolved" if halted else "filled"
            if not halted:
                cash += row.net_usd
        else:
            units, _ = size_order(setup, profile, profile.costs, cash)
            required = units * adverse(setup.entry_raw, profile.costs, buy=True) * (1 + profile.costs.fee)
            if not required > cash:
                raise ValueError(f"An executable {account} setup is absent from the saved ledger at {ts}")
            pending, reason = setup.entry_at, "cash_rejected"
        result.append(reason)
    if seen != set(fills):
        raise ValueError(f"Saved {account} fills do not match the independently traced decisions")
    return result


def compare_journals(journals, metrics) -> dict:
    columns = ["signal_at", "entry_at", "exit_at", "exit_bar_open", "reason", "units_btc",
               "entry_price", "exit_price", "fees_usd", "net_usd"]
    baseline, candidate = (journals[k].reindex(columns=columns) for k in ("baseline", "candidate"))
    baseline_ids, candidate_ids = set(baseline.signal_at), set(candidate.signal_at)
    return {
        "identical_trade_identities_and_accounting": baseline.equals(candidate),
        "baseline_only_signals": sorted(baseline_ids - candidate_ids),
        "candidate_only_signals": sorted(candidate_ids - baseline_ids),
        "baseline_net_return_full_precision": metrics["baseline"]["net_return"],
        "candidate_net_return_full_precision": metrics["candidate"]["net_return"],
        "net_return_difference_candidate_minus_baseline": (
            metrics["candidate"]["net_return"] - metrics["baseline"]["net_return"]
        ),
    }


def accounting(journal, metrics, profile) -> dict:
    closed = journal[journal.exit_at.notna()] if len(journal) else journal
    fee = profile.costs.fee
    proxy = profile.costs.proxy
    if len(closed):
        buy = closed.entry_price / (1 + proxy)
        sell = closed.exit_price / (1 - proxy)
        gross = float(((sell - buy) * closed.units_btc).sum())
        drag = float(((closed.entry_price - buy + sell - closed.exit_price) * closed.units_btc).sum())
        fees = float(closed.fees_usd.sum())
        net = float(closed.net_usd.sum())
        np.testing.assert_allclose(gross - drag - fees, net, atol=1e-8)
        np.testing.assert_allclose((closed.entry_notional + closed.exit_notional) * fee, closed.fees_usd)
    else:
        gross = drag = fees = net = 0.0
    np.testing.assert_allclose(profile.starting_cash + net + metrics["unrealized_usd"],
                               metrics["ending_equity_usd"], atol=1e-8)
    return {
        **metrics, "closed_trade_attribution_usd": {
            "raw_gross": gross, "adverse_price_drag": drag, "fees": fees, "net": net,
            "method": "Same executed quantity and order events; no zero-cost counterfactual run",
        },
        "closed_holding_minutes": distribution(closed.holding_minutes) if len(closed) else {"count": 0},
    }


def hand_check(profile, candles, folder, signals, start, end) -> dict:
    ledger = pd.read_parquet(folder / "evaluation-baseline-trades.parquet")
    row = ledger.iloc[0]
    s = {item.signal_at: item for item in plan_setups(
        clip(candles, end), signals.loc[(signals.index >= start) & (signals.index < end)],
        profile, profile.costs, end,
    )}[row.signal_at]
    assert row.entry_at == execution_open(row.signal_at + pd.Timedelta(seconds=profile.costs.processing_seconds),
                                         profile.step, profile.costs.delay_bars)
    assert row.entry_at > row.available_at and row.exit_at <= row.deadline
    entry_raw = candles.loc[row.entry_at, "open"]
    exit_bar = candles.loc[row.exit_bar_open]
    exit_raw = row.stop if row.reason == "stop_intrabar" else exit_bar.open
    if row.reason == "stop_intrabar":
        assert exit_bar.low <= row.stop < exit_bar.open
    fee = profile.costs.fee_bps / 10000
    proxy = profile.costs.proxy_bps / 10000
    decision = signals.loc[row.signal_at, "close"]
    risk_unit = decision * (1 + proxy) * (1 + fee) - row.stop * (1 - proxy) * (1 - fee)
    units = min(25 / risk_unit, 2500 / (decision * (1 + proxy)))
    buy, sell = entry_raw * (1 + proxy), exit_raw * (1 - proxy)
    entry_fee, exit_fee = buy * units * fee, sell * units * fee
    net = units * (sell - buy) - entry_fee - exit_fee
    np.testing.assert_allclose(
        [units, buy, sell, entry_fee + exit_fee, net, units * risk_unit],
        [row.units_btc, row.entry_price, row.exit_price, row.fees_usd, row.net_usd, row.planned_risk_usd],
        atol=1e-9,
    )
    assert s.exit_at == row.exit_at and s.exit_raw == exit_raw
    return {
        "signal_close": row.signal_at, "available_at": row.available_at, "entry": row.entry_at,
        "exit": row.exit_at, "exit_bar_open": row.exit_bar_open, "reason": row.reason,
        "raw_entry": float(entry_raw), "raw_exit": float(exit_raw), "adverse_entry": float(buy),
        "adverse_exit": float(sell), "units_btc": float(units), "entry_fee_usd": float(entry_fee),
        "exit_fee_usd": float(exit_fee), "planned_risk_usd": float(units * risk_unit), "net_usd": float(net),
        "decision_close_price": float(decision), "stop": float(row.stop), "hard_deadline": row.deadline,
    }


def audit_profile(profile: Profile, candles, missing, directory: Path, folder: Path, manifest: dict) -> dict:
    rules = manifest["windows"]
    valid, evaluate, hold = (utc(rules[k]) for k in ("validation_start", "evaluation_start", "holdout_start"))
    development = clip(candles, hold)
    signals = decisions(development, profile)
    assert development.index.max() < hold and development.close_at.max() <= hold
    for cutoff in (valid, evaluate, hold):
        prefix = decisions(clip(development, cutoff), profile)
        pd.testing.assert_frame_equal(signals.loc[prefix.index].tail(4), prefix.tail(4))
    labels = pd.read_parquet(folder / "hypothetical-labels.parquet")
    eligible = labels[labels.target.notna()]
    fee, proxy = profile.costs.fee_bps / 10000, profile.costs.proxy_bps / 10000
    buy = eligible.entry_raw * (1 + proxy)
    sell = eligible.exit_raw * (1 - proxy)
    np.testing.assert_allclose(eligible.target, (sell - buy - fee * (sell + buy)) / buy, atol=1e-12)
    model = load_model(manifest["model"], profile, profile.costs)
    cols = feature_columns(profile)
    train, _ = split_labels(labels, None, valid)
    train = train.dropna(subset=cols)
    scaler = model["standardscaler"]
    np.testing.assert_allclose(scaler.mean_, train[cols].mean(), rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(scaler.var_, train[cols].var(ddof=0), rtol=1e-12, atol=1e-12)
    assert scaler.n_samples_seen_ == len(train) and train.resolved_at.max() < valid
    scores = labels.dropna(subset=["predicted_net_return"])
    np.testing.assert_allclose(model.predict(scores[cols]), scores.predicted_net_return, rtol=1e-12, atol=1e-12)
    assert labels.index.max() < hold and eligible.resolved_at.max() <= hold
    data = {"feature_count": len(cols), "feature_columns": cols,
            "latest_fitting_resolution": train.resolved_at.max(), "cutoffs": rules,
            "sealed_holdout": True, "normal_inference_compatible": True, "candidate_promoted": False,
            "target_unit": "Fraction of adverse entry notional; multiply by 100 for percent",
            "fitting_sample_count": len(train), "windows": {}}
    traces = []
    for name, start, end in (("training", None, valid), ("validation", valid, evaluate), ("evaluation", evaluate, hold)):
        info, trace = audit_window(development, signals, labels, start, end, folder, profile, name,
                                   manifest["periods"].get(name))
        data["windows"][name] = info
        if trace is not None:
            trace["window"] = name
            traces.append(trace)
    pd.concat(traces).to_parquet(directory / f"{profile.id}-funnel.parquet")
    data["manual_trade_check"] = hand_check(profile, development, folder, signals, evaluate, hold)
    data["reference_sensitivity"] = manifest["sensitivity"]
    assert all(s["training_model_version"] == manifest["model"]["model_version"]
               and not s["promotion_eligible"] and s["research_only"] for s in data["reference_sensitivity"])
    data["gap_impact"] = gap_impact(development, signals, missing, profile, folder, hold)
    return data


def gap_impact(candles, signals, missing, profile, folder, hold):
    setups = plan_setups(candles, signals, profile, profile.costs, hold)
    marked = list(chain.from_iterable(
        pd.read_parquet(path).to_dict("records") for path in folder.glob("*-trades.parquet")
    ))
    effects = []
    for gap in missing:
        left, right = gap["start"], gap["end"]
        item = dict(gap)
        if left >= hold:
            item["scope"] = "reserved_data_integrity_only"
        else:
            item["scope"] = "development"
            affected = {}
            for frame in [profile.decision, *[x.frame for x in profile.context]]:
                step = pd.Timedelta(frame)
                first = ORIGIN + ((left - ORIGIN) // step) * step
                last = ORIGIN + ((right - pd.Timedelta("1ns") - ORIGIN) // step) * step
                affected[frame] = int((last - first) // step + 1)
            next_ready = signals.index[(signals.index > right) & signals.baseline_ready]
            through = min(next_ready[0], hold) if len(next_ready) else hold
            item.update(
                incomplete_buckets_by_frame=affected, first_warmed_close_after_gap=next_ready[0] if len(next_ready) else None,
                completed_bars_without_warmup=int((
                    (signals.index > left) & (signals.index < through) & ~signals.baseline_ready).sum()),
                missing_entry_setups=sum(s.reason == "missing_entry_bar" and left <= s.entry_at < right for s in setups),
                unresolved_open_setups=sum(s.reason == "execution_gap" and s.observed_through == left for s in setups),
                affected_saved_trades=[{"model": row["model_version"], "entry": row["entry_at"],
                                        "reason": row["reason"], "deadline": row["deadline"]}
                                       for row in marked if row["reason"] == "execution_gap"
                                       and row["entry_at"] < left
                                       and row["entry_at"] + pd.Timedelta(minutes=row["holding_minutes"]) == left],
            )
        effects.append(item)
    return effects


def audit() -> Path:
    with writer_lock(AUDITS / ".writer.lock"):
        original = preserve()
        folder = AUDITS / utc_now().strftime("%Y%m%dT%H%M%SZ")
        folder.mkdir()
        runs = frozen_runs()
        frames = {seconds: frozen_prices(runs[key][1]["source_snapshot"]) for seconds, key in (
            (3600, "swing"), (300, "day"),
        )}
        end = utc(runs["swing"][1]["windows"]["data_cutoff"])
        missing = {s: gaps(frame, utc(TARGET_START[s]), end, s) for s, frame in frames.items()}
        differences, detail = reconciliation(frames[3600], frames[300])
        detail.to_parquet(folder / "resolution-differences.parquet")
        data = {
            "canonical": {"position_spot": "native 3600s", "swing": "native 3600s", "day": "native 300s"},
            "feeds": {s: {"integrity": integrity(f, s), "gaps": missing[s],
                         "missing_bars": sum(g["bars"] for g in missing[s]),
                         "longest_gap_minutes": max((g["duration_minutes"] for g in missing[s]), default=0)}
                      for s, f in frames.items()},
            "reconciliation": differences, "retries": retries(frames, missing, detail, folder),
            "policy": "Retry evidence is separate; original snapshots remain canonical and unresolved gaps stay missing.",
        }
        write_json(folder / "data-audit.json", data)
        audits = {}
        for key, profile in load_profiles().items():
            run_folder, manifest = runs[key]
            audited = audit_profile(profile, frames[profile.seconds], missing[profile.seconds], folder,
                                    run_folder, manifest)
            write_json(folder / f"{key}.json", audited)
            audits[key] = audited
        write_json(folder / "split-integrity.json", {
            "utc_boundaries": runs["swing"][1]["windows"],
            "closed_bar_at_right_boundary": "Permitted as a mark when its open is earlier; no order or signal at right boundary",
            "maturity_rule": "Outcomes must resolve strictly before each sample cutoff; actual interval times were used",
            "sealed_holdout": True, "holdout_prices_read_for_integrity_only": True,
            "scaler_and_score_reproduced": list(audits), "causal_prefix_cutoffs_tested": 3,
            "latest_training_resolution_by_profile": {key: value["latest_fitting_resolution"]
                                                     for key, value in audits.items()},
        })
        markdown(folder, data, audits, original)
        write_json(AUDITS / "latest.json", {"directory": folder.name})
        return folder


def markdown(folder, data, audits, original):
    out = ["# Phase 2 checkpoint audit", "", "All returns are development-only and reference the preserved checkpoint.",
           "", "## Chronology", "",
           "- Training uses signals before 2025-01-01 00:00 UTC and outcomes resolved strictly before that cutoff.",
           "- Validation assigns decision closes in [2025-01-01 00:00, 2025-05-01 00:00) UTC. Evaluation assigns "
           "closes in [2025-05-01 00:00, 2025-11-08 00:00) UTC. Both error samples require resolution strictly before "
           "their right boundary.",
           "- A candle opened before a boundary may contribute its closing mark exactly at that boundary. An order "
           "cannot use the candle opening at the boundary. The holdout is [2025-11-08 00:00, 2026-11-08 00:00) UTC.",
           "- The audit reproduced the training scaler means and variances and every saved candidate score. "
           "Truncating the feature input at each cutoff preserved the last four causal feature rows. "
           "Reserved prices were inspected only for data integrity.",
           "", "## Decisions and outcomes", "",
           "| Profile | Fit / validation / evaluation labels | Evaluation score passes / rejects | "
           "Baseline trades and net return | Candidate trades and net return |",
           "| --- | ---: | ---: | ---: | ---: |"]
    for key, info in audits.items():
        train, val, ev = (info["windows"][k] for k in ("training", "validation", "evaluation"))
        baseline, candidate = (ev["accounts"][k] for k in ("baseline", "candidate"))
        out.append(f"| {key} | {train['label_sample']['with_all_features']} / {val['label_sample']['with_all_features']} "
                   f"/ {ev['label_sample']['with_all_features']} | {ev['score_pass']} / {ev['score_reject']} | "
                   f"{baseline['trade_count']}, {baseline['net_return']:.4%} | "
                   f"{candidate['trade_count']}, {candidate['net_return']:.4%} |")
    for key, info in audits.items():
        ev = info["windows"]["evaluation"]
        same = ev["account_comparison"]["identical_trade_identities_and_accounting"]
        out.extend(["", f"### {key}", "",
                    f"- Identical evaluated trade identities and ledger values **{same}**.",
                    f"- Baseline outcomes {ev['accounts']['baseline']['decision_outcomes']}.",
                    f"- Candidate outcomes {ev['accounts']['candidate']['decision_outcomes']}.",
                    f"- Score distribution (decimal fraction) {ev['all_baseline_score_distribution_decimal']}.",
                    f"- Training used {info['feature_count']} features and "
                    f"{info['windows']['training']['label_sample']['overlapping_outcome_pairs']} overlapping outcome pairs.",
                    f"- [Funnel, accounting, sample counts and checked trade]({key}.json)"])
    pos = audits["position_spot"]["windows"]
    day = audits["day"]["windows"]["evaluation"]
    ev = day["all_baseline_score_distribution_decimal"]
    targets = day["target_distribution_decimal"]
    out.extend(["", "## Why the results look this way", "",
                "- Position and Swing passed all evaluation scores. Their candidates executed the same trade identities "
                "with the same full-precision prices, quantities, fees, and net P&L. Reloading the artifacts reproduced "
                "the scores, and the account trace found no baseline substitution.",
                f"- Position generated {pos['validation']['baseline_entry_signals']} qualifying setup in "
                f"{pos['validation']['baseline_warmed_up']} warmed-up validation bars. It resolved before May 1. "
                f"The following evaluation window generated {pos['evaluation']['baseline_entry_signals']} setups "
                "and is a separate population. The five-label validation floor is still unmet.",
                f"- Day rejected every scored setup at zero. Scores ranged from {ev['p0']:.3%} to {ev['p100']:.3%}. "
                f"Of {targets['count']} mature evaluation outcomes, {targets['positive']} were positive and the "
                f"median was {targets['p50']:.3%}. {day['label_sample']['status_counts'].get('entry_cutoff', 0)} "
                "setups were independently ineligible at the Day entry cutoff. Candidate cash remained USD 10,000.",
                "- Targets divide unit net P&L by adverse entry notional. Each side uses one adverse price adjustment "
                "and one proportional fee on its own notional. A frozen cost test and all saved targets reconcile.",
                "", "## Same-trade baseline attribution", "",
                "| Profile | Raw gross USD | Adverse-price drag USD | Fees USD | Net USD | Checked example USD |",
                "| --- | ---: | ---: | ---: | ---: | ---: |"])
    for key, info in audits.items():
        account = info["windows"]["evaluation"]["accounts"]["baseline"]["closed_trade_attribution_usd"]
        out.append(f"| {key} | {account['raw_gross']:.2f} | {account['adverse_price_drag']:.2f} | "
                   f"{account['fees']:.2f} | {account['net']:.2f} | {info['manual_trade_check']['net_usd']:.2f} |")
    out.extend(["", "The attribution holds the recorded trades and quantities fixed. A zero-cost strategy "
                "counterfactual could change quantities and decisions.", "",
                "Cash and the two original passive BTC benchmarks use the same account dates. Buy-and-hold uses 100% "
                "of starting capital including its entry fee, and the passive benchmark uses 25%. Both retain "
                "their original BTC amount through the last observed mark and pay no invented ending liquidation fee."])
    r = data["reconciliation"]
    out.extend(["", "## Data integrity", "",
                f"Of {r['overlapping_complete_hours']:,} overlapping complete hours, "
                f"{r['ohlc_hours']} differed in OHLC and {r['volume_only_hours']} differed only in volume.",
                f"Longest gaps were {data['feeds'][3600]['longest_gap_minutes']} minutes hourly and "
                f"{data['feeds'][300]['longest_gap_minutes']} minutes at five minutes. "
                "Missing candles invalidate incomplete buckets and can restart indicator warmup. No reference "
                "development trade or hypothetical outcome in the preserved checkpoint ran into a missing execution bar.",
                "Targeted retry observations and any proposed revisions are separate from the frozen datasets.",
                "", "- [All gaps, magnitudes and targeted retry results](data-audit.json)",
                "- [Split and holdout integrity](split-integrity.json)",
                "- [Original evidence hashes and model identities](../checkpoint/evidence.json)",
                "- [Checkpoint source archive](../checkpoint/source.zip)", ""])
    put_bytes(folder / "SUMMARY.md", "\n".join(out).encode("utf-8"))
