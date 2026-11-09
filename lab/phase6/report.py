from pathlib import Path

import pandas as pd

from lab.phase2 import store
from lab.phase5.design import LABEL
from lab.phase5.observations import decision_event
from lab.phase6.coverage import snapshot
from lab.phase6.design import OUTPUT, freeze, origin, read_manifest
from lab.phase6.feature_audit import diagnose
from lab.phase6.risk import reconcile
from lab.storage import file_hash, put_bytes, read_json, utc, writer_lock, write_json


def coverage_text(data):
    lines = [f"Coverage through {data['as_of_utc']}. Original activation {data['original_activation_utc']}.", "",
             "### A. Full 24-hour closed-candle completeness", "",
             "| Native interval | Present / expected | Missing | Closes before activation | Received within 120 s |",
             "| --- | --- | ---: | ---: | ---: |"]
    for seconds, row in data["historical_completeness"].items():
        lines.append(f"| {int(seconds) // 60} min | {row['present']} / {row['expected']} | {row['missing']} | "
                     f"{row['pre_activation_closes']} | {row['within_120_seconds']} |")
    lines.extend(["", "### B. Post-activation first receipts", "",
                  "| Native interval | Elapsed horizons | Timely | Late | Catch-up | Missing | Unknown | Pending | Delay n / median / p90 (s) |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"])
    for seconds, row in data["active_first_receipts"].items():
        delay = row["receipt_delay_seconds"]
        latency = f"{delay['count']} / {delay['p50']:.2f} / {delay['p90']:.2f}" if delay["count"] else "0 / — / —"
        lines.append(f"| {int(seconds) // 60} min | {row['denominator']} | {row['timely']} | {row['late']} | "
                     f"{row['catch_up']} | {row['missing']} | {row['unknown']} | {row['closed_pending_deadline']} | {latency} |")
    lines.append("")
    for seconds, row in data["active_first_receipts"].items():
        lines.append(f"- {int(seconds) // 60}-minute receipt evidence starts {row['receipt_instrumentation_start_utc']}. "
                     f"The last elapsed horizon covers the close at {row['last_deadline_elapsed_close_utc']}.")
    lines.extend(["", "The 120-second horizon is descriptive. The denominator starts at original activation and includes downtime. "
                  "The first saved revision uses its recorded response receipt, with unknown timing tracked separately.", "",
                  "### C. Prospective decisions", "",
                  "| Profile and cadence | Assessed | Timely / late / missing / unknown | Complete / blocked / unknown | Timely and ready | Outcomes |",
                  "| --- | ---: | --- | --- | ---: | --- |"])
    for key, row in data["decisions"].items():
        timing = " / ".join(map(str, row["timing"].values()))
        readiness = " / ".join(map(str, row["readiness"].values()))
        outcomes = ", ".join(f"{k.replace('_', ' ')} ({v})" for k, v in row["outcomes"].items()) or "—"
        lines.append(f"| {key} {row['schedule']} | {row['assessable']} | {timing} | {readiness} | "
                     f"{row['timely_and_ready']} | {outcomes} |")
    fills = sum(row["execution_events_since_activation"] for row in data["decisions"].values())
    intents = sum(row["precommitted_intents"] for row in data["decisions"].values())
    pending = sum(row["pending_deadline"] for row in data["decisions"].values())
    lines.extend(["", f"Decision lookback is {data['decision_lookback_hours']} hours, bounded by activation. "
                  f"{pending} opportunities await cutoff. This window has {intents} durably precommitted intents. "
                  f"The journal has {fills} simulated fill events since activation."])
    if not fills:
        lines.append("Fill quality remains unobserved.")
    if current := data.get("current_worker_health"):
        heartbeat = current.get("heartbeat") or {}
        elapsed = current["current_worker_elapsed_seconds"]
        uptime = f"{elapsed:.0f} s" if elapsed is not None else "unknown"
        lines.extend(["", f"Worker PID {current['worker_pid']} is {current['state']}, alive={current['process_alive']}, "
                      f"stale={current['runner_stale']}, heartbeat healthy={heartbeat.get('healthy')}. "
                      f"Current process uptime {uptime}, started {current['worker_started_at_utc']}. "
                      f"Experiment {current.get('experiment_id')}.",
                      f"Observation instrumentation {current['observation_instrumentation']}. "
                      "Logoff and reboot persistence remain unverified."])
    return "\n".join(lines)


def causal_examples(data):
    selection = (
        ("swing", "feature-blocked", lambda row: row["data_blocked"]),
        ("position_spot", "feature-ready with no signal", lambda row: row["ready_features"] and row["no_signal"]),
        ("day", "session-ineligible opportunity", lambda row: not row["session_allows_entry_at_open"]),
        ("day", "session eligible with no signal", lambda row: row["session_allows_entry_at_open"] and row["no_signal"]),
    )
    examples = []
    with store.connect(readonly=True) as db:
        db.execute("BEGIN")
        for profile, label, gate in selection:
            row = next(item for item in data["decisions"][profile]["event_evidence"]
                       if item["classification"] == "timely_before_modeled_open" and gate(item))
            close = row["decision_close_utc"]
            event = decision_event(db, profile, f"decision:{close.isoformat()}")
            examples.append({"profile": profile, "gate": label, "receipt_and_decision": row, "saved_journal": event})
    return {
        "examples": examples,
        "ordering": "Completed native input bars, first durable response availability, receipts of selected revisions, "
                    "original input load, journaled decision, eligibility, nominal executable open.",
        "quantity_evidence": "The examples have no order quantity. Pre-open sizing uses available cash, decision close and "
                             "stop before durable acknowledgement. The frozen Phase 2 missed-event rules still apply.",
        "pending_intent_rule": "A decision at or past its open creates no historical intent. A precommitted intent may later "
                               "resolve from the saved candle, with delayed reconstruction explicitly tagged.",
    }


def draft_handoff(manifest):
    return {
        "original_registered_protocol": manifest["mexc_draft"],
        "state": "DRAFT_BLOCKED", "selected_preference": "MEXC",
        "known_residency_location": "Toronto, Canada", "eligibility": "RESTRICTED",
        "eligibility_source": "https://www.mexc.com/terms", "pair": "BTC/USDT (BTCUSDT), provisional",
        "activation_utc": None, "automatic_promotion": False, "approved_lane": "No validated model",
        "before_activation": {
            "eligibility": "Confirm current residency/location and permitted use. If Canada still applies, "
                           "a different eligible venue is needed before a venue-specific study.",
            "spot_pair": "Confirm final native spot pair.",
            "execution_channel": "Specify manual web/app or API.",
            "fees": "Provide dated account, pair and channel maker/taker fees, commission asset, MX settings, "
                    "and any applicable discounts or promotions with their conditions and expiry.",
            "market_data_and_constraints": "Verify native price coverage, gaps, candle alignment and receipt evidence for the "
                                           "same venue/pair plus available order types, ticks, minimums, size/precision, "
                                           "protective exits, fees and fill handling for that execution channel.",
            "paper_limits": "User input pending for paper sizing/risk, drawdown tolerance, and pause/resume preferences.",
        },
        "future_protocol": "Freeze eligible venue, pair, cost evidence and strategy/execution versions at the actual activation. "
                           "Keep 90 days as a first review point.",
        "descriptive_trade_count_at_90d": 68 / 861 * 90,
        "sample_note": "Historical extrapolation only. The study has no forecast or sample target based on this figure.",
    }


def markdown(manifest, risk, feature, coverage, previous):
    low, worst = risk["historical_lower_fee"], risk["worst_trade"]
    old = previous["worker_health"]["feeds"]
    old_counts = []
    for seconds in (300, 3600):
        feed = old[str(seconds)]
        step = pd.Timedelta(seconds=seconds)
        closes = pd.date_range(utc(feed["window_24h_left_utc"]) + step, utc(feed["window_24h_right_utc"]), freq=step)
        old_counts.append((feed["received_within_120_seconds_of_close"], len(closes),
                           int((closes < coverage["original_activation_utc"]).sum())))
    (fine, hourly) = old_counts
    source = feature["loaded_source"]
    return "\n".join([
        "# Phase 6 risk reconciliation and observation readiness", "", LABEL, "",
        f"Audit frozen {manifest['observation_as_of_utc']}. The Phase 5 historical finding remains **INCONCLUSIVE**. "
        "The performance holdout remains sealed. Approved lane **No validated model**.", "",
        "## Historical risk", "",
        f"The original unfiltered lower-fee Swing ledger returned {low['net_return']:+.2%} total and "
        f"{low['annualized_net_return']:+.2%} annualized, with {low['drawdown_on_observed_marks']:.2%} drawdown on hourly "
        f"marks over {risk['historical_window_days']:.0f} days. Net P&L was ${risk['net_pnl_usd']:.2f}. "
        f"The five largest winners contributed ${risk['top_five_pnl_usd']:,.2f}. "
        f"The wider-proxy and one-hour-delay stresses returned {risk['historical_stresses_net_return']['proxy_10bp']:+.2%} "
        f"and {risk['historical_stresses_net_return']['extra_hour']:+.2%}.", "",
        f"All {risk['trades']} ledger rows reconcile. {risk['overruns']} exceeded 0.25% of decision-time equity, "
        f"including {risk['overruns']} of {risk['losers']} losers. Total excess was ${risk['total_excess_usd']:.2f}, "
        f"median ${risk['median_overrun_usd']:.2f}, and worst ${worst['overrun_usd']:.2f} "
        f"on the {worst['signal_at']} decision. That trade lost ${-worst['net_usd']:.2f} against "
        f"a ${worst['planned_risk_budget_usd']:.2f} budget ({worst['realized_loss_in_budgets']:.2f}×).", "",
        f"Cause counts {risk['cause_counts']}. All 20 paid a higher raw entry price than the close used for sizing and "
        "then stopped at the original raw stop under the hourly intrabar model. Cost differences followed the changed "
        "notionals. The quantity was fixed at decision time. Risk bound all 68 plans, so the planned cost-inclusive "
        "loss matched the budget within float tolerance. No cash or exposure cap bound and no venue rounding applied.", "",
        "An opening at or below the stop would take priority over scheduled and intrabar exits. No saved entry opened "
        "at or below the stop. The intrabar trigger can only be bounded by its hourly candle. The audit confirmed the "
        "frozen implementation and used zero corrective replays.", "",
        "## Swing feature block", "",
        f"The saved {feature['saved_decision']['decision_close']} decision used the pinned Coinbase native hourly "
        f"snapshot and revisions through watermark {feature['saved_decision']['data']['latest_forward_revision_id']}. "
        f"It loaded {source['native_bars']:,} native bars from {source['first_open']} through source close "
        f"{source['last_close']}. The final selected input was received at {source['selected_forward_receipts_through']}, "
        f"and the worker began loading at {source['original_load_started_at']}. The journal was written at "
        f"{feature['saved_decision']['recorded_at']} before the {feature['saved_decision']['modeled_fill_at']} modeled open. "
        "The precise original load completion time was unavailable.", "",
        f"The daily context SMA requires 200 consecutive completed daily bars. Five missing hourly opens on "
        f"{feature['latest_incomplete_daily_open'].date()} (02:00 through 06:00 UTC) reset the sequence. "
        f"The decision had {feature['consecutive_daily_bars']} complete days and {feature['consecutive_4h_bars']:,} contiguous "
        f"4-hour bars. The daily context spread was the only failed baseline input. Earliest possible daily readiness is "
        f"{feature['earliest_possible_ready_daily_close_utc']} if all remaining required bars arrive without another gap. "
        "No loader or model incompatibility was found. The past record stays blocked.", "",
        "The live Position, Swing and Day shadows use their frozen Ridge net-return filters with 50 bp fees and 5 bp adverse "
        "adjustment per side. The historical lower-fee Swing ledger was unfiltered with a 5 bp fee and 5 bp adverse adjustment. "
        "Swing's Ridge filter uses momentum, ATR fraction, breakout distance, relative volume and the shared daily spread. "
        "All other Swing inputs passed at the audited decision.", "",
        "## Frozen coverage", "",
        f"The earlier Phase 5 tallies were {fine[0]}/{fine[1]} five-minute and {hourly[0]}/{hourly[1]} hourly within 120 s. "
        f"Those denominators included {fine[2]} and {hourly[2]} pre-activation closes respectively. "
        "Use table B for active-period timing.", "",
        coverage_text(coverage), "",
        "## Readiness", "",
        "| Area | Status |", "| --- | --- |",
        "| Risk accounting | Verified against 68 saved trades. Planned risk and realized loss can differ. |",
        "| Swing features | Blocked by the contiguous daily lookback. Future readiness is conditional. |",
        "| Causal decision timing | Pre-open ordering verified on saved examples. Fill quality unobserved. |",
        "| Worker continuity | Original activation and pins preserved. Use the live coverage command for process state. |",
        "| Venue eligibility | MEXC restricted in the known Canadian context. The draft stays blocked. |",
        "| Pair, channel, fees and data | Confirmation and matching executable constraints pending. |",
        "| Strategy evidence | INCONCLUSIVE. No validated model. |", "",
        "The venue handoff adds the missing inputs to the preserved 90-day draft. At 68 closes over 861 days the historical "
        "pace translates to roughly seven closes in 90 days. That is a descriptive extrapolation. Review new observations weekly "
        "using the existing Phase 2 runbook.", "",
        "Evidence files include the frozen manifest, full risk table, 22 detailed reconstructions, feature diagnosis, "
        "frozen coverage, causal traces and blocked draft amendment in this directory.", "",
    ])


def check_report(directory, manifest):
    completed = read_json(directory / "complete.json")
    if completed["manifest_sha256"] != manifest["manifest_sha256"]:
        raise ValueError("Phase 6 completion points at a different manifest")
    for name, sha in completed["files"].items():
        if file_hash(directory / name) != sha:
            raise ValueError(f"Completed Phase 6 evidence changed: {name}")


def run():
    directory = freeze()
    manifest = read_manifest(directory)
    with writer_lock(OUTPUT / ".writer.lock"):
        if (directory / "complete.json").exists():
            check_report(directory, manifest)
            return directory
        plan, phase3, phase4, phase5, _ = origin()
        for name, folder in (("phase3", phase3), ("phase4", phase4), ("phase5", phase5)):
            if file_hash(folder / "complete.json") != manifest[f"{name}_completion_sha256"]:
                raise ValueError(f"{name} completion snapshot changed")
        previous_path = phase5.parent / "verification.json"
        if file_hash(previous_path) != manifest["phase5_verification_sha256"]:
            raise ValueError("Phase 5 verification snapshot changed")
        prior = read_json(previous_path)["prospective"]
        table, details, risk = reconcile(manifest, plan, phase4, phase5)
        feature = diagnose(manifest, phase5)
        observations = snapshot(manifest, utc(manifest["observation_as_of_utc"]))
        records = {"risk-summary.json": risk, "risk-detail.json": details, "swing-feature-block.json": feature,
                   "coverage.json": observations, "causal-traces.json": causal_examples(observations),
                   "draft-handoff.json": draft_handoff(manifest)}
        for name, record in records.items():
            write_json(directory / name, record)
        put_bytes(directory / "risk-all-trades.csv", table.to_csv(index=False).encode("utf-8"))
        put_bytes(directory / "SUMMARY.md", markdown(manifest, risk, feature, observations, prior).encode("utf-8"))
        names = (*records, "risk-all-trades.csv", "SUMMARY.md")
        code = Path(__file__).parent.glob("*.py")
        write_json(directory / "complete.json", {"manifest_sha256": manifest["manifest_sha256"],
                                                 "files": {name: file_hash(directory / name) for name in names},
                                                 "audit_code_sha256": {file.name: file_hash(file) for file in sorted(code)}})
        return directory
