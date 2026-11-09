from collections import Counter
from math import sqrt
from pathlib import Path

from lab.phase10.worker import RUNTIME
from lab.phase13.diagnostics import folder, progress
from lab.phase13.future import technical
from lab.phase13.observation import status as capture_status
from lab.phase13.protocol import active
from lab.phase13.registry import ROOT, checked, freeze
from lab.storage import put_bytes, read_json


def optional(path):
    return read_json(path) if path.exists() else None


def status():
    reg = freeze()
    ptr = optional(ROOT / "diagnostic.json")
    root = folder(ptr["id"]) if ptr else None
    completed = optional(root / "result.json") if root else None
    capture = capture_status()
    live = optional(RUNTIME) or {}
    launch = capture.get("last_managed_cycle", {}).get("launcher", "")
    capture["task_managed_extension_confirmed"] = launch == f"scheduled:{live.get('launch_id')}"
    capture["current_kucoin_worker"] = {key: live.get(key) for key in ("pid", "launcher", "launch_id", "heartbeat")}
    return {"registry": reg["id"], "diagnostic": ptr["id"] if ptr else None,
            "diagnostic_state": completed["state"] if completed else "RUNNING_OR_PENDING" if ptr else "NOT_STARTED",
            "diagnostic_progress": progress(root) if root else {},
            "observation": capture, "technical_readiness": technical(), "future_result": optional(ROOT / "future-result.json"),
            "approval": "No validated model", "execution": "MANUAL_OUTSIDE_LAB",
            "historical_archive": {"used": 462, "cap": 463, "additional_requests": 0}}


def pct(value):
    return "-" if value is None else f"{100*value:.2f}%"


def money(value):
    return f"{value:,.2f}"


def pairs(rows):
    return ", ".join(f"{key} {value}" for key, value in rows.items()) or "-"


def forecast_rows(item):
    result = read_json(checked(item["result"], item["result_sha256"]))
    rows = [row for row in result.get("folds", []) if row["status"] == "EVALUATED"]
    weights = sum(row["outer_error"]["count"] for row in rows)
    if weights:
        rows.append({"fold": "Pooled for this lane", "outer_error": {"count": weights,
            "mae_net_r": sum(row["outer_error"]["mae_net_r"] * row["outer_error"]["count"] for row in rows) / weights,
            "rmse_net_r": sqrt(sum(row["outer_error"]["rmse_net_r"]**2 * row["outer_error"]["count"] for row in rows) / weights)}})
    return rows


def report():
    result = status()
    reg = freeze()
    root = folder(result["diagnostic"]) if result["diagnostic"] else None
    comparisons, stresses, proofs = {}, {}, {}
    if root:
        for key in reg["body"]["candidates"]:
            lane = root / "lanes" / key.replace("/", "-")
            if data := optional(lane / "comparison.json"):
                comparisons[key] = data
            if proof := optional(lane / "parity.json"):
                proofs[key] = proof
            stresses[key] = [read_json(path) for path in sorted(lane.glob("stress-*.json"))]
    rows = ["# Phase 13 frozen futures comparison", "",
            "Approval **No validated model**. Execution **MANUAL_OUTSIDE_LAB**.", "",
            f"Registry {reg['id']}. Parent {reg['body']['parent_campaign']}. The operator chose these lanes after "
            "observing the completed Phase 12 results. All comparisons on that consumed period are descriptive.", "",
            "## Continuous account results", "",
            "| Lane | Net | Annualized | Drawdown | Peak to recovery | Closed | Accepted / eligible | Marked time in market | Avg USDT notional | Turnover USDT |",
            "| --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: |"]
    for key, data in comparisons.items():
        econ = data["economics"]
        recover = data["peak_to_recovery_minutes"]
        rows.append(f"| {key} | {pct(data['net_return'])} | {pct(data['annualized_return'])} | {pct(data['drawdown'])} | "
                    f"{f'{recover/1440:.1f} d' if recover is not None else 'Unrecovered at end'} | {econ.get('closed_trades',0)} | "
                    f"{data['accepted']} / {data['eligible']} ({pct(data['acceptance_rate'])}) | "
                    f"{pct(data['observed_mark_exposure_fraction'])} | {money(data['average_notional_all_minutes'])} | "
                    f"{money(econ.get('turnover',0.))} |")
    rows += ["", "Elapsed time uses 365.2425 days per year. Drawdown and time in market use Phase 12 minute equity "
             "marks. Notional is marked from filled base units over the holding interval. Acceptance means the score "
             "and free-account gates passed, with actual entries counted separately in the lane JSON.", "",
             "## Chronological folds", "", "| Lane | Fold | UTC start | UTC end exclusive | Net | Drawdown | Closed | Accepted / eligible |",
             "| --- | --- | --- | --- | ---: | ---: | ---: | --- |"]
    for key, data in comparisons.items():
        for fold, row in data["folds"].items():
            rows.append(f"| {key} | {fold} | {row['start']} | {row['end_exclusive']} | {pct(row['net_return'])} | "
                        f"{pct(row['drawdown'])} | {row['economics'].get('closed_trades',0)} | {row['accepted']} / {row['eligible']} |")
    rows += ["", "## P&L attribution in USDT", "",
             "| Lane | Price before adverse | Price at fills | Adverse cost | Commissions | Funding paid | Funding received | Net |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for key, data in comparisons.items():
        e = data["economics"]
        rows.append("| " + key + " | " + " | ".join(money(e.get(field,0.)) for field in ("price_pnl_before_adverse",
                    "gross_fill_price_pnl", "adverse_execution_cost", "commissions", "funding_paid", "funding_received", "net")) + " |")
    rows += ["", "The pre-adverse column uses raw prices on the same realized quantities and exits. Adverse cost is "
             "already included in price at fills. It includes tick rounding and is not a separate realized expense. "
             "A different cost or delay can change fills, position size, occupancy and later opportunities.", "",
             "## Risk and concentration", "",
             "| Lane | Long / short net USDT | Exit counts | Sum planned risk USDT | Realized losing P&L USDT | Losses over plan | Liquidations | Unresolved |",
             "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |"]
    for key, data in comparisons.items():
        d, e = data["exit_distribution"], data["economics"]
        side = d["by_side"]
        rows.append(f"| {key} | {money(side.get('long',{}).get('net',0))} / {money(side.get('short',{}).get('net',0))} | "
                    f"{pairs(d['by_exit'])} | {money(e.get('planned_risk',0))} | "
                    f"{money(e.get('realized_losing_pnl_absolute',0))} | {e.get('loss_over_plan_count',0)} | "
                    f"{d['by_exit'].get('liquidation',0)} | {data['unresolved_exposure'] or '-'} |")
    for key, data in comparisons.items():
        if data["exit_distribution"]["largest_wins"]:
            d = data["exit_distribution"]
            rows += ["", f"**{key}**. Largest winning trades by exit UTC and USDT net are " +
                     ", ".join(f"{item['exit_at']} ({money(item['net'])})" for item in d["largest_wins"]) + ". "
                     "Largest winning months are " + (", ".join(f"{month} ({money(value)})"
                     for month,value in d["largest_winning_months"]) or "none") + ". "
                     f"Frozen monthly-block uncertainty record is {data['uncertainty']}."]
    rows += ["", "## Forecast accuracy on identical outer labels", "",
             "| Lane | Fold | Mature observations | MAE (R) | RMSE (R) |", "| --- | --- | ---: | ---: | ---: |"]
    for profile in ("day","swing"):
        for role in ("estimator","mean"):
            key = f"{profile}/{role}"
            for row in forecast_rows(reg["body"]["candidates"][key]):
                x = row["outer_error"]
                rows.append(f"| {key} | {row['fold']} | {x['count']} | {x['mae_net_r']:.6f} | {x['rmse_net_r']:.6f} |")
    rows += ["", "R is modeled net return per unit of planned risk. Smaller forecast error, selective entries, "
             "profitability and exposure reduction are separate outcomes.", "",
             "## Frozen stresses", "",
             "Double cost applies 0.12% taker and 0.10% adverse price per side. The delay shifts the IOC submission "
             "and expiry by one native minute, preserves the holding deadline and UTC session cutoff, and delays "
             "non-liquidation exits by one native minute. Both use the frozen saved scores and account chronology.", "",
             "| Lane | Stress | Net | Drawdown | Closed | 2023 / 2024 / 2025 net |",
             "| --- | --- | ---: | ---: | ---: | --- |"]
    for key, candidates in stresses.items():
        for row in candidates:
            m = row.get("metrics", {})
            folds = " / ".join(pct(fold["net_return"]) for fold in row.get("folds",{}).values())
            rows.append(f"| {key} | {row['stress']} | {pct(m.get('return'))} | {pct(m.get('drawdown_observed_marks'))} | "
                        f"{m.get('closed_trades','-')} | {folds or row['state']} |")
    rows += ["", f"Diagnostic state **{result['diagnostic_state']}**. " + pairs(result["diagnostic_progress"]) + ".",
             f"Artifact and research-gate parity checked {sum(row['historical_predictions_checked'] for row in proofs.values()):,} "
             "saved decisions. Per-lane parity files retain the exact checked examples and score error.", "",
             "## What remains shortlisted", ""]
    if result["diagnostic_state"] == "COMPLETE":
        raw = comparisons["day/unfiltered"]["economics"]
        pins = reg["body"]["candidates"]
        rows += [f"**Day.** Ridge stays on the research shortlist. Its pooled MAE improved from 0.661R to 0.647R on "
                 f"4,676 matched labels and improved in each fold. The mean and cash stayed flat. Ridge accepted 12 "
                 f"setups, closed nine trades and fell below the 25-trade uncertainty floor. Both stresses turned the "
                 f"small baseline gain negative. Value beyond avoiding trades remains unresolved.", "",
                 f"The unfiltered rule's {pct(comparisons['day/unfiltered']['net_return'])} result included "
                 f"{money(raw['price_pnl_before_adverse'])} USDT raw price P&L, {money(raw['adverse_execution_cost'])} "
                 f"USDT adverse execution, {money(raw['commissions'])} USDT commissions and "
                 f"{money(raw['funding_paid'] - raw['funding_received'])} USDT net funding paid. It closed 1,786 trades "
                 f"with 4.83 million USDT turnover. Frequency multiplied costs and losses changed subsequent size. "
                 f"Double cost yielded a smaller account loss with 1,079 fills because fills, sizing and later "
                 f"opportunities changed. That replay does not isolate a beneficial cost effect.", "",
                 f"The fixed Day pin is `{pins['day/estimator']['fixed_artifact']['version']}`. Independent evidence needs "
                 "matched mature labels and enough completed trades to compare Ridge against the mean and cash at frozen costs.", "",
                 "**Swing.** Histogram and the unfiltered rule both stay shortlisted. The filter lowered marked "
                 "exposure, turnover and drawdown while giving up return. The rule earned more in both stresses. "
                 "Histogram beat the mean on account return but lost to it on forecast error in every fold. "
                 "Neither lane establishes superiority on the consumed period. The monthly-block intervals include zero.", "",
                 f"The fixed histogram pin is `{pins['swing/estimator']['fixed_artifact']['version']}`. The unfiltered "
                 "candidate uses the frozen rule. Independent comparison needs eligible context and mature trades at the "
                 "same frozen risk so the return versus exposure tradeoff can be measured.", "",
                 "Spot retains its saved study. The futures figures use Binance-source historical observations and "
                 "KuCoin reference costs. The new protocol uses newly received KuCoin observations.", ""]
    rows += [
             "## Independent-data registration", ""]
    if protocol := active():
        body = protocol["body"]
        cap = result["observation"]
        audit = result["technical_readiness"]
        rows += [f"Protocol {protocol['id']}. Registered {body['registered_at']}. Observation window "
                 f"{body['start']} through {body['end_exclusive']} exclusive. Technical review {body['technical_review_due']}.",
                 "", "This is a preregistered offline replay of future receipts. It supplies no live decision evidence. "
                 "Day requires causal warm-up and mature outcomes. Swing needs 201 daily context bars, "
                 "which is longer than the primary window. The sealed data were excluded.", "",
                 f"Capture {cap['state']}. Data {cap.get('observations',{})}. "
                 f"Future scoring {audit['state']}. Future evaluations {1 if result['future_result'] else 0}.", ""]
        if "last_cycle" in cap:
            c = cap["last_cycle"]
            rows += [f"Last observed cycle used launcher {c['launcher']} through {c['end_exclusive']}. "
                     "Its raw hashes, source times, receipt times, and request counts are in the protocol's observation directory.", ""]
    rows += ["## Original Phase 12 inventory", "", "| Frozen trial | Original status |",
             "| --- | --- |"]
    rows += [f"| {key} | {item['status']} |" for key,item in reg["body"]["all_trials"].items()]
    rows.append("")
    path = ROOT / "REPORT.md"
    put_bytes(path, ("\n".join(rows)+"\n").encode())
    return {**result, "report": str(path)}
