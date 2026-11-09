from lab.learning.state import status as learning_status
from lab.phase7.operation import health
from lab.phase9.data import inspect_batch, metadata
from lab.phase9.ideas import recent
from lab.phase9.public import latest
from lab.phase9.reference import NAMES, OUTPUT, controls, freeze_reference, parent_design, reference
from lab.storage import put_bytes, read_json, utc_now, write_json


def research_gate():
    cfg, parent, rules = reference(), parent_design(), metadata()
    probes = {}
    for kind in ("probe", "extended"):
        if folder := latest(kind):
            probes[kind] = {"plan": folder.name, "rows": inspect_batch(folder, rules)[0]}
    settings = parent["design"]["settings"]
    result = {
        "reference_id": freeze_reference(cfg), "parent_manifest": parent["id"], "as_of": utc_now().isoformat(),
        "scope": "PUBLIC_DATA_CAPABILITY_PROBE", "actual_fits": 0, "eligible_evaluation_windows": [],
        "holdout": parent["design"]["holdout"], "probes": probes, "folds": parent["design"]["folds"],
        "sample_floors": {"ridge_training": settings["models"]["ridge_v1"]["minimum_train"],
                          "hist_training": settings["models"]["hist_v1"]["minimum_train"],
                          "inner": settings["evaluation"]["minimum_inner"], "outer": settings["evaluation"]["minimum_outer"]},
        "profiles": {
            "position": {"status": "BLOCKED_DATA", "needed": "Import qualifying BTC-USDT hourly trade coverage through "
                         "the frozen train, inner and outer windows. A short spot probe verifies availability only."},
            **{name: {"status": "BLOCKED_DATA", "needed": "Import aligned XBTUSDTM minute trade and mark history "
                      "from before the frozen inner start, plus all dated funding settlements and an explicit rule scenario."}
               for name in ("swing", "day")},
        },
        "official_archive": {"url": "https://www.kucoin.com/markets/historydata", "advertises": "Spot candles, trades and depth",
                             "futures_mark_and_funding_archives_verified": False},
        "imported_assets": len(list((OUTPUT / "imports").glob("*/manifest.json"))),
        "readiness": "This is the public-probe readiness result. Named import bundles get separate per-fold checks.",
    }
    for path in sorted((OUTPUT / "campaigns").glob("*/result.json")):
        saved = read_json(path)
        profile = read_json(path.parent / "manifest.json")["body"]["profile"]
        result["profiles"][profile] = {
            "status": "EVALUATED_RESEARCH", "scope": saved["scope"], "campaign": saved["campaign"],
            "saved_fit_count": saved["actual_fits"], "forecast_evaluation_windows": saved["forecast_evaluation_windows"],
            "rule_evaluation_windows": saved["rule_evaluation_windows"],
        }
    write_json(OUTPUT / "research-readiness.json", result)
    return result


def snapshot():
    cfg, observer, training = reference(), health(), learning_status()
    return {
        "reference": cfg["version"], "venue": cfg["reference_venue"], "access_report": cfg["access_report"],
        "eligibility": cfg["execution_eligibility"], "personal_fees": cfg["personal_fees"], "execution_mode": cfg["execution_mode"],
        "validation": cfg["validation"], "controls": controls(), "historical_finding": "INCONCLUSIVE",
        "monitoring": {"mode": "ON_DEMAND_PUBLIC_BATCHES", "unattended": False},
        "training": {"legacy_state": training["state"], "legacy_campaigns": training["campaigns"],
                     "legacy_candidate_attempts": training["cumulative_candidate_attempts"],
                     "legacy_fit_attempts": training["cumulative_fit_attempts"],
                     "kucoin_campaigns": len(list((OUTPUT / "campaigns").glob("*/manifest.json"))),
                     "kucoin_candidate_attempts": len(list((OUTPUT / "campaigns").glob("*/attempts/*/*.json"))),
                     "kucoin_fit_attempts": len(list((OUTPUT / "campaigns").glob("*/fits/*.json"))),
                     "mode": "OPERATOR_REQUESTED_BOUNDED_RUNS"},
        "observer": {"pid": observer["worker"]["pid"], "launcher": observer["worker"].get("launcher"),
                     "state": observer["process_state"], "heartbeat": observer["heartbeat_state"],
                     "experiment": observer["experiment_id"], "lock_held": observer["worker_lock_held"]},
        "last_acquisition": {kind: str(path) for kind in ("metadata", "probe", "extended", "current", "quotes", "history")
                             if (path := latest(kind))},
        "imported_assets": [read_json(path) for path in sorted((OUTPUT / "imports").glob("*/manifest.json"))],
        "ideas": {name: recent(name)[:1] for name in NAMES},
    }


def render():
    state, rules, research = snapshot(), metadata(), research_gate()
    current = latest("current")
    current_evidence = inspect_batch(current, rules)[0] if current else []
    campaigns = [read_json(path) for path in sorted((OUTPUT / "campaigns").glob("*/result.json"))]
    write_json(OUTPUT / "report.json", {"state": state, "metadata": rules, "research": research,
                                       "campaigns": campaigns, "current_acquisition": current_evidence})
    lines = ["# KuCoin reference and manual ideas", "",
             f"Execution mode **{state['execution_mode']}**. Approved lane **{state['validation']}**.", "",
             "## Observed current rules and fee references", ""]
    if spot := rules.get("spot"):
        lines.append(f"- BTC-USDT spot is {spot['feeCategory']}, with {spot['baseOrderStep']} BTC quantity increments "
                     f"and a {spot['tickSize']} USDT price tick.")
    if perp := rules.get("contract"):
        lines.append(f"- XBTUSDTM settles in {perp['settleCurrency']}. Each contract represents {perp['multiplier']} BTC. "
                     f"Lot size {perp['lotSize']}, tick {perp['tickSize']} USDT, max market size "
                     f"{perp['marketMaxOrderQty']} contracts, venue max leverage {perp['maxLeverage']}x.")
    if tier := next((item for item in rules.get("isolated", []) if item["level"] == 1), None):
        lines.append(f"- Observed first isolated tier tops out at {tier['maxRiskLimit']:,} USDT, with "
                     f"{tier['initialMargin']:.2%} initial and {tier['maintainMargin']:.2%} maintenance margin.")
    lines.extend(["- Published VIP 0 decimals are 0.001/0.001 maker/taker for Class A spot and 0.0002/0.0006 for futures. "
                  "The primary scenario charges taker both sides and applies a separate 0.0005 adverse-price proxy. "
                  "Personal fees are unverified.", "",
                  "## Development coverage", "",
                  "The preserved BTC embargo begins at 2025-11-08 00:00 UTC. The latest frozen fold still requires "
                  "training before its 2024-07-01 inner start.", "",
                  "| Sample | Rows | First UTC timestamp | Result |", "| --- | ---: | --- | --- |"])
    for probe in research["probes"].values():
        for row in probe["rows"]:
            lines.append(f"| {row['key']} | {row.get('rows', '—')} | {row.get('first') or '—'} | {row['status']} |")
    lines.extend(["", "The bounded probes alone established zero eligible historical windows and zero fits. "
                  "The capability record lists the data required for each profile.", ""])
    if campaigns:
        lines.extend(["## Completed research campaigns", "",
                      "| Campaign | Scope | Fits | Forecast-qualified outer windows | Trial outcomes |",
                      "| --- | --- | ---: | --- | --- |"])
        for item in campaigns:
            windows = ", ".join(item["forecast_evaluation_windows"]) or "None"
            lines.append(f"| {item['campaign'][:16]} | {item['scope']} | {item['actual_fits']} | {windows} | {item['trials']} |")
            root = OUTPUT / "campaigns" / item["campaign"]
            if (root / "lineage-erratum.json").exists():
                lines.append("")
                lines.append("A linked lineage erratum records the KuCoin instrument and fee fields used by this campaign.")
            lines.extend(["", "| Strategy | Exit | Model | Status | Return | Drawdown | Closes |",
                          "| --- | --- | --- | --- | ---: | ---: | ---: |"])
            trials = [read_json(path) for path in (root / "trials").glob("*.json")]
            for row in sorted(trials, key=lambda value: value["trial"]["id"]):
                trial, metrics = row["trial"], row.get("metrics", {})
                value, drawdown = metrics.get("return"), metrics.get("drawdown_observed_marks")
                ret = f"{value:+.2%}" if value is not None else "—"
                dd = f"{drawdown:.2%}" if drawdown is not None else "—"
                lines.append(f"| {trial.get('entry', '—')} | {trial.get('exit_policy', '—')} | {trial['model']} | "
                             f"{row['status']} | {ret} | {dd} | {metrics.get('closed_trades', '—')} |")
        lines.append("")
    lines.extend([
                  "## Prospective decisions", "", "| Profile | Decision UTC | State | Reason |", "| --- | --- | --- | --- |"])
    for name, items in state["ideas"].items():
        for item in items:
            lines.append(f"| {name} | {item['decision_at']} | {item['market_state']} | {item['reason']} |")
    lines.extend(["", "The Coinbase observer remains a separate scheduled process. KuCoin collection and ideas run "
                  "on demand. Conditions, simulations, and user-reported execution have separate evidence.", ""])
    path = OUTPUT / "REPORT.md"
    put_bytes(path, "\n".join(lines).encode("utf-8"))
    return path
