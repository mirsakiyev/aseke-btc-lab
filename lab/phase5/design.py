from pathlib import Path

from lab.config import Execution, fingerprint
from lab.phase4.config import load_config
from lab.phase4.protocol import readiness
from lab.phase4.sensitivity import OUTPUT as PHASE4, PHASE3_ATTEMPT, PHASE3_ID, source
from lab.storage import RUNS, file_hash, read_json, utc, utc_now, write_json, writer_lock

LABEL = "Coinbase BTC-USD historical research with a hypothetical published-MEXC-fee scenario."
VERSION = "coinbase-swing-robustness-v1"
OUTPUT = RUNS / "phase5"
PHASE4_ID = "swing-c999cacc99024b"
ANNUAL_DAYS = 365.2425


def checked_completion(path: Path) -> dict:
    complete = read_json(path / "complete.json")
    for name, digest in complete["files"].items():
        if file_hash(path / name) != digest:
            raise ValueError(f"Completed artifact changed: {path / name}")
    return complete


def sources():
    plan, prior, phase3 = source()
    checked_completion(phase3)
    phase4 = PHASE4 / PHASE4_ID
    checked_completion(phase4)
    manifest = read_json(phase4 / "manifest.json")
    report = read_json(phase4 / "results.json")
    if manifest["phase3_design_sha256"] != plan["design_sha256"] or (
        manifest["phase3_completion_sha256"] != file_hash(phase3 / "complete.json")
    ):
        raise ValueError("Phase 4 reference differs from the pinned Phase 3 checkpoint")
    return plan, prior, phase3, manifest, report, phase4


def freeze(root: Path = OUTPUT) -> Path:
    plan, prior, phase3, phase4_manifest, report, phase4 = sources()
    phase4_config = load_config()
    ref = Execution(**phase4_manifest["reference_execution"])
    low = Execution(**phase4_manifest["counterfactual_execution"])
    if (ref.fee_bps, ref.proxy_bps, low.fee_bps, low.proxy_bps) != (50, 5, 5, 5):
        raise ValueError("The preserved reference or lower-fee assumption is different")
    if low.model_dump(exclude={"fee_bps"}) != ref.model_dump(exclude={"fee_bps"}):
        raise ValueError("The prior lower-fee replay changed more than its fee")
    previous = prior["scenarios"]["reference"]
    start, end = (utc(previous[key]) for key in ("account_start", "requested_end"))
    if end != utc(plan["windows"]["holdout_start"]) or not report["comparison"]["full_match"]:
        raise ValueError("Unexpected preserved evaluation boundary or validity")
    record = {
        "label": LABEL, "version": VERSION, "account_currency": "USD", "price_venue": "coinbase_exchange",
        "instrument": "BTC-USD", "profile": plan["profile"], "profile_hash": plan["profile_hash"],
        "phase3_run": PHASE3_ID, "phase3_attempt": PHASE3_ATTEMPT,
        "phase3_design_sha256": plan["design_sha256"], "phase3_completion_sha256": file_hash(phase3 / "complete.json"),
        "phase4_run": PHASE4_ID, "phase4_design_sha256": phase4_manifest["design_sha256"],
        "phase4_completion_sha256": file_hash(phase4 / "complete.json"),
        "price_snapshot": plan["source_snapshot"], "strategy_execution_logic_sha256": plan["phase1_logic_sha256"],
        "dependencies": plan["dependencies"], "mexc_config_sha256": phase4_config.identity,
        "mexc_state": readiness(phase4_config), "holdout": plan["windows"],
        "requested_start_utc": start.isoformat(), "requested_end_utc": end.isoformat(),
        "requested_calendar_seconds": (end - start).total_seconds(),
        "frozen_windows": [{**row, "entry_eligible": next(p["fit_state"] == "fitted" for p in prior["folds"]
                                                        if p["id"] == row["id"])} for row in plan["folds"]],
        "costs": {
            "reference": ref.model_dump(), "fixed_ledger": low.model_dump(), "lower_fee": low.model_dump(),
            "proxy_10bp": {**low.model_dump(), "proxy_bps": 10},
            "extra_hour": {**low.model_dump(), "delay_bars": low.delay_bars + 1},
        },
        "execution_stress_budget": {"maximum_new_replays": 2, "cases": ["proxy_10bp", "extra_hour"],
                                    "reference_fixed_and_lower_fee": "reuse checksummed Phase 3 and Phase 4 ledgers"},
        "timing": "Same signal and availability. Added delay is one 60-minute bar after the original strict next-open rule. "
                  "Sizing uses only decision close, stop, equity and scenario costs.",
        "periods": "Use preserved Phase 3 windows and UTC calendar years, partial at the ends. "
                   "First insufficient window remains excluded. Comparison ends at the common last valid mark.",
        "annualization": {"calendar_days_per_year": ANNUAL_DAYS,
                          "rule": "Geometric from elapsed UTC calendar days, full scheduled continuous comparison only."},
        "equity": "Observed hourly closes; no interpolation through missing candles. Include cash and BTC mark. "
                  "Gap during exposure halts that path. No liquidation assumed at the sealed holdout boundary.",
        "benchmarks": "Zero yield cash and the existing 100% and 25% buy-once BTC allocations. "
                      "Use each case's fee and proxy, no rebalancing, same terminal mark. Fixed case retains original units.",
        "attribution": "Gross is change in marked equity plus fees plus adverse price drag. Break-even holds each "
                       "ledger and proxy fixed. Separate lower fees, cost-dependent sizing and equity compounding using "
                       "the identical Phase 4 trade identities. Top-winner deletion is arithmetic only.",
        "metrics": [
            "calendar and marked coverage", "net and annualized return", "mark-to-market drawdown", "gross/fees/drag",
            "average and peak marked BTC weight", "time exposure", "entry notional", "turnover", "holding",
            "open and closed positions", "win/loss magnitudes", "calendar and window concentration",
        ],
        "observation": "Read-only live journal and receipt lookback. Respect the pinned modeled-open cutoff. "
                       "Use original activation, schedule, receipt and decision timestamps. No new latency SLA.",
        "holdout_performance_revealed": False, "models_loaded_or_fitted": False, "approved": False,
    }
    design_hash = fingerprint(record)
    folder = root / f"swing-{design_hash[:14]}"
    with writer_lock(root / ".writer.lock"):
        pointer = root / "frozen.json"
        if pointer.exists() and read_json(pointer)["directory"] != folder.name:
            raise ValueError("Phase 5 already has a different frozen design")
        path = folder / "manifest.json"
        if path.exists():
            existing = read_json(path)
            if existing["design_sha256"] != design_hash or any(existing[key] != value for key, value in record.items()):
                raise ValueError("A frozen Phase 5 manifest changed")
        else:
            write_json(path, {**record, "design_sha256": design_hash, "frozen_at_utc": utc_now().isoformat()})
        if not pointer.exists():
            write_json(pointer, {"label": LABEL, "directory": folder.name})
    return folder
