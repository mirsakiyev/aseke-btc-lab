import argparse
import json
import os

for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ[key] = "1"

from lab.phase6.coverage import snapshot
from lab.phase6.design import OUTPUT, read_manifest
from lab.phase6.report import check_report, coverage_text
from lab.phase7.observation import observations
from lab.phase7.operation import checkpoint, health
from lab.storage import read_json, utc_now


def inspection(days):
    directory = OUTPUT / read_json(OUTPUT / "frozen.json")["directory"]
    manifest = read_manifest(directory)
    check_report(directory, manifest)
    now = utc_now()
    return {"coverage": snapshot(manifest, now, decision_days=days), "diagnostics": observations(directory, manifest, now)}


def observation_text(result):
    data = result["diagnostics"]
    swing = data["swing_warmup"]
    lines = [coverage_text(result["coverage"]), "", "### Swing warmup", "",
             f"{swing['consecutive_daily_bars']}/{swing['required_daily_bars']} consecutive completed daily bars. "
             f"{swing['daily_bars_remaining']} remain. Native source {swing['source']}. "
             f"Latest invalid daily interval {swing['latest_incomplete_daily_open']} to {swing['latest_incomplete_daily_close']}.",
             f"Conditional readiness {swing['conditional_readiness_utc']} or {swing['conditional_readiness_toronto']} Toronto. "
             "A new scheduled record must confirm readiness.",
             "Swing also needs a complete 4-hour bucket, prior range windows, ATR and a positive stop. The pinned Ridge filter "
             "requires finite momentum, relative volume, ATR fraction, breakout distance and the daily spread.", "",
             "### Actual instrumented decisions", "",
             "| Profile | New records | Latest outcome | Verification | Next scheduled close (UTC) |",
             "| --- | ---: | --- | --- | --- |"]
    for name, row in data["profiles"].items():
        last = row["latest_saved_decision"] or {}
        lines.append(f"| {name} | {row['new_decision_count']} | {last.get('decision_outcome', 'none')} | "
                     f"{row['verification']} | {row['next_decision_utc']} |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Phase 7")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("health")
    state = actions.add_parser("checkpoint")
    state.add_argument("--watermark", type=int)
    for command in ("coverage", "weekly"):
        actions.add_parser(command).add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.action == "health":
        result = health()
    elif args.action == "checkpoint":
        result = checkpoint(args.watermark)
    else:
        result = inspection(7 if args.action == "weekly" else 1)
        if not args.json:
            print(observation_text(result))
            return
    print(json.dumps(result, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
