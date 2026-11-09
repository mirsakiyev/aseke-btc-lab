import argparse
import os

for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[name] = "2"

import json
from pathlib import Path

from lab.phase9.data import import_data, metadata, publish_history, read_import_json
from lab.phase9.ideas import decide, observe, user_execution
from lab.phase9.public import collect
from lab.phase9.reference import NAMES, set_control
from lab.phase9.report import render, research_gate, snapshot
from lab.phase9.research import fit_worker, run_bundle
from lab.storage import read_json


def main():
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Phase 9")
    actions = parser.add_subparsers(dest="action", required=True)
    for name in ("status", "report"):
        actions.add_parser(name)
    for name in ("monitor", "training", "ideas"):
        action = actions.add_parser(name)
        if name == "ideas":
            action.add_argument("profile", choices=NAMES)
        action.add_argument("state", choices=("on", "off"))
    fetch = actions.add_parser("collect")
    fetch.add_argument("kind", nargs="?", choices=("metadata", "probe", "extended", "current", "quotes", "history"))
    fetch.add_argument("--resume")
    fetch.add_argument("--profile", choices=NAMES)
    fetch.add_argument("--basis", choices=("trade", "mark", "index", "funding"))
    fetch.add_argument("--start")
    fetch.add_argument("--end")
    local = actions.add_parser("import")
    local.add_argument("--file", required=True, type=Path)
    local.add_argument("--descriptor", required=True, type=Path)
    run = actions.add_parser("run-once")
    run.add_argument("--profile", choices=NAMES)
    run.add_argument("--bundle", type=Path)
    decision = actions.add_parser("decide")
    decision.add_argument("--profile", choices=NAMES)
    decision.add_argument("--collect", action="store_true")
    actions.add_parser("observe").add_argument("--collect", action="store_true")
    manual = actions.add_parser("record-execution")
    manual.add_argument("--idea", required=True)
    manual.add_argument("--file", required=True, type=Path)
    actions.add_parser("_study").add_argument("job")
    args = parser.parse_args()

    if args.action == "status":
        result = snapshot()
    elif args.action == "report":
        print(render().read_text(encoding="utf-8"))
        return
    elif args.action in ("monitor", "training", "ideas"):
        result = set_control(args.action, args.state == "on", getattr(args, "profile", None))
    elif args.action == "collect":
        historical = None
        if args.kind == "history" and not args.resume:
            historical = {name: getattr(args, name) for name in ("profile", "basis", "start", "end")}
            if not all(historical.values()):
                parser.error("history requires --profile, --basis, --start and --end")
        if not args.kind and not args.resume:
            parser.error("collect requires a kind or --resume")
        folder = collect(args.kind, resume=args.resume, historical=historical)
        kind = read_json(folder / "plan.json")["design"]["kind"]
        result = publish_history(folder) if kind == "history" else metadata(folder) if kind == "metadata" else {"plan": str(folder)}
    elif args.action == "import":
        result = import_data(args.file, args.descriptor)
    elif args.action == "run-once":
        if bool(args.profile) != bool(args.bundle):
            parser.error("Provide --profile and --bundle together")
        result = run_bundle(args.profile, args.bundle) if args.bundle else research_gate()
    elif args.action == "decide":
        result = decide([args.profile] if args.profile else NAMES, collect("current") if args.collect else None)
    elif args.action == "observe":
        result = observe(collect("quotes") if args.collect else None)
    elif args.action == "record-execution":
        result = user_execution(args.idea, read_import_json(args.file))
    else:
        fit_worker(args.job)
        return
    print(json.dumps(result, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
