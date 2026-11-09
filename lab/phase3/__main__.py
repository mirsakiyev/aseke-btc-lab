import argparse
import json
import os
import re

for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ[key] = "1"

from lab.phase3.design import OUTPUT, freeze
from lab.phase3.observation import health
from lab.storage import read_json


def main():
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Phase 3")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("freeze")
    actions.add_parser("health")
    for name in ("run", "report"):
        actions.add_parser(name).add_argument("--run-id")
    args = parser.parse_args()
    if args.action == "health":
        print(json.dumps(health(), indent=2, default=str, allow_nan=False))
        return
    if args.action == "freeze":
        print(freeze())
        return
    if args.run_id and not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{10}", args.run_id):
        parser.error("Invalid Phase 3 run ID")
    if args.action == "run":
        from lab.phase3.study import run
        directory = OUTPUT / args.run_id if args.run_id else freeze()
        attempt = run(directory)
        print(attempt / "SUMMARY.md")
    else:
        if args.run_id:
            candidates = sorted((OUTPUT / args.run_id).glob("attempt-*/complete.json"))
            if not candidates:
                parser.error("No completed attempt for this run")
            attempt = candidates[-1].parent
        else:
            latest = read_json(OUTPUT / "latest.json")
            attempt = OUTPUT / latest["directory"] / latest["attempt"]
        verified = attempt.parent / "SUMMARY.md"
        print((verified if verified.is_file() else attempt / "SUMMARY.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
