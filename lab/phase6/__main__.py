import argparse
import json
import os

for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ[key] = "1"

from lab.phase6.coverage import current_coverage
from lab.phase6.design import freeze, read_manifest
from lab.phase6.report import check_report, coverage_text, run
from lab.storage import read_json


def main():
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Phase 6")
    actions = parser.add_subparsers(dest="action", required=True)
    for name in ("freeze", "run", "report"):
        actions.add_parser(name)
    coverage = actions.add_parser("coverage")
    coverage.add_argument("--json", action="store_true")
    coverage.add_argument("--decision-days", type=int, choices=(1, 7), default=1)
    args = parser.parse_args()
    if args.action == "run":
        print(run() / "SUMMARY.md")
    else:
        directory = freeze()
        manifest = read_manifest(directory)
        if args.action == "freeze":
            print(directory)
        elif args.action == "report":
            check_report(directory, manifest)
            print((directory / "SUMMARY.md").read_text(encoding="utf-8"))
            if (restart := directory / "worker-restart.json").exists():
                audit = read_json(restart)
                print(f"Recorded worker restart {audit['verified_at_utc']}. "
                      f"PID {audit['before']['pid']} → {audit['after']['pid']}. Details in {restart}.")
        else:
            data = current_coverage(manifest, decision_days=args.decision_days)
            print(json.dumps(data, indent=2, default=str, allow_nan=False) if args.json else coverage_text(data))


if __name__ == "__main__":
    main()
