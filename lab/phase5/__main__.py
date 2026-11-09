import argparse
import json
import os

for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ[key] = "1"

from lab.phase5.design import OUTPUT, LABEL, freeze
from lab.phase5.observations import prospective_report
from lab.phase5.study import observation_text, run
from lab.storage import read_json


def main():
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Phase 5")
    actions = parser.add_subparsers(dest="action", required=True)
    for name in ("freeze", "run", "report"):
        actions.add_parser(name)
    actions.add_parser("observations").add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.action == "freeze":
        print(freeze())
    elif args.action == "run":
        print(run() / "SUMMARY.md")
    elif args.action == "observations":
        result = prospective_report()
        print(json.dumps(result, indent=2, default=str, allow_nan=False)
              if args.json else LABEL + "\n\n" + observation_text(result))
    else:
        folder = OUTPUT / read_json(OUTPUT / "latest.json")["directory"]
        print((folder / "SUMMARY.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
