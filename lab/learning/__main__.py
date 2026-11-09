import argparse
import os

for key in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS", "NUMBA_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[key] = "2"

from pathlib import Path

from lab.learning import state
from lab.learning.control import command, latest_report, serve
from lab.learning.design import OUTPUT, PROFILE_IDS, STATE, freeze
from lab.storage import writer_lock


def main():
    parser = argparse.ArgumentParser(prog="ASEKE BTC Lab Learning")
    actions = parser.add_subparsers(dest="action", required=True)
    for name in ("status", "run-once", "start", "pause", "stop", "resume", "report", "_serve"):
        actions.add_parser(name)
    plan = actions.add_parser("freeze")
    plan.add_argument("--reviewed-new", action="store_true")
    profile = actions.add_parser("profile")
    profile.add_argument("profile", choices=PROFILE_IDS)
    profile.add_argument("enabled", choices=("on", "off"))
    child = actions.add_parser("_child")
    child.add_argument("job")
    args = parser.parse_args()
    if args.action == "_serve":
        serve()
        return
    if args.action == "_child":
        from lab.learning.study import Interrupted, run
        try:
            with writer_lock(STATE / ".fit.lock"):
                run(args.job)
        except Interrupted:
            return
        return
    if args.action == "freeze":
        live = state.status()
        if args.reviewed_new and (live["mode"] != "off" or live["job"]):
            raise ValueError("Finish the current campaign before freezing a reviewed revision")
        manifest = freeze(reviewed_new=args.reviewed_new)
        print(OUTPUT / manifest["id"] / "manifest.json")
        return
    if args.action == "report":
        result = latest_report()
        if result and result["report"]:
            print((Path(result["directory"]) / "REPORT.md").read_text(encoding="utf-8"))
        else:
            print(state.encode({"report": result, "state": state.status()}))
        return
    if args.action == "status":
        result = state.status()
    elif args.action == "profile":
        result = command("profile", profile=args.profile, enabled=args.enabled == "on")
    else:
        result = command(args.action)
    print(state.encode({key: value for key, value in result.items() if key != "latest_results"}))
    if args.action == "run-once" and result["state"] == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
