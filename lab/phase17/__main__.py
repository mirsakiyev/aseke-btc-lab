import argparse
import json


def main():
    parser = argparse.ArgumentParser(prog="phase17")
    commands = parser.add_subparsers(dest="action", required=True)
    for command in ("register", "catalog", "discover", "acquisition", "status", "report", "enable", "pause", "disable", "pilot-register", "pilot-status"):
        commands.add_parser(command)
    forward = commands.add_parser("pilot-evaluate")
    forward.add_argument("profile", choices=("day","position"))
    for command in ("_supervise", "_label"):
        child = commands.add_parser(command)
        child.add_argument("profile", choices=("day", "position", "swing"))
        child.add_argument("cutoff")
        child.add_argument("--kind", choices=("original","pilot-check","pilot-fit","pilot-evaluation"), default="original")
    args = parser.parse_args()
    if args.action in {"_supervise", "_label"}:
        from lab.phase17.supervisor import supervise, work
        if args.action == "_label":
            work(args.profile, args.cutoff, kind=args.kind)
            return
        result = supervise(args.profile, args.cutoff, kind=args.kind)
    elif args.action == "pilot-register":
        from lab.phase17.pilot_registry import register
        result = register()
    elif args.action == "pilot-status":
        from lab.phase17.pilot_report import status
        result = status()
    elif args.action == "pilot-evaluate":
        from lab.phase17.pilot import pointed, profile_dir
        from lab.phase17.supervisor import supervise
        from lab.storage import utc, utc_now
        evaluation = pointed(profile_dir(args.profile),"evaluation","evaluations")
        if not evaluation:
            result = {"state":"WAITING_FOR_FORWARD_REGISTRATION"}
        elif utc_now() < utc(evaluation["body"]["scoring_available_at"]):
            result = {"state":"FORWARD_HORIZON_OPEN","due":evaluation["body"]["scoring_available_at"]}
        else:
            result = supervise(args.profile, evaluation["body"]["end_exclusive"], kind="pilot-evaluation")
    elif args.action in {"enable", "pause", "disable"}:
        from lab.phase16.control import change
        from lab.phase17.registry import registered
        if not registered():
            raise ValueError("Register the native development policy first")
        result = change("stop" if args.action == "disable" else args.action)
    elif args.action in {"discover", "acquisition"}:
        from lab.phase17.discovery import acquisition_decision, discover
        result = (discover if args.action == "discover" else acquisition_decision)()
    elif args.action in {"register", "catalog"}:
        from lab.phase17.registry import data_catalog, register
        result = (register if args.action == "register" else data_catalog)()
    else:
        from lab.phase17.report import report, status
        result = (report if args.action == "report" else status)()
    print(json.dumps(result, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
