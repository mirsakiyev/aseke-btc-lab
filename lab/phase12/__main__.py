import argparse
import json

from lab.phase12 import commands, view


def main():
    parser = argparse.ArgumentParser(prog="phase12")
    actions = parser.add_subparsers(dest="action", required=True)
    for action in ("freeze", "status", "report", "run-once"):
        actions.add_parser(action)
    actions.add_parser("resume").add_argument("campaign")
    args = parser.parse_args()
    if args.action in ("status", "report"):
        from lab.phase12 import report
        result = getattr(report, args.action)()
    else:
        result = (commands.resume(args.campaign) if args.action == "resume" else
                  {"freeze": view.freeze, "run-once": commands.run}[args.action]())
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
