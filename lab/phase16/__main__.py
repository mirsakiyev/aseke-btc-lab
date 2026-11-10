import argparse
import json


def main():
    parser = argparse.ArgumentParser(prog="phase16")
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("plan", "status", "enable", "pause", "resume", "stop"):
        sub.add_parser(name)
    sub.add_parser("run-once").add_argument("--campaign")
    sub.add_parser("report").add_argument("--campaign")
    sub.add_parser("allocate").add_argument("descriptor")
    sub.add_parser("_work").add_argument("id")
    sub.add_parser("_supervise").add_argument("id")
    args = parser.parse_args()
    from lab.phase16 import control
    if args.action == "_work":
        from lab.phase16.evaluate import work
        work(args.id)
        return
    if args.action == "_supervise":
        result = control.supervise(args.id, recurring=True)
    elif args.action == "run-once":
        result = control.run_once(args.campaign)
    elif args.action == "resume":
        result = control.resume()
    elif args.action in {"enable", "pause", "stop"}:
        result = control.change(args.action)
    elif args.action == "plan":
        from lab.phase16.plan import freeze_initial
        result = freeze_initial()
    elif args.action == "allocate":
        from lab.phase16.allocation import allocate
        result = allocate(args.descriptor)
    else:
        from lab.phase16.report import report, status
        result = status() if args.action == "status" else report(args.campaign)
    print(json.dumps(result, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
