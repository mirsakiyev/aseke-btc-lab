import argparse
import json
import sys

from lab.phase9.reference import NAMES
from lab.phase10 import archive, worker


def main():
    if sys.argv[1:2] == ["_manual"]:
        from lab.phase9.__main__ import main as phase9
        from lab.storage import writer_lock
        if worker.process_state()[1]:
            raise SystemExit("Phase 10 owns prospective collection and publication. See .\\phase10.ps1 status.")
        with writer_lock(worker.LOCK):
            del sys.argv[1]
            phase9()
        return
    parser = argparse.ArgumentParser(prog="phase10")
    commands = parser.add_subparsers(dest="action", required=True)
    for name in ("status", "start-local", "probe-archive", "requirements", "allow-managed"):
        commands.add_parser(name)
    stop = commands.add_parser("stop-local")
    stop.add_argument("--drain-seconds", type=int, choices=range(1, 181), metavar="SECONDS")
    process = commands.add_parser("worker")
    process.add_argument("--managed", action="store_true")
    for key in ("monitor", "ideas", "training"):
        control = commands.add_parser(key)
        if key != "monitor":
            control.add_argument("profile", choices=NAMES)
        control.add_argument("value", choices=("on", "off"))
    timezone = commands.add_parser("timezone")
    timezone.add_argument("value")
    queue = commands.add_parser("queue")
    queue.add_argument("profile", choices=("swing", "day"))
    queue.add_argument("bundle")
    for name in ("_supervise", "_fit"):
        commands.add_parser(name).add_argument("id")
    args = parser.parse_args()
    if args.action == "worker":
        worker.run(args.managed)
        return
    if args.action in ("queue", "_supervise", "_fit"):
        from lab.phase10 import research
        if args.action == "queue":
            print(json.dumps(research.qualify(args.profile, args.bundle), indent=2))
        elif args.action == "_supervise":
            research.supervise(args.id)
        else:
            research.fit(args.id)
        return
    if args.action == "allow-managed":
        worker.set_control("paused", False)
        return
    if args.action in ("monitor", "training", "ideas", "timezone"):
        value = args.value if args.action == "timezone" else args.value == "on"
        result = worker.set_control(args.action, value, getattr(args, "profile", None))
    elif args.action == "stop-local":
        result = worker.stop_local(drain_seconds=args.drain_seconds)
    else:
        result = {"status": worker.status, "start-local": worker.start_local,
                  "probe-archive": archive.probe, "requirements": archive.calendar}[args.action]()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
