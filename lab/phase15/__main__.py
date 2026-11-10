import argparse
from contextlib import closing
import json
from pathlib import Path

from lab.phase15 import paper
from lab.phase15.catalog import CONTROLS, ROOT, db_open, registered, register, set_control
from lab.phase15.worker import cycle, opportunity_rows, status
from lab.storage import read_json, utc_now, writer_lock


def main():
    parser = argparse.ArgumentParser(prog="phase15")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("register", "status", "verify", "analyze", "cycle", "report"):
        sub.add_parser(name)
    control = sub.add_parser("control")
    control.add_argument("name", choices=tuple(CONTROLS))
    control.add_argument("state", choices=("on", "off"))
    execution = sub.add_parser("record-execution")
    execution.add_argument("--book", required=True)
    execution.add_argument("--idea", required=True)
    execution.add_argument("--file", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "control":
        value = set_control(args.name, args.state == "on")
    elif args.command == "record-execution":
        record = registered()
        if not record or args.book not in record["body"]["lanes"]:
            raise ValueError("Select a registered Phase 15 book")
        with writer_lock(ROOT / ".writer.lock"), closing(db_open()) as db:
            book = paper.load(db, record["body"]["lanes"][args.book])
            paper.user_execution(db, book, args.idea, read_json(args.file), utc_now())
            db.commit()
        value = {"state": "USER_REPORTED_EXECUTION_RECORDED", "book": args.book, "idea": args.idea}
    elif args.command == "report":
        from lab.phase15.report import report
        value = report()
    else:
        value = {"register": register, "status": status, "verify": status, "analyze": lambda: opportunity_rows(registered()),
                 "cycle": cycle}[args.command]()
    print(json.dumps(value, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
