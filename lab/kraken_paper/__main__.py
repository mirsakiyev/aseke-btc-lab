"""Paper-only Kraken approval, inspection and managed collection commands."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

from lab.kraken_paper import authorization, catalog, retirement
from lab.storage import utc_now


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("draft", "list", "source-status", "collect", "run-once", "retirement-history"):
        commands.add_parser(action)
    review = commands.add_parser("verify-authorization")
    review.add_argument("--review", type=Path, required=True)
    register = commands.add_parser("register")
    register.add_argument("--review", type=Path, required=True)
    register.add_argument("--asset-pairs", type=Path, required=True)
    register.add_argument("--asset-pairs-received-at", required=True)
    status = commands.add_parser("status")
    status.add_argument("--book", choices=catalog.PROFILES)
    events = commands.add_parser("events")
    events.add_argument("--book", choices=catalog.PROFILES, required=True)
    events.add_argument("--limit", type=int, choices=range(1, 101), default=25, metavar="1–100")
    producer = commands.add_parser("_produce", help=argparse.SUPPRESS)
    producer.add_argument("--root", type=Path, required=True)
    return parser


def _run(args):
    if args.action == "draft":
        return {"state": "PREPARED", "registrations": None, "strategies": catalog.strategy_specs()}
    if args.action == "list":
        return {} if retirement.history() else catalog.registrations()
    if args.action == "retirement-history":
        return retirement.history()
    if args.action == "verify-authorization":
        record = authorization.read_review(args.review, at=utc_now())
        return {"id": authorization.evidence_id(record), "record": record}
    if args.action == "register":
        return catalog.register_books(args.review, authorization.bounded_bytes(args.asset_pairs),
                                      args.asset_pairs_received_at)
    if args.action == "events":
        if not (catalog.ROOT / "paper.sqlite3").is_file():
            return []
        with closing(catalog.db_open(readonly=True)) as db:
            lane = catalog.registrations(db).get(args.book)
            if lane is None:
                return []
            return [{"id": key, "type": kind, "idea_id": idea, "source_at": source_at,
                     "recorded_at": recorded_at, "details": json.loads(body)} for key, kind, idea, source_at,
                    recorded_at, body in db.execute("SELECT key,kind,idea,source_at,recorded_at,body FROM event "
                    "WHERE book=? ORDER BY rowid DESC LIMIT ?", (lane["body"]["book_id"], args.limit))]
    if args.action in {"status", "run-once"}:
        from lab.kraken_paper import worker

        if args.action == "run-once":
            return worker.cycle()
        if not catalog.registrations():
            return {"state": "NOT_REGISTERED", "books": {}}
        report = worker.status()
        return report["books"][args.book] if args.book else report
    from lab.kraken_paper import managed, source

    if args.action == "_produce":
        return managed.run(root=args.root)
    if args.action == "collect":
        return source.collect_registered(root=catalog.ROOT)
    return {"producer": managed.status(), "evidence": source.status(root=catalog.ROOT / "source")}


def main():
    args = _parser().parse_args()
    try:
        result = _run(args)
        if result is not None:
            print(json.dumps(result, indent=2, default=str, allow_nan=False))
    except (OSError, ValueError, KeyError, RuntimeError, sqlite3.Error) as exc:
        print(f"Kraken paper: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
