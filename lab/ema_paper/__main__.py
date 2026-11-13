import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

from lab.ema_paper import catalog, commissioning, dispatcher, proxy_protocol, source, synthetic_protocol, worker
from lab.storage import utc_now


def _document(path, maximum=3 * 1024**2):
    with path.open("rb") as stream:
        content = stream.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError("Input exceeds the EMA receipt intake size limit")
    result = json.loads(content)
    if not isinstance(result, dict):
        raise ValueError("Expected a JSON object")
    return result


def _parser():
    parser = argparse.ArgumentParser(prog="ema-paper")
    commands = parser.add_subparsers(dest="action", required=True)
    registration = commands.add_parser("register")
    registration.add_argument("--operator", required=True)
    commands.add_parser("list")
    commands.add_parser("run-once")
    commands.add_parser("compare")
    status = commands.add_parser("status")
    status.add_argument("--book", choices=catalog.PROFILES)
    proxy = commands.add_parser("proxy-status")
    proxy.add_argument("--book", choices=catalog.PROFILES)
    prepare = commands.add_parser("prepare-proxy")
    prepare.add_argument("--kraken-review", required=True, type=Path)
    prepare.add_argument("--append-runtime-revision", action="store_true")
    commission = commands.add_parser("commission-proxy")
    commission.add_argument("--review", required=True, type=Path)
    synthetic = commands.add_parser("commission-synthetic")
    synthetic.add_argument("--kraken-review", required=True, type=Path)
    live = commands.add_parser("synthetic-status")
    live.add_argument("--book", choices=catalog.PROFILES)
    commands.add_parser("commission-chart-recovery", help="append the chart-operations amendment for existing synthetic books")
    commands.add_parser("commission-position-seam", help="append the future UTC daily seam for the original EMA Position book")
    commands.add_parser("prepare-position-history", help="fetch the missed daily candle as feature-only context")
    events = commands.add_parser("events")
    events.add_argument("--book", choices=catalog.PROFILES, required=True)
    events.add_argument("--limit", type=int, choices=range(1, 101), default=20, metavar="1–100")
    for action, field in (("authorize-source", "--descriptor"), ("ingest", "--file")):
        command = commands.add_parser(action)
        command.add_argument("--book", choices=catalog.PROFILES, required=True)
        command.add_argument(field, type=Path, required=True, dest="path")
    return parser


def _summary(records):
    return {name: {key: record["body"][key] for key in
                   ("profile", "book_id", "strategy_version", "strategy_hash", "registered_at", "execution_clock", "session")}
            | {"registration_id": record["id"]} for name, record in records.items()}


def _run(args):
    from lab.ema_paper import position_seam, recovery_protocol

    synthetic = synthetic_protocol.active()
    if args.action == "register":
        return _summary(catalog.register_books(args.operator))
    if args.action == "list":
        return _summary(catalog.registrations())
    if args.action == "prepare-proxy":
        if synthetic:
            raise PermissionError("The original books already have a synthetic Kraken protocol revision")
        return proxy_protocol.prepare(args.kraken_review, append_runtime_revision=args.append_runtime_revision)
    if args.action == "commission-proxy":
        if synthetic:
            raise PermissionError("The original books already have a synthetic Kraken protocol revision")
        return commissioning.commission(args.review)
    if args.action == "commission-synthetic":
        saved = synthetic_protocol.register(args.kraken_review)
        return {"revision_id": saved["id"], "recorded_at": saved["body"]["recorded_at"],
                "books": {name: {"amendment_id": item["id"], "book_id": item["body"]["book_id"],
                                 "execution_model_id": item["body"]["execution_model_id"],
                                 "seam": item["body"]["seed_end"]} for name, item in saved["body"]["books"].items()}}
    if args.action == "synthetic-status":
        if not synthetic:
            return {"state": "UNREGISTERED"}
        value = position_seam.protocol_status()
        return value[args.book] if args.book else value
    if args.action == "commission-chart-recovery":
        saved = recovery_protocol.register()
        return {"amendment_id": saved["id"], "recorded_at": saved["body"]["recorded_at"],
                "predecessor": saved["body"]["predecessor"], "protocol": saved["body"]["version"]}
    if args.action == "commission-position-seam":
        saved = position_seam.register()
        record = saved["body"]["position"]["body"]
        seam = synthetic_protocol.utc(record["seed_end"])
        return {"amendment_id": saved["id"], "book_id": record["book_id"], "seam": seam.isoformat(),
                "earliest_possible_decision_at": (seam + position_seam.DAY).isoformat()}
    if args.action == "prepare-position-history":
        return position_seam.prepare_history()
    if args.action == "proxy-status":
        status = dispatcher.protocol_status()
        if synthetic:
            effective = position_seam.protocol_status()
            for profile in status:
                status[profile]["effective_synthetic_protocol"] = effective[profile]
        return status[args.book] if args.book else status
    if args.action == "run-once":
        if synthetic:
            from lab.ema_paper import synthetic_runtime

            return position_seam.cycle() if recovery_protocol.active() else synthetic_runtime.cycle()
        if proxy_protocol.amended():
            return dispatcher.cycle()
        return worker.cycle()
    if args.action == "compare":
        from lab.ema_paper.paper_view import report

        return report()
    if args.action == "status":
        value = worker.status()
        source_status = position_seam.protocol_status if synthetic else dispatcher.protocol_status
        for profile, item in source_status(runtime_books=value["books"]).items():
            if item["amendment_id"]:
                value["books"][profile]["source_protocol"] = item
                if synthetic:
                    value["books"][profile].update(state=item["state"], reason=item.get("reason"))
        return value["books"][args.book] if args.book else value
    lane = catalog.registrations()[args.book]["body"]
    if args.action == "events":
        with closing(catalog.db_open(True)) as db:
            return [{"id": key, "type": kind, "idea_id": idea, "source_at": source_at,
                     "recorded_at": recorded_at, "details": json.loads(body)} for key, kind, idea, source_at, recorded_at, body
                    in db.execute("SELECT key,kind,idea,source_at,recorded_at,body FROM event "
                                  "WHERE book=? ORDER BY rowid DESC LIMIT ?", (lane["book_id"], args.limit))]
    if synthetic or proxy_protocol.amended():
        raise PermissionError("Use the reviewed cross-venue dispatch path after commissioning. Legacy source intake is closed for these books.")
    if args.action == "authorize-source":
        return source.authorize(lane, _document(args.path), utc_now())
    return source.ingest(lane, _document(args.path), utc_now())


def main():
    args = _parser().parse_args()
    try:
        print(json.dumps(_run(args), indent=2, default=str, allow_nan=False))
    except (OSError, ValueError, KeyError, RuntimeError, sqlite3.Error) as exc:
        print(f"EMA paper: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
