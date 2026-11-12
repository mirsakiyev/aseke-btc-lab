import argparse
import json
from pathlib import Path
import sys

from lab.storage import put_bytes, write_json
from lab.strategy_lab import comparison, jobs, prospective
from lab.strategy_lab.datasets import admit, data_id
from lab.strategy_lab.evaluator import code_hash, evaluate, test_version, tested
from lab.strategy_lab.registry import PROFILES, ROOT, STATES, connect, get, inventory, source_content, transition
from lab.strategy_lab.seed import seed
from lab.strategy_lab.specification import EMA_BRACKET, EMA_ENTRY, EMA_EXIT, EMA_FAMILY, EMA_INDICATORS, ENTRIES, EXITS, INDICATORS, stage, template, validate


def read_object(path, limit=4 * 1024**2):
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("Input JSON exceeds the size limit")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def parser():
    cli = argparse.ArgumentParser(prog="strategy-lab")
    commands = cli.add_subparsers(dest="command", required=True)
    for action in ("seed", "import-ema-pullback", "list", "recipes", "datasets", "jobs", "run-next", "proposals"):
        commands.add_parser(action)
    commands.add_parser("template").add_argument("profile", choices=PROFILES)
    commands.add_parser("validate").add_argument("file", type=Path)
    commands.add_parser("export-source").add_argument("sha256")
    draft = commands.add_parser("stage")
    draft.add_argument("file", type=Path)
    draft.add_argument("--source-file", type=Path)
    draft.add_argument("--previous")
    for action in ("show", "test"):
        commands.add_parser(action).add_argument("version")
    state = commands.add_parser("state")
    state.add_argument("version")
    state.add_argument("state", choices=STATES)
    state.add_argument("--reason", required=True)
    state.add_argument("--flags", type=Path)
    dataset = commands.add_parser("admit-data")
    dataset.add_argument("file", type=Path)
    dataset.add_argument("--approve", required=True, metavar="OPERATOR")
    replay = commands.add_parser("request")
    replay.add_argument("version")
    replay.add_argument("dataset")
    replay.add_argument("--development", nargs=2, required=True, metavar=("START", "END"))
    replay.add_argument("--validation", nargs=2, required=True, metavar=("START", "END"))
    replay.add_argument("--approve", required=True, metavar="OPERATOR")
    for action in ("status", "logs", "cancel", "resume", "export"):
        command = commands.add_parser(action)
        command.add_argument("job")
        if action == "export":
            command.add_argument("--save", action="store_true")
    commands.add_parser("compare").add_argument("jobs", nargs="+")
    signal = commands.add_parser("evaluate")
    signal.add_argument("version")
    signal.add_argument("dataset")
    signal.add_argument("decision")
    signal.add_argument("--as-of", required=True)
    signal.add_argument("--position-open", action="store_true", help="Evaluate the entry gate with an existing position")
    for action in ("preview-proposal", "propose"):
        command = commands.add_parser(action)
        command.add_argument("version")
        command.add_argument("job")
        if action == "propose":
            command.add_argument("file", type=Path)
    review = commands.add_parser("review-proposal")
    review.add_argument("proposal")
    review.add_argument("decision", choices=("REVIEWED", "CHANGES_REQUESTED", "DECLINED"))
    review.add_argument("--operator", required=True)
    review.add_argument("--reason", required=True)
    worker = commands.add_parser("_work", help=argparse.SUPPRESS)
    worker.add_argument("job")
    worker.add_argument("parent")
    return cli


def summary(record):
    spec = record["spec"]
    return {**{key: record[key] for key in ("hash", "version", "strategy_id", "family_id", "state")},
            **{key: spec.get(key) for key in ("name", "profile", "origin")}}


def execute(args):
    command = args.command
    if command == "template":
        return template(args.profile)
    if command == "recipes":
        return {"entries": ENTRIES, "exits": EXITS, "indicators": INDICATORS,
                "ema_pullback": {"entry": EMA_FAMILY, "exit": EMA_BRACKET, "entry_defaults": EMA_ENTRY,
                                 "exit_defaults": EMA_EXIT, "indicators": EMA_INDICATORS}}
    if command == "validate":
        issues = validate(read_object(args.file))
        return {"status": "NEEDS_CLARIFICATION" if issues else "SPECIFIED", "issues": issues}
    if command == "run-next":
        return jobs.run_next()
    if command == "_work":
        try:
            jobs.work(ROOT, args.job, args.parent)
        except RuntimeError as exc:
            if str(exc).startswith("Another writer owns"):
                raise SystemExit(2) from exc
            raise
        return {"worker": "FINISHED"}
    writes = {"seed", "import-ema-pullback", "stage", "state", "test", "admit-data", "request", "cancel", "resume",
              "propose", "review-proposal"}
    with connect(write=command in writes) as db:
        if command == "seed":
            result = seed(db)
            return {**result, "versions": [summary(record) for record in result["versions"]]}
        if command == "import-ema-pullback":
            from lab.strategy_lab.ema_registration import register_ema_pullback

            code = code_hash()
            versions = [record if tested(record, code) else test_version(db, record["hash"])
                        for record in register_ema_pullback(db)]
            return {"versions": [summary(record) for record in versions], "historical_readiness": "BLOCKED_DATA",
                    "evaluator_code": code}
        if command == "list":
            return [summary(record) for record in inventory(db)]
        if command == "show":
            return get(db, args.version)
        if command == "export-source":
            content = source_content(db, args.sha256)
            path = ROOT / "exports" / "sources" / args.sha256
            put_bytes(path, content)
            return {"attachment": str(path), "sha256": args.sha256}
        if command == "stage":
            spec = read_object(args.file)
            record = stage(db, spec, expected=args.previous, source_file=args.source_file)
            return {**summary(record), "issues": validate(record["spec"])}
        if command == "state":
            return transition(db, args.version, args.state, args.reason, flags=read_object(args.flags) if args.flags else None)
        if command == "test":
            return test_version(db, args.version)
        if command == "admit-data":
            return admit(db, ROOT, read_object(args.file), args.file.parent, args.approve)
        if command == "datasets":
            return [{**dict(row), "body": data_id(db, row["hash"])}
                    for row in db.execute("SELECT hash,admitted_at FROM dataset ORDER BY admitted_at DESC")]
        if command == "request":
            return jobs.request_job(db, args.version, args.dataset, args.development, args.validation, args.approve)
        if command == "jobs":
            return [jobs.status(db, row[0]) for row in db.execute("SELECT hash FROM job ORDER BY created_at DESC")]
        if command == "logs":
            return [dict(row) for row in db.execute("SELECT at,message FROM job_event WHERE job_hash=? ORDER BY seq", (args.job,))]
        if command in ("status", "cancel", "resume"):
            return getattr(jobs, command)(db, args.job)
        if command == "export":
            result = comparison.report(db, args.job)
            if args.save:
                path = ROOT / "exports" / f"{args.job}.json"
                write_json(path, result)
                return {"report": str(path)}
            return result
        if command == "compare":
            return comparison.compare(db, args.jobs)
        if command == "evaluate":
            from lab.strategy_lab.datasets import load_market
            spec = get(db, args.version)["spec"]
            body = data_id(db, args.dataset)
            if any(spec[key] != body[key] for key in ("profile", "venue", "market", "instrument")):
                raise ValueError("Dataset does not match the frozen strategy")
            market, _, _ = load_market(ROOT, body)
            return evaluate(market.trade, spec, args.decision, as_of=args.as_of, data_hash=args.dataset,
                            position_open=args.position_open)
        if command == "preview-proposal":
            return prospective.preview(db, args.version, args.job)
        if command == "propose":
            return prospective.propose(db, args.version, args.job, read_object(args.file))
        if command == "review-proposal":
            return prospective.review(db, args.proposal, args.operator, args.decision, args.reason)
        if command == "proposals":
            return [prospective.read(db, row[0]) for row in db.execute("SELECT hash FROM proposal")]
    raise ValueError("Unknown command")


def main():
    args = parser().parse_args()
    try:
        print(json.dumps(execute(args), indent=2, default=str, allow_nan=False))
    except (OSError, ValueError, KeyError, RuntimeError, TypeError) as exc:
        print(f"Strategy Lab: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
