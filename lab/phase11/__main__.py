import argparse
import json


def main():
    from lab.phase11 import archive, benchmark, campaign, plan, qualify, repair, report, resources

    cli = argparse.ArgumentParser(prog="phase11")
    sub = cli.add_subparsers(dest="action", required=True)
    for name in ("plan", "benchmark", "amend", "repair", "daily-fallback", "qualify", "run-once", "status", "report"):
        sub.add_parser(name)
    acquire = sub.add_parser("acquire")
    acquire.add_argument("--batches", type=int, default=1)
    acquire.add_argument("--preflight", action="store_true")
    for name in ("resume", "_fit"):
        sub.add_parser(name).add_argument("campaign")
    args = cli.parse_args()
    if args.action == "_fit":
        campaign.fit(args.campaign)
        return
    if args.action == "acquire":
        result = archive.acquire(max_batches=args.batches, preflight=args.preflight)
    elif args.action == "resume":
        result = campaign.resume(args.campaign)
    else:
        result = {"plan": plan.freeze_plan, "benchmark": benchmark.run, "amend": resources.freeze,
                  "repair": repair.repair, "daily-fallback": repair.daily_fallback, "qualify": qualify.qualify,
                  "run-once": campaign.run_once, "status": report.status, "report": report.report}[args.action]()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
