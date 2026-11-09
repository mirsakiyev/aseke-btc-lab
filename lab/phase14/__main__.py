import argparse
import json

import pandas as pd

from lab.phase13.diagnostics import gates_off
from lab.phase14.collection import managed_evidence, register
from lab.phase14.context import admit, freeze_plan, original_minutes, pointer
from lab.phase14.features import readiness
from lab.phase14.outcomes import audit, market_evidence, price_frame
from lab.storage import utc, utc_now


def status():
    seed, plan, amendment = (pointer(name) for name in ("seed", "plan", "amendment"))
    now = utc_now()
    # Commissioning checks can be reproduced from the pre-start seed and observed bridge.
    bridge = original_minutes(utc(seed["body"]["cutoff"]), min(now, utc(amendment["body"]["start"]))) if seed and amendment else None
    features = readiness(seed, now=now, minutes=bridge)
    managed = managed_evidence()
    return {"approval": "No validated model", "execution": "MANUAL_OUTSIDE_LAB", "continuous_training_off": gates_off(),
            "historical_estimator_fits": 0, "warmup_outcomes": 0,
            "acquisition_plan": plan["id"] if plan else None, "feature_seed": seed["id"] if seed else None,
            "new_warmup_requests": seed["body"]["new_http_requests"] if seed else None,
            "feature_readiness": features, "collection_health": managed,
            "amendment": {"id": amendment["id"], **{key: amendment["body"][key] for key in
                ("registered_at", "start", "end_exclusive", "technical_review_due", "dates_toronto", "original_dates_utc",
                 "original_dates_toronto")}} if amendment else None,
            "outcomes": audit(amendment)}


def report():
    from lab.phase14.report import write_report
    value = status()
    return {**value, "report": str(write_report(value))}


def technical_review():
    amendment, seed = pointer("amendment"), pointer("seed")
    now = utc_now()
    value = audit(amendment, now)
    if not amendment or not seed:
        return value
    body = amendment["body"]
    start = utc(body["start"])
    end = min(utc(value["cutoff"]), utc(body["technical_review_due"]), now)
    bridge_start = utc(seed["body"]["cutoff"])
    minutes = (price_frame(market_evidence(amendment, bridge_start, end, basis="trade"), "trade")
               if end > bridge_start else None)
    timing = {}
    if end > start:
        allowance = pd.Timedelta(seconds=body["acquisition"]["decision_max_receipt_delay_seconds"])
        for basis in ("trade", "mark"):
            rows = market_evidence(amendment, start, end, basis=basis)
            delays = [(receipt - close.ceil("15min")).total_seconds() for _, close, receipt, _, _ in rows if receipt <= now]
            timing[basis] = {"receipts": len(delays), "after_quarter_allowance": sum(d > allowance.total_seconds() for d in delays),
                             "latest_seconds_after_quarter": max(delays, default=None)}
    return {**value, "feature_continuity": readiness(seed, now=now, minutes=minutes),
            "first_week_receipt_audit_through": end.isoformat(), "first_week_receipt_timing": timing}


def main():
    parser = argparse.ArgumentParser(prog="phase14")
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("plan", "admit", "register", "status", "verify", "technical-review", "evaluate", "report"):
        sub.add_parser(name)
    sub.add_parser("_evaluate").add_argument("amendment")
    args = parser.parse_args()
    if args.action in {"evaluate", "_evaluate"}:
        from lab.phase14.replay import evaluate, work
        if args.action == "_evaluate":
            work(args.amendment)
            return
        value = evaluate()
    elif args.action in {"plan", "admit", "register"}:
        value = {"plan": freeze_plan, "admit": admit, "register": register}[args.action]()
        if "body" in value:
            body = {k: v for k,v in value["body"].items() if k not in {"bars", "cache_sources", "fine_parity_rows"}}
            value = {"id": value["id"], "body": body}
    else:
        value = {"status": status, "verify": status, "technical-review": technical_review, "report": report}[args.action]()
    print(json.dumps(value, indent=2, default=str))


if __name__ == "__main__":
    main()
