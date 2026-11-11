import json

import pandas as pd

from lab.storage import utc_now
from lab.strategy_lab.datasets import timestamp
from lab.strategy_lab.jobs import records, status
from lab.strategy_lab.registry import MODE, MODEL_VALIDATION, canonical, digest, event, get, now


def preview(db, version, job):
    strategy = get(db, version)
    experiment = status(db, job)
    if experiment["request"]["version"] != version:
        raise ValueError("The historical job must match the frozen strategy version")
    verdict = records(db, job).get("verdict", {})
    issues = []
    if experiment["state"] != "COMPLETE" or verdict.get("status") != "EXPLORATORY_SCREEN":
        issues.append("A completed historical screen with sufficient evidence is required")
    if strategy["state"] not in ("SHORTLISTED", "PROSPECTIVE_PENDING"):
        issues.append("An explicit shortlist decision is required")
    return {"strategy": strategy, "job": job, "eligibility_issues": issues,
            "approval": "AWAITING_EXPLICIT_APPROVAL", "activation": "DISABLED",
            "risks": ["Historical validation uses consumed development data.",
                      "Future receipt, execution, and evidence coverage require separate review."]}


def propose(db, version, job, settings):
    readiness = preview(db, version, job)
    if readiness["eligibility_issues"]:
        raise ValueError("; ".join(readiness["eligibility_issues"]))
    spec = readiness["strategy"]["spec"]
    start = timestamp(settings["start"])
    horizon, minimum = settings.get("horizon_days"), settings.get("minimum_closed_trades")
    if not utc_now() < start or start != start.floor(spec["timeframe"]) or (
        type(horizon) is not int or not 1 <= horizon <= 366 or type(minimum) is not int or not 25 <= minimum <= 10000
    ):
        raise ValueError("Provide an aligned future start, bounded horizon and an explicit evidence floor")
    contract = settings.get("data_availability")
    execution = settings.get("execution_model")
    if not isinstance(contract, dict) or any(not isinstance(contract.get(key), str) or not contract[key].strip()
        for key in ("source", "receipt_rule", "gap_policy", "economic_path")) or not isinstance(execution, str) or not execution.strip():
        raise ValueError("Record the future source, receipt, gap and economic-path rules and the proposed execution model")
    manifest = {"version": version, "strategy_id": spec["strategy_id"], "historical_job": job,
                "start": start.isoformat(), "end_exclusive": (start + pd.Timedelta(days=horizon)).isoformat(),
                "profile": spec["profile"], "venue": spec["venue"], "instrument": spec["instrument"],
                "data_availability": contract, "risk": spec["risk"], "cost": spec["cost"], "execution_model": execution,
                "accounting": "New standalone hypothetical book with its own capital, journal and limits",
                "minimum_closed_trades": minimum, "benchmark": f"phase15/{spec['profile']}/fixed rules as a new comparison plus cash",
                "historical_baseline_version": status(db, job)["request"]["identity"]["baseline"],
                "approval": "AWAITING_EXPLICIT_APPROVAL", "activation": "DISABLED", "mode": MODE, "model_validation": MODEL_VALIDATION}
    identity = digest(manifest)
    if db.execute("SELECT 1 FROM proposal WHERE hash=?", (identity,)).fetchone():
        return read(db, identity)
    body = {"manifest": manifest, "registered_at": now(), "account_id": f"strategy-lab-paper/{identity}"}
    db.execute("INSERT INTO proposal VALUES(?,?)", (identity, canonical(body)))
    event(db, version, "PROSPECTIVE_PENDING", "Disabled proposal saved for operator review", reference={"proposal": identity})
    return {"hash": identity, **body}


def read(db, identity):
    row = db.execute("SELECT body FROM proposal WHERE hash=?", (identity,)).fetchone()
    if row is None:
        raise ValueError("Unknown proposal")
    body = json.loads(row["body"])
    if digest(body["manifest"]) != identity:
        raise ValueError("Proposal checksum mismatch")
    return {"hash": identity, **body}


def review(db, identity, operator, decision, reason):
    proposal = read(db, identity)
    if decision not in ("REVIEWED", "CHANGES_REQUESTED", "DECLINED") or not operator.strip() or not reason.strip():
        raise ValueError("Provide the operator, review disposition and rationale")
    version = proposal["manifest"]["version"]
    strategy = get(db, version)
    event(db, version, strategy["state"], reason,
          reference={"proposal_review": {"proposal": identity, "operator": operator, "decision": decision}})
    return {**proposal, "review": get(db, version)["history"][-1]}
