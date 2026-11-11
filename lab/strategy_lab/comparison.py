import json

from lab.strategy_lab.datasets import data_id
from lab.strategy_lab.jobs import records, status
from lab.strategy_lab.registry import MODE, MODEL_VALIDATION, get


def report(db, key):
    job = status(db, key)
    if job["state"] != "COMPLETE":
        raise ValueError("A reproducible report requires a completed job")
    request = job["request"]
    return {"job": job, "strategy": get(db, request["version"]), "dataset": data_id(db, request["dataset"]),
            "baseline": get(db, request["identity"]["baseline"]),
            "cases": records(db, key), "mode": MODE, "validation": MODEL_VALIDATION,
            "selection": selection(db, request["profile"])}


def selection(db, profile):
    rows = db.execute("SELECT state,request FROM job WHERE json_extract(request,'$.profile')=?", (profile,)).fetchall()
    attempts = [json.loads(row["request"]) for row in rows]
    return {"registered_requests": len(rows), "completed_requests": sum(row["state"] == "COMPLETE" for row in rows),
            "versions": len({item["version"] for item in attempts}), "families": len({item["family"] for item in attempts}),
            "scope": "All sandbox requests for this profile, including queued, rejected and cancelled research",
            "selection_adjusted": False,
            "interpretation": "Consumed development evidence. Variant searches and prior external trials can inflate results. "
                              "Intervals are descriptive and do not control the false discovery rate."}


def comparability(request):
    identity = request["identity"]
    return [identity[k] for k in ("dataset", "intervals", "baseline", "baseline_recipe", "code", "runtime", "assumptions")] + [
        request["risk"], request["cost"]
    ]


def compare(db, keys):
    keys = list(dict.fromkeys(keys))
    if not keys:
        raise ValueError("Select at least one completed experiment")
    if len(keys) > 20:
        raise ValueError("Compare up to 20 experiments at a time")
    rows, basis = [], None
    for key in keys:
        job = status(db, key)
        if job["state"] != "COMPLETE":
            raise ValueError("Comparison requires completed jobs")
        request = job["request"]
        current = comparability(request)
        if basis is not None and basis != current:
            raise ValueError("Jobs require identical data, dates, baseline, risk, cost and simulator assumptions")
        basis = current
        spec = get(db, request["version"])["spec"]
        cases = records(db, key)
        base = cases["validation/base"]
        rows.append({"job": key, "strategy": spec["strategy_id"], "version": request["version"],
                     "profile": spec["profile"], "entry_family": spec["family_id"], "exit": spec["recipe"]["exit"],
                     "status": cases["verdict"]["status"], "validation": MODEL_VALIDATION,
                     "reproducibility": "PINNED_INPUTS", "start": base["start"], "end_exclusive": base["end_exclusive"],
                     "metrics": base["metrics"], "coverage": base["coverage"], "equity": base["equity"],
                     "baseline": cases["validation/baseline"],
                     "stress": {name: {"status": value["status"], "metrics": value["metrics"],
                                      "coverage": value["coverage"]}
                                for name, value in cases.items() if name.startswith("validation/")
                                and name not in ("validation/base", "validation/baseline")}})
    eligible = [row for row in rows if row["status"] == "EXPLORATORY_SCREEN"]
    ranked = sorted(eligible, key=lambda row: (
        -row["metrics"]["uncertainty"]["mean_net_r_95_interval"][0],
        -row["metrics"]["drawdown_observed_marks"], row["strategy"]
    ))
    return {"status": "EXPLORATORY_SCREEN" if ranked else "INCONCLUSIVE",
            "ranked_job_ids": [row["job"] for row in ranked], "rows": rows,
            "selection": selection(db, rows[0]["profile"]), "cash_reference_return": 0.,
            "ranking_method": "Among screens clearing accounting, sample, baseline and stress checks, sort by the "
                              "lower descriptive expectancy bound, then observed drawdown."}
