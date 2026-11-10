from dataclasses import replace
from functools import partial
from math import isclose
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from lab.config import fingerprint
from lab.learning.design import STATE as SHARED_STATE
from lab.learning.evaluation import errors, fit_model, label_batch, matured, prepare, scores
from lab.learning.simulator import Cost, ExecutionAssumptions, run_trade as spot_trade
from lab.phase9.research import input_market
from lab.phase9.reference import OUTPUT as PHASE9
from lab.phase10.evaluation import account_replay
from lab.phase10.trading import run_trade as futures_trade
from lab.phase11.campaign import BudgetExhausted, load_market, reserve
from lab.phase12.study import EquityCurve, lane_path, profile_data, read_lane, release_buffers, write_orders
from lab.phase13.economics import distribution
from lab.phase13.registry import CAMPAIGN, checked, load_parent, sealed
from lab.phase16.plan import SPOT, code_version, folder
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


def stage(root, state, **fields):
    write_json(root / "stage.json", {"state": state, "at": utc_now().isoformat(), **fields})


def check_deadline(root):
    if utc_now() >= utc(read_json(root / "deadline.json")["deadline"]):
        raise BudgetExhausted("Original two-hour campaign deadline elapsed")


def attempt(root, kind, key, limit, max_each):
    check_deadline(root)
    for n in range(1, max_each+1):
        path = root / "reservations" / kind / (fingerprint([*key, n])+".json")
        if not path.exists():
            return reserve(root, kind, [*key, n], 1, limit)
    raise BudgetExhausted(f"Original attempts consumed for {kind} {key}")


def open_profile(manifest, name):
    body = manifest["body"]
    cfg = body["profiles"][name]
    trial = cfg["trial"]
    if cfg["source"] == "PHASE12_PARTITIONED":
        parent, old_result, fit, view, resource = load_parent()
        if (view["id"] != cfg["data_manifest"] or parent["id"] != body["source_futures_campaign"] or
                fit["id"] != body["source_futures_fit_manifest"] or resource["id"] != body["resource_manifest"]):
            raise ValueError("Previously admitted futures source identities changed")
        checked(CAMPAIGN/"result.json", body["source_futures_result_sha256"])
        design = parent["body"]["design"]
        market = load_market(view, resource, design)
        data = profile_data(market, design["settings"], trial["profile"], parent["body"]["contract"])
        src = lane_path(CAMPAIGN, name, trial)
        checked(src/"mature-labels.parquet", cfg["label_sha256"])
        batch = read_lane(CAMPAIGN, name, design, data, trial, cfg["preparation"])
        return design, data, batch, futures_trade
    if cfg["source"] == "PHASE9_SPOT":
        source = PHASE9 / "campaigns" / SPOT
        checked(source/"result.json", body["source_spot_result_sha256"])
        checked(source/"lineage-erratum.json", cfg["lineage_sha256"])
        source_body = sealed(source/"manifest.json", cfg["data_manifest"])["body"]
        design = source_body["design"]
        _, market = input_market(source_body)
        data = prepare(design, trial["profile"], market, source_body["contract"])
        return design, data, label_batch(design, data, trial), spot_trade
    from lab.phase16.allocation import open_allocated_profile
    return open_allocated_profile(manifest, name)


def feature_matrix(features, columns, setups, cost):
    index = pd.DatetimeIndex(list(setups), tz="UTC")
    synthetic = {"modeled_cost_to_stop", "directional_breakout_atr"}
    frame = features.loc[index, [key for key in columns if key not in synthetic]].copy()
    if "modeled_cost_to_stop" in columns:
        frame["modeled_cost_to_stop"] = [2*(cost.fee+cost.adverse)*s.reference/abs(s.reference-s.stop) for s in setups.values()]
    if "directional_breakout_atr" in columns:
        levels = features.loc[index, ["prior_high", "prior_low"]]
        frame["directional_breakout_atr"] = [s.direction*(s.reference-levels.at[at,"prior_high" if s.direction > 0 else "prior_low"])/s.atr
                                             for at,s in setups.items()]
    result = frame.loc[:, columns]
    if not np.isfinite(result.to_numpy()).all():
        raise ValueError("Selected features include nonfinite values on eligible setups")
    return result


def mature_ids(labels):
    return sorted(fingerprint([at.isoformat(), row.mature_at.isoformat()]) for at, row in labels.iterrows())


def fold_samples(labels, train_start, fold):
    inner, left, end = (utc(fold[key]) for key in ("inner_start", "outer_start", "outer_end"))
    return {"inner_train": matured(labels, train_start, inner), "outer_train": matured(labels, train_start, left),
            "inner_validation": matured(labels, inner, left), "outer_mature": matured(labels, left, end)}


def fitted_model(root, manifest, name, fold, kind, train, features, policy, seed):
    cutoff = fold["inner_start" if kind == "inner" else "outer_start"]
    key = [name, fold["id"], kind]
    schema = features["columns"]
    input_digest = fingerprint([[at.isoformat(), row.mature_at.isoformat(), float(row.net_r),
                                 features["features"].loc[at, schema].tolist()] for at,row in train.iterrows()])
    version = fingerprint([manifest["id"], key, cutoff, input_digest, schema, policy["alpha"], seed])[:24]
    marker = root / "fits" / (version+".json")
    destination = root / "models" / (version+".joblib")
    if marker.exists():
        receipt = read_json(marker)
        artifact = joblib.load(checked(destination, receipt["sha256"]))
        if artifact["version"] != version or artifact["input_digest"] != input_digest:
            raise ValueError("Saved model identity changed")
        return artifact["model"], receipt
    reservation = attempt(root, "fits", key, policy["max_fit_attempts"], policy["max_attempts_per_fit"])
    stage(root, "TRAINING", profile=name, fold=fold["id"], partition=kind, cutoff=cutoff, mature_labels=len(train))
    try:
        model = fit_model("ridge", {"alpha": policy["alpha"]}, seed, features, train)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_suffix(".tmp")
        joblib.dump({"model": model, "version": version, "input_digest": input_digest, "campaign": manifest["id"],
                     "scope": "RESEARCH_ONLY", "cutoff": cutoff, "features": schema}, temp, compress=3)
        temp.replace(destination)
        receipt = {"profile": name, "fold": fold["id"], "partition": kind, "training_cutoff": cutoff,
                   "version": version, "sha256": file_hash(destination), "mature_labels": len(train), "scope": "RESEARCH_ONLY"}
        write_json(marker, receipt)
        return model, receipt
    except Exception as exc:
        write_json(root / "failures" / reservation.name, {"type": type(exc).__name__, "reason": str(exc), "fit": key})
        raise


def forecast_folds(root, manifest, name, design, data, batch):
    cfg = manifest["body"]["profiles"][name]
    policy = manifest["body"]["policy"]
    setups, cost, labels, _, _ = batch
    feature = {"features": feature_matrix(data["features"], cfg["features"], setups, cost), "columns": cfg["features"]}
    outputs, candidate, mean, versions = {}, {}, {}, {}
    times = feature["features"].index
    floors = {"inner_train": cfg["minimum_train"], "outer_train": cfg["minimum_train"],
              "inner_validation": cfg["minimum_inner"], "outer_mature": cfg["minimum_outer"]}
    for fold in cfg["folds"]:
        code = fold["id"]
        slices = fold_samples(labels, utc(cfg["train_start"]), fold)
        counts = {key: len(frame) for key,frame in slices.items()}
        exclusions = {key: f"{count} mature; {floors[key]} required" for key,count in counts.items() if count < floors[key]}
        record = {"fold": code, "sample_counts": counts, "floors": floors, "boundaries": fold, "insufficiency": exclusions}
        path = root / "folds" / name / f"{code}.json"
        if exclusions:
            record["state"] = "INSUFFICIENT_EVIDENCE"
            write_json(path, record)
            outputs[code] = record
            continue
        inner_model, inner_receipt = fitted_model(root, manifest, name, fold, "inner", slices["inner_train"], feature, policy, design["seed"])
        outer_model, outer_receipt = fitted_model(root, manifest, name, fold, "outer", slices["outer_train"], feature, policy, design["seed"])
        stage(root, "EVALUATING", profile=name, fold=code)
        window = times[(times >= utc(fold["outer_start"])) & (times < utc(fold["outer_end"]))]
        candidate[code] = scores(outer_model, feature, window)
        inner_mean = float(slices["inner_train"].net_r.mean())
        outer_mean = float(slices["outer_train"].net_r.mean())
        mean[code] = scores(outer_mean, feature, window)
        versions[code] = outer_receipt["version"]
        record.update(state="EVALUATED", artifacts={"inner": inner_receipt, "outer": outer_receipt},
            inner_forecast={"challenger": errors(scores(inner_model, feature, slices["inner_validation"].index), slices["inner_validation"]),
                            "training_mean": errors(scores(inner_mean, feature, slices["inner_validation"].index), slices["inner_validation"])},
            outer_forecast={"challenger": errors(candidate[code], slices["outer_mature"]), "training_mean": errors(mean[code], slices["outer_mature"])},
            forecast_rows=len(window), mean_net_r=outer_mean)
        write_json(path, record)
        outputs[code] = record
    return outputs, candidate, mean, versions


def account_economics(market, metrics, orders):
    closed = [row for row in orders if row["status"] in {"COMPLETE", "AMBIGUOUS"}]
    gross = sum(row["gross_realized"] for row in closed)
    raw, paid, received = 0., 0., 0.
    for row in closed:
        quantity = row["signed_base_quantity"]
        index = int(market.times.searchsorted(utc(row["entry_at"]).value))
        raw += quantity*(float(row["fills"][0]["raw_trade_price"])-float(market.trade_prices[index][0]))
        for event in row.get("settlements", []):
            amount = -quantity*event["mark_proxy"]*event["rate"]
            paid -= min(amount, 0.)
            received += max(amount, 0.)
    expected_net = gross + sum(row["funding"]-row["fees"]-row["liquidation_cost"]+row["unfunded_deficit"] for row in closed)
    if not isclose(expected_net, metrics["total_realized_net"], abs_tol=1e-7):
        raise ValueError("Price, commissions, and funding do not reconcile with closed net P&L")
    return {**metrics, "gross_at_modeled_prices": gross, "adverse_already_in_gross": raw-gross,
            "funding_paid": paid, "funding_received": received,
            "worst_closed_trade": min((row["net"] for row in closed), default=None),
            "planned_risk_overruns": sum(row["realized_loss_overrun"] > 0 for row in closed),
            "mean_hold_minutes": float(np.mean([(utc(row["exit_at"])-utc(row["entry_at"])).total_seconds()/60 for row in closed])) if closed else None,
            "ambiguous_closed_trades": sum(row["status"] == "AMBIGUOUS" for row in closed),
            "accounting_complete": bool(metrics["full_window_observed"] and not any(row.get("remaining_base") for row in orders)),
            "closed_sample_sufficient": len(closed) >= 25, "concentration": distribution(closed)}


def account(root, name, role, stress, design, data, trial, setups, cost, predictions, versions, policy, executor):
    key = [name, role, stress]
    path = root / "accounts" / name / f"{role}-{stress}.json"
    orders_path = path.with_suffix(".jsonl.gz")
    if path.exists():
        result = read_json(path)
        checked(orders_path, result["orders_sha256"])
        return result
    attempt(root, "accounts", key, policy["max_account_attempts"], policy["max_attempts_per_account"])
    stage(root, "EVALUATING", profile=name, comparison=role, case=stress)
    if role == "half_risk_rule":
        risk = data["risk"]
        scale = policy["risk_control_multiplier"]
        data = {**data, "risk": replace(risk, risk_fraction=risk.risk_fraction*scale, notional_cap=risk.notional_cap*scale)}
    market = data["market"]
    if stress == "double_cost":
        cost = Cost(cost.fee*2, cost.adverse*2)
    elif stress == "delay":
        session = data["profile"].get("entry_cutoff_minute_utc")
        delayed = {}
        for at, setup in setups.items():
            submit = setup.submit_at+market.step
            if submit < setup.deadline and (session is None or submit < at.normalize()+pd.Timedelta(minutes=session)):
                delayed[at] = replace(setup, submit_at=submit, expires_at=setup.expires_at+market.step)
        setups = delayed
        predictions = {key: group.loc[group.index.isin(delayed)] for key,group in predictions.items()}
        executor = partial(executor, assumptions=ExecutionAssumptions(delay_exit_bars=1))
    metrics, orders = account_replay(design, data, {**trial, "id": f"phase16/{name}/{role}", "model": role},
                                    (setups, cost, None, [], {}), predictions, versions,
                                    curve=EquityCurve(design, market.step), executor=executor)
    breakdown = account_economics(market, metrics, orders)
    write_orders(orders_path, orders)
    result = {"role": role, "case": stress, "cost": {"taker": cost.fee, "adverse": cost.adverse}, "metrics": breakdown,
              "orders_sha256": file_hash(orders_path)}
    write_json(path, result)
    return result


def conclusion(accounts, all_folds, cases):
    challenger = [accounts[f"challenger/{case}"]["metrics"] for case in cases]
    if not all_folds or not all(row["accounting_complete"] and row["closed_sample_sufficient"] for row in challenger):
        return "INCONCLUSIVE", "Original fold, accounting or 25-trade economic floor not met in every case"
    base = challenger[0]
    rivals = [accounts[f"{role}/base"]["metrics"]["return"] for role in ("unfiltered", "half_risk_rule")]
    if all(value is not None and base["return"] < value for value in rivals):
        return "DETERIORATION", "Base return falls below both fixed rule controls"
    uncertainty = base["uncertainty"].get("mean_net_r_95_interval", [0])
    for case, row in zip(cases, challenger):
        comparators = [accounts[f"{role}/{case}"]["metrics"]["return"] for role in ("unfiltered", "training_mean", "half_risk_rule")]
        if not all(value is not None and row["return"] > value for value in [0., *comparators]):
            return "INCONCLUSIVE", "Net account return did not beat every frozen comparator across the stresses"
    if uncertainty[0] > 0 and base["drawdown_observed_marks"] >= accounts["unfiltered/base"]["metrics"]["drawdown_observed_marks"]:
        return "DESCRIPTIVE_IMPROVEMENT", "Frozen economic thresholds cleared on consumed development windows"
    return "INCONCLUSIVE", "Dependence-aware expectancy or drawdown gate did not clear"


def evaluate_profile(root, manifest, name):
    cfg = manifest["body"]["profiles"][name]
    policy = manifest["body"]["policy"]
    design, data, batch, executor = open_profile(manifest, name)
    mature = batch[2]
    ids = mature_ids(matured(mature, utc(cfg["train_start"]), utc(cfg["exclusive_data_end"])))
    write_json(root/"input-progress"/f"{name}.json", {"data_manifest": cfg["data_manifest"], "mature_ids": ids,
               "mature_labels": len(ids), "cutoff": cfg["exclusive_data_end"]})
    from lab.phase16.allocation import incremental_check
    if blocked := incremental_check(manifest, name, ids):
        return blocked
    folds, predictions, means, versions = forecast_folds(root, manifest, name, design, data, batch)
    if not predictions:
        return {"state": "WAITING_FOR_NEW_MATURE_LABELS", "reason": "No chronological fold satisfies the frozen label floors", "folds": folds}
    design = {**design, "folds": [fold for fold in cfg["folds"] if fold["id"] in predictions]}
    setups, cost, *_ = batch
    rule = {key: pd.Series(1., index=group.index) for key,group in predictions.items()}
    accounts = {}
    for case in policy["cases"]:
        for role, series in (("challenger", predictions), ("unfiltered", rule), ("training_mean", means), ("half_risk_rule", rule)):
            accounts[f"{role}/{case}"] = account(root, name, role, case, design, data, cfg["trial"], setups, cost, series,
                                                   versions if role == "challenger" else {}, policy, executor)
    verdict, reason = conclusion(accounts, len(design["folds"]) == len(cfg["folds"]), policy["cases"])
    return {"state": "COMPLETED_RESEARCH_ONLY", "conclusion": verdict, "reason": reason, "folds": folds,
            "accounts": accounts, "cash": {"cases": policy["cases"], "return": 0., "drawdown": 0., "closed_trades": 0},
            "mature_labels_available": len(ids), "decision_policy": "No promotion or independent confirmation from consumed development"}


def counts(root):
    return {"fit_attempts": len(list((root/"reservations/fits").glob("*.json"))),
            "actual_fits": len(list((root/"fits").glob("*.json"))),
            "account_attempts": len(list((root/"reservations/accounts").glob("*.json"))),
            "completed_accounts": len(list((root/"accounts").glob("*/*.json"))),
            "matched_forecast_evaluations": sum(4 for p in (root/"folds").glob("*/*.json") if read_json(p)["state"] == "EVALUATED")}


def work(identifier):
    root = folder(identifier)
    manifest = sealed(root/"manifest.json", identifier)
    if manifest["body"]["code"] != code_version():
        raise ValueError("Frozen campaign code changed")
    profiles = {}
    with writer_lock(SHARED_STATE/".fit.lock"), threadpool_limits(limits=2):
        for name in manifest["body"]["profiles"]:
            path = root/"profiles"/f"{name}.json"
            if path.exists():
                profiles[name] = read_json(path)
                continue
            stage(root, "EVALUATING", profile=name, step="SOURCE_AND_MATURE_LABELS")
            try:
                check_deadline(root)
                value = evaluate_profile(root, manifest, name)
            except (OSError, ValueError) as exc:
                value = {"state": "DATA_NOT_ADMITTED", "reason": str(exc)}
            profiles[name] = value
            write_json(path, value)
            release_buffers()
        write_json(root/"result.json", {"campaign": identifier, "state": "COMPLETED_RESEARCH_ONLY", "completed_at": utc_now().isoformat(),
                   "profiles": profiles, **counts(root), "scope": "RESEARCH_ONLY", "historical_new_requests": 0})
