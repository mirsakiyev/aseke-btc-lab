"""Incremental admission is separate from the initial consumed-development run."""
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from lab.config import ROOT as PROJECT, fingerprint
from lab.learning.data import Market, validate_prices
from lab.learning.evaluation import label_batch as spot_labels, prepare
from lab.learning.simulator import Contract, run_trade as spot_trade
from lab.phase9.reference import parent_design
from lab.phase10.evaluation import label_batch as futures_labels
from lab.phase10.partition import Chunk, PartitionedMarket
from lab.phase10.research import checked_schedule
from lab.phase10.trading import run_trade as futures_trade
from lab.phase12.study import profile_data
from lab.phase13.registry import checked, sealed
from lab.phase14.context import pointer as phase14
from lab.phase15.catalog import registered as phase15
from lab.phase16.plan import HYPOTHESES, POLICY, ROOT, code_version, folder, freeze_initial
from lab.storage import file_hash, read_json, utc, utc_now, write_json, writer_lock


def eligibility(input_id, previous_input, mature, prior_mature, minimum, interval_days, last_completed, now):
    new = len(set(mature)-set(prior_mature))
    due = utc(last_completed)+pd.Timedelta(days=interval_days) if last_completed else now
    if input_id == previous_input:
        state, reason = "NO_NEW_INPUTS", "Same data manifest and policy already examined"
    elif new < minimum:
        state, reason = "WAITING_FOR_NEW_MATURE_LABELS", f"{new} new mature setups; {minimum} required"
    elif now < due:
        state, reason = "WAITING_FOR_INTERVAL", "Profile retraining interval has not elapsed"
    else:
        state, reason = "QUEUED", "New mature-label and interval requirements satisfied"
    return {"state": state, "reason": reason, "new_mature_labels": new, "required": minimum, "next_eligible_check": due.isoformat()}


def incremental_check(manifest, name, ids):
    body = manifest["body"]
    if body["kind"] == "INITIAL_DEVELOPMENT":
        return None
    cfg = body["profiles"][name]
    previous = body["previous"]
    receipt = read_json(folder(manifest["id"])/"allocation-maturity.json")
    if receipt["campaign"] != manifest["id"] or not set(receipt["new_decision_mature_ids"]) <= set(ids):
        raise ValueError("Allocated mature-label receipt is inconsistent with the outcome ledger")
    result = eligibility(cfg["data_manifest"], previous["data_manifest"], receipt["new_decision_mature_ids"], previous["mature_ids"], cfg["minimum_new_mature"],
                         cfg["interval_days"], previous["completed_at"], utc_now())
    return result if result["state"] != "QUEUED" else None


def permitted_interval(descriptor, protected, now):
    if descriptor.get("allocation_role") != "CHALLENGER_DEVELOPMENT_ONLY":
        raise ValueError("Declare an explicitly separate challenger-development allocation")
    profile = descriptor["profile"]
    source = descriptor["source"]
    symbol = "BTC-USDT" if profile == "position" else "XBTUSDTM"
    if (profile not in HYPOTHESES or source.get("venue") != "KuCoin" or source.get("symbol") != symbol
            or source.get("settlement") != "USDT" or source.get("scope") != "RESEARCH_ONLY"
            or not all(source.get(key) for key in ("permission", "evidence", "terms"))):
        raise ValueError("Venue, symbol, USDT settlement and public research permissions must be explicit")
    start, inner, outer, end = (utc(descriptor[key]) for key in ("start", "inner_start", "outer_start", "end_exclusive"))
    step = pd.Timedelta(hours=1) if profile == "position" else pd.Timedelta(minutes=1)
    if not max(protected) <= start < inner < outer < end <= now or any(value != value.floor(step) for value in (start,inner,outer,end)):
        raise ValueError("Allocate complete chronological data after all protected windows and before the present")
    return start, inner, outer, end


def outside_protected_roots(base, name):
    path = (base / name).resolve()
    if any(path.is_relative_to(PROJECT / part) for part in ("data", "runs", "models")):
        raise ValueError("A protected lab store cannot be allocated as independent research data")
    return path


def completed_for(profile):
    results = []
    for path in (ROOT/"campaigns").glob("*/result.json"):
        row = read_json(path)
        if row["profiles"].get(profile, {}).get("state") == "COMPLETED_RESEARCH_ONLY":
            results.append((utc(row["completed_at"]), row["campaign"], path.parent))
    if not results:
        raise ValueError("Finish the initial profile campaign before allocating incremental inputs")
    _, identifier, root = max(results)
    value = read_json(root/"input-progress"/f"{profile}.json")
    return {**value, "campaign": identifier, "completed_at": read_json(root/"result.json")["completed_at"]}


def allocate(descriptor_path, *, native_id=None):
    native = None
    if native_id:
        from lab.phase17.registry import native_bundle
        native, path = native_bundle(native_id)
        body = native["body"]["descriptor"]
    else:
        path = Path(descriptor_path).resolve()
        outside_protected_roots(path.parent, path.name)
        body = read_json(path)
    initial = freeze_initial()["body"]
    p14, p15 = phase14("amendment"), phase15()
    if not p14 or not p15:
        raise ValueError("Both protected prospective registrations must be available")
    now = utc_now()
    protected = [utc(initial["holdout"]["end_exclusive"]), utc(p14["body"]["end_exclusive"]), utc(p15["body"]["follow_end_exclusive"])]
    if native and body["profile"] == "position":
        # Phase 17 registers independent BTC-USDT hourly trade observations.
        # Protected futures and Phase 15 paper events are separate artifacts.
        protected = protected[:1]
    start, inner, outer, end = permitted_interval(body, protected, now)
    name = body["profile"]
    previous = completed_for(name)
    cfg = deepcopy(initial["profiles"][name])
    if outer < utc(previous["cutoff"]):
        raise ValueError("The next outer comparison must follow the previous allocated evaluation end")
    contract = Contract.load(body["contract"])
    if contract.quote != "USDT" or contract.family != ("spot" if name == "position" else "linear_perp") or not body.get("contract_evidence"):
        raise ValueError("Missing compatible, dated public contract evidence")
    if not native and not body["chunks"]:
        raise ValueError("Explicit checked price partitions are required")
    chunks = []
    for entry in body.get("chunks", []):
        item = {}
        for basis in (("trade",) if name == "position" else ("trade", "mark")):
            asset = outside_protected_roots(path.parent, entry[basis])
            if pq.ParquetFile(checked(asset, entry[f"{basis}_sha256"])).metadata.num_rows > 50000:
                raise ValueError("Price partition exceeds the existing 50,000-row limit")
            item.update({basis: str(asset), f"{basis}_sha256": entry[f"{basis}_sha256"]})
        chunks.append(item)
    funding = None
    if name != "position" and not native:
        funding = deepcopy(body["funding"])
        funding["path"] = str(checked(outside_protected_roots(path.parent, funding["path"]), funding["sha256"]))
    input_id = native["body"]["fit_input"] if native else fingerprint([name, body["source"], body["contract"], start.isoformat(), end.isoformat(),
                            [{key:value for key,value in item.items() if key.endswith("sha256")} for item in chunks],
                            {key:value for key,value in funding.items() if key != "path"} if funding else None])
    if native:
        for saved in (ROOT/"queue").glob("*.json"):
            prior = read_json(saved)
            if prior["input"] == input_id:
                return {**prior, "reused": True}
    if input_id == previous["data_manifest"]:
        return {"state": "NO_NEW_INPUTS", "reason": "Identical native source, funding and contract identity"}
    cfg.update(source="ALLOCATED_KUCOIN_PUBLIC", data_manifest=input_id,
               source_scope="PHASE17_KUCOIN_NATIVE_DEVELOPMENT_RESEARCH_ONLY" if native else "ALLOCATED_DEVELOPMENT_RESEARCH_ONLY",
               train_start=start.isoformat(), exclusive_data_end=end.isoformat(),
               folds=[{"id":"allocated", "inner_start":inner.isoformat(), "outer_start":outer.isoformat(), "outer_end":end.isoformat()}])
    parent = parent_design()["design"]
    settings = deepcopy(parent["settings"])
    settings["evaluation"]["train_start"] = start.isoformat()
    settings["costs"][cfg["trial"]["cost"]] = cfg["costs"]
    settings["risk"][cfg["profile"]["risk"]] = cfg["risk"]
    policy = {**POLICY, "max_fit_attempts": 4, "max_account_attempts": 24}
    manifest_body = {"kind": "INCREMENTAL_ALLOCATION", "profiles": {name: cfg}, "previous": previous, "policy": policy,
        "holdout": initial["holdout"], "code": code_version(), "allocation_code": file_hash(Path(__file__)), "scope": "RESEARCH_ONLY",
        "source_descriptor_sha256": file_hash(path), "protected_through": max(protected).isoformat(), "source": body["source"],
        "contract": body["contract"], "contract_evidence": body["contract_evidence"], "chunks": chunks, "funding": funding,
        "design": {"settings": settings, "seed": 308, "available_at_cutoff_exclusive": end.isoformat(), "folds": cfg["folds"],
                   "feature_version": parent["feature_version"], "label_version": parent["label_version"]},
        "phase14_and_phase15_used": False}
    if native:
        manifest_body["native_bundle"] = native_id
    identifier = fingerprint(manifest_body)
    due = utc(previous["completed_at"])+pd.Timedelta(days=cfg["interval_days"])
    with writer_lock(ROOT/".queue.lock"):
        for candidate in (ROOT/"queue").glob("*.json"):
            prior = read_json(candidate)
            if prior["input"] == input_id:
                return {**prior, "reused": True}
            if prior["profile"] == name and not (folder(prior["id"])/"result.json").exists():
                clock = folder(prior["id"])/"deadline.json"
                if not clock.exists() or now < utc(read_json(clock)["deadline"]):
                    raise ValueError("Finish the existing allocated profile campaign before adding a new one")
        if completed_for(name)["campaign"] != previous["campaign"]:
            raise ValueError("The latest completed profile changed during allocation. Recheck the outer boundary and retry.")
        write_json(folder(identifier)/"manifest.json", {"id": identifier, "body": manifest_body})
        row = {"id": identifier, "profile": name, "input": input_id, "at": now.isoformat(), "not_before": due.isoformat(),
               "state": "QUEUED" if now >= due else "WAITING_FOR_INTERVAL"}
        write_json(ROOT/"queue"/f"{identifier}.json", row)
    return row


def open_allocated_profile(manifest, name):
    body = manifest["body"]
    if body["allocation_code"] != file_hash(Path(__file__)):
        raise ValueError("Allocated data verification code changed")
    if body.get("native_bundle"):
        from lab.phase17.admission import open_admitted
        return open_admitted(manifest, name)
    cfg = body["profiles"][name]
    design = body["design"]
    start, end = utc(cfg["train_start"]), utc(cfg["exclusive_data_end"])
    if start < utc(body["protected_through"]):
        raise ValueError("Allocated coverage overlaps protected observations")
    trial = cfg["trial"]
    cap = design["settings"]["budget"]["max_native_rows"]
    if name == "position":
        step = pd.Timedelta(hours=1)
        rows = []
        count = 0
        for part in body["chunks"]:
            path = checked(part["trade"], part["trade_sha256"])
            count += pq.ParquetFile(path).metadata.num_rows
            if count > cap:
                raise ValueError("Spot allocation exceeds the native row cap")
            rows.append(pd.read_parquet(path).set_index("open_at"))
        trade = pd.concat(rows)
        if not trade.index.equals(pd.date_range(start,end,freq=step,inclusive="left")):
            raise ValueError("Allocated Spot hourly grid has missing or out-of-scope rows")
        validate_prices(trade, step)
        market = Market(trade, trade, {}, step, "HYPOTHETICAL_REPLAY")
        data = prepare(design, trial["profile"], market, body["contract"])
        batch, executor = spot_labels(design, data, trial), spot_trade
    else:
        chunks = [Chunk(Path(row["trade"]), Path(row["mark"]), row["trade_sha256"], row["mark_sha256"]) for row in body["chunks"]]
        market = PartitionedMarket(chunks, {}, end, cap)
        if market.missing_mark or not market.index.equals(pd.date_range(start,end,freq=market.step,inclusive="left")):
            raise ValueError("Allocated futures minute grid has missing, unpaired, or out-of-scope rows")
        market.set_funding(checked_schedule({"funding": body["funding"]}, market.index))
        data = profile_data(market, design["settings"], trial["profile"], body["contract"])
        batch, executor = futures_labels(design, data, trial), futures_trade
    from lab.learning.evaluation import matured
    from lab.phase16.evaluate import mature_ids
    first_new = max(start, utc(body["previous"]["cutoff"]))
    new = mature_ids(matured(batch[2], first_new, end))
    write_json(folder(manifest["id"])/"allocation-maturity.json", {"campaign": manifest["id"], "new_decisions_from": first_new.isoformat(),
               "new_decision_mature_ids": new, "excluded_prefix_reason": "Previous evaluated cutoff"})
    return design, data, batch, executor
