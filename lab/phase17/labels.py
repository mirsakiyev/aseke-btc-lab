from collections import Counter
from dataclasses import asdict, replace
from pathlib import Path

import pandas as pd

from lab.config import fingerprint
from lab.execution import execution_open
from lab.learning.data import Market, validate_prices
from lab.learning.evaluation import candidates, prepare
from lab.learning.simulator import Cost, run_trade as spot_trade
from lab.phase9.ideas import current_rules
from lab.phase9.reference import parent_design, reference
from lab.phase10.partition import Chunk, PartitionedMarket
from lab.phase10.trading import run_trade as futures_trade
from lab.phase12.study import profile_data
from lab.phase13.future import expected_funding
from lab.phase16.evaluate import fold_samples
from lab.phase16.plan import freeze_initial
from lab.phase17.collection import HOUR, MINUTE
from lab.phase17.source import frame, open_metadata, put_frame
from lab.storage import utc


RECEIPT_ALLOWANCE = pd.Timedelta(seconds=120)
RULE_LIFETIME = pd.Timedelta(hours=24)
FUNDING_TOLERANCE = pd.Timedelta(seconds=5)
VERSION = "phase17-received-net-r-v1"


def design_for(record, name, end):
    cfg = record["body"]["profiles"][name]
    parent = parent_design()["design"]
    settings = dict(parent["settings"])
    settings["evaluation"] = {**settings["evaluation"], "train_start": cfg["start"]}
    settings["costs"] = {**settings["costs"], cfg["recipe"]["cost"]: cfg["costs"]}
    baseline = freeze_initial()["body"]["profiles"][name]
    settings["profiles"] = {**settings["profiles"], cfg["recipe"]["profile"]: baseline["profile"]}
    settings["risk"] = {**settings["risk"], baseline["profile"]["risk"]: cfg["risk"]}
    fold = {"id": "allocated", "inner_start": cfg["inner_start"], "outer_start": cfg["outer_start"], "outer_end": end}
    return {**parent, "settings": settings, "folds": [fold], "available_at_cutoff_exclusive": end,
            "label_version": VERSION, "feature_version": parent["feature_version"]}


def metadata_path(source, start, end):
    config = reference()
    parsed = []
    for row in source["metadata"]:
        stamp, received = utc(row["as_of"]), utc(row["received_at"])
        left = max(stamp, received)
        if left >= end or stamp+RULE_LIFETIME <= start:
            continue
        raw = open_metadata(row)["metadata"]
        try:
            contract = current_rules(source["market"], raw, config)
            if source["market"] == "FUTURES" and contract.base_per_contract != .001:
                contract = None
        except (KeyError, ValueError, StopIteration):
            contract = None
        parsed.append((left, stamp, contract, raw))
    parsed.sort(key=lambda item: item[:2])
    first = next((p[2] for p in parsed if p[2]), None)
    if not first:
        raise ValueError("No compatible dated public instrument or margin tier evidence")
    compatible, expected = [], set()
    for index, (seen, stamp, contract, raw) in enumerate(parsed):
        left = max(seen, start)
        right = min(stamp+RULE_LIFETIME, end, parsed[index+1][0] if index+1 < len(parsed) else end)
        if left >= right or contract != first:
            continue
        if source["market"] == "FUTURES":
            slots = expected_funding(raw, left, right)
            if slots is None:
                continue
            expected.update(slots)
        if compatible and compatible[-1][1] == left:
            compatible[-1] = (compatible[-1][0], right)
        else:
            compatible.append((left, right))
    return first, compatible, expected


def settlement_map(funding, expected):
    actual, unsafe = {}, set()
    for at, received, rate in funding:
        nominal = at.round("min")
        if nominal not in expected or abs(at-nominal) > FUNDING_TOLERANCE or nominal in actual:
            unsafe.add(nominal)
            continue
        actual[nominal] = (at, received, float(rate))
    return {at: rate for nominal, (at, _, rate) in actual.items() if nominal not in unsafe}, actual, sorted((expected-actual.keys()) | unsafe)


def audited_executor(executor, intervals, observed, unsafe, trade_receipts, mark_receipts, *, step):
    def execute(market, setup, contract, cost, risk, equity, rules, cutoff, **kwargs):
        if not any(a <= setup.available_at < b and setup.submit_at < b for a, b in intervals):
            return {**asdict(setup), "status": "REJECTED", "reason": "contract_rules_unavailable_at_entry"}
        outcome = executor(market, setup, contract, cost, risk, equity, rules, cutoff, **kwargs)
        if outcome["status"] == "AMBIGUOUS":
            return {**asdict(setup), "status": "INCOMPLETE", "reason": "ambiguous_intrabar_path", "ambiguity": True,
                    "entry_at": outcome["entry_at"], "remaining_base": abs(outcome["signed_base_quantity"])}
        if outcome["status"] != "COMPLETE":
            return outcome
        entry, exit_at = outcome["entry_at"], outcome["exit_at"]
        required = [slot for slot in unsafe if entry <= slot <= exit_at]
        if required or not any(a <= entry <= exit_at < b for a, b in intervals):
            reason = "funding_path_missing_or_conflicting" if required else "dated_contract_path_unavailable"
            return {**asdict(setup), "status": "INCOMPLETE", "reason": reason, "entry_at": entry,
                    "remaining_base": abs(outcome["signed_base_quantity"])}
        # A candle-open exit still depends on a public candle receipt. Use
        # the conservative full receipt of its bucket, including exact funding.
        last_bucket = exit_at.floor(step)
        latest = [exit_at, trade_receipts.loc[entry:last_bucket].max(), mark_receipts.loc[entry:last_bucket].max()]
        latest.extend(observed[utc(s["settlement_at"]).round("min")][1] for s in outcome.get("settlements", []))
        outcome["mature_at"] = max(stamp for stamp in latest if not pd.isna(stamp))
        if outcome["mature_at"] >= cutoff:
            return {**asdict(setup), "status": "INCOMPLETE", "reason": "path_receipts_reach_cutoff", "entry_at": entry,
                    "remaining_base": abs(outcome["signed_base_quantity"])}
        entry_index = market.index.get_loc(entry)
        raw = sum((fill["raw_trade_price"]-market.trade_prices[entry_index][0])*fill["base"]*setup.direction
                  for fill in outcome["fills"])
        outcome.update(raw_gross=raw, adverse_cost=raw-outcome["gross_realized"])
        return outcome
    return execute


def available_features(data, receipts):
    features = data["features"]
    # A prefix maximum enforces causal availability for recursive context.
    available = receipts.cummax().reindex(features.index, method="ffill")
    if len(features):
        available = pd.Series(available.array, index=features.index)
    timely = features.ready & available.notna() & (available <= features.index+RECEIPT_ALLOWANCE)
    return available, timely


def label(record, snapshot, *, feature_builder=None, decision_start=None):
    source = snapshot["body"]
    name, start, end = source["profile"], utc(source["start"]), utc(source["end_exclusive"])
    cfg = record["body"]["profiles"][name]
    design = design_for(record, name, source["end_exclusive"])
    trial, settings = cfg["recipe"], design["settings"]
    contract, rules, expected = metadata_path(source, start, end)
    spot = cfg["market"] == "SPOT"
    receipts, funding, chunks = {"trade": [], "mark": []}, [], []
    spot_frames = []
    for part in source["parts"]:
        bases = part["bases"]
        if spot:
            if trade := bases.get("trade"):
                spot_frames.append(frame(trade).set_index("open_at"))
        elif trade := bases.get("trade"):
            mark = bases.get("mark")
            if not mark:
                continue
            trade_index = frame(trade, ["open_at", "received_at"])
            mark_index = frame(mark, ["open_at", "received_at"])
            # Drop unpaired mark-only rows; preserve a missing mark on any
            # trade minute so the simulator can censor affected paths.
            if not mark_index.open_at.isin(trade_index.open_at).all():
                paired = frame(mark)
                mark = put_frame(paired.loc[paired.open_at.isin(trade_index.open_at)])
                mark_index = paired.loc[paired.open_at.isin(trade_index.open_at), ["open_at", "received_at"]]
            if not len(mark_index):
                continue
            for basis, values in (("trade", trade_index), ("mark", mark_index)):
                receipts[basis].append(values.set_index("open_at").received_at)
            chunks.append(Chunk(Path(trade["path"]), Path(mark["path"]), trade["sha256"], mark["sha256"]))
        if "funding" in bases:
            rates = frame(bases["funding"], ["open_at", "received_at", "rate"])
            funding.extend(rates.itertuples(index=False, name=None))
    if spot and not spot_frames or not spot and not chunks:
        raise ValueError("No admissible native price partitions")
    if spot:
        bars = pd.concat(spot_frames)
        if not bars.index.is_unique or not bars.index.is_monotonic_increasing:
            raise ValueError("Spot native snapshots overlap or are unordered")
        validate_prices(bars, HOUR)
        market = Market(bars, bars, {}, HOUR, "HYPOTHETICAL_REPLAY")
        trade_receipts = mark_receipts = bars.received_at
        data = prepare(design, trial["profile"], market, asdict(contract))
    else:
        market = PartitionedMarket(chunks, {}, end, record["body"]["budget"]["max_native_rows"])
        trade_receipts, mark_receipts = (pd.concat(receipts[key]) for key in ("trade", "mark"))
        data = profile_data(market, settings, trial["profile"], asdict(contract))
    rates, settled, unsafe = settlement_map(funding, expected)
    if not spot:
        market.set_funding({at: rate for at, rate in rates.items() if market.index[0] <= at < market.index[-1]+MINUTE})
    executor = audited_executor(spot_trade if spot else futures_trade, rules, settled, unsafe, trade_receipts, mark_receipts, step=market.step)
    if feature_builder is None:
        features = data["features"].copy()
        receipt_at_close = trade_receipts.rename(index=lambda at: at+market.step)
        available, timely = available_features(data, receipt_at_close)
    else:
        features, data["columns"], available, timely = feature_builder(data, trade_receipts)
    eligible_start = max(start, utc(decision_start)) if decision_start is not None else start
    timely &= (features.index >= eligible_start) & (features.index < end)
    data["features"] = features.assign(ready=timely)
    processing = pd.Timedelta(seconds=data["rules"]["processing_seconds"])
    trail = {}
    for at, row in features.loc[timely & features.atr.gt(0)].iterrows():
        trail[execution_open(max(at, available.at[at])+processing, market.step)] = (row.close, row.atr)
    data["trail"] = trail
    cost = Cost.load(settings["costs"][trial["cost"]])
    proposed, rejected = candidates(market, data["profile"], trial["entry"], settings["strategies"][trial["entry"]],
                                    trial["exit_policy"], settings["exits"][trial["exit_policy"]], data["features"], data["rules"])
    setups, rows, training = {}, [row for row in rejected if start <= row["decision_at"] < end], []
    for at, setup in proposed.items():
        if not start <= at < end:
            continue
        submitted = execution_open(max(at, available.at[at])+processing, market.step)
        deadline = submitted + pd.Timedelta(minutes=data["profile"]["max_hold_minutes"])
        if "entry_cutoff_minute_utc" in data["profile"]:
            if submitted >= at.normalize()+pd.Timedelta(minutes=data["profile"]["entry_cutoff_minute_utc"]):
                rows.append({"decision_at": at, "status": "REJECTED", "reason": "session_ineligible_after_receipt"})
                continue
            deadline = min(deadline, at.normalize()+pd.Timedelta(minutes=data["profile"]["flat_minute_utc"]))
        setup = replace(setup, available_at=max(at, available.at[at])+processing, submit_at=submitted,
                        expires_at=submitted+market.step, deadline=deadline)
        outcome = executor(market, setup, data["contract"], cost, data["risk"], data["rules"]["label_equity"],
                           data["rules"], end, trail_updates=trail)
        rows.append(outcome)
        if "entry_at" in outcome:
            setups[at] = setup
        if outcome["status"] == "COMPLETE":
            training.append((at, outcome["mature_at"], outcome["net_r"]))
    for row in rows:
        at = row["decision_at"]
        decision_data = features.loc[at, data["columns"]].astype(float)
        row.update(profile=name, recipe=trial["entry"], exit_policy=trial["exit_policy"], target_version=VERSION,
                   source_bundle=snapshot["id"], first_input_receipt_cutoff=available.at[at],
                   input_identity=fingerprint([at.isoformat(), str(available.at[at]), decision_data.tolist()]),
                   evidence_state=row["status"], ambiguity=row.get("ambiguity", False))
    labels = pd.DataFrame.from_records(training, columns=["decision_at", "mature_at", "net_r"])
    for key in ("decision_at", "mature_at"):
        labels[key] = pd.to_datetime(labels[key], utc=True)
    labels = labels.set_index("decision_at")
    counts = {"feature_ready": int(timely.sum()), "eligible_baseline_setups": len(setups), "hypothetical_fills": sum("entry_at" in row for row in rows),
              "mature_permitted_labels": len(labels), "observed_funding_slots": len(rates), "unresolved_funding_slots": len(unsafe),
              "feature_receipt_exclusions": int((features.ready & ~timely).sum()),
              "exclusions": dict(Counter(row["reason"] for row in rows if row["status"] != "COMPLETE")),
              "source_state": market.coverage() if not spot else {"rows": len(market.index), "missing_trade": int((end-start)//HOUR)-len(market.index)}}
    return design, data, (setups, cost, labels, rows, counts), executor, asdict(contract)


def qualify(cfg, design, labels, previous_cutoff):
    end = utc(design["available_at_cutoff_exclusive"])
    new_count = int((labels.index >= utc(previous_cutoff)).sum())
    if not cfg["queue"]:
        return "OBSERVATION_ONLY", "Shelved Swing challenger is outside the fitting queue", {}, new_count
    if end < utc(cfg["first_cutoff"]):
        return "WAITING_FOR_REGISTERED_CUT", "The first chronological development cut is still open", {}, new_count
    if (end-utc(cfg["first_cutoff"])) % pd.Timedelta(days=7):
        return "WAITING_FOR_REGISTERED_CUT", "Admission uses the next preregistered seven-day cutoff", {}, new_count
    fold, = design["folds"]
    counts = {k: len(v) for k, v in fold_samples(labels, utc(cfg["start"]), fold).items()}
    floors = {"inner_train": cfg["minimum_train"], "outer_train": cfg["minimum_train"],
              "inner_validation": cfg["minimum_inner"], "outer_mature": cfg["minimum_outer"]}
    missing = [f"{key} {counts[key]}/{minimum}" for key, minimum in floors.items() if counts[key] < minimum]
    if new_count < cfg["minimum_new_mature"]:
        missing.append(f"new mature {new_count}/{cfg['minimum_new_mature']}")
    if missing:
        return "WAITING_FOR_NEW_MATURE_LABELS", ", ".join(missing), counts, new_count
    return "ADMITTED", "Frozen chronological, native evidence and maturity floors met", counts, new_count
