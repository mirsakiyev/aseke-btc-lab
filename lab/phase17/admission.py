from pathlib import Path

import pandas as pd

from lab.config import fingerprint
from lab.phase12.study import write_orders
from lab.phase13.registry import checked, sealed
from lab.phase16.evaluate import mature_ids
from lab.phase16.plan import folder
from lab.phase17.labels import VERSION, label, qualify
from lab.phase17.registry import ROOT, native_bundle, registered, save
from lab.phase17.source import snapshot
from lab.storage import file_hash, read_json, utc, utc_now, write_json


def code_identity():
    return fingerprint({name: file_hash(Path(__file__).with_name(name)) for name in ("admission.py", "labels.py", "source.py")})


def training_identity(labels):
    return fingerprint([[at.isoformat(), row.mature_at.isoformat(), row.net_r] for at, row in labels.iterrows()])


def publish_intent(name, intent):
    from lab.phase16.allocation import allocate

    queued = allocate(None, native_id=intent["bundle"])
    receipt = {"bundle": intent["bundle"], "campaign": queued["id"], "queue_state": queued["state"], "not_before": queued["not_before"]}
    readiness = {**intent["readiness"], **receipt}
    write_json(ROOT / "admitted" / (name + ".json"), receipt)
    write_json(ROOT / "readiness" / (name + ".json"), readiness)
    return readiness


def run_labels(name, cutoff):
    from lab.phase16.allocation import completed_for

    record = registered()
    cfg = record["body"]["profiles"][name]
    end = utc(cutoff)
    now = utc_now()
    if end > (now-pd.Timedelta(hours=2)).floor("1D") or end < utc(cfg["context_lower_bound"]):
        raise ValueError("Labeling must use a closed registered cutoff with its receipt grace and feature context")
    path = ROOT / "admitted" / f"{name}.json"
    intent_path = ROOT / "admission-intents" / f"{name}.json"
    if intent_path.exists() and not path.exists():
        return publish_intent(name, read_json(intent_path))
    source = snapshot(record, name, end, end+pd.Timedelta(hours=2))
    if not source["body"]["parts"]:
        return persist(name, cutoff, source, "DATA_NOT_ADMITTED", "No native observations in the eligible interval")
    try:
        design, _, (_, _, labels, rows, counts), _, contract = label(record, source)
    except ValueError as error:
        return persist(name, cutoff, source, "DATA_NOT_ADMITTED", str(error))
    ledger = ROOT / "labels" / (source["id"] + ".jsonl.gz")
    if not ledger.exists():
        write_orders(ledger, rows)
    label_sha = file_hash(ledger)
    previous = completed_for(name)
    state, reason, folds, newly_mature = qualify(cfg, design, labels, previous["cutoff"])
    if state != "ADMITTED":
        return persist(name, cutoff, source, state, reason, counts=counts, folds=folds,
                       new_mature=newly_mature, label_sha=label_sha)
    if path.exists():
        prior = read_json(path)
        return persist(name, cutoff, source, "ALLOCATION_ALREADY_ADMITTED", "One native campaign per profile under this registration",
                       counts=counts, folds=folds, new_mature=newly_mature, label_sha=label_sha, **prior)
    docs = record["body"]["docs"]
    descriptor = {"allocation_role": "CHALLENGER_DEVELOPMENT_ONLY", "profile": name, "start": cfg["start"],
                  "inner_start": cfg["inner_start"], "outer_start": cfg["outer_start"], "end_exclusive": source["body"]["end_exclusive"],
                  "source": {"venue": "KuCoin", "symbol": cfg["instrument"], "settlement": "USDT", "scope": "RESEARCH_ONLY",
                             "permission": record["body"]["rights"]["source"], "evidence": docs["kline"], "terms": docs["terms"]},
                  "contract": contract, "contract_evidence": source["id"]}
    price_parts = [{k: (v["sha256"], v["rows"]) for k, v in row["bases"].items()} for row in source["body"]["parts"]]
    # Alias paths and parent pointer changes alone cannot make a new fit input.
    fit_input = fingerprint([name, cfg["start"], source["body"]["end_exclusive"], price_parts,
                             contract, training_identity(labels), VERSION])
    document = {"version": "phase17-native-admission-v1", "state": "ADMITTED", "registration": record["id"],
                "source_bundle": source["id"], "source_content": source["body"]["content_identity"], "scope": "RESEARCH_ONLY",
                "descriptor": descriptor, "fold_counts": folds, "new_mature": newly_mature, "label_ledger_sha256": label_sha,
                "label_identity": training_identity(labels), "fit_input": fit_input, "label_version": VERSION,
                "processing_code": code_identity(), "used_for": "Consumed development. Later independent evaluation requires registration."}
    bundle = save("bundles", document)
    intent = {"bundle": bundle["id"], "readiness": {"profile": name, "cutoff": str(cutoff), "source_bundle": source["id"],
              "state": state, "reason": reason, "counts": counts, "folds": folds, "new_mature": newly_mature, "label_sha": label_sha}}
    write_json(intent_path, intent)
    return publish_intent(name, intent)


def persist(name, cutoff, source, state, reason, **values):
    row = {"profile": name, "cutoff": str(cutoff), "source_bundle": source["id"], "state": state, "reason": reason, **values}
    write_json(ROOT / "readiness" / (name + ".json"), row)
    return row


def open_admitted(manifest, name):
    """Phase 16 rechecks the frozen native source and regenerated training labels."""
    body = manifest["body"]
    bundle, _ = native_bundle(body["native_bundle"])
    proof = bundle["body"]
    source_id = proof["source_bundle"]
    source = sealed(ROOT / "sources" / (source_id + ".json"), source_id)
    if (proof["processing_code"] != code_identity() or source["body"]["content_identity"] != proof["source_content"]
            or proof["fit_input"] != body["profiles"][name]["data_manifest"]):
        raise ValueError("Native bundle processing or content identity changed")
    checked(ROOT / "labels" / (source_id + ".jsonl.gz"), proof["label_ledger_sha256"])
    registration = registered()
    design, data, batch, executor, contract = label(registration, source)
    if proof["label_identity"] != training_identity(batch[2]) or contract != proof["descriptor"]["contract"]:
        raise ValueError("Native replay did not reproduce the sealed training labels")
    cutoff = utc(body["previous"]["cutoff"])
    new = batch[2].loc[batch[2].index >= cutoff]
    write_json(folder(manifest["id"]) / "allocation-maturity.json", {"campaign": manifest["id"],
               "new_decisions_from": cutoff.isoformat(), "new_decision_mature_ids": mature_ids(new), "source_bundle": source_id})
    return design, data, batch, executor
