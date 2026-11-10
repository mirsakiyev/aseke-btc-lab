from contextlib import closing
import json
from pathlib import Path
from uuid import uuid4

import pandas as pd

from lab.config import fingerprint
from lab.data import OHLCV
from lab.phase13 import observation
from lab.phase13.registry import checked, sealed
from lab.phase17.collection import HOUR, MINUTE, futures_stores, spot_store
from lab.phase17.registry import DAY, ROOT, save
from lab.storage import file_hash, read_json, utc, write_json


def latest_snapshot(name, namespace=None):
    base = namespace or ROOT
    path = base / "snapshots" / f"{name}.json"
    if path.exists():
        identifier = read_json(path)["id"]
        return sealed(ROOT / "sources" / f"{identifier}.json", identifier)


def open_metadata(row):
    identifier = row["id"]
    return sealed(ROOT / "metadata" / (identifier + ".json"), identifier)["body"]


def verify_metadata(row):
    from lab.phase9.reference import OUTPUT as PUBLIC
    from lab.phase10.worker import OUTPUT as MONITOR

    value = row["metadata"]
    plan = value.get("plan")
    if not plan or Path(plan).name != plan or not value.get("sources"):
        return False
    folders = (MONITOR / "metadata" / plan / "raw", PUBLIC / "acquisition" / plan / "raw")
    for receipt in value["sources"].values():
        digest = receipt["raw_sha256"]
        asset = next((root / (digest + ".json") for root in folders if (root / (digest + ".json")).exists()), None)
        if asset is None or file_hash(asset) != digest:
            return False
    return True


def put_frame(frame):
    root = ROOT / "partitions"
    root.mkdir(parents=True, exist_ok=True)
    temporary = root / (uuid4().hex + ".tmp")
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        digest = file_hash(temporary)
        path = root / (digest + ".parquet")
        if not path.exists():
            temporary.replace(path)
        checked(path, digest)
        return {"path": str(path), "sha256": digest, "rows": len(frame)}
    finally:
        temporary.unlink(missing_ok=True)


def snapshot(record, name, cutoff, observed_at, *, namespace=None, storage_record=None):
    body = record["body"]
    cfg = body["profiles"][name]
    start, end = utc(cfg["start"]), utc(cutoff)
    if not start < end <= min(utc(cfg["end_exclusive"]), observed_at):
        raise ValueError("Snapshot interval is outside the registered market allocation")
    spot = cfg["market"] == "SPOT"
    step = HOUR if spot else MINUTE
    if (end-start)//step > body["budget"]["max_native_rows"]:
        raise ValueError("Phase 17 snapshot exceeds the frozen row cap")
    storage = storage_record or record
    if storage["id"] != record["id"]:
        raise ValueError("Source storage is bound to a different registration")
    stores = (spot_store(storage),) if spot else tuple(futures_stores(storage))
    stores = [s for s in stores if (observation.directory(s) / "observations.sqlite3").exists()
              and (spot or utc(s["body"]["start"]) < end)]
    bases = ("trade",) if spot else ("trade", "mark", "funding")
    metadata, changes, chunks = {}, [], []
    paired = 0
    verified = set()
    for store in stores:
        root = observation.directory(store)
        with closing(observation.db_open(store, readonly=True)) as db:
            # Fixed receipt cutoff over append-only evidence keeps this read
            # reproducible without holding a transaction across partitions.
            for stamp, receipt, raw in db.execute("SELECT as_of,received_at,body FROM metadata WHERE received_at<=? ORDER BY as_of",
                                                   (observed_at.isoformat(),)).fetchall():
                row = {"as_of": stamp, "received_at": receipt, "metadata": json.loads(raw)}
                if (stamp not in metadata or receipt < metadata[stamp]["received_at"]) and verify_metadata(row):
                    metadata[stamp] = row
            left = start if spot else max(start, utc(store["body"]["start"]))
            right = end if spot else min(end, utc(store["body"]["end_exclusive"]))
            for a in pd.date_range(left, right, freq=7*DAY, inclusive="left"):
                b = min(right, a+7*DAY)
                args = (a.isoformat(), b.isoformat(), observed_at.isoformat())
                changed = {}
                for basis, stamp, received, raw_sha in db.execute("SELECT basis,source_at,received_at,raw_sha FROM correction "
                        "WHERE source_at>=? AND source_at<? AND received_at<=? ORDER BY basis,source_at,received_at,raw_sha", args):
                    changed.setdefault((basis, stamp), []).append((received, raw_sha))
                part = {"start": a.isoformat(), "end_exclusive": b.isoformat(), "store": store["id"], "bases": {}}
                price_times = {}
                for basis in bases:
                    rows = []
                    for stamp, close, received, raw_sha, raw in db.execute("SELECT source_at,source_close,received_at,raw_sha,body FROM item "
                            "WHERE basis=? AND source_at>=? AND source_at<? AND received_at<=? ORDER BY source_at", (basis, *args)).fetchall():
                        path = root / "raw" / (raw_sha + ".json")
                        if path not in verified:
                            checked(path, raw_sha)
                            verified.add(path)
                        if (basis, stamp) in changed:
                            revisions = changed[basis, stamp]
                            for _, digest in revisions:
                                checked(root / "raw" / (digest + ".json"), digest)
                            changes.append({"basis": basis, "source_at": stamp, "first_receipt": received, "first_hash": raw_sha,
                                            "revisions": revisions, "store": store["id"]})
                            continue
                        value = json.loads(raw)
                        fields = {key: value[key] for key in (OHLCV if basis == "trade" else OHLCV[:4])} if basis != "funding" else {"rate": value["rate"]}
                        if basis == "mark":
                            fields["volume"] = 0.
                        rows.append({**fields, "open_at": utc(stamp), "close_at": utc(close), "received_at": utc(received), "raw_sha256": raw_sha})
                    if rows:
                        frame = pd.DataFrame(rows)
                        if basis != "funding":
                            price_times[basis] = set(frame.open_at)
                        part["bases"][basis] = {**put_frame(frame), "source_first": rows[0]["open_at"].isoformat(),
                                                 "source_last_close": rows[-1]["close_at"].isoformat(),
                                                 "receipt_first": frame.received_at.min().isoformat(),
                                                 "receipt_last": frame.received_at.max().isoformat()}
                if part["bases"]:
                    chunks.append(part)
                if not spot:
                    paired += len(price_times.get("trade", set()) & price_times.get("mark", set()))
    # Source digest includes first receipt evidence. Candidate deduplication also
    # computes a value digest independent of local file paths or acquisition IDs.
    expected = int((end-start)//step)
    basis_counts = {basis: sum(part["bases"].get(basis, {}).get("rows", 0) for part in chunks) for basis in bases}
    coverage = {"expected_per_price_basis": expected, "basis_rows": basis_counts, "paired_minutes": paired if not spot else None,
                "price_gaps": {basis: expected-basis_counts[basis] for basis in bases if basis != "funding"}}
    source = {"version": "phase17-source-v1", "registration": record["id"], "profile": name, "start": start.isoformat(),
              "end_exclusive": end.isoformat(), "venue": "KuCoin", "instrument": cfg["instrument"], "market": cfg["market"],
              "price_bases": list(bases), "price_cadence": str(step), "scope": "RESEARCH_ONLY", "permitted_uses": body["rights"],
              "protected": {"holdout": body["holdout"], "phase14": body["phase14_evaluation"], "phase15_spot": body["phase15_spot_overlap"]},
              "parent_protocols": body["parent_protocols"], "coverage": coverage,
              "prior_consumption": "No protected futures or Phase 15 paper outcomes. Shared Spot calendar is logged.",
              "parts": chunks, "metadata": [{"id": save("metadata", metadata[key])["id"], "as_of": key,
                                             "received_at": metadata[key]["received_at"]} for key in sorted(metadata)],
              "corrections": changes}
    parts_without_paths = [{**part, "bases": {basis: {key: value for key, value in asset.items() if key != "path"}
                            for basis, asset in part["bases"].items()}} for part in chunks]
    identity = fingerprint({**source, "parts": parts_without_paths})
    base = namespace or ROOT
    index = base / "source-content" / (identity + ".json")
    if index.exists():
        prior_id = read_json(index)["id"]
        return sealed(ROOT / "sources" / (prior_id + ".json"), prior_id)
    prior = latest_snapshot(name, namespace)
    result = save("sources", {**source, "content_identity": identity, "parent_bundle": prior["id"] if prior else None})
    write_json(index, {"id": result["id"]})
    write_json(base / "snapshots" / f"{name}.json", {"id": result["id"]})
    return result


def frame(asset, columns=None):
    return pd.read_parquet(checked(Path(asset["path"]), asset["sha256"]), columns=columns)
