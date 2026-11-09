from pathlib import Path
from uuid import uuid4

import pandas as pd

from lab.phase13.registry import checked
from lab.phase14.context import ROOT, combine, pointer, save
from lab.phase14.outcomes import execution_market, market_evidence, native_market, price_frame
from lab.storage import file_hash, utc


def asset(frame, kind):
    folder = ROOT / "snapshot-assets"
    folder.mkdir(parents=True, exist_ok=True)
    temporary = folder / f".{uuid4().hex}.tmp"
    try:
        frame.to_parquet(temporary, index=True)
        sha = file_hash(temporary)
        path = folder / f"{kind}-{sha}.parquet"
        if path.exists():
            checked(path, sha)
        else:
            temporary.replace(path)
        return {"path": str(path), "sha256": sha, "rows": len(frame)}
    finally:
        temporary.unlink(missing_ok=True)


def freeze_snapshot(amendment, seed, qualification, as_of):
    body = amendment["body"]
    start, end = (utc(body[key]) for key in ("start", "end_exclusive"))
    binding = {"amendment": amendment["id"], "registry": body["registry"], "seed": seed["id"],
               "original_protocol": body["original_protocol"], "start": start.isoformat(), "end_exclusive": end.isoformat()}
    if qualification["state"] != "QUALIFIED_FOR_OFFLINE_REPLAY" or utc(qualification["cutoff"]) != end:
        raise ValueError("Evaluation snapshot requires the qualified primary horizon")
    if existing := pointer("snapshot"):
        if any(existing["body"][key] != value for key, value in binding.items()):
            raise ValueError("Evaluation snapshot belongs to another cohort or seed")
        return existing
    market, funding = execution_market(amendment, qualification["funding"])
    cutoff = utc(seed["body"]["cutoff"])
    bridge = (price_frame(market_evidence(amendment, cutoff, start, basis="trade"), "trade", verify=True)
              if cutoff < start else combine([]))
    frames = {"trade": market.trade, "mark": market.mark, "feature_bridge": bridge}
    if (any(not frame.empty and (frame.received_at > as_of).any() for frame in frames.values())
            or any(utc(row["received_at"]) > as_of for row in funding)):
        raise ValueError("Snapshot includes receipts after the evaluation as-of time")
    return save("snapshot", {**binding, "as_of": as_of.isoformat(), "qualification": qualification,
                            "funding": funding, "assets": {key: asset(frame, key) for key, frame in frames.items()}})


def load_snapshot(snapshot):
    body = snapshot["body"]
    frames = {}
    for kind, row in body["assets"].items():
        frame = pd.read_parquet(checked(Path(row["path"]), row["sha256"]))
        if len(frame) != row["rows"]:
            raise ValueError("Evaluation snapshot row count changed")
        frames[kind] = frame
    market = native_market(frames["trade"], frames["mark"], utc(body["start"]), utc(body["end_exclusive"]), body["funding"])
    return market, frames["feature_bridge"]
