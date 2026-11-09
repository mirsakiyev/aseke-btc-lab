from contextlib import suppress
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import json
import math
import time
from uuid import uuid4
from zipfile import ZipFile

import httpx
import numpy as np
import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.learning.design import MIB, inventory
from lab.phase10.archive import FIELDS
from lab.phase9.public import retry_boundary
from lab.phase11.plan import ROOT, TERMS, freeze_plan
from lab.storage import file_hash, put_bytes, read_json, utc, utc_now, write_json, writer_lock


def folder(plan):
    return ROOT / "datasets" / plan["id"]


def manifest_path(directory, spec):
    return directory / "streams" / (spec["key"] + ".json")


def size_under(directory):
    total = 0
    for path in directory.rglob("*"):
        with suppress(FileNotFoundError):
            if path.is_file():
                total += path.stat().st_size
    return total


def normalize(payload, spec):
    kind = spec["kind"]
    left, right = utc(spec["start"]), utc(spec["end"])
    if kind == "fundingREST":
        raw = json.loads(payload)
        if not isinstance(raw, list) or not raw or any(row["symbol"] != "BTCUSDT" for row in raw):
            raise ValueError("Official cutoff funding response has no usable records")
        frame = pd.DataFrame({"settlement_at": pd.to_datetime([row["fundingTime"] for row in raw], unit="ms", utc=True),
                              "rate": [float(row["fundingRate"]) for row in raw], "interval_minutes": 0})
    else:
        fields = FIELDS[kind]
        with ZipFile(BytesIO(payload)) as archive:
            members = archive.infolist()
            if len(members) != 1 or not members[0].filename.endswith(".csv") or members[0].file_size > 16 * MIB:
                raise ValueError("Unexpected archive members or expanded size")
            with archive.open(members[0]) as handle:
                header = handle.readline().decode("utf-8-sig").strip().split(",")
            named = header[0] == fields[0]
            if len(header) != len(fields) or (named and tuple(header[:3 if kind == "fundingRate" else 7]) !=
                                               fields[:3 if kind == "fundingRate" else 7]):
                raise ValueError(f"Unexpected archive columns: {header}")
            use = list(fields if kind == "fundingRate" else fields[:7])
            with archive.open(members[0]) as handle:
                frame = pd.read_csv(handle, names=fields, header=0 if named else None, usecols=use)
        if kind == "fundingRate":
            frame = pd.DataFrame({"settlement_at": pd.to_datetime(frame.calc_time, unit="ms", utc=True),
                                  "rate": frame.last_funding_rate.astype(float),
                                  "interval_minutes": frame.funding_interval_hours.astype(int) * 60})
            if (frame.interval_minutes <= 0).any():
                raise ValueError("Invalid published funding cadence")
        else:
            if (frame.open_time % 60000).any() or not frame.close_time.eq(frame.open_time + 59999).all():
                raise ValueError("Invalid source minute grid or close timestamp")
            frame.insert(0, "open_at", pd.to_datetime(frame.open_time, unit="ms", utc=True))
            frame = frame[["open_at", "open", "high", "low", "close", "volume"]].copy()
            if (not np.isfinite(frame.iloc[:, 1:]).all().all() or (frame.low <= 0).any() or
                    (frame.low > frame[["open", "close"]].min(axis=1)).any() or
                    (frame.high < frame[["open", "close"]].max(axis=1)).any() or (frame.volume < 0).any()):
                raise ValueError("Invalid archive OHLCV")
            if kind == "markPriceKlines":
                frame = frame.drop(columns="volume")
    funding = kind.startswith("funding")
    times = frame.iloc[:, 0]
    if frame.empty or not times.is_monotonic_increasing or not times.is_unique or times.iloc[0] < left or times.iloc[-1] >= right:
        raise ValueError("Empty, unordered, duplicate, or out-of-plan timestamps")
    if funding and not np.isfinite(frame.rate).all():
        raise ValueError("Invalid settled funding rate")
    expected = None if funding else int((right-left).total_seconds() // 60)
    return frame, {"rows": len(frame), "first": times.iloc[0].isoformat(), "last": times.iloc[-1].isoformat(),
                   "missing_in_object_envelope": None if funding else expected-len(frame), "source_unit": "ms",
                   "volume_unit": "BTC" if kind == "klines" else None}


class BatchLimit(RuntimeError):
    pass


class Restricted(RuntimeError):
    pass


def acquisition_clock(plan, directory):
    path = directory / "acquisition-clock.json"
    if not path.exists():
        cap = plan["body"]["acquisition"]
        prior = sorted((directory / "batches").glob("*.json"))
        start = utc(read_json(prior[0])["start"]) if prior else utc_now()
        # Two explicitly staged checks cover the public REST gap pass and alternate packaging.
        max_batches = math.ceil(cap["max_get_requests"] / cap["requests_per_batch"]) + 2
        deadline = start + pd.Timedelta(seconds=max_batches * cap["batch_wall_seconds"])
        write_json(path, {"start": start.isoformat(), "deadline": deadline.isoformat(), "max_batches": max_batches})
    return read_json(path)


class Download:
    def __init__(self, plan, directory, batch):
        self.plan, self.directory, self.batch = plan, directory, batch
        self.cap = plan["body"]["acquisition"]
        self.started = time.monotonic()
        self.used = self.count = 0
        self.next_request = 0.
        self.ledger = directory / "requests"
        self.prior = len(list(self.ledger.glob("*.json")))
        self.clock = acquisition_clock(plan, directory)

    def get(self, client, url, params=None):
        elapsed = time.monotonic() - self.started
        if (elapsed >= self.cap["batch_wall_seconds"] or self.count >= self.cap["requests_per_batch"]
                or self.used >= self.cap["batch_bytes"]):
            raise BatchLimit("Frozen acquisition batch exhausted")
        if self.batch > self.clock["max_batches"] or utc_now() >= utc(self.clock["deadline"]):
            raise Restricted("Original archive campaign deadline or batch ceiling reached")
        if self.prior + self.count >= self.cap["max_get_requests"]:
            raise Restricted("Frozen campaign HTTP request allowance exhausted")
        pause = max(0., self.next_request - time.monotonic())
        if pause:
            time.sleep(pause)
        self.next_request = time.monotonic() + self.cap["min_spacing_seconds"]
        self.count += 1
        path = self.ledger / f"{self.prior+self.count:04}.json"
        row = {"batch": self.batch, "url": url, "params": params, "sent": utc_now().isoformat(), "bytes": 0}
        write_json(path, row)
        try:
            with client.stream("GET", url, params=params) as reply:
                row["status"] = reply.status_code
                if reply.status_code in (401, 403, 418, 429, 451):
                    row["retry_at"] = (retry_boundary(reply.headers.get("Retry-After"), utc_now()).isoformat()
                                       if reply.status_code in (418, 429) else None)
                if reply.status_code != 200:
                    return row, None
                contents = bytearray()
                for block in reply.iter_bytes():
                    contents.extend(block)
                    self.used += len(block)
                    row["bytes"] += len(block)
                    if (len(contents) > self.cap["response_bytes"] or self.used > self.cap["batch_bytes"]
                            or time.monotonic() - self.started >= self.cap["batch_wall_seconds"]):
                        raise BatchLimit("Response exceeded frozen byte or elapsed guard")
                row["sha256"] = sha256(contents).hexdigest()
                return row, bytes(contents)
        except httpx.HTTPError as exc:
            row["status"] = "TRANSPORT_ERROR"
            row["error"] = type(exc).__name__
            return row, None
        finally:
            row["received"] = utc_now().isoformat()
            write_json(path, row)

    def bytes_left(self):
        return self.cap["max_retained_dataset_bytes"] - size_under(self.directory)


def handle_object(client, fetch, spec):
    directory = fetch.directory
    path = manifest_path(directory, spec)
    cached = fetch.plan["body"]["cached"].get(spec["key"])
    receipt = {"spec": spec, "at": utc_now().isoformat()}
    rest = spec["kind"] == "fundingREST"
    if cached:
        origin = Path(cached["path"])
        if file_hash(origin) != cached["sha256"]:
            raise ValueError("Verified Phase 10 source checksum changed")
        contents = origin.read_bytes()
        receipt["verification"] = "REUSED_CHECKSUMMED_PHASE10_SAMPLE"
        receipt["published_sha256"] = cached["sha256"]
    else:
        checksum_path = directory / "checksums" / f"{spec['key']}.json"
        if not rest and not checksum_path.exists():
            row, payload = fetch.get(client, spec["url"] + ".CHECKSUM")
            if payload:
                parts = payload.decode("ascii").split()
                if (len(parts) != 2 or len(parts[0]) != 64 or any(c not in "0123456789abcdef" for c in parts[0]) or
                        parts[1] != spec["url"].rsplit("/", 1)[-1]):
                    raise ValueError(f"Invalid published checksum for {spec['key']}")
                write_json(checksum_path, {"hash": parts[0], "published_name": parts[1], "receipt": row})
            else:
                return failed(path, receipt, row, "CHECKSUM")
        row, contents = fetch.get(client, spec["url"], spec.get("params"))
        if contents is None:
            return failed(path, receipt, row, "OBJECT")
        if not rest:
            receipt["published_sha256"] = read_json(checksum_path)["hash"]
            if receipt["published_sha256"] != row["sha256"]:
                raise ValueError(f"Published checksum mismatch for {spec['key']}")
        else:
            receipt["verification"] = "OFFICIAL_CUTOFF_REST_RESPONSE_SHA256"
        receipt["response_receipt"] = row
    frame, stats = normalize(contents, spec)
    receipt["raw_sha256"] = sha256(contents).hexdigest()
    receipt["compressed_bytes"] = len(contents)
    normalized = directory / "normalized" / (spec["key"] + ".parquet")
    normalized.parent.mkdir(exist_ok=True, parents=True)
    temporary = normalized.with_name(f".{uuid4().hex}.tmp")
    try:
        frame.to_parquet(temporary, index=False, compression="zstd", compression_level=6)
        if fetch.bytes_left() < 0:
            raise Restricted("Normalized dataset would exceed the 256 MiB retained archive envelope")
        temporary.replace(normalized)
    finally:
        temporary.unlink(missing_ok=True)
    write_json(path, {**receipt, **stats, "status": "VERIFIED", "file": str(normalized),
                      "normalized_sha256": file_hash(normalized), "normalized_bytes": normalized.stat().st_size})


def failed(path, receipt, row, where):
    status = row["status"]
    terminal = status in (401, 403, 404, 410, 451)
    write_json(path, {**receipt, "status": "UNAVAILABLE" if terminal else "RETRY",
                      "where": where, "response_receipt": row})
    if status in (418, 429):
        raise Restricted(f"Provider backoff until {row.get('retry_at')}")
    if status in (401, 403, 451):
        if receipt["spec"]["kind"] == "fundingREST":
            return
        raise Restricted(f"Archive access denied by source ({status})")
    if not terminal:
        raise Restricted(f"Transport or provider error {status}; checkpointed for explicit resume")


def overview(plan):
    directory = folder(plan)
    rows = [read_json(path) for path in sorted((directory / "streams").glob("*.json"))]
    requests = [read_json(path) for path in (directory / "requests").glob("*.json")]
    sizes = [row for row in rows if row["status"] == "VERIFIED"]
    effective = []
    for row in sizes:
        overlay = directory / "overlays" / f'{row["spec"]["key"]}.json'
        effective.append(read_json(overlay) if overlay.exists() else row)
    by_kind = {}
    for kind in ("klines", "markPriceKlines", "fundingRate", "fundingREST"):
        items = [row for row in effective if row["spec"]["kind"] == kind]
        by_kind[kind] = {"objects": len(items), "rows": sum(row["rows"] for row in items),
                         "first": min((row["first"] for row in items), default=None),
                         "last": max((row["last"] for row in items), default=None)}
    return {"plan": plan["id"], "objects_verified": len(sizes), "objects_planned": len(plan["body"]["sources"]),
            "requests_used": len(requests), "requests_remaining": plan["body"]["acquisition"]["max_get_requests"]-len(requests),
            "downloaded_bytes": sum(row.get("bytes", 0) for row in requests), "retained_bytes": size_under(directory),
            "normalized_bytes": sum(row["normalized_bytes"] for row in sizes),
            "effective_normalized_bytes": sum(row["normalized_bytes"] for row in effective),
            "coverage": by_kind, "problems": [row for row in rows if row["status"] != "VERIFIED"],
            "batches_started": len(list((directory / "batches").glob("*.json")))}


def acquire(plan=None, *, max_batches=1, preflight=False):
    plan = plan or freeze_plan()
    directory = folder(plan)
    cap = plan["body"]["acquisition"]
    specs = plan["body"]["sources"]
    # Prove the launch month and the embargo funding endpoint before bulk work.
    if preflight:
        specs = [spec for spec in specs if spec["start"].startswith(("2019-09-", "2019-10-"))
                 or spec["kind"] == "fundingREST"]
    if any(utc(item["end"]) > utc(plan["body"]["holdout"]["start"]) for item in specs):
        raise ValueError("Acquisition plan crosses sealed holdout")
    if not 1 <= max_batches <= (cap["max_get_requests"] + cap["requests_per_batch"] - 1) // cap["requests_per_batch"]:
        raise ValueError("Invalid bounded batch count")
    psutil.Process().nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    with writer_lock(directory / ".acquire.lock"), threadpool_limits(limits=2):
        budget = plan["body"]["parent_budget"]
        terms = directory / "terms.md"
        for _ in range(max_batches):
            existing = overview(plan)
            retries = [row["response_receipt"].get("retry_at") for row in existing["problems"]]
            if any(value and utc_now() < utc(value) for value in retries):
                break
            if (inventory()["disk_free_mib"] < budget["min_free_disk_mib"] + ACQUISITION_RESERVE_MIB
                    or inventory()["ram_available_mib"] < budget["min_available_ram_mib"]):
                break
            finished = {row["spec"]["key"] for row in existing["problems"] if row["status"] == "UNAVAILABLE"}
            remaining = [spec for spec in specs if spec["key"] not in finished and
                         (not manifest_path(directory, spec).exists()
                          or read_json(manifest_path(directory, spec))["status"] != "VERIFIED")]
            if not remaining:
                break
            batch = existing["batches_started"] + 1
            batch_path = directory / "batches" / f"{batch:03}.json"
            begin = utc_now()
            fetch = Download(plan, directory, batch)
            write_json(batch_path, {"start": begin.isoformat(), "status": "STARTED", "remaining_objects": len(remaining)})
            outcome = "COMPLETE"
            try:
                with httpx.Client(timeout=8, follow_redirects=False) as client:
                    if not terms.exists():
                        row, content = fetch.get(client, TERMS)
                        if (content is None or sha256(content).hexdigest() !=
                                plan["body"]["source"]["terms_cached_sha256"]):
                            raise Restricted("Current source terms differ from the reviewed, frozen evidence or are unavailable")
                        put_bytes(terms, content)
                    elif file_hash(terms) != plan["body"]["source"]["terms_cached_sha256"]:
                        raise Restricted("Frozen terms checksum changed")
                    for spec in remaining:
                        if inventory()["disk_free_mib"] < budget["min_free_disk_mib"]:
                            raise Restricted("Workstation free-disk reserve")
                        handle_object(client, fetch, spec)
            except BatchLimit as exc:
                outcome = str(exc)
            except (ValueError, OSError, Restricted) as exc:
                outcome = str(exc)
                break
            finally:
                write_json(batch_path, {"start": begin.isoformat(), "end": utc_now().isoformat(), "requests": fetch.count,
                                        "received_bytes": fetch.used, "status": outcome})
        report = overview(plan)
        write_json(directory / "coverage.json", report)
        return report


ACQUISITION_RESERVE_MIB = 256
