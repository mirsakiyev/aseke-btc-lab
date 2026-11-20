"""A prospective transport amendment bound to the existing EMA books."""

import re

import pandas as pd

from lab.config import fingerprint
from lab.phase13.registry import sealed
from lab.paper_run import ema, execution, gate
from lab.storage import read_json, utc, utc_now, write_json, writer_lock


PROTOCOL = "ema-kraken-trade-websocket-v2"
ENDPOINT = "wss://ws.kraken.com/v2"
SLOTS = ("ema/day", "ema/swing", "ema/position")


def _clocks(record):
    return {slot: {"book_id": record["body"]["lanes"][slot]["book_id"],
                   "publication_end_exclusive": record["body"]["lanes"][slot]["publication_end_exclusive"],
                   "follow_end_exclusive": record["body"]["lanes"][slot]["follow_end_exclusive"]} for slot in SLOTS}


def active(record):
    pointer = gate.ROOT / "ema-live-source.json"
    if not pointer.exists():
        return None
    identifier = read_json(pointer)["id"]
    if not re.fullmatch(r"[a-f0-9]{64}", identifier):
        raise ValueError("Invalid EMA live source amendment identity")
    amendment = sealed(gate.ROOT / "source-amendments" / f"{identifier}.json", identifier)
    body = amendment["body"]
    if (body["protocol"] != PROTOCOL or body["generation"] != record["id"] or body["books"] != _clocks(record)
            or body["source"] != ema.SOURCE or body["symbol"] != ema.SYMBOL or body["endpoint"] != ENDPOINT
            or body["freshness_seconds"] != 2 or utc(body["effective_at"]) <= utc(body["registered_at"])):
        raise ValueError("EMA source amendment must preserve the registered identities, horizons and reference basis")
    return amendment


def install(record, qualification, *, now=None):
    now = utc_now() if now is None else now
    with writer_lock(gate.ROOT / ".writer.lock"):
        if gate.reset_pending():
            raise ValueError("Resume the pending generation reset before amending its source")
        pointer = gate.ROOT / "current.json"
        if pointer.exists() and read_json(pointer)["id"] != record["id"]:
            raise ValueError("The requested generation is no longer current")
        return _install_locked(record, qualification, now=now)


def _install_locked(record, qualification, *, now):
    if qualification.get("state") != "QUALIFIED":
        raise ValueError("A sustained Kraken trade-stream qualification is required")
    if prior := active(record):
        return prior
    effective = now.floor("min") + pd.Timedelta(minutes=1)
    books = _clocks(record)
    if any(effective >= utc(book["publication_end_exclusive"]) for book in books.values()):
        raise ValueError("An EMA publication horizon has already elapsed")
    body = {"protocol": PROTOCOL, "generation": record["id"], "books": books,
            "registered_at": now.isoformat(), "effective_at": effective.isoformat(),
            "source": ema.SOURCE, "symbol": ema.SYMBOL, "endpoint": ENDPOINT, "freshness_seconds": 2,
            "observation_kind": execution.WS_TRADE, "qualification": qualification}
    amendment = {"id": fingerprint(body), "body": body}
    write_json(gate.ROOT / "source-amendments" / f"{amendment['id']}.json", amendment)
    write_json(gate.ROOT / "ema-live-source.json", {"id": amendment["id"]})
    return amendment


def lanes(record, amendment):
    result = record["body"]["lanes"]
    if not amendment:
        return result
    body = amendment["body"]
    return {slot: {**lane, "source_effective_at": body["effective_at"], "source_amendment_id": amendment["id"],
                   "observation_kind": execution.WS_TRADE, "qualification": body["qualification"]}
            if slot in SLOTS else lane for slot, lane in result.items()}
