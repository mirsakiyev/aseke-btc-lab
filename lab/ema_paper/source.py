"""Explicitly scoped first-receipt handoff for prospective EMA paper books."""

from array import array
from contextlib import closing, contextmanager
from dataclasses import asdict
from hashlib import sha256
import json
from math import isfinite
from pathlib import Path
import shutil
import sqlite3

import pandas as pd

from lab.config import fingerprint
from lab.data import OHLCV, aggregate
from lab.learning.simulator import Contract
from lab.phase9.data import candles, matching, settled_rates
from lab.phase9.ideas import current_rules
from lab.phase9.public import INTERVALS, ROUTES, funding as funding_request, kline
from lab.phase13.future import expected_funding
from lab.storage import RUNS, utc, writer_lock


ROOT = RUNS / "ema_paper" / "source"
SCOPE = "EMA_PAPER_PROSPECTIVE_ONLY"
MINUTE = pd.Timedelta(minutes=1)
RAW_LIMIT = 2 * 1024**2
STORE_LIMIT = 512 * 1024**2
DISK_RESERVE = 2 * 1024**3
READ_LIMIT = 220_000
WARMUP = 200
METADATA_AGE = pd.Timedelta(hours=24)
SCHEMA = """
CREATE TABLE IF NOT EXISTS grant_scope(
    book TEXT PRIMARY KEY, stream TEXT NOT NULL, body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw(
    sha TEXT PRIMARY KEY, body BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS receipt(
    id TEXT PRIMARY KEY, stream TEXT NOT NULL, role TEXT NOT NULL,
    received TEXT NOT NULL, observed TEXT NOT NULL, raw_sha TEXT NOT NULL REFERENCES raw(sha), body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS receipt_stream_role ON receipt(stream,role,received);
CREATE TABLE IF NOT EXISTS observation(
    stream TEXT NOT NULL, role TEXT NOT NULL, source TEXT NOT NULL, close TEXT NOT NULL,
    receipt TEXT NOT NULL REFERENCES receipt(id), body TEXT NOT NULL, PRIMARY KEY(stream,role,source)
);
CREATE TABLE IF NOT EXISTS correction(
    stream TEXT NOT NULL, role TEXT NOT NULL, source TEXT NOT NULL, receipt TEXT NOT NULL REFERENCES receipt(id),
    PRIMARY KEY(stream,role,source,receipt)
);
"""


def timestamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Evidence timestamps require explicit UTC offsets")
    return stamp.tz_convert("UTC")


def encode(value):
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"), allow_nan=False)


@contextmanager
def database(root, *, write=False):
    root = Path(root)
    path = root / "receipts.sqlite3"
    if not write and not path.exists():
        yield None
        return
    if not write:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            yield db
        return
    with writer_lock(root / ".handoff.lock"):
        if shutil.disk_usage(root).free < DISK_RESERVE or (path.exists() and path.stat().st_size >= STORE_LIMIT):
            raise ValueError("EMA receipt handoff disk budget exhausted")
        with closing(sqlite3.connect(path)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.executescript(SCHEMA)
            for table in ("grant_scope", "raw", "receipt", "observation", "correction"):
                for action in ("UPDATE", "DELETE"):
                    db.execute(f"CREATE TRIGGER IF NOT EXISTS sealed_{table}_{action} BEFORE {action} ON {table} "
                               "BEGIN SELECT RAISE(ABORT,'EMA source evidence is immutable'); END")
            with db:
                yield db


def _scope(db, lane):
    return db.execute("SELECT * FROM grant_scope WHERE book=?", (lane["book_id"],)).fetchone() if db else None


def authorize(lane, descriptor, now, *, root=ROOT):
    """Freeze a separately attested local public-source grant for one paper book."""
    required = ("operator", "producer", "rights_reference")
    if (descriptor.get("scope") != SCOPE or descriptor.get("book_id") != lane["book_id"]
            or descriptor.get("venue") != "KuCoin" or descriptor.get("market") != lane["market"]
            or descriptor.get("instrument") != lane["instrument"]
            or descriptor.get("independent_forward_receipts") is not True
            or descriptor.get("protected_evidence_excluded") is not True
            or any(not isinstance(descriptor.get(field), str) or not descriptor[field].strip()
                   or len(descriptor[field]) > 1000 for field in required)):
        raise ValueError("Explicit independent KuCoin prospective source rights are required for this book")
    now = timestamp(now)
    if now < timestamp(lane["registered_at"]):
        raise ValueError("Source grant cannot precede the paper registration")
    fields = (*required, "scope", "venue", "market", "instrument", "independent_forward_receipts",
              "protected_evidence_excluded")
    body = {key: descriptor[key] for key in fields}
    stream = fingerprint(body)
    with database(root, write=True) as db:
        old = _scope(db, lane)
        if old:
            if old["stream"] != stream:
                raise ValueError("The immutable book's approved source stream already exists")
            return json.loads(old["body"])
        result = {**body, "source_id": stream, "book_id": lane["book_id"], "approved_at": now.isoformat()}
        db.execute("INSERT INTO grant_scope VALUES (?,?,?)", (lane["book_id"], stream, encode(result)))
        return result


def _normalize(lane, request, payload, received):
    """Validate official endpoint identity, then reuse the existing KuCoin decoders."""
    route, params = request["route"], request["params"]
    market, instrument = lane["market"], lane["instrument"]
    if request.get("url") != ROUTES.get(route) or route not in {"kline", "funding", "instrument", "contract", "isolated_tiers"}:
        raise ValueError("Handoff requires an official KuCoin public market data route")
    if route == "kline":
        interval, basis = params["interval"], params["klineType"]
        permitted = {"TRADE": {"1hour", "1min"}} if market == "SPOT" else {"TRADE": {"1min"}, "MARK_PRICE": {"1min"}}
        if interval not in permitted.get(basis, set()):
            raise ValueError("Unsupported prospective candle basis or native interval")
        left, right = map(timestamp, (request["start"], request["end_exclusive"]))
        step = pd.Timedelta(INTERVALS[interval])
        if not left < right <= received or left != left.floor(step) or right != right.floor(step) or (right-left)//step > 200:
            raise ValueError("Candle request must name a bounded, closed native range")
        reference = kline(request["key"], instrument, market, basis, interval, left, right)
        if params != reference["params"]:
            raise ValueError("Saved request does not match the frozen market and candle interval")
        if market == "FUTURES" and basis == "TRADE":
            # Existing decoder requires a multiplier. Retain raw contracts here and
            # apply the contemporaneous sourced multiplier on the decision path.
            frame = candles(payload, reference, received, multiplier=1)
        else:
            frame = candles(payload, reference, received)
        return ("trade" if basis == "TRADE" else "mark") + "/" + interval, frame
    if route == "funding":
        left, right = map(timestamp, (request["start"], request["end_exclusive"]))
        if (market != "FUTURES" or not left < right <= received or right-left > pd.Timedelta(days=7)
                or params != funding_request(request["key"], left, right)["params"]):
            raise ValueError("Funding request must name a bounded, settled XBTUSDTM range")
        return route, settled_rates(payload, request)
    if route == "instrument":
        if params != {"tradeType": market, "symbol": instrument}:
            raise ValueError("Instrument metadata request differs from the registered market")
        return route, matching(payload, instrument, market)
    if market != "FUTURES" or params:
        raise ValueError("Perpetual metadata requires the registered futures market and official route")
    if route == "contract":
        if (payload["symbol"] != instrument or payload["quoteCurrency"] != "USDT"
                or payload["settleCurrency"] != "USDT" or payload["baseCurrency"] not in {"XBT", "BTC"}
                or payload["isInverse"] or payload.get("isQuanto") or payload.get("expireDate") is not None):
            raise ValueError("Unsupported KuCoin perpetual contract")
    elif not isinstance(payload, list) or not payload or any(row["symbol"] != instrument for row in payload):
        raise ValueError("Missing isolated tier evidence for the registered perpetual")
    return route, payload


def ingest(lane, packet, now, *, root=ROOT):
    """Store immutable raw receipt and the normalized first observations for an approved stream."""
    now, received = timestamp(now), timestamp(packet["received_at"])
    if not isinstance(packet.get("raw_response"), str):
        raise ValueError("Handoff requires the original UTF-8 public response text")
    content = packet["raw_response"].encode("utf-8")
    digest = sha256(content).hexdigest()
    if not 0 < len(content) <= RAW_LIMIT or packet.get("raw_sha256") != digest:
        raise ValueError("Handoff raw response checksum or byte limit failed")
    decoded = json.loads(content)
    if packet.get("http_status") != 200 or not isinstance(decoded, dict) or decoded.get("code") != "200000" or "data" not in decoded:
        raise ValueError("Handoff requires a successful KuCoin public receipt")
    with database(root, write=True) as db:
        grant = _scope(db, lane)
        if not grant or packet.get("source_id") != grant["stream"]:
            raise ValueError("No matching independent prospective source grant")
        scope = json.loads(grant["body"])
        if packet.get("producer") != scope["producer"] or not timestamp(scope["approved_at"]) <= received <= now:
            raise ValueError("First receipt is outside the approved forward handoff")
        request = packet["request"]
        left = max(timestamp(scope["approved_at"]), timestamp(lane["registered_at"]))
        if request["route"] in {"kline", "funding"} and timestamp(request["start"]) < left:
            raise ValueError("Handoff request crosses its approved forward source boundary")
        role, normalized = _normalize(lane, request, decoded["data"], received)
        if isinstance(normalized, pd.DataFrame) and len(normalized) != len(decoded["data"]["list"]):
            raise ValueError("Handoff response includes data outside the declared closed range or repeats source rows")
        identifier = fingerprint([grant["stream"], packet["request"], received.isoformat(), digest])
        if db.execute("SELECT 1 FROM receipt WHERE id=?", (identifier,)).fetchone():
            return {"receipt_id": identifier, "state": "ALREADY_RECORDED"}
        path = Path(root) / "receipts.sqlite3"
        if path.stat().st_size + RAW_LIMIT * 2 >= STORE_LIMIT:
            raise ValueError("EMA receipt handoff storage ceiling reached")
        db.execute("INSERT OR IGNORE INTO raw VALUES (?,?)", (digest, content))
        db.execute("INSERT INTO receipt VALUES (?,?,?,?,?,?,?)", (identifier, grant["stream"], role,
                   received.isoformat(), now.isoformat(), digest,
                   encode(packet["request"] if isinstance(normalized, pd.DataFrame) else normalized)))
        count = 0
        if isinstance(normalized, pd.DataFrame):
            for row in normalized.to_dict("records"):
                source = row["settlement_at"] if role == "funding" else row["open_at"]
                close = row.get("close_at", source)
                if not left <= source <= close <= received:
                    continue
                key = (grant["stream"], role, source.isoformat())
                prior = db.execute("SELECT body FROM observation WHERE stream=? AND role=? AND source=?", key).fetchone()
                text = encode(row)
                if prior is None:
                    db.execute("INSERT INTO observation VALUES (?,?,?,?,?,?)", (*key, close.isoformat(), identifier, text))
                    count += 1
                elif prior["body"] != text:
                    db.execute("INSERT OR IGNORE INTO correction VALUES (?,?,?,?)", (*key, identifier))
        return {"receipt_id": identifier, "state": "RECORDED", "new_observations": count}


def _metadata(db, lane, stream, now, at, since):
    roles = ("instrument",) if lane["market"] == "SPOT" else ("instrument", "contract", "isolated_tiers")
    rows = {}
    for role in roles:
        found = db.execute("SELECT * FROM receipt WHERE stream=? AND role=? AND received>=? AND received<=? AND observed<=? "
                           "ORDER BY received DESC,observed DESC,id LIMIT 1",
                           (stream, role, since.isoformat(), at.isoformat(), now.isoformat())).fetchone()
        if found is None or timestamp(found["received"]) + METADATA_AGE <= at:
            raise ValueError(f"Current dated public {role} receipt is unavailable")
        rows[role] = found
    value = {role: json.loads(row["body"]) for role, row in rows.items()}
    if lane["market"] == "SPOT":
        reference = {"spot": value["instrument"]}
        capacity = {"minimum_notional": float(value["instrument"]["minFunds"]),
                    "maximum_quantity": float(value["instrument"]["maxBaseOrderSize"])}
    else:
        reference = {"perp": value["instrument"], "contract": value["contract"], "isolated": value["isolated_tiers"]}
        capacity = {"minimum_notional": 0., "maximum_quantity": float(value["contract"]["marketMaxOrderQty"])}
        if expected_funding(reference, at, at + MINUTE) is None:
            raise ValueError("Dated funding cycle or anchor is unavailable")
    fee = float(lane["source"].get("modeled_liquidation_fee", .003))
    contract = Contract.load(asdict(current_rules(lane["market"], reference, {"ideas": {"modeled_liquidation_fee": fee}})))
    if (not all(isfinite(x) and x >= 0 for x in (*capacity.values(),
            *(v for v in asdict(contract).values() if isinstance(v, (int, float)))))
            or capacity["maximum_quantity"] <= 0):
        raise ValueError("Contract or order capacity metadata is invalid")
    evidence = {**capacity, "metadata_raw_sha256": [row["raw_sha"] for row in rows.values()],
                "metadata_latest_provider_received_at": max(row["received"] for row in rows.values()),
                "metadata_latest_handoff_at": max(row["observed"] for row in rows.values())}
    return contract, reference, evidence


def _rows(db, stream, role, left, right, now):
    params = (stream, role, left.isoformat(), right.isoformat(), now.isoformat())
    return db.execute(
        "SELECT o.source,o.close,o.body,r.received,r.observed,r.raw_sha,"
        "EXISTS(SELECT 1 FROM correction c JOIN receipt cr ON cr.id=c.receipt WHERE c.stream=o.stream "
        "AND c.role=o.role AND c.source=o.source AND cr.observed<=?) AS corrected "
        "FROM observation o JOIN receipt r ON r.id=o.receipt WHERE o.stream=? AND o.role=? AND o.source>=? "
        "AND o.source<? AND r.observed<=? ORDER BY o.source",
        (params[-1], *params),
    )


def _bar(row):
    return {**json.loads(row["body"]), "received_at": row["received"], "observed_at": row["observed"],
            "raw_sha256": row["raw_sha"]}


def _empty(state, reason, **evidence):
    return {"state": state, "reason": reason, "frame": pd.DataFrame(), "contract": None,
            "warmup_bars": 0, "required_bars": WARMUP, "evidence": evidence}


def _trade_frame(rows, chart_step, grace, native_grace):
    """Build compact typed columns without retaining one Python dict per minute."""
    prices = {key: array("d") for key in OHLCV}
    times = {key: array("q") for key in ("open_at", "close_at", "received_at", "provider_received_at", "observed_at")}
    proof = sha256()
    for row in rows:
        close, received, observed = map(timestamp, (row["close"], row["received"], row["observed"]))
        bucket_end = (close - pd.Timedelta("1ns")).floor(chart_step) + chart_step
        available = max(received, observed)
        if row["corrected"] or available > min(close + native_grace, bucket_end + grace):
            continue
        values = json.loads(row["body"])
        for key in OHLCV:
            prices[key].append(values[key])
        for key, value in zip(times, (timestamp(row["source"]), close, available, received, observed)):
            times[key].append(value.value)
        proof.update(encode((row["source"], row["raw_sha"], row["received"], row["observed"])).encode())
    return pd.DataFrame({**prices, **{key: pd.to_datetime(values, unit="ns", utc=True)
                                    for key, values in times.items()}}).set_index("open_at"), proof.hexdigest()


def decision(lane, at, now, *, root=ROOT):
    """Read bounded current chart inputs. A first receipt plus handoff controls causality."""
    at, now = timestamp(at), timestamp(now)
    timeframe = lane["spec"]["timeframe"]
    chart_step = pd.Timedelta(timeframe)
    spot = lane["market"] == "SPOT"
    native = pd.Timedelta(hours=1) if spot else MINUTE
    role = "trade/1hour" if spot else "trade/1min"
    grace = pd.Timedelta(seconds=lane["source"].get("decision_receipt_seconds", 300))
    native_grace = (pd.Timedelta(hours=1) if spot else pd.Timedelta(minutes=15)) + grace
    with database(root) as db:
        if not (grant := _scope(db, lane)):
            return _empty("BLOCKED_PERMISSION", "Independent post-registration KuCoin source grant is missing")
        if at != at.floor(chart_step) or at > now or at <= timestamp(lane["registered_at"]):
            return _empty("WAITING_FOR_DATA", "Awaiting a completed post-registration decision boundary")
        scope = json.loads(grant["body"])
        stream = grant["stream"]
        since = timestamp(scope["approved_at"])
        left = max(since, timestamp(lane["registered_at"]))
        if (at-left)//native > READ_LIMIT:
            return _empty("BLOCKED_SOURCE", "Frozen EMA source clock exceeds the bounded native history budget",
                          source_id=stream, maximum_native_rows=READ_LIMIT)
        frame, proof = _trade_frame(_rows(db, stream, role, left, at, now), chart_step, grace, native_grace)
        status = _empty("WAITING_FOR_DATA", "No timely permitted trade candles", source_id=stream)
        if frame.empty:
            return status
        latest = frame.received_at.max()
        provider_latest = frame.provider_received_at.max()
        handoff_latest = frame.observed_at.max()
        completed = aggregate(frame, timeframe, native)
        tail = pd.date_range(end=at, periods=WARMUP, freq=chart_step, tz="UTC")
        warm = 0
        available_closes = set(completed.close_at)
        for close in reversed(tail):
            if close not in available_closes:
                break
            warm += 1
        status.update(frame=frame, warmup_bars=warm, reason=f"{warm}/{WARMUP} consecutive timely closed chart candles")
        latest_expected = int(WARMUP * chart_step // native)
        latest_native = int((frame.index >= at - WARMUP * chart_step).sum())
        status["evidence"].update(latest_receipt_at=latest.isoformat(), latest_provider_received_at=provider_latest.isoformat(),
                                  latest_handoff_at=handoff_latest.isoformat(), trade_receipts_sha256=proof,
                                  decision_receipt_deadline=(at+grace).isoformat(), native_rows=len(frame),
                                  expected_native_rows=latest_expected, trade_native_gaps=latest_expected-latest_native)
        if warm < WARMUP:
            return status
        if not spot:
            paired_mark = list(_rows(db, stream, "mark/1min", at-chart_step, at, now))
            if len(paired_mark) != int(chart_step // MINUTE) or any(
                row["corrected"] or timestamp(row["observed"]) > min(timestamp(row["close"]) + native_grace, at + grace)
                for row in paired_mark
            ):
                return {**status, "state": "BLOCKED_EXECUTION_EVIDENCE",
                        "reason": "Latest chart bucket lacks timely independent minute mark evidence"}
            provider_latest = max(provider_latest, *(timestamp(row["received"]) for row in paired_mark))
            handoff_latest = max(handoff_latest, *(timestamp(row["observed"]) for row in paired_mark))
            latest = max(provider_latest, handoff_latest)
            status["evidence"].update(latest_receipt_at=latest.isoformat(),
                                      latest_provider_received_at=provider_latest.isoformat(),
                                      latest_handoff_at=handoff_latest.isoformat())
        try:
            contract, _, metadata = _metadata(db, lane, stream, now, now, since)
        except (KeyError, TypeError, ValueError, StopIteration) as exc:
            return {**status, "state": "BLOCKED_EXECUTION_EVIDENCE", "reason": str(exc)}
        if not spot:
            # Prove the multiplier was unchanged throughout retained native inputs.
            # A fresh current snapshot by itself cannot validate earlier volume.
            snapshots = db.execute("SELECT body FROM receipt WHERE stream=? AND role='contract' AND received>=? "
                                   "AND received<=? AND observed<=?", (stream, since.isoformat(), now.isoformat(),
                                                                       now.isoformat())).fetchall()
            if not snapshots or any(float(json.loads(row["body"])["multiplier"]) != contract.base_per_contract
                                    for row in snapshots):
                return {**status, "state": "BLOCKED_EXECUTION_EVIDENCE",
                        "reason": "Compatible futures multiplier evidence is unavailable"}
            earliest = db.execute("SELECT MIN(received) FROM receipt WHERE stream=? AND role='contract' AND received>=? "
                                  "AND observed<=?", (stream, since.isoformat(), now.isoformat())).fetchone()[0]
            if timestamp(earliest) > frame.index[0]:
                return {**status, "state": "BLOCKED_EXECUTION_EVIDENCE",
                        "reason": "Futures multiplier receipt does not precede retained warmup candles"}
            frame = frame.assign(volume=frame.volume * contract.base_per_contract)
        return {**status, "state": "READY", "reason": "Complete causal inputs and dated instrument evidence",
                "contract": contract, "frame": frame,
                "evidence": {**status["evidence"], **metadata}}


def _funding(db, stream, metadata, at, now):
    end, tolerance = at + MINUTE, pd.Timedelta(seconds=5)
    expected = set(expected_funding(metadata, at, end + MINUTE))
    nominal = {}
    for row in _rows(db, stream, "funding", at - tolerance, end + tolerance, now):
        source = timestamp(row["source"])
        slot = source.round("min")
        if slot not in expected or abs(source-slot) > tolerance or row["corrected"] or slot in nominal:
            if at <= source < end or slot in expected:
                return None, "Unexpected, conflicting or duplicate public funding settlement", True
            continue
        nominal[slot] = _bar(row)
    if expected - nominal.keys():
        return None, "Waiting for observed settlement(s) on the frozen dated funding schedule", False
    return [row for row in nominal.values() if at <= timestamp(row["settlement_at"]) < end], None, False


def execution(lane, at, now, *, root=ROOT):
    """Return only closed, separately approved minute execution evidence."""
    at, now = timestamp(at), timestamp(now)
    empty = {"state": "WAITING_FOR_DATA", "trade": None, "mark": None, "funding": [], "contract": None,
             "permanent": False, "waiting_for_funding": False, "evidence": {}}
    if at != at.floor(MINUTE) or at <= timestamp(lane["registered_at"]) or at + MINUTE > now:
        return {**empty, "reason": "Awaiting a closed prospective execution minute"}
    with database(root) as db:
        if not (grant := _scope(db, lane)):
            return {**empty, "state": "BLOCKED_PERMISSION", "reason": "Independent KuCoin execution source grant is missing"}
        stream = grant["stream"]
        since = timestamp(json.loads(grant["body"])["approved_at"])
        if at < since:
            return {**empty, "state": "BLOCKED_PERMISSION", "reason": "Minute precedes this book's source grant"}
        spot = lane["market"] == "SPOT"
        bars = {}
        for basis in (("trade",) if spot else ("trade", "mark")):
            row = _rows(db, stream, basis + "/1min", at, at + MINUTE, now).fetchone()
            if row is None:
                return {**empty, "reason": f"Awaiting independent closed {basis} minute"}
            if row["corrected"]:
                return {**empty, "state": "BLOCKED_EXECUTION_EVIDENCE", "permanent": True,
                        "reason": f"Later correction conflicts with the first {basis} minute"}
            bars[basis] = _bar(row)
        if spot:
            bars["mark"] = bars["trade"]
        # A metadata snapshot must be available before the modeled open.
        try:
            contract, metadata, evidence = _metadata(db, lane, stream, at, at, since)
        except (KeyError, TypeError, ValueError, StopIteration) as exc:
            return {**empty, "state": "BLOCKED_EXECUTION_EVIDENCE", "reason": str(exc)}
        settlements, reason, permanent = ([], None, False) if spot else _funding(db, stream, metadata, at, now)
        if reason:
            return {**empty, "state": "BLOCKED_EXECUTION_EVIDENCE", "reason": reason, "permanent": permanent,
                    "waiting_for_funding": not permanent}
    records = [*bars.values(), *settlements]
    return {**empty, **bars, "state": "READY", "reason": None, "funding": settlements, "contract": contract,
            "evidence": {**evidence, "source_id": stream,
                         "latest_provider_received_at": max(row["received_at"] for row in records),
                         "latest_handoff_at": max(row["observed_at"] for row in records)}}
