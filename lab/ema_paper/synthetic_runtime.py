"""Advance the original EMA paper ledgers on timely Kraken Spot reference evidence."""

from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
import shutil
import sqlite3
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from lab.config import fingerprint
from lab.ema_paper import catalog, proxy_protocol, synthetic_protocol as protocol, worker
from lab.execution import execution_open
from lab.kraken_paper.worker import _minute_row, _trailing_start
from lab.phase15 import paper
from lab.storage import utc, utc_now, writer_lock
from lab.strategy_lab import ema_pullback, feature_warmup as warmup
from lab.strategy_lab.evaluator import proposal
from lab.strategy_lab.market_data import Purpose


MINUTE = pd.Timedelta(minutes=1)
MIB = 1024**2
SCHEMA = """
CREATE TABLE IF NOT EXISTS synthetic_seed (book TEXT PRIMARY KEY, digest TEXT NOT NULL, body TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS synthetic_seed_update BEFORE UPDATE ON synthetic_seed
    BEGIN SELECT RAISE(ABORT, 'immutable Kraken synthetic EMA feature seed'); END;
CREATE TRIGGER IF NOT EXISTS synthetic_seed_delete BEFORE DELETE ON synthetic_seed
    BEGIN SELECT RAISE(ABORT, 'immutable Kraken synthetic EMA feature seed'); END;
"""


def _policy(record, start, content_id, scopes, lane):
    step, processing = worker._clocks(lane)
    return warmup.WarmupPolicy(record["id"], lane["book_id"], utc(record["body"]["registered_at"]),
                               scopes[Purpose.WARMUP].id, scopes[Purpose.FORWARD_DECISION].id, content_id,
                               start, utc(record["body"]["seed_end"]), step, step, step, warmup.ema_indicators(), 200,
                               processing=processing)


def _seed(db, record, lane, scopes, data, now):
    book = lane["book_id"]
    existing = db.execute("SELECT digest,body FROM synthetic_seed WHERE book=?", (book,)).fetchone()
    if existing:
        identifier, payload = existing
        body = json.loads(payload)
        if fingerprint(body) != identifier or body["revision"] != record["id"]:
            raise ValueError("Frozen same-market feature seed failed verification")
        policy = _policy(record, utc(body["start"]), body["content_id"], scopes, lane)
        seed = warmup.freeze_seed(data, policy, as_of=utc(body["initialized_at"]))
        if seed.id != body["seed_id"]:
            raise ValueError("Kraken archived seed no longer matches its frozen state")
        return policy, seed
    grant = scopes[Purpose.WARMUP]
    if now < grant.end:
        raise ValueError(f"Waiting for the closed Kraken seam at {grant.end.isoformat()}")
    step, _ = worker._clocks(lane)
    snapshot = data.read(grant.id, book, Purpose.WARMUP, protocol.TRADE, grant.start, grant.end, now, step)
    start = _trailing_start(snapshot, step)
    if (grant.end - start) // step < 200:
        raise ValueError("Waiting for 200 contiguous completed Kraken candles ending at the frozen seam")
    if start != snapshot.start:
        snapshot = data.read(grant.id, book, Purpose.WARMUP, protocol.TRADE, start, grant.end, now, step)
    if snapshot.observations[-1].available_at > grant.end + pd.Timedelta(seconds=lane["source"]["decision_receipt_seconds"]):
        raise ValueError("The Kraken seam candle missed the registered first-receipt deadline")
    policy = _policy(record, start, snapshot.content_id, scopes, lane)
    seed = warmup.freeze_seed(data, policy, as_of=now)
    body = {"revision": record["id"], "start": start.isoformat(), "content_id": snapshot.content_id,
            "seed_id": seed.id, "initialized_at": now.isoformat(), "snapshot_id": snapshot.id,
            "bars": seed.bars, "values": dict(seed.values)}
    db.execute("INSERT INTO synthetic_seed VALUES (?,?,?)", (book, fingerprint(body), paper.encode(body)))
    return policy, seed


def _minute_bridge(data, grant, lane, now, seam):
    right = min(now.floor(MINUTE), grant.end)
    left = max(grant.start, right - 6 * MINUTE)
    if right - left < 3 * MINUTE:
        raise ValueError("Waiting for three independently received execution minutes after registration")
    snapshot = data.read(grant.id, grant.book_id, grant.purpose, protocol.TRADE, left, right, now, MINUTE)
    observations = [row for row in snapshot.observations if row.values["volume"] > 0 and
                    row.available_at <= row.end + pd.Timedelta(seconds=lane["source"]["price_receipt_seconds"])]
    tail = observations[-3:]
    if (len(tail) != 3 or tail[-1].end < seam or now - tail[-1].end >= 3 * MINUTE
            or any(current.end != following.start for current, following in zip(tail, tail[1:]))):
        raise ValueError("The timely Kraken one-minute execution bridge is incomplete")
    return snapshot.id, tail[-1]


def _fixed(db, record, lane, seed, data, grant, now):
    step, processing = worker._clocks(lane)
    evidence, latest = _minute_bridge(data, grant, lane, now, seed.end)
    if now < max(seed.source_available_at, latest.available_at) + processing:
        raise ValueError("Waiting for the frozen processing allowance after source qualification")
    first = execution_open(now, step)
    end = first + pd.Timedelta(days=lane["horizon_days"])
    follow = end + pd.Timedelta(minutes=lane["follow_through_minutes"])
    if follow > grant.end:
        raise PermissionError("Kraken execution grant does not cover the frozen follow-through")
    frozen = {"revision_id": record["id"], "seed_id": seed.id, "source_ready_at": now.isoformat(),
              "first_eligible_at": first.isoformat(), "primary_end_exclusive": end.isoformat(),
              "follow_end_exclusive": follow.isoformat(), "source_id": grant.source.id,
              "execution_bridge_snapshot_id": evidence}
    db.execute("INSERT INTO eligibility VALUES (?,?)", (lane["book_id"], paper.encode(frozen)))
    return frozen, latest


def _missed(db, book, at, now, reason):
    if book["pending"]:
        paper.expire(db, book, at, now, reason)
    elif book["position"]:
        paper.problem(db, book, reason, at, now, permanent=True)


def _advance(db, book, lane, data, grant, now, follow, deadline):
    from lab.strategy_lab import synthetic_btc

    if not book["cursor"] or book["permanent_block"]:
        return
    at = utc(book["cursor"])
    if at >= follow:
        _missed(db, book, at, now, "Synthetic follow-through ended with unresolved Kraken execution evidence")
        return
    right = min(now.floor(MINUTE), at + lane["limits"]["minutes_per_book"] * MINUTE, follow, grant.end)
    if at >= right:
        return
    snapshot = data.read(grant.id, grant.book_id, grant.purpose, protocol.TRADE, at, right, now, MINUTE)
    rows = {row.start: row for row in snapshot.observations}
    grace = pd.Timedelta(seconds=lane["source"]["price_receipt_seconds"])
    while book["cursor"] and at < right and time.monotonic() < deadline:
        row = rows.get(at)
        if row is None or row.available_at > row.end + grace or row.values["volume"] <= 0:
            if now > at + MINUTE + grace:
                _missed(db, book, at, now, "Kraken reference minute missing, late, or without trade volume")
            return
        if not synthetic_btc.advance(db, book, at, now, _minute_row(row)):
            return
        at += MINUTE


def _status_detail(record, lane):
    revision = record["body"]
    return {"synthetic": True, "source_mode": protocol.SOURCE_MODE, "feature_mode": protocol.SOURCE_MODE,
            "protocol": protocol.VERSION, "amendment_id": record["id"], "execution_model": revision["execution_model"],
            "execution_model_id": revision["execution_model_id"], "price_source": revision["price_source"],
            "book_id": lane["book_id"], "registration_id": revision["registration_id"], "strategy_hash": revision["strategy_hash"],
            "strategy_id": f"ema_pullback_trend_continuation/{lane['profile']}",
            "primary_leverage": lane["risk"]["leverage"], "counterfactual_leverage": revision["counterfactual_leverage"],
            "currency": "NOMINAL_USD_USDT_PARITY_V1", "telegram_state": "LOCAL_ONLY_KRAKEN_SCOPE",
            "actual_futures_mark": "NOT_AVAILABLE", "actual_funding": "NOT_AVAILABLE",
            "warmup_provider": "Kraken", "forward_provider": "Kraken", "forward_instrument": "BTC-USD",
            "warmup_count": 0, "warmup_seed_id": None, "latest_close_at": None, "archive_snapshot_id": None,
            "last_native_receipt_at": None, "current_indicators": None,
            "native_volume": {"complete": 0, "required": 20}, "native_swing": {"complete": 0, "required": 5},
            "readiness": {"same_market_seed_ready": False, "execution_minute_ready": False},
            "source_permission": "Reviewed private Kraken Spot data use"}


def _decision(db, book, lane, record, data, policy, seed, grant, fixed, state, at, now):
    from lab.strategy_lab import synthetic_btc

    if now > at + policy.publication_grace:
        state.update(last_evaluated_at=at.isoformat(), last_signal={"decision": "NO_TRADE", "reason": "Publication window elapsed"})
        return
    _, last_minute = _minute_bridge(data, grant, lane, now, seed.end)
    original, _, history = warmup._seed_data(data, policy, seed.initialized_at, None)
    if original != seed:
        raise ValueError("Decision history differs from the frozen same-source feature seed")
    ids, _, forward, available = warmup._forward(data, policy, seed.end, at, now)
    candles = pd.concat([history, forward])
    features = ema_pullback.features(candles, policy.timeframe, policy.timeframe)
    proof = {"source_mode": protocol.SOURCE_MODE, "execution_model": record["body"]["execution_model"],
             "execution_model_id": record["body"]["execution_model_id"], "protocol_amendment_id": record["id"],
             "price_source": "KRAKEN_BTC_USD_SPOT", "source_id": grant.source.id, "source_received_at": available.isoformat(),
             "publication_end_exclusive": fixed["primary_end_exclusive"], "feature_snapshot_id": fingerprint([seed.id, *ids])}
    signal = proposal(features, at, lane["spec"], data_hash=proof["feature_snapshot_id"],
                      position_open=bool(book["pending"] or book["position"] or book["problem"]))
    setup = signal.pop("setup")
    state.update(state="ACTIVE", reason="Timely Kraken reference evidence qualified", last_evaluated_at=at.isoformat(),
                 last_signal={**signal, "observed_at": now.isoformat(), "source_mode": protocol.SOURCE_MODE})
    state["source_protocol"].update(current_indicators={key: signal["values"].get(key) for key in
                                                        ("ema_fast", "ema_slow", "rsi", "atr", "volume_sma")},
                                    latest_close_at=at.isoformat(), last_native_receipt_at=available.isoformat(),
                                    last_execution_receipt_at=last_minute.received_at.isoformat())
    if setup is None:
        return
    setup = replace(setup, available_at=max(at, available) + policy.processing)
    model = synthetic_btc.model(lane["profile"])
    plan, reason = synthetic_btc.make_plan(lane, setup, book["equity"], now, proof, model)
    if not plan:
        state["last_signal"].update(decision="NO_TRADE", reason=reason)
    else:
        idea = paper.publish(db, book, {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": at, "plan": plan,
                                       "direction": "LONG" if setup.direction == 1 else "SHORT",
                                       "strategy_version": lane["strategy_version"], "exit_policy": setup.exit_policy}, now)
        if idea:
            state["last_signal"]["idea_id"] = idea["id"]
    try:
        state["source_protocol"]["counterfactual_risk"] = synthetic_btc.counterfactual_plans(lane, setup, book["equity"], now, proof)
        stress = {}
        for name in ("double_cost", "adverse_funding", "combined_adverse", "usd_usdt_minus_3pct", "usd_usdt_plus_3pct"):
            scenario = synthetic_btc.model(lane["profile"], scenario=name)
            candidate, failure = synthetic_btc.make_plan(lane, setup, book["equity"], now, proof, scenario)
            stress[name] = {"model_id": scenario.id, "sizing": candidate.get("sizing") if candidate else None,
                            "reason": failure, "counterfactual_only": True}
        state["source_protocol"]["stress_risk"] = stress
    except (ArithmeticError, ValueError) as exc:
        state["source_protocol"]["counterfactual_risk"] = {"state": "UNAVAILABLE", "reason": str(exc)}


def _cycle_book(db, original, record, scopes, data, now, deadline):
    from lab.strategy_lab import synthetic_btc

    lane, revision = original["body"], record["body"]
    book_id = lane["book_id"]
    if (original["id"] != revision["registration_id"] or book_id != revision["book_id"]
            or lane["strategy_hash"] != revision["strategy_hash"]
            or synthetic_btc.model(lane["profile"]).id != revision["execution_model_id"]):
        raise PermissionError("Original EMA registration or frozen synthetic execution model differs")
    fixed = worker._record(db, "eligibility", book_id)
    state = worker._record(db, "readiness", book_id)
    if state.get("source_protocol", {}).get("amendment_id") != record["id"]:
        state = {"source_protocol": _status_detail(record, lane)}
    detail = state["source_protocol"]
    book = paper.load(db, lane)
    step, processing = worker._clocks(lane)
    at = now.floor(step)
    state.update(last_checked_at=now.isoformat(), next_evaluation_at=(execution_open(at, step) + processing).isoformat())
    execution = scopes[Purpose.EXECUTION_EVIDENCE]
    try:
        if fixed and fixed.get("revision_id") != record["id"]:
            raise PermissionError("An earlier eligibility clock cannot be adopted by the synthetic paper study")
        if fixed:
            _advance(db, book, lane, data, execution, now, utc(fixed["follow_end_exclusive"]), deadline)
        if fixed and at >= utc(fixed["primary_end_exclusive"]):
            closed = db.execute("SELECT count(*) FROM event WHERE book=? AND kind='CLOSED'", (book_id,)).fetchone()[0]
            assessment = ("INSUFFICIENT_EVIDENCE" if closed < lane["min_complete_trades"] else
                          "UNRESOLVED" if book["problem"] or book["position"] else "REVIEW_REQUIRED")
            state.update(state="COMPLETE" if not (book["pending"] or book["position"]) or now >= utc(fixed["follow_end_exclusive"])
                         else "FOLLOW_THROUGH", reason="Frozen synthetic primary horizon ended", assessment=assessment,
                         next_evaluation_at=None)
        elif book["problem"]:
            state.update(state="BLOCKED_DATA", reason=book["problem"])
        elif not fixed or state.get("last_evaluated_at") != at.isoformat():
            policy, seed = _seed(db, record, lane, scopes, data, now)
            detail.update(warmup_count=seed.bars, warmup_seed_id=seed.id, archive_snapshot_id=seed.snapshot_id,
                          seam=seed.end.isoformat(), native_volume={"complete": 20, "required": 20},
                          native_swing={"complete": 5, "required": 5})
            detail["readiness"]["same_market_seed_ready"] = True
            if not fixed:
                detail.update(current_indicators=dict(seed.values), latest_close_at=seed.end.isoformat(),
                              last_native_receipt_at=seed.last_provider_receipt_at.isoformat())
                fixed, last = _fixed(db, record, lane, seed, data, execution, now)
                detail["readiness"]["execution_minute_ready"] = True
                detail["last_execution_receipt_at"] = last.received_at.isoformat()
            if at < utc(fixed["first_eligible_at"]):
                state.update(state="WAITING_FOR_START", reason="Qualified Kraken seed and execution minutes. Future boundary frozen",
                             next_evaluation_at=(utc(fixed["first_eligible_at"]) + processing).isoformat())
            else:
                _decision(db, book, lane, record, data, policy, seed, execution, fixed, state, at, now)
    except PermissionError as exc:
        state.update(state="BLOCKED_PERMISSION", reason=str(exc))
    except (ValueError, OSError, LookupError) as exc:
        state.update(state="WAITING_FOR_DATA", reason=str(exc))
    state.update(fixed)
    detail.update(state=state["state"], reason=state.get("reason"), first_eligible_at=state.get("first_eligible_at"),
                  next_evaluation_at=state.get("next_evaluation_at"))
    worker._save_status(db, book_id, state)
    paper.save(db, book)
    return {key: state.get(key) for key in ("state", "reason", "source_protocol", "first_eligible_at", "next_evaluation_at")}


def cycle(*, now=None, root=None, protocol_root=None, source_root=None):
    from lab.ema_paper import kraken_reference

    root = catalog.ROOT if root is None else Path(root)
    now = utc_now() if now is None else utc(now)
    saved = protocol.load(root=protocol_root, now=now)
    grants = protocol.grants(saved)
    data = kraken_reference.market_data(grants) if source_root is None else kraken_reference.market_data(grants, root=source_root)
    process, limits = psutil.Process(), catalog.LIMITS
    if (psutil.virtual_memory().available < limits["min_available_mib"] * MIB
            or shutil.disk_usage(root).free < limits["min_free_disk_mib"] * MIB
            or (root / "paper.sqlite3").stat().st_size >= limits["max_ledger_mib"] * MIB
            or process.memory_info().rss >= limits["worker_rss_mib"] * MIB):
        return {"state": "WAITING_FOR_RESOURCES", "synthetic": True}
    started = time.monotonic()
    deadline = started + limits["cycle_seconds"]
    with writer_lock(root / ".cycle.lock"), closing(catalog.db_open(root=root)) as db, threadpool_limits(limits=1):
        db.executescript(SCHEMA)
        results = {}
        for profile, original in catalog.registrations(db).items():
            if time.monotonic() >= deadline:
                results[profile] = {"state": "WAITING_FOR_RESOURCES"}
                continue
            record = saved["body"]["books"][profile]
            scopes = {grant.purpose: grant for grant in grants if grant.book_id == original["body"]["book_id"]}
            with db:
                results[profile] = _cycle_book(db, original, record, scopes, data, now, deadline)
    return {"state": "CHECKED", "synthetic": True, "revision_id": saved["id"], "books": results,
            "wall_seconds": round(time.monotonic() - started, 3), "rss_mib": round(process.memory_info().rss / MIB, 1)}


def protocol_status(*, registrations=None, runtime_books=None):
    originals = catalog.registrations() if registrations is None else registrations
    archive = proxy_protocol.status(registrations=originals)
    try:
        saved = protocol.load()
    except (OSError, ValueError, KeyError, sqlite3.Error) as exc:
        return {profile: {**row, "state": "BLOCKED_PERMISSION", "reason": str(exc)} for profile, row in archive.items()}
    runtime_books = worker.status()["books"] if runtime_books is None else runtime_books
    output = {}
    for profile, original in originals.items():
        record = saved["body"]["books"][profile]
        state = runtime_books.get(profile, {})
        details = state.get("source_protocol", {})
        if details.get("amendment_id") != record["id"]:
            details = {}
        prior = archive[profile]
        output[profile] = {**_status_detail(record, original["body"]),
                           "prior_archive_count": prior["warmup_count"], "prior_archive_snapshot_id": prior["archive_snapshot_id"],
                           "prior_archive_indicators": prior["seed_indicators"],
                           "last_signal": state.get("last_signal"),
                           "state": "WAITING_FOR_DATA", "reason": "Waiting for the frozen Kraken seam",
                           "seam": record["body"]["seed_end"], **details}
    return output
