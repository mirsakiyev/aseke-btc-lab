"""Fetch one exact historical Kraken chart candle through the shared receipt store."""

import sqlite3

import pandas as pd

from lab.ema_paper import kraken_reference as reference
from lab.kraken_paper import authorization, source
from lab.storage import utc_now
from lab.strategy_lab.market_data import Purpose
from lab.strategy_lab.spot_sources import KRAKEN_MAX_ROWS, SpotCandleRequest, Venue


REPAIR_COOLDOWN = pd.Timedelta(minutes=15)
PROFILE_MINUTES = {"day": 15, "swing": 240, "position": 1440}
ATTEMPT_SCHEMA = """
CREATE TABLE IF NOT EXISTS historical_repair_attempt(
    stream TEXT NOT NULL, minutes INTEGER NOT NULL, opened INTEGER NOT NULL,
    attempt_at INTEGER NOT NULL, retry_at INTEGER NOT NULL,
    PRIMARY KEY(stream,minutes,opened)
);
"""


def repair(close_at, timeframe, *, now=None, root=reference.ROOT):
    """Capture a missing chart candle, keeping its actual late receipt clocks."""
    step = pd.Timedelta(timeframe)
    if step not in {pd.Timedelta(minutes=minutes) for minutes in source.CHART_MINUTES}:
        raise ValueError("Recovery accepts only the registered Kraken chart intervals")
    end = authorization.utc(close_at)
    wall = utc_now()
    as_of = wall if now is None else authorization.utc(now)
    if as_of > wall or end != end.floor(step) or end > as_of:
        raise ValueError("Historical repair requires an aligned, already closed UTC candle")
    start = end - step
    result = {"mode": "HISTORICAL_CHART_REPAIR", "open_at": start.isoformat(), "close_at": end.isoformat(),
              "interval_seconds": int(step.total_seconds())}
    root = reference._root(root)
    report = reference.qualification(root=root)
    if not report:
        raise PermissionError("Kraken EMA reference history has not been qualified")
    scope = reference._qualified_permission(report, wall)
    if start < scope.approved_at:
        raise PermissionError("Requested repair precedes the reviewed prospective collection scope")
    with source._writer(root / "source") as db:
        return {**result, **_repair_one(db, scope, start, end, step)}


def _repair_one(db, scope, start, end, step, *, rate_bound=False):
    minutes = int(step / pd.Timedelta(minutes=1))
    key = (scope.source.id, minutes, start.value)
    prior = db.execute("SELECT count(*) FROM observation WHERE stream=? AND minutes=? AND opened=?", key).fetchone()[0]
    if prior:
        return {"state": "ALREADY_PRESENT" if prior == 1 else "CONFLICT"}
    wall = utc_now()
    if start < wall.floor(step) - KRAKEN_MAX_ROWS * step:
        return {"state": "OUTSIDE_KRAKEN_HISTORY"}
    if rate_bound:
        db.execute(ATTEMPT_SCHEMA)
        retry = db.execute("SELECT retry_at FROM historical_repair_attempt WHERE stream=? AND minutes=? AND opened=?",
                           key).fetchone()
        if retry and retry[0] > wall.value:
            return {"state": "REPAIR_COOLDOWN"}
        _, source_retry, _, _ = source._rate_state(db, scope.source.id)
        if source_retry > wall.value:
            return source._deferred(db, scope.source.id)
        with db:
            db.execute("INSERT OR REPLACE INTO historical_repair_attempt VALUES (?,?,?,?,?)",
                       (*key, wall.value, (wall + REPAIR_COOLDOWN).value))
    request = SpotCandleRequest(Venue.KRAKEN, minutes, start, end)
    grant = scope.grant(Purpose.WARMUP, start, end)
    # Use the production immutable receipt writer while suppressing only this
    # connection's historical target from replacing the live checkpoint.
    db.execute("CREATE TEMP TRIGGER IF NOT EXISTS preserve_live_checkpoint BEFORE INSERT ON main.checkpoint "
               "BEGIN SELECT RAISE(IGNORE); END")
    with reference._client() as client:
        return reference._fetch(db, scope, grant, request, client, utc_now, reference.sleep)


def repair_pending(results, grants, *, root, now):
    """Select at most one fetch globally from the audited, currently granted missing chart ranges."""
    now = authorization.utc(now)
    wall = utc_now()
    if now > wall:
        raise ValueError("Historical repair cannot run before its audited cycle clock")
    grants = {grant.book_id: grant for grant in grants if grant.purpose == Purpose.FORWARD_DECISION
              and grant.kinds == {source.TRADE}}
    candidates = []
    for profile, response in results.items():
        protocol = response.get("source_protocol") or {}
        detail = protocol.get("chart_recovery") or {}
        closes = detail.get("missing_closes") or ()
        grant = grants.get(protocol.get("book_id"))
        if profile not in PROFILE_MINUTES or not closes or grant is None or not grant.approved_at <= now < grant.end:
            continue
        step = pd.Timedelta(minutes=PROFILE_MINUTES[profile])
        for close in closes:
            end = authorization.utc(close)
            start = end - step
            if (end == end.floor(step) and grant.start <= start < end <= min(grant.end, now)
                    and start >= wall.floor(step) - KRAKEN_MAX_ROWS * step):
                candidates.append((end, profile, step, grant))
    if not candidates:
        return {"state": "NO_ELIGIBLE_REPAIR"}
    root = reference._root(root)
    report = reference.qualification(root=root)
    if not report:
        return {"state": "BLOCKED_PERMISSION", "reason": "Kraken reference source is unqualified"}
    try:
        scope = reference._qualified_permission(report, wall)
        with source._writer(root / "source") as db:
            for end, profile, step, grant in sorted(candidates, key=lambda candidate: candidate[:2]):
                start = end - step
                if grant.source.id != scope.source.id or start < scope.approved_at:
                    continue
                result = _repair_one(db, scope, start, end, step, rate_bound=True)
                if result["state"] not in {"ALREADY_PRESENT", "CONFLICT", "REPAIR_COOLDOWN", "OUTSIDE_KRAKEN_HISTORY"}:
                    return {**result, "mode": "HISTORICAL_CHART_REPAIR", "profile": profile,
                            "open_at": start.isoformat(), "close_at": end.isoformat()}
    except PermissionError as exc:
        return {"state": "BLOCKED_PERMISSION", "reason": str(exc)}
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        return {"state": "WAITING_SOURCE", "reason": type(exc).__name__}
    return {"state": "NO_ELIGIBLE_REPAIR"}
