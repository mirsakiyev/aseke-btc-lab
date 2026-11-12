from contextlib import closing
import json
import sqlite3

import numpy as np
import pandas as pd
import pytest

from lab.ema_paper import catalog, worker
from lab.learning.simulator import Contract
from lab.phase15 import paper
from lab.strategy_lab import ema_registration
from lab.strategy_lab.evaluator import compile_spec
from lab.strategy_lab.registry import connect, get, register
from test_ema_pullback_registration import baseline_spec


@pytest.fixture
def registered(tmp_path, monkeypatch):
    registry_root = tmp_path / "registry"
    paper_root = tmp_path / "paper"
    now = pd.Timestamp("2024-01-01T00:01:30Z")
    with connect(registry_root, write=True) as db:
        baselines = {name: register(db, baseline_spec(name)) for name in ("position", "swing")}
        day = baseline_spec("swing")
        day["strategy_id"] = "phase15/day/fixed"
        day["session"] = {"timezone": "UTC", "max_hold_minutes": 480,
                          "entry_cutoff_minute_utc": 1320, "flat_minute_utc": 1435}
        baselines["day"] = register(db, day)
        records = ema_registration.register_ema_pullback(db)
        pins = {row["spec"]["profile"]: row["hash"] for row in records if not row["strategy_id"].endswith("/source")}
    monkeypatch.setattr(catalog, "connect", lambda **kwargs: connect(registry_root, **kwargs))
    monkeypatch.setattr(catalog, "VERSION_PINS", pins)
    monkeypatch.setattr(catalog, "utc_now", lambda: now)
    comparison = {"id": "fixture-phase15", "body": {"lanes": {
        key: {"book_id": f"fixture/{key}"} for keys in catalog.BENCHMARKS.values() for key in keys
    }}}
    monkeypatch.setattr(catalog, "phase15_registration", lambda: comparison)
    registrations = catalog.register_books("Synthetic operator", root=paper_root)
    return paper_root, registry_root, baselines, pins, registrations


def test_day_registration_and_independent_balances(registered):
    paper_root, registry_root, baselines, pins, registrations = registered
    assert set(registrations) == {"day", "swing", "position"}
    assert registrations == catalog.register_books("Synthetic operator", root=paper_root)
    assert len({r["body"]["book_id"] for r in registrations.values()}) == 3
    assert all(registrations[name]["body"]["strategy_hash"] == value for name, value in pins.items())
    day = registrations["day"]["body"]
    spec = day["spec"]
    profile, *_ = compile_spec(spec)
    assert spec["implementation_version"] == "aseke_day_v1"
    assert spec["source"]["sha256"] == ema_registration.ARTIFACTS[0][1]
    assert spec["session"] == {"timezone": "UTC", "max_hold_minutes": 480}
    assert "entry_cutoff_minute_utc" not in profile and "flat_minute_utc" not in profile
    assert spec["sides"] == ["long", "short"] and day["execution_clock"]["entry_seconds"] == 900
    assert spec["risk"]["leverage"] == 3
    with connect(registry_root) as db:
        assert get(db, day["strategy_hash"])["state"] == "UNIT_TESTED"
        assert get(db, baselines["day"]["hash"])["spec"]["session"]["flat_minute_utc"] == 1435
        assert db.execute("SELECT COUNT(*) FROM dataset").fetchone()[0] == 0
    with closing(catalog.db_open(root=paper_root)) as db:
        books = [json.loads(row[0]) for row in db.execute("SELECT state FROM book")]
        assert [book["equity"] for book in books] == [10000, 10000, 10000]
        assert db.execute("SELECT COUNT(*) FROM event").fetchone()[0] == 0
        with pytest.raises(sqlite3.IntegrityError, match="immutable registration"):
            db.execute("UPDATE registration SET body='{}'")


def synthetic_chart(at):
    step = pd.Timedelta(minutes=1)
    opens = pd.date_range(end=at-step, periods=206 * 15, freq=step, tz="UTC")
    slots = np.arange(len(opens)) // 15
    close = 100 + slots * .2
    data = pd.DataFrame({"open": close, "high": close + .2, "low": close - .2, "close": close,
                         "volume": 1., "close_at": opens + step, "received_at": opens + step}, index=opens)
    data.loc[opens[-16], "low"] -= 12
    data.loc[opens[-15:], "volume"] = 3
    return data


def test_future_start_duplicate_cycle_and_weekend_entry(registered, monkeypatch):
    paper_root, _, _, _, registrations = registered
    lane = registrations["day"]["body"]
    at = pd.Timestamp("2024-01-06T23:45:00Z")
    frame = synthetic_chart(at)
    contract = Contract("linear_perp", "USDT", .001, 1., .1, 1., 250000., 5., .004, .003)

    def ready(lane, boundary, now, *, root):
        return {"state": "READY", "reason": "synthetic", "frame": frame, "contract": contract,
                "warmup_bars": 200, "required_bars": 200,
                "evidence": {"source_id": "fixture", "latest_receipt_at": (boundary + pd.Timedelta(seconds=20)).isoformat()}}

    def one_minute(lane, minute, now, *, root):
        price = float(frame.close.iloc[-1])
        bar = {"open_at": minute, "close_at": minute + worker.MINUTE, "received_at": now,
               "open": price, "high": price + .1, "low": price - .1, "close": price}
        return {"state": "READY", "contract": contract, "trade": bar, "mark": bar, "funding": [],
                "evidence": {"source_id": "fixture"}}

    monkeypatch.setattr(worker.source, "decision", ready)
    monkeypatch.setattr(worker.source, "execution", one_minute)
    # Complete readiness on Saturday without evaluating the already-closed candle.
    with closing(catalog.db_open(root=paper_root)) as db, db:
        state = worker._cycle_book(db, lane, at - pd.Timedelta(minutes=14), paper_root, float("inf"))
        assert state["first_eligible_at"] == at.isoformat()
        assert db.execute("SELECT COUNT(*) FROM event").fetchone()[0] == 0
    for _ in range(2):
        with closing(catalog.db_open(root=paper_root)) as db, db:
            state = worker._cycle_book(db, lane, at + worker.MINUTE, paper_root, float("inf"))
            assert state["state"] == "ACTIVE"
    with closing(catalog.db_open(root=paper_root)) as db, db:
        book = paper.load(db, lane)
        opening = at + pd.Timedelta(minutes=15)
        assert db.execute("SELECT COUNT(*) FROM event WHERE kind='PUBLISHED'").fetchone()[0] == 1
        assert pd.Timestamp(book["pending"]["plan"]["eligible_from"]) == opening
        worker._cycle_book(db, lane, opening + worker.MINUTE, paper_root, float("inf"))
        book = paper.load(db, lane)
        assert book["position"]["entry_at"] == opening.isoformat()
        assert book["position"]["deadline"] == (opening + pd.Timedelta(hours=8)).isoformat()
        assert paper.load(db, registrations["swing"]["body"])["cash"] == 10000
        assert paper.load(db, registrations["position"]["body"])["cash"] == 10000


def test_resource_guard_leaves_all_books_unfunded_by_events(registered, monkeypatch):
    paper_root, *_ = registered
    monkeypatch.setattr(worker.psutil, "virtual_memory", lambda: type("Memory", (), {"available": 1})())
    assert worker.cycle(root=paper_root) == {"state": "WAITING_FOR_RESOURCES"}
    with closing(catalog.db_open(True, root=paper_root)) as db:
        assert db.execute("SELECT count(*) FROM book").fetchone()[0] == 3
        assert db.execute("SELECT count(*) FROM event").fetchone()[0] == 0
