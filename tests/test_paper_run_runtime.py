from contextlib import closing
from dataclasses import asdict
import json
import subprocess
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.learning.simulator import Contract
from lab.paper_run import ema, execution, gate, runtime, sources
from lab.phase15 import paper
from lab.storage import utc, write_json, writer_lock


REGISTERED = utc("2026-11-17T06:10:00Z")
START = utc("2026-11-17T06:15:00Z")
NOW = utc("2026-11-17T06:16:04Z")
END = utc("2026-12-17T06:15:00Z")
MARK_BOOK = "breakout/day/fixed"
MARK_PENDING = "breakout/swing/fixed"
MARK_PENDING_2 = "breakout/swing/trail"
KRAKEN_BOOK = "ema/day"
SLOTS = (MARK_BOOK, MARK_PENDING, MARK_PENDING_2, "breakout/position/fixed", KRAKEN_BOOK, "ema/swing", "ema/position")
MARK_CONTRACT = Contract("linear_perp", "USDT", .001, 1., .1, 1., 999999., 10., .004, .005)


def documents(root):
    retired_body = {"path": str(root.parent / "archive"), "cutoff": REGISTERED.isoformat(), "slots": {
        slot: {"book_id": f"predecessor-{slot}", "registration_id": f"old-registration-{slot}"} for slot in SLOTS}}
    archive = {"id": fingerprint(retired_body), "body": retired_body}
    lanes = {}
    for slot, predecessor in retired_body["slots"].items():
        is_ema = slot.startswith("ema/")
        is_spot = slot == "breakout/position/fixed"
        source = ema.SOURCE if is_ema else sources.SPOT if is_spot else sources.MARK
        basis = "Kraken Spot last trade" if is_ema else "KuCoin Spot reference" if is_spot else "KuCoin mark"
        model = ema.LEVERAGED_MODE if is_ema else "FRESH_REFERENCE"
        lanes[slot] = {"book_id": fingerprint([slot, archive["id"]]), "family": "EMA" if is_ema else "BREAKOUT",
                       "source": source, "symbol": ema.SYMBOL if is_ema else "BTC-USDT" if is_spot else "XBTUSDTM",
                       "price_basis": basis, "entry_model": model, "exit_model": model, "label": slot,
                       "legacy_book_id": predecessor["book_id"], "legacy_registration": predecessor["registration_id"],
                       "long_only": is_spot or slot.endswith("/position"), "timeframe": "15min", "qualification": {"state": "QUALIFIED"},
                       "profile": slot.split("/")[1], "risk": {"starting_equity": 10000.},
                       "publication_end_exclusive": END.isoformat(), "follow_end_exclusive": (END + pd.Timedelta(days=7)).isoformat()}
    body = {"archive_id": archive["id"], "archive": retired_body["path"], "cutoff": retired_body["cutoff"],
            "protocol": runtime.VERSION, "start": START.isoformat(), "freshness_seconds": 2, "lanes": lanes, "config": {}}
    return archive, body


@pytest.fixture
def setup(tmp_path, monkeypatch):
    root = tmp_path / "current"
    monkeypatch.setattr(gate, "ROOT", root)
    monkeypatch.setattr(runtime, "utc_now", lambda: NOW)
    monkeypatch.setattr(runtime.monitoring, "controls", lambda: {
        "monitor": True, "paused": False, "ideas": {"day": True, "swing": True, "position": True}})
    return root, *documents(root)


def initialize(setup):
    root, marker, body = setup
    write_json(root / "legacy-retired.json", marker)
    record = runtime.initialize(marker, {}, now=REGISTERED, prepared=body)
    return record, body["lanes"]


def test_initialize_requires_verified_retirement_and_records_seven_new_endowments(setup):
    root, marker, body = setup
    with pytest.raises(ValueError, match="Retire and verify"):
        runtime.initialize(marker, {}, now=REGISTERED, prepared=body)
    record, lanes = initialize(setup)
    assert runtime.current() == record
    assert runtime.initialize(marker, {}, now=NOW, prepared=body) == record
    with closing(runtime.connect(readonly=True)) as db:
        assert db.execute("SELECT count(*) FROM book").fetchone()[0] == 7
        rows = db.execute("SELECT book,kind,body FROM event ORDER BY rowid").fetchall()
        assert len(rows) == 7 and all(kind == "OPENING_BALANCE" for _, kind, _ in rows)
        for book_id, _, raw in rows:
            event = json.loads(raw)
            lane = lanes[event["slot"]]
            assert lane["book_id"] == book_id != event["retired_book_id"]
            assert event["opening_balance"] == 10000.
            state = paper.load(db, lane)
            assert state["cash"] == state["equity"] == state["starting_equity"] == 10000.
            assert state["position"] is None and state["pending"] is None
            assert state["fees"] == state["drawdown"] == state["funding"] == state["gross"] == 0.
    view = runtime.status()
    assert set(view["books"]) == set(SLOTS)
    assert view["generation_id"] == record["id"] and view["archive"] == marker["body"]["path"]
    assert not (root / "last-launch.json").exists()


def test_interruption_after_ledger_commit_does_not_double_endowments(setup, monkeypatch):
    root, marker, body = setup
    write_json(root / "legacy-retired.json", marker)
    original = runtime.write_json

    def interrupted(path, value):
        if path.name == "current.json":
            raise OSError("simulated interruption before pointer write")
        original(path, value)

    monkeypatch.setattr(runtime, "write_json", interrupted)
    with pytest.raises(OSError, match="interruption"):
        runtime.initialize(marker, {}, now=REGISTERED, prepared=body)
    assert runtime.current() is None and gate.legacy_entries_closed()
    with closing(runtime.connect(readonly=True)) as db:
        assert db.execute("SELECT count(*) FROM event WHERE kind='OPENING_BALANCE'").fetchone()[0] == 7
    monkeypatch.setattr(runtime, "write_json", original)
    record = runtime.initialize(marker, {}, now=NOW)
    assert runtime.current() == record
    assert record["body"]["start"] == START.isoformat()
    with closing(runtime.connect(readonly=True)) as db:
        assert db.execute("SELECT count(*) FROM event WHERE kind='OPENING_BALANCE'").fetchone()[0] == 7


def test_windows_single_runner_lock_survives_supervision_and_allows_restart(setup, monkeypatch):
    if sys.platform != "win32":
        pytest.skip("This repository uses Windows process locks")
    root, _, _ = setup
    initialize(setup)
    child = (
        "from pathlib import Path; import sys; "
        "from lab.paper_run import runtime,gate; gate.ROOT=Path(sys.argv[1]); "
        "assert runtime.ensure_runner() == {'state':'RUNNER_LOCKED'}"
    )
    launched = []

    def spawn(args, **options):
        launched.append((args, options))
        return SimpleNamespace(pid=11 + len(launched))

    # The child must open a second Windows file handle before we fake launch.
    with writer_lock(root / ".runner.lock"):
        result = subprocess.run([sys.executable, "-c", child, str(root)], capture_output=True, text=True,
                                timeout=20, creationflags=subprocess.CREATE_NO_WINDOW)
        assert result.returncode == 0, result.stderr
        assert runtime.ensure_runner() == {"state": "RUNNER_LOCKED"}
    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    monkeypatch.setattr(runtime, "utc_now", lambda: NOW)
    assert runtime.ensure_runner()["state"] == "RUNNER_STARTED"
    assert runtime.ensure_runner()["state"] == "LAUNCH_IN_PROGRESS"
    assert launched[0][0][-3:] == ["-m", "lab.paper_run", "run"]
    assert launched[0][1]["creationflags"] & subprocess.CREATE_NO_WINDOW
    monkeypatch.setattr(runtime, "utc_now", lambda: NOW + pd.Timedelta(minutes=1))
    assert runtime.ensure_runner()["state"] == "RUNNER_STARTED"
    assert len(launched) == 2


def observation(at, *, source=sources.MARK, event="event", price=64300.07):
    return {"source": source, "symbol": "XBTUSDTM", "status": "RECEIVED", "event_id": event,
            "source_at": (at - pd.Timedelta(milliseconds=400)).isoformat(),
            "requested_at": (at - pd.Timedelta(milliseconds=300)).isoformat(),
            "received_at": (at - pd.Timedelta(milliseconds=100)).isoformat(), "price": price, "price_text": str(price),
            "request_mono_ns": 1_000_000_000, "received_mono_ns": 1_200_000_000, "elapsed_ns": 200_000_000}


def test_source_replay_is_blocked_after_reopening_the_database(setup, monkeypatch):
    _, lanes = initialize(setup)
    lane = lanes[MARK_BOOK]
    now = NOW
    observed = observation(now)
    fired = []
    monkeypatch.setattr(runtime, "_source_permission", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(execution, "clock", lambda: (now, 1_300_000_000))
    monkeypatch.setattr(runtime, "_observe", lambda source, previous: fired.append(previous) or observed)
    with closing(runtime.connect()) as db:
        assert runtime.sample(db, sources.MARK, lane, now, open_positions=True) == observed
        db.commit()
        assert runtime.sample(db, sources.MARK, lane, now, open_positions=True) is None
    now += pd.Timedelta(seconds=16)
    observed = observation(now)
    with closing(runtime.connect()) as db:
        rejected = runtime.sample(db, sources.MARK, lane, now, open_positions=True)
        assert rejected["status"] == "DUPLICATE_OR_REORDERED_REFERENCE"
        assert db.execute("SELECT count(*) FROM observation").fetchone()[0] == 1
        assert runtime._source_state(db, sources.MARK)["last_observation"]["source_at"] == observation(NOW)["source_at"]
    assert fired == [None, NOW.isoformat()]


def _idea(book, stop, *, at=START):
    return {"id": fingerprint([book["book_id"], at.isoformat()]), "published_at": at.isoformat(), "boundary": at.isoformat(),
            "strategy_version": "breakout", "exit_policy": "fixed", "plan": {
                "eligible_from": (at + pd.Timedelta(seconds=6)).isoformat(),
                "entry_expiry": (at + pd.Timedelta(minutes=8)).isoformat(),
                "direction": 1, "stop": stop, "entry": {"minimum": 64000., "maximum": 65100.}, "max_hold_minutes": 60,
                "cost": {"taker": .0006}, "contract": asdict(MARK_CONTRACT),
            }}


def _terms(book, lane, plan, price, at):
    return None, {"fee": price * .001 * .0006, "quantity": .001, "margin": price * .001 / 3,
                  "notional": price * .001, "target": 66000., "planned_risk": 40.}


def test_one_forward_sample_covers_shared_open_and_pending_books_during_other_source_outage(setup, monkeypatch):
    _, lanes = initialize(setup)
    monkeypatch.setattr(runtime.protocol, "current_metadata", lambda: {})
    monkeypatch.setattr(runtime, "_source_permission", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    monkeypatch.setattr(runtime, "_settle", lambda *_a, **_k: False)
    monkeypatch.setattr(runtime, "evaluate", lambda *_: None)
    monkeypatch.setattr(runtime.breakout, "trail", lambda *_: None)
    monkeypatch.setattr(runtime.breakout, "entry_terms", lambda b, l, p, metadata, config, price, at: _terms(b, l, p, price, at))
    monkeypatch.setattr(execution, "utc_now", lambda: NOW)
    monkeypatch.setattr(execution, "time", SimpleNamespace(monotonic_ns=lambda: 1_300_000_000))
    previous = NOW - pd.Timedelta(seconds=30)
    with closing(runtime.connect()) as db:
        for slot in (MARK_BOOK, MARK_PENDING, MARK_PENDING_2, KRAKEN_BOOK):
            lane = lanes[slot]
            book = paper.load(db, lane)
            book["pending"] = _idea(book, 64600. if slot == MARK_BOOK else 63000.)
            if slot == MARK_BOOK:
                execution.entry(db, book, lane, observation(previous, event="opened", price=64800.), _terms,
                                read_clock=lambda: (previous, 1_300_000_000))
                assert book["position"]
            paper.save(db, book)
        db.commit()
    calls = []

    def reference(source, previous_request):
        calls.append(source)
        if source == ema.SOURCE:
            raise OSError("entry-only Kraken source unavailable")
        return observation(NOW)

    monkeypatch.setattr(runtime, "_observe", reference)
    runtime.cycle()
    assert calls == [ema.SOURCE, sources.MARK]
    with closing(runtime.connect(readonly=True)) as db:
        closed = json.loads(db.execute("SELECT body FROM event WHERE book=? AND kind='CLOSED'",
                                       (lanes[MARK_BOOK]["book_id"],)).fetchone()[0])
        assert closed["reason"] == "stop" and closed["exit_price"] == 64300.07
        assert closed["exit_adverse_rate"] == 0.
        for slot in (MARK_PENDING, MARK_PENDING_2):
            book = paper.load(db, lanes[slot])
            assert book["position"]["position"]["entry"] == 64300.07
        assert paper.load(db, lanes[KRAKEN_BOOK])["position"] is None
        assert db.execute("SELECT count(*) FROM observation WHERE source=?", (sources.MARK,)).fetchone()[0] == 1
