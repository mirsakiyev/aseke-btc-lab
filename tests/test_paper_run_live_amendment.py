from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from lab.learning.simulator import Order
from lab.paper_run import ema, execution, gate, runtime, source_amendment
from lab.phase15 import paper
from lab.storage import file_hash, utc, write_json
from test_paper_run_ema import successor
from test_paper_run_runtime import REGISTERED, documents


APPROVED = utc("2026-11-17T06:16:04Z")
EFFECTIVE = utc("2026-11-17T06:17:00Z")
CONFIRMED = EFFECTIVE + pd.Timedelta(seconds=5)
ELIGIBLE = EFFECTIVE + pd.Timedelta(minutes=1)
TICK = ELIGIBLE + pd.Timedelta(milliseconds=50)
SOURCE_EPOCH = APPROVED - pd.Timedelta(seconds=10)
QUALIFICATION = {"state": "QUALIFIED", "transport": "websocket_v2_trade"}
DAY, SWING, POSITION = source_amendment.SLOTS


def stamp(value):
    return utc(value).isoformat()


def mono(value):
    return (utc(value) - SOURCE_EPOCH).value


@pytest.fixture
def generation(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, "ROOT", tmp_path / "current")
    marker, body = documents(gate.ROOT)
    for slot in source_amendment.SLOTS:
        lane = body["lanes"][slot]
        original = successor(lane["profile"])
        body["lanes"][slot] = {**original, **lane, "risk": original["risk"],
                              "entry_model": original["entry_model"], "exit_model": original["exit_model"],
                              "qualification": {"state": "BLOCKED", "reason": "STALE_REFERENCE"}}
    write_json(gate.ROOT / "legacy-retired.json", marker)
    record = runtime.initialize(marker, {}, now=REGISTERED, prepared=body)
    now = [TICK + pd.Timedelta(milliseconds=125)]
    monkeypatch.setattr(runtime, "utc_now", lambda: now[0])
    monkeypatch.setattr(execution, "utc_now", lambda: now[0])
    monkeypatch.setattr(execution, "time", SimpleNamespace(monotonic_ns=lambda: mono(now[0])))
    monkeypatch.setattr(runtime, "synchronized", lambda _: True)
    return record, now


def live_point(event_id, *, price="99.876500", at=TICK, receipt=None, epoch=1):
    returned = at + pd.Timedelta(milliseconds=100) if receipt is None else receipt
    requested, ack = APPROVED, APPROVED + pd.Timedelta(milliseconds=150)
    return {"source": ema.SOURCE, "symbol": ema.SYMBOL, "observation_kind": execution.WS_TRADE,
            "event_id": str(event_id), "price": float(price), "price_text": price,
            "source_at": stamp(at), "received_at": stamp(returned), "received_mono_ns": mono(returned),
            "subscription_requested_at": stamp(requested), "subscription_ack_at": stamp(ack),
            "subscription_ack_mono_ns": mono(ack), "subscription_epoch": epoch}


def intent(lane, amendment, *, side=1, expiry=ELIGIBLE + pd.Timedelta(minutes=3)):
    policy = ema.synthetic_btc.model(lane["profile"])
    stop, target = 100. - 5. * side, 100. + 5. * side
    plan = {"direction": side, "eligible_from": ELIGIBLE, "entry_expiry": expiry,
            "entry": {"minimum": 98., "maximum": 102.}, "stop": stop, "signal_stop": stop,
            "target": target, "signal_target": target, "atr": 2., "max_hold_minutes": 90, "session_flat": None,
            "contract": asdict(policy.contract), "trigger_basis": ema.SOURCE, "cost": {"taker": policy.taker_fee},
            "model_financing_rate": policy.funding_cost_rate, "model_financing_interval_minutes": policy.funding_interval_minutes,
            "order": asdict(Order(2., 100. + side * 2, 1000., 25., 12., policy.leverage, None, None)),
            "source_amendment_id": amendment["id"]}
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        assert paper.publish(db, book, {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": EFFECTIVE,
                             "direction": "LONG" if side > 0 else "SHORT", "plan": plan,
                             "strategy_version": "fixture", "exit_policy": "signal_atr_bracket_v1"}, CONFIRMED)
        paper.save(db, book)
    return plan


def history(lane, kind):
    with closing(runtime.connect(readonly=True)) as db:
        return [json.loads(row[0]) for row in db.execute("SELECT body FROM event WHERE book=? AND kind=? ORDER BY rowid",
                                                       (lane["book_id"], kind))]


def saved_book(lane):
    with closing(runtime.connect(readonly=True)) as db:
        return paper.load(db, lane)


def test_source_amendment_is_future_only_and_preserves_every_account_and_horizon(generation):
    record, _ = generation
    originals = deepcopy(record)
    pointer = file_hash(gate.ROOT / "current.json")
    ledger = file_hash(gate.ROOT / "paper.sqlite3")
    assert source_amendment.active(record) is None
    with pytest.raises(ValueError, match="sustained"):
        source_amendment.install(record, {"state": "BLOCKED"}, now=APPROVED)
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    assert source_amendment.active(record) == amendment
    assert source_amendment.install(record, QUALIFICATION, now=EFFECTIVE) == amendment
    assert utc(amendment["body"]["effective_at"]) == EFFECTIVE
    assert runtime.current() == originals == record
    assert file_hash(gate.ROOT / "current.json") == pointer
    assert file_hash(gate.ROOT / "paper.sqlite3") == ledger

    lanes = source_amendment.lanes(record, amendment)
    for slot, lane in lanes.items():
        original = originals["body"]["lanes"][slot]
        for key in ("book_id", "legacy_book_id", "publication_end_exclusive", "follow_end_exclusive", "source", "price_basis"):
            assert lane[key] == original[key]
        if slot in source_amendment.SLOTS:
            assert not runtime._can_enter(lane, record["body"], EFFECTIVE - pd.Timedelta(nanoseconds=1))
            assert runtime._can_enter(lane, record["body"], EFFECTIVE)
            assert lane["observation_kind"] == execution.WS_TRADE
        else:
            assert lane == original


def test_daily_chart_boundary_must_also_follow_amendment_effective_time(generation, monkeypatch):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = {**source_amendment.lanes(record, amendment)[POSITION], "timeframe": "1D"}
    calls = []

    def confirm(_, __, ___, at, *, db):
        calls.append(at)
        return {"state": "EVALUATED", "signal": {"decision": "NO_TRADE"}}

    monkeypatch.setattr(ema, "confirmation", confirm)
    with closing(runtime.connect()) as db, db:
        book = paper.load(db, lane)
        book_map = {POSITION: book}
        now[0] = EFFECTIVE + pd.Timedelta(hours=1)
        runtime.evaluate(db, record, book_map, {POSITION: lane}, now[0], {})
        assert not calls and "last_evaluation" not in book
        now[0] = EFFECTIVE.ceil("1D") + pd.Timedelta(seconds=4)
        runtime.evaluate(db, record, book_map, {POSITION: lane}, now[0], {})
        assert calls == [now[0]]
        assert book["last_evaluation"]["boundary"] == stamp(EFFECTIVE.ceil("1D"))


def test_live_consumer_prices_long_short_exactly_and_rejects_position_short(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lanes = source_amendment.lanes(record, amendment)
    long = intent(lanes[DAY], amendment)
    short = intent(lanes[SWING], amendment, side=-1)
    intent(lanes[POSITION], amendment, side=-1)
    early = live_point(90, at=ELIGIBLE - pd.Timedelta(milliseconds=1))
    runtime.live_prices(record, amendment, [early], {})
    for slot in (DAY, SWING, POSITION):
        assert not history(lanes[slot], "HYPOTHETICAL_FILL")
        assert history(lanes[slot], "ENTRY_EVIDENCE_REJECTED")[-1]["reason"] == "REFERENCE_PRECEDES_ELIGIBILITY"
    observed = live_point(91)
    assert execution.valid_snapshot(observed, lanes[DAY], now[0], mono(now[0])) is None
    runtime.live_prices(record, amendment, [observed], {})
    for slot, plan, direction in ((DAY, long, 1), (SWING, short, -1)):
        fill, = history(lanes[slot], "HYPOTHETICAL_FILL")
        assert fill["price"] == observed["price"]
        assert fill["reference_price_text"] == "99.876500"
        assert fill["observation_kind"] == execution.WS_TRADE
        assert fill["exchange_source_at"] == observed["source_at"]
        assert fill["source_received_at"] == observed["received_at"]
        assert fill["signal_boundary"] == stamp(EFFECTIVE)
        assert fill["confirmed_at"] == stamp(CONFIRMED)
        assert fill["eligible_from"] == stamp(ELIGIBLE)
        assert fill["receipt_to_commit_ns"] == 25_000_000
        assert fill["observation_age_at_commit_ns"] == 125_000_000
        assert fill["entry_adverse_rate"] == fill["exit_adverse_rate"] == 0.
        assert fill["target"] == plan["target"] and fill["target_basis"] == "CONFIRMATION_CLOSE"
        assert saved_book(lanes[slot])["position"]["position"]["direction"] == direction
    assert history(lanes[POSITION], "INVALIDATED")[0]["reason"] == "DIRECTION_OUTSIDE_POLICY"
    assert saved_book(lanes[POSITION])["position"] is None


@pytest.mark.parametrize("staged_ms,expiry_ms,terminal,reason", [
    (401, 400, "EXPIRED", "NO_QUALIFYING_FORWARD_REFERENCE"),
    (2001, 4000, "ENTRY_EVIDENCE_REJECTED", "LEDGER_COMMIT_STALE_REFERENCE"),
])
def test_ledger_stage_cannot_commit_an_expired_or_aged_websocket_fill(
        generation, monkeypatch, staged_ms, expiry_ms, terminal, reason):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment, expiry=TICK + pd.Timedelta(milliseconds=expiry_ms))
    save = paper.save

    def delay_staged_fill(db, state):
        if state["position"] is not None:
            now[0] = TICK + pd.Timedelta(milliseconds=staged_ms)
        save(db, state)

    monkeypatch.setattr(paper, "save", delay_staged_fill)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    assert not history(lane, "HYPOTHETICAL_FILL")
    assert history(lane, terminal)[0]["reason"] == reason
    after = saved_book(lane)
    assert not after["position"] and after["cash"] == 10000. and after["fees"] == 0.
    assert bool(after["pending"]) == (terminal != "EXPIRED")


def test_equal_exchange_times_accept_increasing_trade_id_and_protect_across_reconnect(generation):
    record, now = generation
    amendment = source_amendment.install(record, QUALIFICATION, now=APPROVED)
    lane = source_amendment.lanes(record, amendment)[DAY]
    intent(lane, amendment)
    runtime.live_prices(record, amendment, [live_point(91)], {})
    next_at = TICK + pd.Timedelta(seconds=1)
    now[0] = next_at + pd.Timedelta(milliseconds=125)
    mark = live_point(92, price="100.102400", at=next_at)
    runtime.live_prices(record, amendment, [mark], {})
    assert saved_book(lane)["position"]["last_mark"] == 100.1024

    # Every live_prices call opens SQLite again. A repeated ID with a
    # different price on a new connection must not touch the position.
    duplicate = live_point(92, price="94.876500", at=next_at, epoch=2)
    runtime.live_prices(record, amendment, [duplicate], {})
    assert saved_book(lane)["position"]["last_mark"] == 100.1024
    assert history(lane, "PROTECTION_SAMPLE_REJECTED")[0]["reason"] == "DUPLICATE_OR_REORDERED_REFERENCE"
    assert not history(lane, "CLOSED")

    now[0] = next_at + pd.Timedelta(milliseconds=150)
    runtime.live_prices(record, amendment, [], {"reason": "FIXTURE_DISCONNECT"}, detect_gap=True)
    assert saved_book(lane)["position"]["unresolved_protection"]
    assert history(lane, "PROTECTION_GAP")[0]["reason"] == "KRAKEN_STREAM_DISCONTINUITY"
    assert not history(lane, "EXIT_OBSERVED")

    recovered = live_point(93, price="94.876500", at=next_at, epoch=2)
    runtime.live_prices(record, amendment, [recovered], {})
    closed, = history(lane, "CLOSED")
    assert closed["source_event_id"] == "93" and closed["reason"] == "stop"
    assert closed["exit_price"] == 94.8765 and closed["reference_price_text"] == "94.876500"
    assert closed["provisional"] and closed["exit_adverse_rate"] == 0.
    assert saved_book(lane)["problem"] == "UNRESOLVED_PROTECTION_INTERVAL"
    with closing(runtime.connect(readonly=True)) as db:
        assert runtime._source_state(db, ema.SOURCE)["last_observation"]["event_id"] == "93"
