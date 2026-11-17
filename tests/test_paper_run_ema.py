from contextlib import closing
from dataclasses import replace
from hashlib import sha256
import sqlite3
from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

from lab.learning.simulator import Position
from lab.ema_paper.catalog import SOURCE_POLICY
from lab.paper_run import ema, execution
from lab.phase15 import catalog, paper
from lab.strategy_lab.market_data import MarketData, Observation, Purpose
from test_cross_venue_warmup import RAW, RAW_HASH, SEAM, STEP, snapshot as warmup_snapshot
from test_ema_paper_worker import registered
from test_strategy_lab_synthetic_btc import AT, MINUTE, bracket, lane


def successor(profile):
    original = lane(profile)
    mode = ema.CASH_MODE if profile == "position" else ema.LEVERAGED_MODE
    return {**original, "risk": {**original["risk"], "leverage": 1 if profile == "position" else 3},
            "source_policy": {**SOURCE_POLICY, **original["source"]}, "source": ema.SOURCE, "symbol": ema.SYMBOL,
            "execution_mode": mode, "entry_model": mode, "exit_model": mode, "price_basis": ema.SOURCE,
            "long_only": profile == "position"}


def snapshot(price, at, *, event_id="521"):
    returned = at + pd.Timedelta(milliseconds=125)
    return {"source": ema.SOURCE, "symbol": ema.SYMBOL, "price": price, "price_text": str(price),
            "source_at": at.isoformat(), "requested_at": at.isoformat(), "received_at": returned.isoformat(),
            "request_mono_ns": 0, "received_mono_ns": 125_000_000, "elapsed_ns": 125_000_000, "event_id": event_id}


def clock(at):
    return lambda: (at + pd.Timedelta(milliseconds=150), 150_000_000)


@pytest.mark.parametrize("profile,side", [("day", 1), ("day", -1), ("swing", 1), ("swing", -1), ("position", 1)])
def test_successor_ema_uses_exact_risk_sized_spot_reference_and_confirmation_target(tmp_path, monkeypatch, profile, side):
    monkeypatch.setattr(catalog, "ROOT", tmp_path)
    lane = successor(profile)
    book = paper.initial(lane)
    plan, reason = ema._plan(lane, book, bracket(side), {"source_mode": "KRAKEN_SPOT_REFERENCE"}, AT + MINUTE)
    assert reason is None
    # The fill is on the favorable side of the confirmation close. EMA's
    # original bracket permits it independently from the directional limit.
    price = 100. - side * .1234
    assert plan["entry"]["signal_reference"] == 100.
    assert "trigger" not in plan["entry"]
    assert plan["target"] == 100. + side * 5.
    assert plan["cost"]["adverse_proxy"] == 0.
    assert plan["model_financing_rate"] == (.0001 if profile != "position" else 0.)
    at = plan["eligible_from"] + pd.Timedelta(milliseconds=100)
    with closing(catalog.db_open()) as db:
        paper.publish(db, book, {"state": "ELIGIBLE_CONDITIONAL_IDEA", "source_cutoff": AT, "plan": plan,
                    "direction": "LONG" if side > 0 else "SHORT", "strategy_version": "fixture",
                    "exit_policy": "signal_atr_bracket_v1"}, AT + MINUTE)
        execution.entry(db, book, lane, snapshot(price, at),
                        lambda account, terms_lane, terms_plan, mark, now:
                        ema.entry_terms(account, terms_lane, terms_plan, None, None, mark, now), read_clock=clock(at))
        position = book["position"]
        quantity = position["position"]["remaining"]
        assert position["position"]["entry"] == price
        assert position["target"] == plan["target"]
        assert position["entry_evidence"]["fee"] == pytest.approx(quantity * price * .001)
        assert position["entry_evidence"]["margin"] == pytest.approx(quantity * price / lane["risk"]["leverage"])
        assert 0 < position["planned_risk"] <= 25.
        gap_exit = plan["target"] + side * .1234
        later = at + pd.Timedelta(seconds=3)
        execution.protect(db, book, lane, snapshot(gap_exit, later, event_id="522"), read_clock=clock(later))
        assert not book["position"]
        assert book["gross"] == pytest.approx(side * quantity * (gap_exit - price))
        assert book["fees"] == pytest.approx(quantity * (gap_exit + price) * .001)
        assert book["cash"] == pytest.approx(10000. + book["gross"] - book["fees"])


def test_cash_ema_rejects_short_and_bracket_overrun():
    cash = successor("position")
    cash_book = paper.initial(cash)
    assert ema._plan(cash, cash_book, bracket(-1), {}, AT + MINUTE)[1] == "INCOMPATIBLE_PROFILE_SIDE"
    plan, _ = ema._plan(cash, cash_book, bracket(), {}, AT + MINUTE)
    reason, _ = ema.entry_terms(cash_book, cash, plan, None, None, plan["target"], AT + MINUTE)
    assert reason == "EMA_ENTRY_OUTSIDE_FROZEN_BRACKET"


def test_confirmation_reuses_authorized_feature_context_and_waits_for_processing(tmp_path, monkeypatch, registered):
    prior_root, _, _, _, registrations = registered
    original = registrations["day"]
    native = warmup_snapshot()
    revision = ema.synthetic_protocol._lane(original, {"id": "prior amendment"}, native.source, SEAM - 5 * MINUTE)
    grants = tuple(map(ema.synthetic_protocol._grant, revision["body"]["grants"]))
    until = next(g.end for g in grants if g.purpose == Purpose.FORWARD_DECISION)
    lane = {**original["body"], "book_id": "successor-fixture", "legacy_book_id": original["body"]["book_id"],
            "feature_revision": revision, "feature_source_id": native.source.id, "feature_scope_end": until.isoformat(),
            "feature_recovery": None, "source_policy": original["body"]["source"], "execution_mode": ema.LEVERAGED_MODE}
    boundary = SEAM + STEP
    receipt = boundary + pd.Timedelta(seconds=4)
    history = list(native.observations)
    history[-1] = replace(history[-1], values={**history[-1].values, "low": 95.})
    history.append(Observation(native.source.id, ema.synthetic_protocol.TRADE, SEAM, boundary, receipt, receipt, RAW_HASH,
                               {"open": 107., "high": 109., "low": 106., "close": 108., "volume": 20_000.}))

    class Provider:
        def read(self, kind, start, end, as_of, interval):
            return (row for row in history if row.kind == kind and row.end - row.start == interval
                    and start <= row.start < end and row.available_at <= as_of)

        def raw(self, digest):
            assert digest == RAW_HASH
            return RAW

    monkeypatch.setattr(ema.kraken_reference, "scope_identity", lambda **_: native.source)
    monkeypatch.setattr(ema.kraken_reference, "market_data", lambda scopes, **_: MarketData(scopes, {native.source.id: Provider()}))
    lane["publication_end_exclusive"] = (boundary+2*STEP).isoformat()
    body = {"start": boundary.isoformat(),
            "ema_seed_ledger": str(prior_root / "paper.sqlite3")}
    initial = paper.initial(lane)
    before = sha256((prior_root / "paper.sqlite3").read_bytes()).hexdigest()
    with closing(sqlite3.connect(tmp_path / "successor.sqlite3")) as db:
        ema.init(db)
        early = ema.confirmation(lane, initial, body, receipt, db=db)
        assert early["state"] == "WAITING" and "Processing allowance" in early["reason"]
        later = ema.confirmation(lane, initial, body, boundary + MINUTE, db=db)
        assert later["state"] == "EVALUATED"
        plan = later["analysis"]["plan"]
        assert pd.Timestamp(plan["eligible_from"]) == boundary + STEP
        assert plan["evidence"]["source_id"] == native.source.id
        assert plan["evidence"]["source_received_at"] == receipt.isoformat()
        assert plan["stop"] == pytest.approx(plan["signal_stop"], abs=.01)
        assert plan["target"] == pytest.approx(108. + 2.5 * plan["atr"], abs=.01)
        assert db.execute("SELECT count(*) FROM synthetic_seed").fetchone() == (1,)
    assert sha256((prior_root / "paper.sqlite3").read_bytes()).hexdigest() == before


def test_ema_synthetic_financing_is_a_separate_debit_for_both_directions(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "ROOT", tmp_path)
    policy = successor("day")
    slot = AT.ceil("8h")
    at = slot + pd.Timedelta(milliseconds=100)
    with closing(catalog.db_open()) as db:
        for side in (-1, 1):
            book = paper.initial({**policy, "book_id": str(side)})
            pos = Position(side, 1., 100., 9999.9, 100. / 3, .1, 100.)
            record = {"id": str(side), "entry_at": (slot - MINUTE).isoformat(),
                      "plan": {"model_financing_rate": .0001, "model_financing_interval_minutes": 480}}
            ema.funding(db, book, record, pos, slot, at, at, 101., at)
            ema.funding(db, book, record, pos, slot, at, at, 101., at)
            assert pos.funding == pytest.approx(-.0101)
            assert pos.wallet == pytest.approx(9999.9 - .0101)
            assert book["funding"] == pytest.approx(-.0101)
            assert db.execute("SELECT json_extract(body, '$.funding_basis') FROM event WHERE book=? AND kind='FUNDING'",
                              (str(side),)).fetchall() == [(ema.FINANCING_BASIS,)]


@pytest.fixture
def reviewed_source(tmp_path, monkeypatch):
    store = tmp_path / "source"
    store.mkdir()
    path = store / ema.kraken_source.FILE
    with closing(sqlite3.connect(path)) as db:
        db.executescript(ema.kraken_source.SCHEMA)
        found = sha256(b"fixture AssetPairs").hexdigest()
        db.execute("INSERT INTO raw VALUES (?,?)", (found, b"fixture AssetPairs"))
        db.execute("INSERT INTO instrument VALUES (?,?,?,?,?,?,?,?,?)", ("test", "stream", AT.value, AT.value, AT.value,
                   found, "READY", "{}", None))
        db.commit()
    monkeypatch.setattr(ema.kraken_reference, "scope_identity", lambda **_: SimpleNamespace(id="stream"))
    return tmp_path, path


def test_kraken_trade_clock_and_decimal_are_preserved_and_stale_trade_is_rejected(reviewed_source):
    root, path = reviewed_source
    source = AT + pd.Timedelta(seconds=3, nanoseconds=827_736_900)
    requested = source + pd.Timedelta(milliseconds=10)
    receipt = requested + pd.Timedelta(milliseconds=90)
    exchange_time = str(source.value)
    raw = ('{"error":[],"result":{"XXBTZUSD":[["69240.123400","0.0123",' + exchange_time[:-9] + '.' + exchange_time[-9:]
           + ',"b","m","",721]],"last":"999999999"}}').encode()
    before = sha256(path.read_bytes()).hexdigest()

    def send(request):
        assert request.method == "GET"
        assert str(request.url) == f"{ema.TRADE_ENDPOINT}?pair=XBTUSD&count=1"
        assert "authorization" not in request.headers
        return httpx.Response(200, content=raw)

    times = iter((requested, requested, receipt))
    mono = iter((50_000_000, 100_000_000, 140_000_000, 200_000_000))
    observed = ema.observe(clock=lambda: next(times), monotonic=lambda: next(mono), source_root=root,
                           pause=lambda _: None, transport=httpx.MockTransport(send))
    assert observed["source_at"] == source.isoformat()
    assert observed["requested_at"] == requested.isoformat() and observed["received_at"] == receipt.isoformat()
    assert observed["price_text"] == "69240.123400" and observed["event_id"] == "721"
    assert execution.valid_snapshot(observed, successor("day"), receipt, 140_000_000) is None
    later = receipt + pd.Timedelta(seconds=3)
    assert execution.valid_snapshot(observed, successor("day"), later, 3_140_000_000) == "STALE_REFERENCE"
    assert sha256(path.read_bytes()).hexdigest() == before


def test_kraken_declines_requests_during_shared_spacing(reviewed_source):
    root, path = reviewed_source
    at = AT + pd.Timedelta(seconds=1)
    with closing(sqlite3.connect(path)) as db:
        db.execute("INSERT INTO request_clock VALUES (?,?,?,?,?)", ("stream", AT.value, 0, 0, None))
        db.commit()
    with pytest.raises(ValueError, match="shared request spacing"):
        ema.observe(clock=lambda: at, source_root=root,
                    transport=httpx.MockTransport(lambda _: pytest.fail("Shared rate guard must stop before HTTP")))
