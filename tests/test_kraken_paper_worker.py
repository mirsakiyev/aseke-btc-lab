from contextlib import closing
from hashlib import sha256

import pandas as pd
import pytest

from lab.kraken_paper import catalog, worker
from lab.phase15 import paper
from lab.strategy_lab.market_data import MarketData, Observation, ProvenanceClass, Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.technical_features import FeatureCache


SEAM = pd.Timestamp("2027-01-02T23:15:00Z")
CHART = pd.Timedelta(minutes=15)
SECOND = pd.Timedelta(seconds=1)
MINUTE = pd.Timedelta(minutes=1)
RAW = b"synthetic Kraken trade evidence"
SHA = sha256(RAW).hexdigest()
TRADE = worker.TRADE


class FixtureProvider:
    def __init__(self, rows):
        self.rows = rows

    def read(self, kind, start, end, as_of, interval):
        return (row for row in self.rows if row.kind == kind and start <= row.start < end
                and row.end - row.start == interval)

    def raw(self, digest):
        assert digest == SHA
        return RAW


def experiment(tmp_path, *, declining=False):
    spec = catalog.strategy_specs()["day"]["spec"]
    source = SourceIdentity("Kraken", "fixture source", "BTC-USD", "SPOT", "fixture review",
                            SHA, ProvenanceClass.INDEPENDENT)
    registered = SEAM - CHART
    start = SEAM - 205 * CHART
    end = SEAM + pd.Timedelta(days=92)
    book = "synthetic day"
    grants = [SourceGrant(source, book, purpose, frozenset({TRADE}), left, right,
                          registered, registered, "fixture review")
              for purpose, left, right in ((Purpose.WARMUP, start, SEAM), (Purpose.FORWARD_DECISION, SEAM, end),
                                           (Purpose.EXECUTION_EVIDENCE, SEAM, end))]
    warmup, decision, execution = grants
    lane = {
        "profile": "day", "book_id": book, "strategy_hash": "fixture strategy", "registered_at": registered.isoformat(),
        "spec": spec, "risk": spec["risk"], "cost": spec["cost"], "execution_clock": {
            "decision_seconds": 900, "entry_seconds": 900, "protection_seconds": 60},
        "horizon_days": 90, "follow_through_minutes": 600, "min_complete_trades": 25,
        "limits": {"minutes_per_book": 60},
        "instrument_terms": {"price_tick": .1, "quantity_step": 1e-8, "min_quantity": .00006, "min_quote": .5,
                             "raw_sha256": SHA, "received_at": registered.isoformat()},
        "source": {"source_id": source.id, "warmup_grant_id": warmup.id, "forward_grant_id": decision.id,
                   "execution_grant_id": execution.id, "seed_end": SEAM.isoformat(), "warmup_bars": 200,
                   "history_seconds": int(205 * CHART.total_seconds()), "decision_receipt_seconds": 120,
                   "publication_max_seconds": 360, "price_receipt_seconds": 120},
    }

    def candle(at, step, *, price, low=None, high=None, volume=1., available=None):
        close = at + step
        seen = close + 10 * SECOND if available is None else available
        return Observation(source.id, TRADE, at, close, seen, seen + SECOND, SHA,
                           {"open": price, "high": high if high is not None else price + .2,
                            "low": low if low is not None else price - .2, "close": price, "volume": volume})

    sign = -1 if declining else 1
    rows = [candle(at, CHART, price=100. + sign * i * .2, available=SEAM + 10 * SECOND)
            for i, at in enumerate(pd.date_range(start, SEAM, freq=CHART, inclusive="left"))]
    previous = 100 + sign * 205 * .2
    touch = {"high": previous + 12} if declining else {"low": previous - 12}
    rows += [candle(SEAM, CHART, price=previous, **touch),
             candle(SEAM + CHART, CHART, price=previous + sign * .2, volume=3.)]
    registration = {"id": "fixture registration", "body": lane}
    with closing(catalog.db_open(root=tmp_path)) as db, db:
        paper.save(db, paper.initial(lane))
        paper.save(db, paper.initial({**lane, "book_id": "another book"}))
    return registration, MarketData(grants, {source.id: FixtureProvider(rows)}), rows, candle


def run(tmp_path, registration, data, now, instrument=None):
    if instrument is None:
        terms = registration["body"]["instrument_terms"]
        instrument = {"state": "READY", "terms": terms, "available_at": terms["received_at"]}
    with closing(catalog.db_open(root=tmp_path)) as db, db:
        state = worker._cycle_book(db, registration, data, now, tmp_path / "warmup", float("inf"), FeatureCache(),
                                   instrument)
        account = paper.load(db, registration["body"])
        events = db.execute("SELECT kind,source_at,recorded_at,body FROM event ORDER BY rowid").fetchall()
        return state, account, events


def test_feature_seed_bridge_future_weekend_trade_and_separate_usd_cash(tmp_path):
    registration, data, rows, candle = experiment(tmp_path)
    seeded, account, events = run(tmp_path, registration, data, SEAM + 11 * SECOND)
    assert seeded["warmup"]["completed"] == 205 and len(seeded["warmup_snapshot_id"]) == 64
    assert not events and not account["pending"]
    first = SEAM + 2 * CHART
    waiting, _, events = run(tmp_path, registration, data, SEAM + CHART + 41 * SECOND)
    assert waiting["state"] == "WAITING_FOR_START" and not events
    assert waiting["first_eligible_at"] == first.isoformat()
    active, account, events = run(tmp_path, registration, data, first + 41 * SECOND)
    assert active["state"] == "ACTIVE"
    assert [row[0] for row in events] == ["PUBLISHED"]
    opening = first + CHART
    assert opening.hour == 0 and opening.day == 3
    assert account["pending"]["plan"]["eligible_from"] == opening.isoformat()
    assert account["pending"]["plan"]["contract"]["quote"] == "USD"
    assert account["pending"]["plan"]["session_flat"] is None
    same, _, repeated = run(tmp_path, registration, data, first + 55 * SECOND)
    assert same["eligibility_id"] == active["eligibility_id"] and repeated == events

    raw_open = 141.2
    rows.append(candle(opening, MINUTE, price=raw_open, volume=5.))
    _, opened, journal = run(tmp_path, registration, data, opening + MINUTE + 11 * SECOND)
    record = opened["position"]
    assert record["entry_at"] == opening.isoformat()
    assert record["deadline"] == (opening + pd.Timedelta(hours=8)).isoformat()
    assert record["position"]["direction"] == 1 and record["position"]["remaining"] > 0
    assert record["entry_fee"] == pytest.approx(record["position"]["entry"] * record["position"]["remaining"] * .008)
    assert record["position"]["entry"] == pytest.approx(141.4)
    assert [row[0] for row in journal].count("HYPOTHETICAL_FILL") == 1

    target = record["target"]
    rows.append(candle(opening + MINUTE, MINUTE, price=target + .2))
    _, closed, journal = run(tmp_path, registration, data, opening + 2 * MINUTE + 11 * SECOND)
    assert not closed["position"]
    assert [row[0] for row in journal].count("CLOSED") == 1
    with closing(catalog.db_open(True, root=tmp_path)) as db:
        report = worker.ledger.report(db, registration["body"])
        assert report["cash_reconciliation_difference"] == pytest.approx(0., abs=1e-10)
        settled = report["recent_closed"][0]
        assert settled["net"] == pytest.approx(settled["gross_before_adverse"] -
                                              settled["adverse_cost_in_gross"] - settled["fees"])
        assert settled["funding"] == settled["liquidation_cost"] == 0.
        assert paper.load(db, {**registration["body"], "book_id": "another book"})["cash"] == 10_000


def test_short_is_diagnostic_and_history_cannot_supply_forward_bridge(tmp_path):
    registration, data, rows, _ = experiment(tmp_path, declining=True)
    seeded, _, _ = run(tmp_path, registration, data, SEAM + 11 * SECOND)
    assert seeded["warmup"]["completed"] == 205
    bridge = rows.pop(205)
    waiting, _, events = run(tmp_path, registration, data, SEAM + CHART + 41 * SECOND)
    assert waiting["state"] == "WAITING_FOR_DATA" and not waiting.get("first_eligible_at")
    assert not events
    rows.append(bridge)
    run(tmp_path, registration, data, SEAM + CHART + 42 * SECOND)
    active, account, events = run(tmp_path, registration, data, SEAM + 2 * CHART + 41 * SECOND)
    assert active["last_signal"]["decision"] == "SIGNAL_ONLY"
    assert active["last_signal"]["direction"] == "SHORT" and not events and not account["pending"]


def test_late_minute_expires_without_a_backfilled_fill(tmp_path):
    registration, data, rows, candle = experiment(tmp_path)
    run(tmp_path, registration, data, SEAM + CHART + 41 * SECOND)
    run(tmp_path, registration, data, SEAM + 2 * CHART + 41 * SECOND)
    opening = SEAM + 3 * CHART
    rows.append(candle(opening, MINUTE, price=141.2, available=opening + 3 * MINUTE + SECOND))
    _, account, events = run(tmp_path, registration, data, opening + 4 * MINUTE)
    assert [row[0] for row in events] == ["PUBLISHED", "EXPIRED"]
    assert account["cash"] == 10_000 and not account["position"]


def test_current_instrument_evidence_must_match_before_eligibility(tmp_path):
    registration, data, _, _ = experiment(tmp_path)
    now = SEAM + CHART + 41 * SECOND
    state, _, events = run(tmp_path, registration, data, now, {"state": "STALE"})
    assert state["warmup"]["completed"] == 205 and state["state"] == "WAITING_FOR_DATA"
    assert not state.get("first_eligible_at") and not events
    terms = {**registration["body"]["instrument_terms"], "min_quantity": .25}
    state, _, events = run(tmp_path, registration, data, now, {
        "state": "READY", "terms": terms, "available_at": terms["received_at"]})
    assert state["state"] == "BLOCKED_SOURCE" and not state.get("first_eligible_at") and not events
