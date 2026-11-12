from contextlib import closing
from dataclasses import replace
from types import SimpleNamespace
import time

import pandas as pd

from lab.config import fingerprint
from lab.ema_paper import catalog, dispatcher
from lab.learning.simulator import Contract
from lab.phase15 import paper
from lab.strategy_lab import cross_venue_warmup as cross
from test_cross_venue_warmup import STEP, native, snapshot
from test_ema_paper_worker import registered


class ReaderFixture:
    """Deterministic receipt seam. The actual source and execution readers have separate tests."""

    def __init__(self, bars):
        self.bars = bars
        self.contract = Contract("linear_perp", "USDT", .001, 1., .1, 1., 250000., 5., .004, .003)
        self.calls = []

    def read_chart(self, at, now):
        self.calls.append(at)
        row = self.bars.loc[[at - STEP]].copy()
        row["received_at"] = row.available_at
        row["receipt_manifest_sha256"] = fingerprint([at.isoformat(), "raw"])
        proof = SimpleNamespace(id=f"chart-{at}", content_id=f"content-{at}", grant_id="forward",
                                source=SimpleNamespace(id="licensed-kucoin"))
        return SimpleNamespace(complete=row.available_at.iloc[0] <= now, bars=row), proof

    def publication_evidence(self, at, now, end):
        return SimpleNamespace(available_at=self.bars.at[at - STEP, "available_at"],
                               decision_trade=SimpleNamespace(id=f"chart-{at}", content_id=f"content-{at}"),
                               trade=SimpleNamespace(id="trade", grant_id="execution"),
                               marks=SimpleNamespace(id="mark"), dated_terms=SimpleNamespace(id="contract"),
                               settled_funding=SimpleNamespace(id="funding"), contract=self.contract,
                               minimum_notional=0., maximum_quantity=100000.)

    def execution_evidence(self, at, as_of, *, published_at):
        price = float(self.bars.close.iloc[-1])
        minute = pd.Timedelta(minutes=1)
        closed = at + minute
        bar = {"open_at": at.isoformat(), "close_at": closed.isoformat(), "received_at": closed.isoformat(),
               "observed_at": closed.isoformat(), "open": price, "high": price + .05, "low": price - .05, "close": price}
        assert pd.Timestamp(published_at) < at
        return {"state": "READY", "contract": self.contract, "trade": bar, "mark": bar, "funding": (),
                "evidence": {"source_id": "licensed-kucoin", "execution_grant_id": "execution",
                             "minimum_notional": 0., "maximum_quantity": 100000.}}


def test_uncommissioned_cycle_leaves_original_sqlite_untouched(registered, tmp_path):
    root, *_ = registered
    ledger = root / "paper.sqlite3"
    before = ledger.read_bytes()
    result = dispatcher.cycle(root=root, protocol_root=tmp_path / "no-review")
    assert result["paper_dispatch"] == "BLOCKED_PERMISSION"
    assert set(result["books"]) == {"day", "swing", "position"}
    assert ledger.read_bytes() == before


def test_dispatcher_qualifies_before_publishing_and_recovers_cursor_after_restart(registered):
    root, _, _, _, registrations = registered
    registration = registrations["day"]
    lane = registration["body"]
    seed = replace(cross.freeze_proxy_seed(snapshot(), timeframe=STEP), book_id=lane["book_id"])
    bars = native(seed, 21)
    bars.loc[bars.index[-2], "low"] = 95.
    clock = pd.Timedelta(seconds=90)
    at = bars.close_at.iloc[-1]
    qualified = bars.close_at.iloc[-2] + clock
    reader = ReaderFixture(bars)
    body = {"book_id": seed.book_id, "registration_id": registration["id"], "policy_id": cross.POLICY_ID,
            "recorded_at": seed.as_of.isoformat(), "seed_id": seed.id}
    amendment = {"id": fingerprint(body), "body": body}

    def cycle(now):
        with closing(catalog.db_open(root=root)) as db:
            db.executescript(dispatcher.SCHEMA)
            with db:
                return dispatcher._run_book(db, registration, (amendment, seed, reader), now, time.monotonic() + 10)

    first = cycle(qualified)
    assert first["state"] == "BLOCKED_DATA"
    assert first["source_protocol"]["chart_cursor"] == bars.close_at.iloc[11].isoformat()
    assert first["source_protocol"]["native_volume"]["complete"] == 12
    second = cycle(qualified)
    assert second["state"] == "ECONOMIC_EXECUTION_READY"
    assert second["first_eligible_at"] == at.isoformat()
    assert len(reader.calls) == 20 and len(set(reader.calls)) == 20
    with closing(catalog.db_open(True, root=root)) as db:
        assert db.execute("SELECT count(*) FROM event").fetchone() == (0,)

    assert cycle(at + clock)["state"] == "ACTIVE"
    assert cycle(at + clock)["state"] == "ACTIVE"
    assert len(reader.calls) == 21
    with closing(catalog.db_open(True, root=root)) as db:
        assert db.execute("SELECT count(*) FROM event WHERE kind='PUBLISHED'").fetchone() == (1,)
        account = paper.load(db, lane)
        assert account["pending"]["plan"]["evidence"]["source_mode"] == "KRAKEN_PROXY_SEEDED"
        assert account["pending"]["plan"]["execution_terms"] == {"min_quote": 0., "max_contracts": 100000.}
        opening = pd.Timestamp(account["cursor"])
        assert opening == at + STEP
    cycle(opening + clock)
    cycle(opening + clock)
    with closing(catalog.db_open(True, root=root)) as db:
        assert db.execute("SELECT count(*) FROM event WHERE kind='HYPOTHETICAL_FILL'").fetchone() == (1,)
        assert db.execute("SELECT count(*) FROM eligibility").fetchone() == (1,)
        assert all(paper.load(db, registrations[key]["body"])["equity"] == 10000 for key in ("position", "swing"))
