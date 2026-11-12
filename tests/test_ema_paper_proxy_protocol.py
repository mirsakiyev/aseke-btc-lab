from contextlib import closing
from dataclasses import replace
import json
import sqlite3

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.ema_paper import catalog, proxy_native, proxy_protocol, proxy_signal
from lab.kraken_paper.catalog import _grant_body
from lab.strategy_lab import cross_venue_warmup as cross
from lab.strategy_lab.market_data import MarketData, Observation, ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity
from test_cross_venue_warmup import SEAM, STEP, evaluate, native, snapshot
from test_ema_paper_proxy_native import DIGEST, LocalProvider
from test_ema_paper_worker import registered


def _drafts(registrations, now, previous):
    for profile, row in registrations.items():
        lane = row["body"]
        chart = pd.Timedelta(lane["spec"]["timeframe"])
        archived = snapshot(chart)
        grant = SourceGrant(archived.source, lane["book_id"], Purpose.WARMUP, frozenset({ObservationKind.TRADE_OHLCV}),
                            archived.start, archived.end, now, lane["registered_at"], "fixture feature scope")
        seed = replace(cross.freeze_proxy_seed(archived, timeframe=chart), book_id=lane["book_id"], grant_id=grant.id)
        record = {"book_id": lane["book_id"], "registration_id": row["id"], "strategy_hash": lane["strategy_hash"],
                  "protocol": proxy_protocol.PROTOCOL, "policy_id": cross.POLICY_ID, "code_pins": proxy_protocol._pins(),
                  "predecessor_amendment_id": previous.get(profile, {}).get("id"),
                  "recorded_at": now.isoformat(), "seed": seed.body(),
                  "warmup_grant": _grant_body(grant), "warmup_grant_id": grant.id, "seed_id": seed.id,
                  "warmup_count": seed.count, "archive_content_id": seed.content_id}
        yield fingerprint(record), lane["book_id"], json.dumps(record)


def test_append_only_prepare_preserves_original_accounts_and_is_idempotent(registered, tmp_path, monkeypatch):
    paper_root, _, _, _, registrations = registered
    monkeypatch.setattr(proxy_protocol, "ORIGINAL_HASHES", {
        profile: record["body"]["strategy_hash"] for profile, record in registrations.items()
    })
    monkeypatch.setattr(proxy_protocol, "_amendments", lambda books, _, review, now, previous: list(_drafts(books, review, previous)))
    protocol_root = tmp_path / "protocol"
    clock = pd.Timestamp("2027-01-04T00:01Z")
    assert not proxy_protocol.amended(root=protocol_root)
    with closing(catalog.db_open(True, root=paper_root)) as ledger:
        books = ledger.execute("SELECT * FROM book ORDER BY id").fetchall()
    first = proxy_protocol.prepare(clock, root=protocol_root, paper_root=paper_root)
    assert first == proxy_protocol.prepare(clock, root=protocol_root, paper_root=paper_root)
    assert proxy_protocol.amended(root=protocol_root)
    assert set(first) == set(registrations)
    for profile, item in first.items():
        assert item["book_id"] == registrations[profile]["body"]["book_id"]
        assert item["feature_mode"] == cross.KRAKEN_PROXY_SEEDED and item["source_mode"] == cross.SOURCE_BLOCKED
        assert item["first_eligible_at"] is None
    with closing(catalog.db_open(True, root=paper_root)) as ledger:
        assert books == ledger.execute("SELECT * FROM book ORDER BY id").fetchall()
        assert ledger.execute("SELECT count(*) FROM event").fetchone() == (0,)
        assert ledger.execute("SELECT count(*) FROM eligibility").fetchone() == (0,)
    with closing(sqlite3.connect(protocol_root / "protocol.sqlite3")) as db:
        with pytest.raises(sqlite3.IntegrityError, match="immutable EMA"):
            db.execute("DELETE FROM amendment")
    monkeypatch.setattr(proxy_protocol, "_pins", lambda: {"fixture": "updated runtime"})
    blocked = proxy_protocol.prepare(clock, root=protocol_root, paper_root=paper_root)
    assert all(not item["protocol_pins_match"] for item in blocked.values())
    revision = proxy_protocol.prepare(clock + pd.Timedelta(minutes=1), root=protocol_root,
                                      paper_root=paper_root, append_runtime_revision=True)
    effective = proxy_protocol._load(protocol_root, registrations)
    assert all(revision[profile]["protocol_pins_match"] for profile in first)
    assert all(effective[profile]["body"]["predecessor_amendment_id"] == first[profile]["amendment_id"]
               for profile in first)
    with closing(sqlite3.connect(protocol_root / "protocol.sqlite3")) as db:
        assert db.execute("SELECT count(*) FROM amendment").fetchone() == (6,)


def test_original_proposal_requires_future_readiness_and_actual_receipt_processing(registered):
    lane = registered[-1]["day"]
    seed = replace(cross.freeze_proxy_seed(snapshot(), timeframe=STEP), book_id=lane["body"]["book_id"])
    bars = native(seed, 21)
    bars.loc[bars.index[-2], "low"] = 95.
    at = bars.close_at.iloc[-1]
    available = bars.available_at.iloc[-1] + pd.Timedelta(seconds=lane["body"]["spec"]["execution"]["processing_seconds"])
    amendment = {"body": {"book_id": seed.book_id, "registration_id": lane["id"], "policy_id": cross.POLICY_ID,
                          "recorded_at": seed.as_of.isoformat(), "seed_id": seed.id}}
    feature = evaluate(seed, bars, as_of=available)
    kwargs = {"decision_at": at, "as_of": available}
    same_bar = proxy_signal.inspect_decision(lane, amendment, feature, ready_at=at, **kwargs)
    assert same_bar["reason"] == "DECISION_OUTSIDE_PROSPECTIVE_CLOCK"
    ready_at = bars.available_at.iloc[-2]
    early = available - pd.Timedelta(seconds=1)
    before_processing = proxy_signal.inspect_decision(lane, amendment, evaluate(seed, bars, as_of=early),
                                                      decision_at=at, as_of=early, ready_at=ready_at)
    assert before_processing["reason"] == "MISSED_OR_UNAVAILABLE_PROSPECTIVE_ENTRY"
    candidate = proxy_signal.inspect_decision(lane, amendment, feature, ready_at=ready_at, **kwargs)
    assert candidate["source_mode"] == cross.KRAKEN_PROXY_SEEDED
    assert candidate["decision"] == "PROPOSAL"
    setup = candidate["setup"]
    assert setup.available_at == available < setup.submit_at == at + STEP
    assert setup.stop < setup.reference < setup.target_price


def test_verified_spot_transition_previews_original_account_plan_without_writing(registered, monkeypatch):
    paper_root, _, _, _, registrations = registered
    record = registrations["position"]
    lane = record["body"]
    book = lane["book_id"]
    day, hour, minute = map(pd.Timedelta, ("1D", "1h", "1min"))
    seed = replace(cross.freeze_proxy_seed(snapshot(day), timeframe=day), book_id=book)
    bars = native(seed, 21)
    bars.loc[bars.index[-2], "low"] = 95.
    at = bars.close_at.iloc[-1]
    now, protocol = at + 2 * minute, SEAM - day
    identity = SourceIdentity("synthetic provider", "synthetic KuCoin Spot", "BTC-USDT", "SPOT", "fixture",
                              DIGEST, ProvenanceClass.INDEPENDENT)
    grant_args = (protocol, at + 130 * day, protocol, lane["registered_at"], "fixture scope")
    forward = SourceGrant(identity, book, Purpose.FORWARD_DECISION, {proxy_native.TRADE}, *grant_args)
    execution = SourceGrant(identity, book, Purpose.EXECUTION_EVIDENCE, {proxy_native.TRADE, proxy_native.SPEC}, *grant_args)
    monkeypatch.setitem(proxy_native.ORIGINAL_BOOKS, "position", (record["id"], book, "SPOT", "BTC-USDT", 86400))
    rows = []
    for opened, bar in bars.iterrows():
        prices = {key: float(bar[key]) for key in ("open", "high", "low", "close")}
        for left in pd.date_range(opened, opened + day, freq=hour, inclusive="left"):
            right = left + hour
            rows.append(Observation(identity.id, proxy_native.TRADE, left, right, right, right + minute / 2,
                                    DIGEST, {**prices, "volume": float(bar.volume) / 24}))
    rows.append(Observation(identity.id, proxy_native.TRADE, at - minute, at, at, at + minute / 2, DIGEST,
                            {**prices, "volume": 1.}))
    terms = {"instrument": {"symbol": "BTC-USDT", "feeCategory": "classA", "tradingStatus": "TradingEnabled",
                            "baseCurrency": "BTC", "quoteCurrency": "USDT", "baseOrderStep": "0.00001",
                            "tickSize": "0.1", "minBaseOrderSize": "0.00001", "maxQuoteOrderSize": "10000",
                            "minFunds": "10", "maxBaseOrderSize": "100"}}
    for time in (at - day - minute, at - hour):
        rows.append(Observation(identity.id, proxy_native.SPEC, time, time, time, time + minute / 2, DIGEST, terms))
    reader = proxy_native.KuCoinProxyEvidence(MarketData([forward, execution], {identity.id: LocalProvider(rows)}),
                                             record, protocol_recorded_at=protocol, source_identity_id=identity.id,
                                             forward_grant_id=forward.id, execution_grant_id=execution.id)
    body = {"book_id": book, "registration_id": record["id"], "policy_id": cross.POLICY_ID,
            "recorded_at": protocol.isoformat(), "seed_id": seed.id, "seed": seed.body()}
    amendment = {"id": fingerprint(body), "body": body}
    result = proxy_signal.inspect_plan(record, amendment, reader, decision_at=at, as_of=now,
                                       ready_at=at - day + 2 * minute, equity=10000)
    assert result["decision"] == "PROPOSAL", result["reason"]
    plan = result["plan"]
    assert now < plan["eligible_from"] == at + hour
    assert plan["contract"]["quote"] == "USDT" and plan["trigger_basis"] == "TRADE"
    assert plan["evidence"]["source_mode"] == cross.KRAKEN_PROXY_SEEDED
    assert plan["evidence"]["source_id"] == identity.id and plan["evidence"]["chart_snapshot_id"]
    with closing(catalog.db_open(True, root=paper_root)) as ledger:
        assert ledger.execute("SELECT count(*) FROM event").fetchone() == (0,)
