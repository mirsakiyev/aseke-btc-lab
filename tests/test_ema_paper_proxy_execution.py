from dataclasses import replace

import pandas as pd
import pytest

from lab.ema_paper import ledger, proxy_native as native
from lab.ema_paper.proxy_execution import KuCoinProxyExecution
from lab.phase15 import paper
from lab.strategy_lab.market_data import Observation
from test_ema_paper_ledger import bodies, db, lane, setup  # noqa: F401
from test_ema_paper_proxy_native import DIGEST, HOUR, MINUTE, OHLC, futures_values, proof
from test_strategy_lab_kucoin_contract import terms_metadata  # noqa: F401


SPOT_TERMS = {"instrument": {"symbol": "BTC-USDT", "feeCategory": "classA", "tradingStatus": "TradingEnabled",
                             "baseCurrency": "BTC", "quoteCurrency": "USDT", "baseOrderStep": "0.00001",
                             "tickSize": "0.1", "minBaseOrderSize": "0.00001", "maxQuoteOrderSize": "10000",
                             "minFunds": "10", "maxBaseOrderSize": "1"}}


def _execution_fixture(monkeypatch, profile, terms):
    monkeypatch.setattr(native, "KuCoinProxyEvidence", KuCoinProxyExecution)
    return proof(monkeypatch, profile, terms)


def _append_minute(provider, grant, at, *, handoff=MINUTE / 2):
    end = at + MINUTE
    trade = Observation(grant.source.id, native.TRADE, at, end, end, end + handoff, DIGEST,
                        dict(OHLC, volume=2))
    provider.rows.append(trade)
    if grant.source.market == "FUTURES":
        mark = Observation(grant.source.id, native.MARK, at, end, end, end + handoff, DIGEST,
                           {key: value + 1 for key, value in OHLC.items()})
        provider.rows.append(mark)


@pytest.mark.parametrize("profile", ["day", "swing", "position"])
def test_post_publication_minutes_keep_native_venue_roles_and_clocks(monkeypatch, terms_metadata, profile):
    spot = profile == "position"
    values = SPOT_TERMS if spot else futures_values(terms_metadata)
    factory, provider, _, end, _, _, forward, execution = _execution_fixture(monkeypatch, profile, values)
    reader, at = factory(), end + 2 * MINUTE
    published = at - MINUTE
    _append_minute(provider, execution, at)
    assert reader.execution_evidence(at, at + MINUTE / 2, published_at=published)["state"] == "WAITING_FOR_DATA"
    assert provider.reads == []
    assert reader.execution_evidence(at, at + MINUTE, published_at=published)["state"] == "WAITING_FOR_DATA"
    seen = reader.execution_evidence(at, at + 2 * MINUTE, published_at=published)
    assert seen["state"] == "READY" and seen["evidence"]["execution_grant_id"] == execution.id != forward.id
    assert seen["trade"]["received_at"] == (at + MINUTE).isoformat()
    assert seen["trade"]["observed_at"] == (at + 1.5 * MINUTE).isoformat()
    assert seen["evidence"]["latest_handoff_at"] == seen["trade"]["observed_at"]
    assert seen["trade"]["raw_sha256"] == seen["evidence"]["contract_raw_sha256"] == DIGEST
    assert seen["trade"]["open"] == OHLC["open"] and seen["trade"]["volume"] == 2
    assert seen["contract"].base_per_contract == (1 if spot else .001)
    assert not seen["funding"]
    if spot:
        assert seen["mark"] == seen["trade"]
        assert {row[0] for row in provider.reads} == {native.TRADE, native.SPEC}
        assert seen["evidence"]["minimum_notional"] == 10
    else:
        assert seen["mark"]["open"] == OHLC["open"] + 1 and "volume" not in seen["mark"]
        assert {row[0] for row in provider.reads} == {native.TRADE, native.MARK, native.SPEC, native.FUNDING}


def test_permission_and_publication_clock_gate_all_reads(monkeypatch, terms_metadata):
    factory, provider, _, end, _, _, _, execution = _execution_fixture(monkeypatch, "day", futures_values(terms_metadata))
    at = end + 2 * MINUTE
    assert factory(ex=None).execution_evidence(at, at + MINUTE, published_at=end)["state"] == "BLOCKED_PERMISSION"
    assert factory(ex=replace(execution, start=end + MINUTE)).execution_evidence(
        at, at + MINUTE, published_at=end)["state"] == "BLOCKED_PERMISSION"
    assert factory().execution_evidence(at, at + MINUTE, published_at=at)["permanent"]
    assert factory().execution_evidence(at + MINUTE / 2, at + 2 * MINUTE, published_at=end)["permanent"]
    assert provider.reads == []


def test_terms_must_be_available_before_open_and_price_corrections_are_permanent(monkeypatch, terms_metadata):
    factory, provider, _, end, _, _, _, execution = _execution_fixture(monkeypatch, "day", futures_values(terms_metadata))
    at, published = end + 2 * MINUTE, end + MINUTE
    spec = next(row for row in provider.rows if row.kind == native.SPEC)
    provider.rows[:] = [row for row in provider.rows if row.kind != native.SPEC]
    provider.rows.append(replace(spec, received_at=at, available_at=at + MINUTE / 2,
                                 values=futures_values(terms_metadata)))
    _append_minute(provider, execution, at)
    reader = factory()
    seen = reader.execution_evidence(at, at + 2 * MINUTE, published_at=published)
    assert seen["state"] == "BLOCKED_EXECUTION_EVIDENCE" and not seen["permanent"] and "dated" in seen["reason"]
    price = next(row for row in provider.rows if row.start == at and row.kind == native.MARK)
    provider.rows.append(replace(price, values=dict(price.values, close=price.values["close"] + 1)))
    seen = reader.execution_evidence(at, at + 2 * MINUTE, published_at=published)
    assert seen["permanent"] and "Conflicting observations" in seen["reason"]


@pytest.mark.parametrize("jitter", [-2, 0, 2])
def test_actual_funding_is_applied_only_in_its_observed_minute(monkeypatch, terms_metadata, jitter):
    factory, provider, start, _, _, _, _, execution = _execution_fixture(monkeypatch, "day", futures_values(terms_metadata))
    slot = start + 8 * HOUR
    previous, publication = slot - MINUTE, slot - 3 * MINUTE
    _append_minute(provider, execution, previous)
    _append_minute(provider, execution, slot)
    reader = factory()
    missing = reader.execution_evidence(previous, slot + MINUTE, published_at=publication)
    assert missing["waiting_for_funding"] and not missing["permanent"]
    actual = slot + pd.Timedelta(seconds=jitter)
    receipt = Observation(execution.source.id, native.FUNDING, actual, actual, slot + MINUTE / 2,
                          slot + MINUTE, DIGEST, {"rate": .0002, "actual_settlement": True})
    provider.rows.append(receipt)
    assert reader.execution_evidence(previous, slot + 0.75 * MINUTE, published_at=publication)["waiting_for_funding"]
    old_minute = reader.execution_evidence(previous, slot + 2 * MINUTE, published_at=publication)
    new_minute = reader.execution_evidence(slot, slot + 2 * MINUTE, published_at=publication)
    assert old_minute["state"] == new_minute["state"] == "READY"
    actual_minute, other = (old_minute, new_minute) if jitter < 0 else (new_minute, old_minute)
    assert other["funding"] == ()
    assert actual_minute["funding"] == ({"settlement_at": actual.isoformat(),
                                        "rate": .0002, "raw_sha256": DIGEST,
                                        "received_at": receipt.received_at.isoformat(),
                                        "observed_at": receipt.available_at.isoformat()},)


@pytest.mark.parametrize("actual,rate", [(False, .001), (True, .01)])
def test_forecast_or_out_of_bounds_funding_permanently_blocks_the_minute(monkeypatch, terms_metadata, actual, rate):
    factory, provider, start, _, _, _, _, execution = _execution_fixture(monkeypatch, "day", futures_values(terms_metadata))
    slot = start + 8 * HOUR
    _append_minute(provider, execution, slot)
    provider.rows.append(Observation(execution.source.id, native.FUNDING, slot, slot, slot, slot, DIGEST,
                                     {"rate": rate, "actual_settlement": actual}))
    seen = factory().execution_evidence(slot, slot + 2 * MINUTE, published_at=slot - MINUTE)
    assert seen["permanent"] and "unverified actual" in seen["reason"]


@pytest.mark.parametrize("early_settlement", [None, True, False])
def test_publication_chart_has_an_exclusive_funding_boundary(monkeypatch, terms_metadata, early_settlement):
    values = futures_values(terms_metadata)
    factory, provider, start, _, _, primary, _, execution = _execution_fixture(monkeypatch, "day", values)
    slot = start + 8 * HOUR
    for minute in pd.date_range(slot - 15 * MINUTE, slot, freq=MINUTE, inclusive="left"):
        _append_minute(provider, execution, minute)
    after = {**values, "contract": {**values["contract"],
             "nextFundingRateDateTime": int((slot + 8 * HOUR).timestamp() * 1000)}}
    provider.rows.append(Observation(execution.source.id, native.SPEC, slot, slot, slot, slot, DIGEST, after))
    if early_settlement is not None:
        actual = slot - pd.Timedelta(seconds=2)
        provider.rows.append(Observation(execution.source.id, native.FUNDING, actual, actual, slot, slot,
                                         DIGEST, {"rate": .0002, "actual_settlement": early_settlement}))
    reader = factory()
    if early_settlement is False:
        with pytest.raises(ValueError, match="unverified actual"):
            reader.publication_evidence(slot, slot + MINUTE, primary)
    else:
        report = reader.publication_evidence(slot, slot + MINUTE, primary)
        assert report.settled_funding.grant_id == execution.id
        assert len(report.settled_funding.observations) == (1 if early_settlement else 0)


def test_checked_execution_reconciles_in_original_ledger_after_restart(monkeypatch, terms_metadata, db):
    factory, provider, start, _, _, _, _, execution = _execution_fixture(monkeypatch, "day", futures_values(terms_metadata))
    slot = start + 8 * HOUR
    candidate = replace(setup(slot - 30 * MINUTE), reference=70_000., stop=69_600., target_price=71_200., atr=500.)
    published = candidate.available_at
    entry = slot - 15 * MINUTE
    for minute in pd.date_range(entry, slot, freq=MINUTE):
        _append_minute(provider, execution, minute)
    provider.rows[-1] = replace(provider.rows[-1], values=dict(provider.rows[-1].values, high=71_250.))
    provider.rows.append(Observation(execution.source.id, native.FUNDING, slot, slot, slot, slot + MINUTE / 2,
                                     DIGEST, {"rate": .0002, "actual_settlement": True}))
    reader = factory()
    first = reader.execution_evidence(entry, entry + 2 * MINUTE, published_at=published)
    spec = lane("day")
    book = paper.initial(spec)
    plan, reason = ledger.make_plan(spec, candidate, first["contract"], book["equity"], published, {})
    assert reason is None and plan["eligible_from"] == entry
    assert paper.publish(db, book, {"state": "ELIGIBLE_CONDITIONAL_IDEA", "plan": plan,
                                   "source_cutoff": candidate.decision_at, "direction": "LONG",
                                   "strategy_version": "fixture", "exit_policy": candidate.exit_policy}, published)
    assert ledger.advance(db, book, entry, entry + 2 * MINUTE, first["trade"], first["mark"], first["funding"])
    paper.save(db, book)
    book, reader = paper.load(db, spec), factory()
    assert not ledger.advance(db, book, entry, entry + 2 * MINUTE, first["trade"], first["mark"], first["funding"])
    for minute in pd.date_range(entry + MINUTE, slot, freq=MINUTE):
        clock = minute + 2 * MINUTE
        seen = reader.execution_evidence(minute, clock, published_at=published)
        assert seen["state"] == "READY", (minute, seen["reason"])
        assert ledger.advance(db, book, minute, clock, seen["trade"], seen["mark"], seen["funding"])
    fill, = bodies(db, "HYPOTHETICAL_FILL")
    funding, = bodies(db, "FUNDING")
    closed, = bodies(db, "CLOSED")
    assert fill["contracts"] * .001 == pytest.approx(fill["quantity"])
    assert funding["cash_flow"] == pytest.approx(-fill["quantity"] * (OHLC["open"] + 1) * .0002)
    assert closed["reason"] == "target" and closed["trigger_basis"] == "MARK" and closed["execution_basis"] == "TRADE"
    assert closed["net"] == pytest.approx(closed["gross_price_pnl"] - closed["fees"] + closed["funding"])
