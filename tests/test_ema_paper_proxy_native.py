from dataclasses import replace
from hashlib import sha256

import pandas as pd
import pytest

from lab.config import fingerprint
from lab.ema_paper import proxy_native as native
from lab.strategy_lab.cross_venue_warmup import freeze_proxy_seed
from lab.strategy_lab.market_data import MarketData, Observation, ProvenanceClass, Purpose, SourceGrant, SourceIdentity
from test_strategy_lab_kucoin_contract import terms_metadata  # noqa: F401


MINUTE = pd.Timedelta(minutes=1)
HOUR = pd.Timedelta(hours=1)
DAY = pd.Timedelta(days=1)
RAW = b"synthetic independent KuCoin provider receipt"
DIGEST = sha256(RAW).hexdigest()
OHLC = {"open": 70_000, "high": 70_010, "low": 69_990, "close": 70_001}


class LocalProvider:
    def __init__(self, rows):
        self.rows = rows
        self.reads = []

    def read(self, kind, start, end, as_of, interval):
        self.reads.append((kind, start, end, as_of, interval))
        return (row for row in self.rows if row.kind == kind and start <= row.start < end
                and (interval is None or row.end - row.start == interval))

    def raw(self, digest):
        assert digest == DIGEST
        return RAW


def proof(monkeypatch, profile, values):
    _, _, market, instrument, chart_seconds = native.ORIGINAL_BOOKS[profile]
    chart = pd.Timedelta(seconds=chart_seconds)
    start = pd.Timestamp("2026-09-08T08:00:00Z").floor(chart)
    end = start + chart
    now = end + MINUTE
    registered = start - DAY
    protocol = start - 3 * HOUR
    book = fingerprint(["synthetic EMA", profile])
    lane = {
        "profile": profile, "book_id": book, "market": market, "instrument": instrument,
        "registered_at": registered.isoformat(), "execution_clock": {"decision_seconds": chart_seconds},
        "source": {"decision_receipt_seconds": 120, "publication_max_seconds": 360, "modeled_liquidation_fee": .003},
        "risk": {"leverage": 1 if market == "SPOT" else 3}, "cost": {"taker": .001, "adverse_proxy": .0005},
        "follow_through_minutes": 600,
    }
    reg = {"id": fingerprint(lane), "body": lane}
    monkeypatch.setitem(native.ORIGINAL_BOOKS, profile, (reg["id"], book, market, instrument, chart_seconds))
    identity = SourceIdentity("synthetic independent supplier", "reviewed kucoin delivery", instrument, market,
                              "synthetic rights evidence", DIGEST, ProvenanceClass.INDEPENDENT)
    primary = end + 90 * DAY
    roles = {native.TRADE, native.SPEC} if market == "SPOT" else {native.TRADE, native.SPEC, native.MARK, native.FUNDING}
    forward = SourceGrant(identity, book, Purpose.FORWARD_DECISION, {native.TRADE}, protocol, primary,
                          protocol, registered, "synthetic forward only")
    execution = SourceGrant(identity, book, Purpose.EXECUTION_EVIDENCE, roles, protocol, primary + DAY,
                            protocol, registered, "synthetic execution only")

    def candle(kind, left, step):
        right = left + step
        prices = dict(OHLC, volume=2) if kind == native.TRADE else OHLC
        return Observation(identity.id, kind, left, right, right, right + MINUTE / 2, DIGEST, prices)

    def point(kind, at, payload, delay=MINUTE / 2):
        return Observation(identity.id, kind, at, at, at, at + delay, DIGEST, payload)

    step = HOUR if market == "SPOT" else MINUTE
    chart_opens = pd.date_range(start, end, freq=step, inclusive="left")
    rows = [candle(native.TRADE, at, step) for at in chart_opens]
    current_values = values if market == "SPOT" else {
        **values, "contract": {**values["contract"], "nextFundingRateDateTime": (
            values["contract"]["nextFundingRateDateTime"] + values["contract"]["currentFundingRateGranularity"]
        )},
    }
    rows += [point(native.SPEC, start - 2 * MINUTE, values), point(native.SPEC, start + chart / 2, current_values)]
    if market == "SPOT":
        rows += [candle(native.TRADE, end - MINUTE, MINUTE)]
    else:
        rows += [candle(native.MARK, at, MINUTE) for at in chart_opens]
        rows += [point(native.FUNDING, start + pd.Timedelta(seconds=2), {"rate": 0.0001, "actual_settlement": True})]
    provider = LocalProvider(rows)

    def bridge(*, source=identity.id, registration=reg, fw=forward, ex=execution):
        reader = MarketData([grant for grant in (fw, ex) if grant is not None], {identity.id: provider})
        return native.KuCoinProxyEvidence(reader, registration, protocol_recorded_at=protocol,
                                          source_identity_id=source, forward_grant_id=fw.id,
                                          execution_grant_id=ex.id if ex else None)

    return bridge, provider, start, end, now, primary, forward, execution


def futures_values(terms):
    return dict(zip(("contract", "instrument", "isolated_tiers"), terms, strict=True))


@pytest.mark.parametrize("profile", ["day", "swing"])
def test_chart_and_execution_evidence_use_separate_grants(monkeypatch, terms_metadata, profile):
    bridge, provider, start, end, now, primary, fw, ex = proof(monkeypatch, profile, futures_values(terms_metadata))
    reader = bridge()
    batch, chart = reader.read_chart(end, now)
    assert batch.complete and chart.grant_id == fw.id
    assert batch.bars.iloc[0].volume == 2 * (end - start) // MINUTE
    report = reader.publication_evidence(end, now, primary)
    assert report.decision_trade.grant_id == fw.id
    assert report.marks.grant_id == report.dated_terms.grant_id == report.settled_funding.grant_id == ex.id
    assert report.pre_chart_terms.observations[-1].available_at < start
    assert report.contract.base_per_contract == 0.001
    assert report.contract.quote == "USDT" and report.cost.fee == .001
    assert report.settled_funding.observations[0].values["actual_settlement"]
    assert report.available_at == end + MINUTE / 2


def test_spot_daily_aggregation_and_execution_do_not_require_futures_roles(monkeypatch):
    terms = {"instrument": {"symbol": "BTC-USDT", "feeCategory": "classA", "tradingStatus": "TradingEnabled",
                            "baseCurrency": "BTC", "quoteCurrency": "USDT", "baseOrderStep": "0.00001",
                            "tickSize": "0.1", "minBaseOrderSize": "0.00001", "maxQuoteOrderSize": "10000",
                            "minFunds": "10", "maxBaseOrderSize": "1"}}
    bridge, provider, start, end, now, primary, fw, ex = proof(monkeypatch, "position", terms)
    reader = bridge()
    batch, _ = reader.read_chart(end, now)
    assert batch.complete and batch.bars.iloc[0].volume == 48
    report = reader.publication_evidence(end, now, primary)
    assert report.contract.family == "spot" and report.minimum_notional == 10
    assert report.trade.observations[0].start == end - MINUTE
    assert report.marks is report.settled_funding is report.futures_terms is None
    assert {call[0] for call in provider.reads} == {native.TRADE, native.SPEC}


def test_permission_and_original_book_fail_closed(monkeypatch, terms_metadata):
    bridge, provider, start, end, now, primary, fw, ex = proof(monkeypatch, "day", futures_values(terms_metadata))
    for kwargs in ({"source": DIGEST}, {"fw": replace(fw, book_id="other")},
                   {"ex": replace(ex, kinds={native.TRADE, native.SPEC})}):
        with pytest.raises(PermissionError, match="original book"):
            bridge(**kwargs)
    with pytest.raises(PermissionError, match="original EMA registration"):
        bridge(registration={"id": "forged", "body": {"profile": "day"}})
    assert provider.reads == []
    signal_only = bridge(ex=None)
    assert signal_only.read_chart(end, now)[0].complete
    for reader in (signal_only, bridge(ex=replace(ex, end=primary))):
        with pytest.raises(PermissionError, match="full follow-through"):
            reader.publication_evidence(end, now, primary)


def test_verified_native_chart_replays_to_transition_with_receipt_provenance(monkeypatch, terms_metadata):
    bridge, provider, seam, end, now, primary, fw, ex = proof(monkeypatch, "day", futures_values(terms_metadata))
    step = end - seam
    kraken = SourceIdentity("Kraken", "synthetic historical grant", "BTC-USD", "SPOT", "fixture rights", DIGEST,
                            ProvenanceClass.INDEPENDENT)
    first = seam - 200 * step
    at = seam + MINUTE
    grant = SourceGrant(kraken, fw.book_id, Purpose.WARMUP, {native.TRADE}, first, seam, at, fw.registered_at,
                        "synthetic feature-only grant")
    opens = pd.date_range(first, seam, freq=step, inclusive="left")
    raw = [Observation(kraken.id, native.TRADE, left, left + step, at, at, DIGEST, dict(OHLC, volume=100))
           for left in opens]
    snapshot = MarketData([grant], {kraken.id: LocalProvider(raw)}).read(
        grant.id, fw.book_id, Purpose.WARMUP, native.TRADE, first, seam, at, step)
    seed = freeze_proxy_seed(snapshot, timeframe=step)
    result = bridge().read_transition(seed, end, now)
    assert result.evidence["forward_source_id"] == fw.source.id
    assert result.evidence["forward_grant_id"] == fw.id
    assert result.evidence["native_contiguous_bars"] == 1
    first_native = result.evidence["first_native_candle"]
    assert first_native["close_at"] == end.isoformat()
    assert first_native["receipt_manifest_sha256"]
    assert result.evidence["native_open_minus_kraken_close"] == -1


@pytest.mark.parametrize(("kind", "mode", "reason"), [
    (native.TRADE, "late", "timely KuCoin native decision chart"),
    (native.MARK, "missing", "timely native KuCoin minute mark"),
    (native.SPEC, "missing", "dated KuCoin instrument"),
    (native.FUNDING, "missing", "settlement is missing"),
    (native.FUNDING, "prediction", "unverified actual"),
])
def test_incomplete_causal_or_economic_evidence_never_qualifies(monkeypatch, terms_metadata, kind, mode, reason):
    bridge, provider, start, end, now, primary, fw, ex = proof(monkeypatch, "day", futures_values(terms_metadata))
    if mode == "late":
        provider.rows = [replace(row, available_at=end + 3 * MINUTE) if row.kind == kind and row.end == end
                         else row for row in provider.rows]
    elif mode == "prediction":
        provider.rows = [replace(row, values={"rate": 0.0001, "actual_settlement": False}) if row.kind == kind
                         else row for row in provider.rows]
    else:
        provider.rows = [row for row in provider.rows if row.kind != kind]
    with pytest.raises(ValueError, match=reason):
        bridge().publication_evidence(end, end + 4 * MINUTE, primary)
