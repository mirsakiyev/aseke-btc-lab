from dataclasses import FrozenInstanceError, replace
from hashlib import sha256

import pandas as pd
import pytest

from lab.strategy_lab.market_data import (
    MarketData, Observation, ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity,
)


START = pd.Timestamp("2024-01-01T00:00:00Z")
MINUTE = pd.Timedelta(minutes=1)
END = START + 3 * MINUTE
AS_OF = END + MINUTE
TRADE = ObservationKind.TRADE_OHLCV
MARK = ObservationKind.MARK_OHLCV
FUNDING = ObservationKind.FUNDING_SETTLEMENT
CONTRACT = ObservationKind.CONTRACT_SPEC
OHLCV = {"open": 10, "high": 15, "low": 8, "close": 11, "volume": 4}
RAW = b"synthetic original response"
RAW_SHA = sha256(RAW).hexdigest()


class LocalProvider:
    def __init__(self, rows=(), raw=None):
        self.rows = rows
        self.bodies = {RAW_SHA: RAW} if raw is None else raw
        self.reads = 0
        self.raw_reads = []

    def read(self, kind, start, end, as_of, interval):
        self.reads += 1
        return (row for row in self.rows if row.kind == kind and start <= row.start < end)

    def raw(self, digest):
        self.raw_reads.append(digest)
        return self.bodies[digest]


@pytest.fixture
def independent():
    return SourceIdentity("synthetic", "test-feed", "TEST-USDT", "FUTURES", "fixture-rights", RAW_SHA,
                          ProvenanceClass.INDEPENDENT)


def grant(source, book, purpose=Purpose.WARMUP, kinds=frozenset({TRADE})):
    registered = START - MINUTE
    approved = END if purpose == Purpose.WARMUP else registered
    return SourceGrant(source, book, purpose, kinds, START, END, approved, registered, "synthetic approval")


def candle(source, at, available=None, raw_sha=RAW_SHA, values=None):
    closed = at + MINUTE
    return Observation(source.id, TRADE, at, closed, closed, closed if available is None else available,
                       raw_sha, OHLCV if values is None else values)


def read(reader, approved, *, book_id=None, purpose=None, kind=TRADE, start=START,
         end=END, as_of=AS_OF, interval=MINUTE):
    return reader.read(approved.id, book_id or approved.book_id, purpose or approved.purpose, kind,
                       start, end, as_of, interval)


def test_shared_observations_require_separate_book_grants(independent):
    first, second = (grant(independent, book) for book in ("book-a", "book-b"))
    provider = LocalProvider([candle(independent, START + i * MINUTE) for i in range(3)])
    reader = MarketData([first, second], {independent.id: provider})
    assert reader.grant(first.id) is first
    with pytest.raises(PermissionError):
        read(reader, first, book_id=second.book_id)
    with pytest.raises(PermissionError):
        read(reader, first, purpose=Purpose.FORWARD_DECISION)
    with pytest.raises(PermissionError):
        read(reader, first, start=START - MINUTE)
    with pytest.raises(PermissionError):
        read(reader, first, as_of=START)
    with pytest.raises(PermissionError):
        read(reader, first, kind=MARK)
    with pytest.raises(PermissionError):
        MarketData([first], {independent.id: provider}).grant(second.id)
    assert provider.reads == 0

    a, b = (read(reader, approved) for approved in (first, second))
    assert a.id != b.id and a.content_id == b.content_id
    assert a.grant_id == first.id and b.grant_id == second.id
    assert a.complete and b.complete and a.frame().equals(b.frame())
    assert a.frame().index.equals(pd.date_range(START, periods=3, freq=MINUTE, name="open_at"))
    assert provider.raw_reads == [RAW_SHA, RAW_SHA]


def test_protected_unverified_and_preapproval_forward_scope_are_denied(independent):
    for restriction in (ProvenanceClass.PROTECTED, ProvenanceClass.UNVERIFIED):
        with pytest.raises(PermissionError, match="Protected or unverified"):
            grant(replace(independent, provenance_class=restriction), "book-a")
    approved = grant(independent, "book-a", Purpose.FORWARD_DECISION)
    with pytest.raises(PermissionError, match="prospective grant"):
        replace(approved, approved_at=END)
    with pytest.raises(ValueError, match="for features only"):
        grant(independent, "book-a", kinds={FUNDING})
    with pytest.raises(ValueError, match="granted market"):
        grant(replace(independent, market="SPOT"), "book-a", Purpose.EXECUTION_EVIDENCE, {MARK})


def test_coverage_filters_future_rows_and_fails_on_visible_correction(independent):
    approved = grant(independent, "book-a")
    correction_raw = b"synthetic corrected receipt"
    correction_sha = sha256(correction_raw).hexdigest()
    original = candle(independent, START)
    late = candle(independent, START + MINUTE, AS_OF + MINUTE)
    correction = replace(original, received_at=AS_OF + MINUTE, available_at=AS_OF + MINUTE,
                         values={**OHLCV, "close": 12}, raw_sha256=correction_sha)
    provider = LocalProvider([correction, original, late, original], {RAW_SHA: RAW, correction_sha: correction_raw})
    reader = MarketData([approved], {independent.id: provider})
    snapshot = read(reader, approved)
    assert not snapshot.complete
    assert snapshot.observations == (original,)
    assert snapshot.missing_opens == (START + MINUTE, START + 2 * MINUTE)
    assert provider.raw_reads == [RAW_SHA]
    with pytest.raises(ValueError, match="Conflicting observations"):
        read(reader, approved, as_of=AS_OF + MINUTE)


def test_snapshot_replay_preserves_first_receipt_and_payload_immutability(independent):
    approved = grant(independent, "book-a")
    first = candle(independent, START)
    later = replace(first, received_at=END, available_at=END)
    provider = LocalProvider([later, first, first])
    reader = MarketData([approved], {independent.id: provider})
    a = read(reader, approved)
    provider.rows = [first, later]
    b = read(reader, approved, as_of=AS_OF + MINUTE)
    assert a.id != b.id and a.content_id == b.content_id
    assert a.observations == b.observations == (first,)
    with pytest.raises(TypeError):
        a.observations[0].values["close"] = 2
    with pytest.raises(FrozenInstanceError):
        a.grant_id = "unapproved"
    with pytest.raises(ValueError, match="actual causal"):
        replace(first, received_at=START)


def test_source_id_raw_hash_alignment_and_size_fail_closed(independent):
    approved = grant(independent, "book-a")
    other = replace(independent, source_id="other")
    for rows, raw, reason in (
        ([candle(other, START)], None, "requested source"),
        ([candle(independent, START)], {RAW_SHA: b"tampered"}, "raw hash"),
        ([candle(independent, START + pd.Timedelta(seconds=1))], None, "misaligned"),
    ):
        reader = MarketData([approved], {independent.id: LocalProvider(rows, raw)})
        with pytest.raises(ValueError, match=reason):
            read(reader, approved)
    provider = LocalProvider()
    reader = MarketData([approved], {independent.id: provider}, max_observations=2)
    with pytest.raises(ValueError, match="observation budget"):
        read(reader, approved)
    assert provider.reads == 0


def test_nontrade_kinds_preserve_point_evidence_and_mark_without_volume(independent):
    approved = grant(independent, "book-a", Purpose.EXECUTION_EVIDENCE, {MARK, CONTRACT, FUNDING})
    prices = {key: value for key, value in OHLCV.items() if key != "volume"}
    mark = Observation(independent.id, MARK, START, START + MINUTE, END, END, RAW_SHA, prices)
    funding = Observation(independent.id, FUNDING, START, START, END, END, RAW_SHA, {"rate": -.0005})
    metadata = {"symbol": "TEST-USDT", "dated_terms": {"tiers": [{"limit": 20}]}}
    contract = Observation(independent.id, CONTRACT, START, START, END, END, RAW_SHA, metadata)
    reader = MarketData([approved], {independent.id: LocalProvider([mark, funding, contract])})
    assert "volume" not in read(reader, approved, kind=MARK).frame()
    settlements = read(reader, approved, kind=FUNDING, interval=None)
    assert settlements.observations == (funding,) and not settlements.complete
    assert read(reader, approved, kind=CONTRACT, interval=None).observations == (contract,)
    metadata["dated_terms"]["tiers"][0]["limit"] = 99
    assert contract.values["dated_terms"]["tiers"][0]["limit"] == 20
    with pytest.raises(ValueError, match="trade volume"):
        replace(mark, values=OHLCV)
    with pytest.raises(ValueError, match="Trade candle volume"):
        candle(independent, START, values=prices)
