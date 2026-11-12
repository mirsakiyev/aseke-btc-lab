"""Read-only native market and execution checks for the original EMA books."""

from dataclasses import dataclass, replace
from math import isfinite

import pandas as pd

from lab.config import ORIGIN, fingerprint
from lab.ema_paper.source import METADATA_AGE, MINUTE, timestamp
from lab.learning.simulator import Contract, Cost
from lab.phase13.future import expected_funding
from lab.phase9.ideas import current_rules
from lab.strategy_lab.cross_venue_warmup import ProxySeed, evaluate_transition
from lab.strategy_lab.kucoin_contract import XbtusdtmTerms, adapt_xbtusdtm
from lab.strategy_lab.market_data import MarketData, ObservationKind, ObservationSnapshot, Purpose
from lab.strategy_lab.technical_features import MAX_CHART_ROWS, build_candles


TRADE = ObservationKind.TRADE_OHLCV
MARK = ObservationKind.MARK_OHLCV
SPEC = ObservationKind.CONTRACT_SPEC
FUNDING = ObservationKind.FUNDING_SETTLEMENT
REPLAY_NATIVE_ROWS = 4_800
ORIGINAL_BOOKS = {
    "day": (
        "b27e29a85276053afe1a261c35eeff20eb71eddcff204c2505754c5b4e23f69d",
        "f05d88eb16138612e5f289cf397bbfcf51327760650c37c3438556749a3298bf",
        "FUTURES", "XBTUSDTM", 900,
    ),
    "swing": (
        "a21b0238f61732c63c38a91ecdf5f8266431a56ea68b5dee8dab3e4a56d09630",
        "5dfb19c760b42613d02d78d33f4da0b8822744091ead17808b4f87d83ee50ed5",
        "FUTURES", "XBTUSDTM", 14400,
    ),
    "position": (
        "d146ff6a9e84c9ef9788caf765adb63b6fa33bad7c23326fb5ffd7a2ea48e3c5",
        "f81697afd94d96cf06fb482186cde451ce08727f4a70db28374515fc3a0c8837",
        "SPOT", "BTC-USDT", 86400,
    ),
}


@dataclass(frozen=True, slots=True)
class PublicationEvidence:
    """Causal pre-publication evidence. Future execution still needs receipts."""

    contract: Contract
    cost: Cost
    minimum_notional: float
    maximum_quantity: float
    available_at: pd.Timestamp
    decision_trade: ObservationSnapshot
    trade: ObservationSnapshot
    marks: ObservationSnapshot | None
    pre_chart_terms: ObservationSnapshot
    dated_terms: ObservationSnapshot
    settled_funding: ObservationSnapshot | None
    futures_terms: XbtusdtmTerms | None


class KuCoinProxyEvidence:
    """Consume an independently licensed local adapter supplied by the caller.

    The review pins its exact source identity hash and separate grants. A provider
    name or a plausible symbol alone does not establish the upstream venue.
    The reviewed adapter must attest original KuCoin receipts, actual funding
    settlements, and futures volume normalized to BTC using contemporaneous terms.
    A contract-spec observation includes the raw-linked decoded `instrument`,
    plus `contract` and `isolated_tiers` for futures. Its point timestamp is the
    time of the dated evidence and all constituent receipts must be known by the
    recorded `available_at`.
    """

    def __init__(self, market_data: MarketData, original_registration, *, protocol_recorded_at,
                 source_identity_id, forward_grant_id, execution_grant_id=None):
        lane = original_registration["body"]
        if lane.get("profile") not in ORIGINAL_BOOKS:
            raise PermissionError("Native proxy evidence must bind an original EMA registration")
        reg, book, market, instrument, chart_seconds = ORIGINAL_BOOKS[lane["profile"]]
        if (original_registration["id"] != reg or fingerprint(lane) != reg or lane["book_id"] != book
                or (lane["market"], lane["instrument"], lane["execution_clock"]["decision_seconds"])
                != (market, instrument, chart_seconds)):
            raise PermissionError("Native proxy evidence must bind an original EMA registration")
        if not isinstance(market_data, MarketData):
            raise TypeError("Native proxy evidence requires the reviewed MarketData interface")
        self._data = market_data
        self._book = book
        self._market = market
        self._instrument = instrument
        self._source = source_identity_id
        self._registered_at = timestamp(lane["registered_at"])
        self._protocol_at = timestamp(protocol_recorded_at)
        if self._protocol_at < self._registered_at:
            raise ValueError("The effective protocol cannot predate the original registration")
        self.chart = pd.Timedelta(seconds=chart_seconds)
        self._spot = market == "SPOT"
        self._native = pd.Timedelta(hours=1) if self._spot else MINUTE
        self._chart_grace = pd.Timedelta(seconds=lane["source"]["decision_receipt_seconds"])
        self._native_grace = self._chart_grace + pd.Timedelta(minutes=60 if self._spot else 15)
        self._publication_grace = pd.Timedelta(seconds=lane["source"]["publication_max_seconds"])
        self._liquidation_fee = lane["source"]["modeled_liquidation_fee"]
        self._leverage = lane["risk"]["leverage"]
        self._cost = Cost(lane["cost"]["taker"], lane["cost"]["adverse_proxy"])
        self._follow_through = pd.Timedelta(minutes=lane["follow_through_minutes"])
        self._forward = self._grant(forward_grant_id, Purpose.FORWARD_DECISION, {TRADE})
        roles = {TRADE, SPEC} if self._spot else {TRADE, SPEC, MARK, FUNDING}
        self._execution = (self._grant(execution_grant_id, Purpose.EXECUTION_EVIDENCE, roles)
                           if execution_grant_id is not None else None)

    def _grant(self, grant_id, purpose, roles):
        grant = self._data.grant(grant_id)
        if (grant.book_id != self._book or grant.registered_at != self._registered_at
                or grant.approved_at < self._protocol_at or grant.purpose != purpose or grant.kinds != roles
                or grant.source.id != self._source
                or (grant.source.market, grant.source.instrument) != (self._market, self._instrument)):
            raise PermissionError("Grant must bind the original book, effective protocol, exact native source and roles")
        return grant

    def _clock(self, decision_at, as_of):
        end, as_of = timestamp(decision_at), timestamp(as_of)
        if (end - ORIGIN) % self.chart or not self._protocol_at < end <= as_of:
            raise ValueError("Expected a closed UTC chart boundary strictly after protocol registration")
        return end, as_of

    def _read(self, grant, kind, start, end, as_of, interval=None):
        return self._data.read(grant.id, self._book, grant.purpose, kind, start, end, as_of, interval)

    def read_chart(self, decision_at, as_of):
        """Read one completed native chart bucket for the shared feature engine."""
        end, as_of = self._clock(decision_at, as_of)
        return self._chart_batch(end - self.chart, end, as_of)

    def _chart_batch(self, start, end, as_of):
        cutoff = min(as_of, end + self._chart_grace)
        trade = self._read(self._forward, TRADE, start, end, cutoff, self._native)
        frame = trade.frame()
        timely = frame.loc[frame.available_at.le(frame.close_at + self._native_grace)
                           & frame.available_at.le(frame.close_at.dt.ceil(self.chart) + self._chart_grace)]
        batch = build_candles(timely, timeframe=self.chart, native_step=self._native, as_of=cutoff,
                              start=start, end=end)
        if not batch.bars.empty:
            proof = {at: fingerprint([[row.Index.isoformat(), row.received_at.isoformat(),
                                       row.available_at.isoformat(), row.raw_sha256] for row in group.itertuples()])
                     for at, group in timely.groupby(pd.Grouper(freq=self.chart, origin=ORIGIN))}
            batch.bars["received_at"] = timely.received_at.resample(self.chart, origin=ORIGIN).max()
            batch.bars["receipt_manifest_sha256"] = batch.bars.index.map(proof)
        return batch, trade

    def read_transition(self, seed: ProxySeed, decision_at, as_of):
        """Replay authorized native batches for read-only inspection of one decision clock."""
        end, as_of = self._clock(decision_at, as_of)
        if seed.book_id != self._book or pd.Timedelta(seconds=seed.timeframe_seconds) != self.chart:
            raise PermissionError("Kraken feature seed must bind the original KuCoin book and chart")
        if as_of >= end + self.chart:
            raise ValueError("Transition inspection must use the as-of clock of the current chart")
        start = max(seed.end, self._forward.start.ceil(self.chart))
        if end > self._forward.end or (end - start) // self.chart > MAX_CHART_ROWS:
            raise ValueError("Native transition replay is outside the reviewed range or chart budget")
        interval = (REPLAY_NATIVE_ROWS // (self.chart // self._native)) * self.chart
        frames, receipts = [], []
        if self._forward.approved_at <= as_of:
            for left in pd.date_range(start, end, freq=interval, inclusive="left"):
                batch, snapshot = self._chart_batch(left, min(left + interval, end), as_of)
                if not batch.bars.empty:
                    frames.append(batch.bars)
                receipts.append(snapshot.id)
        history = pd.concat(frames) if frames else None
        transition = evaluate_transition(seed, history, as_of=as_of,
                                         forward_provider="KuCoin", forward_market=self._market,
                                         forward_instrument=self._instrument)
        evidence = {**transition.evidence, "forward_source_id": self._source, "forward_grant_id": self._forward.id,
                    "forward_receipts_hash": fingerprint(receipts)}
        if evidence["first_native_candle"] is not None:
            evidence["first_native_candle"] = {**evidence["first_native_candle"], "receipt_manifest_sha256":
                                               history.at[seed.end, "receipt_manifest_sha256"]}
        return replace(transition, evidence=evidence)

    def _terms(self, at):
        grant = self._execution
        left = max(grant.start, at - METADATA_AGE)
        # Metadata must precede the modeled open or publication clock.
        snapshot = self._read(grant, SPEC, left, at, at)
        if not snapshot.observations or snapshot.observations[-1].received_at + METADATA_AGE <= at:
            raise ValueError("Current dated KuCoin instrument or futures tier receipt is unavailable")
        return snapshot, snapshot.observations[-1].values

    def _economics(self, values):
        terms = None
        if self._spot:
            row = values["instrument"]
            if row.get("symbol") != self._instrument:
                raise ValueError("Instrument receipt is not exact KuCoin BTC-USDT Spot")
            contract = current_rules("SPOT", {"spot": row}, {})
            minimum_notional = float(row["minFunds"])
            maximum_quantity = float(row["maxBaseOrderSize"])
        else:
            terms = adapt_xbtusdtm(values["contract"], values["instrument"], values["isolated_tiers"],
                                  modeled_liquidation_fee=self._liquidation_fee)
            contract = terms.contract
            minimum_notional, maximum_quantity = 0.0, float(terms.max_market_contracts)
        numeric = (minimum_notional, maximum_quantity, contract.base_per_contract, contract.quantity_step,
                   contract.price_tick, contract.min_contracts, contract.tier_notional)
        if (not all(isfinite(value) for value in numeric) or minimum_notional < 0
                or min(numeric[1:]) <= 0 or maximum_quantity < contract.min_contracts
                or contract.max_leverage < self._leverage):
            raise ValueError("Dated KuCoin execution capacity is invalid for the frozen profile")
        return contract, minimum_notional, maximum_quantity, terms

    def _settlements(self, start, end, cutoff, terms, futures):
        expected = set(expected_funding(terms, start, end))
        tolerance = pd.Timedelta(seconds=5)
        grant = self._execution
        funding = self._read(grant, FUNDING, max(grant.start, start - tolerance),
                             min(cutoff, end + tolerance), cutoff)
        found = set()
        for row in funding.observations:
            slot = row.start.round("min")
            if not start <= row.start < end and not start <= slot < end:
                continue
            if (slot not in expected or slot in found or abs(row.start - slot) > tolerance
                    or row.values.get("actual_settlement") is not True
                    or not futures.funding_floor <= row.values["rate"] <= futures.funding_cap):
                raise ValueError("Unexpected, duplicate or unverified actual KuCoin funding settlement")
            found.add(slot)
        if expected - found:
            raise ValueError("Actual KuCoin funding settlement is missing on the dated schedule")
        return funding

    def publication_evidence(self, decision_at, as_of, primary_end_exclusive):
        """Check current economics, separate execution coverage, and latest chart receipts."""
        end, as_of = self._clock(decision_at, as_of)
        start = end - self.chart
        if as_of > end + self._publication_grace:
            raise ValueError("The frozen prospective publication window has elapsed")
        primary_end = timestamp(primary_end_exclusive)
        if end >= primary_end:
            raise ValueError("Decision is outside the primary prospective window")
        grant = self._execution
        if (grant is None or self._forward.end < primary_end
                or not grant.start < start or grant.end < primary_end + self._follow_through):
            raise PermissionError("Separate KuCoin execution grant must cover dated terms, the chart and full follow-through")
        batch, trade = self.read_chart(end, as_of)
        if not batch.complete:
            raise ValueError("Complete timely KuCoin native decision chart is unavailable")
        cutoff = min(as_of, end + self._chart_grace)
        execution_trade = self._read(grant, TRADE, end - MINUTE, end, cutoff, MINUTE)
        if (not execution_trade.complete or (not self._spot
                and execution_trade.observations[0] != trade.observations[-1])):
            raise ValueError("Independent KuCoin execution trade minute is missing or inconsistent")
        dated_terms, values = self._terms(start)
        current_terms, current_values = self._terms(as_of)
        contract, min_notional, max_quantity, futures = self._economics(current_values)
        first_contract, first_notional, first_quantity, first_futures = self._economics(values)
        if (first_contract, first_notional, first_quantity) != (contract, min_notional, max_quantity):
            raise ValueError("KuCoin execution economics changed across the latest chart bucket")
        mark = funding = None
        if not self._spot:
            mark = self._read(grant, MARK, start, end, cutoff, MINUTE)
            if not mark.complete or any(row.available_at > row.end + self._native_grace for row in mark.observations):
                raise ValueError("Latest chart lacks complete timely native KuCoin minute mark receipts")
            # The next settlement timestamp rolls forward on an unchanged cycle.
            first_cycle = replace(first_futures, funding_anchor_ms=first_futures.funding_anchor_ms
                                  % first_futures.funding_interval_ms)
            cycle = replace(futures, funding_anchor_ms=futures.funding_anchor_ms % futures.funding_interval_ms)
            first_effective = pd.Timestamp(int(values["contract"]["effectiveFundingRateCycleStartTime"]),
                                           unit="ms", tz="UTC")
            current_effective = pd.Timestamp(int(current_values["contract"]["effectiveFundingRateCycleStartTime"]),
                                             unit="ms", tz="UTC")
            next_settlement = pd.Timestamp(futures.funding_anchor_ms, unit="ms", tz="UTC")
            if first_cycle != cycle or max(first_effective, current_effective) > start:
                raise ValueError("KuCoin funding schedule changed across the latest chart bucket")
            if next_settlement <= as_of:
                raise ValueError("Current dated KuCoin terms do not identify the next funding settlement")
            funding = self._settlements(start, end, as_of, values, futures)
        snapshots = (trade, execution_trade, dated_terms, current_terms, mark, funding)
        latest = max(row.available_at for snapshot in snapshots if snapshot is not None
                     for row in snapshot.observations)
        return PublicationEvidence(contract, self._cost, min_notional, max_quantity, latest, trade,
                                   execution_trade, mark, dated_terms, current_terms, funding, futures)
