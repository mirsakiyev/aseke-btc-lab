"""Read causal KuCoin execution minutes for the original EMA paper ledger."""

import pandas as pd

from lab.ema_paper.proxy_native import FUNDING, MARK, MINUTE, TRADE, KuCoinProxyEvidence
from lab.ema_paper.source import timestamp
from lab.phase13.future import expected_funding


FUNDING_TOLERANCE = pd.Timedelta(seconds=5)


def _blocked(state, reason, *, permanent=False, waiting_for_funding=False):
    return {"state": state, "reason": reason, "permanent": permanent, "waiting_for_funding": waiting_for_funding,
            "trade": None, "mark": None, "funding": (), "contract": None, "evidence": {}}


def _receipt(row):
    return {"received_at": row.received_at.isoformat(), "observed_at": row.available_at.isoformat(),
            "raw_sha256": row.raw_sha256}


def _candle(row):
    values = {key: row.values[key] for key in ("open", "high", "low", "close")}
    if row.kind == TRADE:
        values["volume"] = row.values["volume"]
    return {**values, **_receipt(row), "open_at": row.start.isoformat(), "close_at": row.end.isoformat()}


class KuCoinProxyExecution(KuCoinProxyEvidence):
    """Read execution minutes through the same independently granted native source."""

    def _checked_funding(self, start, end, as_of, metadata, futures, *, require_next=False):
        # A settlement at the next nominal slot can be stamped a few seconds
        # early. Reconciliation must account for it before closing the minute.
        schedule_end = end + MINUTE
        scheduled = {slot for slot in expected_funding(metadata, start, schedule_end) if slot < schedule_end}
        required = scheduled if require_next else {slot for slot in scheduled if slot < end}
        grant = self._execution
        snapshot = self._read(grant, FUNDING, max(grant.start, start - FUNDING_TOLERANCE),
                              min(grant.end, as_of, end + FUNDING_TOLERANCE), as_of)
        found = {}
        for row in snapshot.observations:
            slot = row.start.round("min")
            if not start <= row.start < end and slot not in required and slot not in found:
                continue
            if (slot not in scheduled or slot in found or abs(row.start - slot) > FUNDING_TOLERANCE
                    or row.values.get("actual_settlement") is not True
                    or not futures.funding_floor <= row.values["rate"] <= futures.funding_cap):
                raise ValueError("Unexpected, conflicting or unverified actual KuCoin funding settlement")
            found[slot] = row
        if required - found.keys():
            return None, snapshot
        return tuple({**_receipt(row), "settlement_at": row.start.isoformat(), "rate": row.values["rate"]}
                     for row in found.values() if start <= row.start < end), snapshot

    def _settlements(self, start, end, cutoff, terms, futures):
        funding, snapshot = self._checked_funding(start, end, cutoff, terms, futures)
        if funding is None:
            raise ValueError("Actual KuCoin funding settlement is missing on the dated schedule")
        return snapshot

    def execution_evidence(self, at, as_of, *, published_at):
        """Return a closed trade/mark minute, pre-open terms and actual funding, with original receipt clocks."""
        at, now, publication = map(timestamp, (at, as_of, published_at))
        end = at + MINUTE
        if at != at.floor(MINUTE) or not self._protocol_at < publication < at:
            return _blocked("BLOCKED_EXECUTION_EVIDENCE", "Execution minute must strictly follow a prospective publication",
                            permanent=True)
        if end > now:
            return _blocked("WAITING_FOR_DATA", "Awaiting a closed prospective KuCoin execution minute")
        grant = self._execution
        if grant is None or not grant.start <= publication or not end <= grant.end or grant.approved_at > publication:
            return _blocked("BLOCKED_PERMISSION", "Separate KuCoin execution grant does not cover publication and this minute",
                            permanent=True)
        try:
            trade = self._read(grant, TRADE, at, end, now, MINUTE)
            if not trade.complete:
                return _blocked("WAITING_FOR_DATA", "Awaiting independent closed KuCoin trade minute")
            mark = None if self._spot else self._read(grant, MARK, at, end, now, MINUTE)
            if mark is not None and not mark.complete:
                return _blocked("WAITING_FOR_DATA", "Awaiting independent closed KuCoin mark minute")
            try:
                terms, values = self._terms(at)
            except ValueError as exc:
                return _blocked("BLOCKED_EXECUTION_EVIDENCE", str(exc))
            contract, minimum, maximum, futures = self._economics(values)
            funding, funding_snapshot = (), None
            if futures is not None:
                effective = pd.Timestamp(int(values["contract"]["effectiveFundingRateCycleStartTime"]), unit="ms", tz="UTC")
                if effective > at:
                    return _blocked("BLOCKED_EXECUTION_EVIDENCE", "Dated funding schedule is not effective at the modeled open")
                funding, funding_snapshot = self._checked_funding(at, end, now, values, futures, require_next=True)
                if funding is None:
                    return _blocked("BLOCKED_EXECUTION_EVIDENCE", "Awaiting actual settlement on the dated KuCoin schedule",
                                    waiting_for_funding=True)
        except PermissionError as exc:
            return _blocked("BLOCKED_PERMISSION", str(exc), permanent=True)
        except LookupError as exc:
            return _blocked("BLOCKED_EXECUTION_EVIDENCE", str(exc))
        except (ValueError, TypeError) as exc:
            return _blocked("BLOCKED_EXECUTION_EVIDENCE", str(exc), permanent=True)

        trade_bar = _candle(trade.observations[0])
        mark_bar = _candle(mark.observations[0]) if mark is not None else trade_bar
        metadata = terms.observations[-1]
        supporting = [trade.observations[0], metadata]
        if mark is not None:
            supporting.extend(mark.observations)
        if funding_snapshot is not None:
            supporting.extend(funding_snapshot.observations)
        return {"state": "READY", "reason": None, "permanent": False, "waiting_for_funding": False,
                "trade": trade_bar, "mark": mark_bar, "funding": funding, "contract": contract,
                "evidence": {"source_id": grant.source.id, "execution_grant_id": grant.id,
                             "minimum_notional": minimum, "maximum_quantity": maximum,
                             "trade_snapshot_id": trade.id, "mark_snapshot_id": mark.id if mark else None,
                             "contract_snapshot_id": terms.id, "contract_raw_sha256": metadata.raw_sha256,
                             "funding_snapshot_id": funding_snapshot.id if funding_snapshot else None,
                             "latest_provider_received_at": max(row.received_at for row in supporting).isoformat(),
                             "latest_handoff_at": max(row.available_at for row in supporting).isoformat()}}
