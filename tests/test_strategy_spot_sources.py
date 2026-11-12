from dataclasses import replace
from hashlib import sha256
import json
import sys

import pandas as pd
import pytest

from lab.strategy_lab.market_data import MarketData, ObservationKind, ProvenanceClass, Purpose, SourceGrant, SourceIdentity
from lab.strategy_lab.spot_sources import (
    COINBASE_URL, INSTRUMENT, KRAKEN_URL, SpotCandleProvider, SpotCandleReceipt, SpotCandleRequest,
    Venue, inspect_receipt, load_captured_receipt, main,
)


SEAM = pd.Timestamp("2026-11-13T00:00:00Z")
SECOND = pd.Timedelta(seconds=1)
TRADE = ObservationKind.TRADE_OHLCV


def _prices(index):
    base = 90000 + 18 * (index % 9) + index / 5
    return [str(base), str(base + 20), str(base - 30), str(base - 5), str(index / 10)]


def _payload(venue, stamps):
    rows = []
    for index, stamp in enumerate(stamps):
        second = int(stamp.timestamp())
        opening, high, low, closing, volume = _prices(index)
        if venue == Venue.KRAKEN:
            rows.append([second, opening, high, low, closing, "17", volume, 23])
        else:
            rows.append(dict(start=str(second), open=opening, high=high, low=low, close=closing, volume=volume))
    if venue == Venue.KRAKEN:
        return {"error": [], "result": {"XXBTZUSD": rows, "last": int(stamps[-1].timestamp())}}
    return {"candles": list(reversed(rows))}


def _receipt(venue, minutes, start, end, *, count=None, stamps=None, started=None):
    step = pd.Timedelta(minutes=minutes)
    request = SpotCandleRequest(venue, minutes, start, end)
    if stamps is None:
        stamps = list(pd.date_range(end=end, periods=count + 1, freq=step))
    started = end + SECOND if started is None else started
    return SpotCandleReceipt(request, started, started + 2 * SECOND, started + 3 * SECOND,
                             200, json.dumps(_payload(venue, stamps)).encode())


def _source(venue):
    return SourceIdentity(venue.value, "fixture public candles", INSTRUMENT, "SPOT",
                          "synthetic fixture rights", sha256(b"fixture").hexdigest(), ProvenanceClass.INDEPENDENT)


def _grant(source, book, purpose, start, end):
    registered = SEAM - SECOND
    return SourceGrant(source, book, purpose, frozenset({TRADE}), start, end, registered, registered,
                       "synthetic fixture approval")


@pytest.mark.parametrize("minutes", [15, 240, 1440])
def test_kraken_recent_bounds_exclude_documented_uncommitted_row(minutes):
    step = pd.Timedelta(minutes=minutes)
    start = SEAM - 720 * step
    receipt = _receipt(Venue.KRAKEN, minutes, start, SEAM, count=720)
    audit = inspect_receipt(receipt)
    assert receipt.request.url == KRAKEN_URL
    assert receipt.request.params == {"pair": "XBTUSD", "interval": minutes, "since": int(start.timestamp())}
    assert audit.raw_rows == 721 and audit.complete_rows == 720
    assert audit.excluded_unfinished == 1 and audit.missing_opens == ()
    assert audit.longest_contiguous_run == 720
    assert audit.coverage_start == start and audit.coverage_end == SEAM
    assert audit.received_at == SEAM + 3 * SECOND and audit.latency_seconds == 2
    assert audit.raw_sha256 == sha256(receipt.raw).hexdigest()
    assert audit.minimum_base_volume == 0 and audit.maximum_base_volume == 71.9 and audit.zero_volume_rows == 1


def test_kraken_response_beyond_720_closed_rows_and_unfinished_tail_is_rejected():
    step = pd.Timedelta(minutes=15)
    stamps = list(pd.date_range(end=SEAM, periods=722, freq=step))
    receipt = _receipt(Venue.KRAKEN, 15, SEAM - 720 * step, SEAM, stamps=stamps)
    with pytest.raises(ValueError, match="unfinished row limit"):
        inspect_receipt(receipt)


@pytest.mark.parametrize(("minutes", "granularity"), [(15, "FIFTEEN_MINUTE"), (240, "FOUR_HOUR"), (1440, "ONE_DAY")])
def test_coinbase_request_cap_and_in_flight_close_never_counts(minutes, granularity):
    step = pd.Timedelta(minutes=minutes)
    start = SEAM - 350 * step
    stamps = list(pd.date_range(start=start, periods=350, freq=step))
    receipt = _receipt(Venue.COINBASE, minutes, start, SEAM, stamps=stamps, started=SEAM - SECOND)
    assert receipt.request.url == COINBASE_URL
    assert receipt.request.params["granularity"] == granularity and receipt.request.params["limit"] == 350
    assert receipt.request.params["end"] == str(int(SEAM.timestamp()) - 1)
    assert receipt.request.headers["Cache-Control"] == "no-cache"
    audit = inspect_receipt(receipt)
    assert audit.raw_rows == 350 and audit.complete_rows == 349
    assert audit.excluded_unfinished == 1 and audit.coverage_end == SEAM - step
    assert audit.missing_opens == () and audit.longest_contiguous_run == 349
    complete = inspect_receipt(_receipt(Venue.COINBASE, minutes, start, SEAM, stamps=stamps))
    assert complete.raw_rows == complete.complete_rows == complete.longest_contiguous_run == 350
    with pytest.raises(ValueError, match="candle limit"):
        SpotCandleRequest(Venue.COINBASE, minutes, start - step, SEAM)


def test_scope_missing_rows_distinct_grants_and_first_receipts():
    source = _source(Venue.KRAKEN)
    step = pd.Timedelta(minutes=15)
    start = SEAM - 4 * step
    stamps = [start, start + step, start + 3 * step, SEAM]
    receipt = _receipt(Venue.KRAKEN, 15, start, SEAM, stamps=stamps)
    late = replace(receipt, received_at=receipt.received_at + SECOND, available_at=receipt.available_at + SECOND)
    provider = SpotCandleProvider(source, [late, receipt, receipt])
    first, other = (_grant(source, book, Purpose.WARMUP, start, SEAM) for book in ("a", "b"))
    data = MarketData([first, other], {source.id: provider})
    snapshots = [data.read(grant.id, grant.book_id, Purpose.WARMUP, TRADE, start, SEAM, late.available_at, step)
                 for grant in (first, other)]
    left, right = snapshots
    assert not left.complete and left.missing_opens == (start + 2 * step,)
    assert left.content_id == right.content_id and left.id != right.id
    assert all(row.received_at == receipt.received_at and row.available_at == receipt.available_at
               for row in left.observations)
    assert left.frame().volume.tolist() == [0, .1, .2]
    with pytest.raises(PermissionError, match="outside"):
        data.read(first.id, other.book_id, Purpose.WARMUP, TRADE, start, SEAM, late.available_at, step)
    with pytest.raises(ValueError, match="venue differs"):
        SpotCandleProvider(_source(Venue.COINBASE), [receipt])
    with pytest.raises(ValueError, match="Spot source"):
        SpotCandleProvider(replace(source, instrument="BTC-USDT"), [receipt])
    with pytest.raises(ValueError, match="Spot source"):
        SpotCandleProvider(replace(source, market="FUTURES"), [receipt])


def test_kraken_unbounded_request_reports_trailing_missing_candles():
    step = pd.Timedelta(minutes=15)
    receipt = _receipt(Venue.KRAKEN, 15, SEAM - 4 * step, SEAM,
                       stamps=[SEAM - 4 * step, SEAM - 3 * step, SEAM])
    audit = inspect_receipt(replace(receipt, request=SpotCandleRequest(Venue.KRAKEN, 15)))
    assert audit.complete_rows == 2
    assert audit.missing_opens == (SEAM - 2 * step, SEAM - step)


def test_local_audit_cli_validates_checksum_and_causal_clock(tmp_path, monkeypatch, capsys):
    step = pd.Timedelta(minutes=15)
    receipt = _receipt(Venue.KRAKEN, 15, SEAM - step, SEAM, count=1)
    with pytest.raises(ValueError, match="out of order"):
        replace(receipt, available_at=receipt.started_at)
    raw_path = tmp_path / "raw.json"
    raw_path.write_bytes(receipt.raw)
    metadata_path = tmp_path / "capture.json"
    request = receipt.request
    metadata = {
        "venue": request.venue.value, "interval_minutes": request.interval_minutes, "start": request.start.isoformat(),
        "end": request.end.isoformat(), "started_at": receipt.started_at.isoformat(), "status": receipt.status,
        "received_at": receipt.received_at.isoformat(), "available_at": receipt.available_at.isoformat(),
        "raw_path": raw_path.name, "raw_sha256": receipt.raw_sha256,
    }
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["spot_sources", str(metadata_path)])
    main()
    assert json.loads(capsys.readouterr().out)["complete_rows"] == 1
    raw_path.write_bytes(receipt.raw + b" ")
    with pytest.raises(SystemExit, match="2"):
        main()
    assert "checksum mismatch" in capsys.readouterr().err


def test_coinbase_capture_preserves_original_wire_end(tmp_path):
    step = pd.Timedelta(hours=4)
    receipt = _receipt(Venue.COINBASE, 240, SEAM - step, SEAM, stamps=[SEAM - step])
    (tmp_path / "raw.json").write_bytes(receipt.raw)
    metadata = {
        "venue": receipt.request.venue.value, "interval_minutes": 240,
        "start": (SEAM - step).isoformat(), "end": SEAM.isoformat(),
        "started_at": receipt.started_at.isoformat(), "received_at": receipt.received_at.isoformat(),
        "available_at": receipt.available_at.isoformat(), "status": 200,
        "raw_path": "raw.json", "raw_sha256": receipt.raw_sha256,
    }
    path = tmp_path / "capture.json"
    path.write_text(json.dumps(metadata), encoding="utf-8")
    previous = load_captured_receipt(path)
    assert previous.request.params["end"] == str(int(SEAM.timestamp()))
    metadata["wire_end"] = (SEAM - SECOND).isoformat()
    path.write_text(json.dumps(metadata), encoding="utf-8")
    current = load_captured_receipt(path)
    assert current.id == receipt.id and current.id != previous.id
    metadata["wire_end"] = None
    path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="recorded wire end"):
        load_captured_receipt(path)
