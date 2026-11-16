import json
import time

import pandas as pd
import pytest

from lab.paper_run import ema, execution, kraken_live
from lab.storage import utc


BASE = utc("2026-11-16T16:00:00Z")


def clock_at(ms):
    return BASE + pd.Timedelta(milliseconds=ms), ms * 1_000_000


def stamp(ms):
    return clock_at(ms)[0].isoformat()


def acknowledgement(incoming, outgoing):
    return json.dumps({"method": "subscribe", "req_id": 1, "success": True,
                       "result": {"channel": "trade", "symbol": "BTC/USD", "snapshot": False},
                       "time_in": incoming, "time_out": outgoing}).encode()


def acknowledge(feed, *, ms=0, epoch=1):
    return feed._message(acknowledgement(stamp(ms + 10), stamp(ms + 15)), clock_at(ms + 30),
                         {"request": clock_at(ms)}, epoch)


def update(trades, *, kind="update"):
    rows = ','.join(f'{{"symbol":"{symbol}","trade_id":{event},"price":{price},"timestamp":"{at}"}}'
                    for symbol, event, price, at in trades)
    return f'{{"channel":"trade","type":"{kind}","data":[{rows}]}}'.encode()


def trade(event, ms, price="69012.345600", symbol="BTC/USD"):
    return symbol, event, price, stamp(ms)


def test_live_batch_preserves_trade_ids_equal_times_exact_price_and_receipt_clocks():
    feed = kraken_live.KrakenTradeFeed()
    session = acknowledge(feed)
    feed._message(update([trade(1, 35), trade(2, 35, "69012.346700")]), clock_at(50), session, 1)
    first, second = feed.drain()
    assert first["price_text"] == "69012.345600" and second["price_text"] == "69012.346700"
    assert first["price"] == 69012.3456
    assert first["source_at"] == second["source_at"] == stamp(35)
    assert first["source"] == ema.SOURCE and first["symbol"] == "BTC/USD"
    assert first["event_id"] == "1" and second["event_id"] == "2"
    assert first["received_at"] == second["received_at"] == stamp(50)
    assert first["received_mono_ns"] == 50_000_000
    assert first["subscription_requested_at"] == stamp(0) and first["subscription_ack_at"] == stamp(30)
    assert first["subscription_ack_mono_ns"] == 30_000_000 and first["subscription_epoch"] == 1
    assert first["observation_kind"] == kraken_live.KIND and "requested_at" not in first
    assert first["raw_sha256"] == second["raw_sha256"]
    assert first["subscription_clock_offset_bounds_seconds"] == pytest.approx([-.015, .01])
    lane = {"source": ema.SOURCE, "symbol": ema.SYMBOL, "observation_kind": kraken_live.KIND}
    assert execution.valid_snapshot(second, lane, *clock_at(2035)) is None
    assert execution.valid_snapshot(second, lane, *clock_at(2036)) == "STALE_REFERENCE"
    assert execution.valid_snapshot(second, {**lane, "observation_kind": None}, *clock_at(60)) == "UNAPPROVED_OBSERVATION_TRANSPORT"


def test_ack_and_control_never_supply_a_price_and_untrusted_trade_rows_are_rejected():
    feed = kraken_live.KrakenTradeFeed()
    feed._message(update([trade(51, 5)]), clock_at(8), {"request": clock_at(0)}, 1)
    session = acknowledge(feed)
    feed._message(b'{"channel":"heartbeat"}', clock_at(31), session, 1)
    feed._message(b'{"channel":"status"}', clock_at(32), session, 1)
    feed._message(update([trade(55, 35)], kind="snapshot"), clock_at(40), session, 1)
    feed._message(update([trade(56, 9), trade(57, 50), trade(58, 38, "true"), trade(60, 40, symbol="ETH/USD")]),
                  clock_at(45), session, 1)
    feed._message(update([trade(61, 40)]), clock_at(2041), session, 1)
    assert feed.drain() == []
    stats = feed.status()["counts"]
    assert stats["TRADE_BEFORE_ACKNOWLEDGEMENT"] == stats["HISTORY_OR_SNAPSHOT"] == 1
    assert stats["heartbeat"] == stats["status"] == 1
    assert stats["HISTORY_BEFORE_SUBSCRIPTION"] == stats["FUTURE_EXCHANGE_CLOCK"] == stats["STALE_AT_RECEIPT"] == 1
    assert stats["INVALID_TRADE"] == 2


def test_reconnect_discards_queue_and_does_not_reuse_previous_subscriptions():
    feed = kraken_live.KrakenTradeFeed(read_clock=lambda: clock_at(100))
    first = acknowledge(feed)
    feed._message(update([trade(1, 35), trade(3, 40)]), clock_at(50), first, 1)
    feed._disconnected("FIXTURE_DISCONNECT")
    assert feed.drain() == []
    assert feed.status()["counts"]["discarded_on_disconnect"] == 2
    assert feed.status()["counts"]["disconnects"] == 1
    second = acknowledge(feed, ms=100, epoch=2)
    feed._message(update([trade(4, 90), trade(2, 140), trade(3, 145), trade(5, 150), trade(6, 148), trade(7, 150)]),
                  clock_at(155), second, 2)
    points = feed.drain()
    assert [(item["event_id"], item["subscription_epoch"]) for item in points] == [("5", 2), ("7", 2)]
    stats = feed.status()["counts"]
    assert stats["HISTORY_BEFORE_SUBSCRIPTION"] == 1
    assert stats["DUPLICATE_OR_REORDERED_TRADE"] == 3


def test_queue_pressure_is_explicit_and_consumer_checks_processing_age_and_clock_step():
    feed = kraken_live.KrakenTradeFeed(queue_size=2)
    session = acknowledge(feed)
    feed._message(update([trade(1, 35), trade(2, 40), trade(3, 45)]), clock_at(50), session, 1)
    metrics = feed.status()
    assert metrics["queue_size"] == metrics["queue_high_water"] == metrics["queue_limit"] == 2
    assert metrics["counts"]["queue_dropped"] == 1
    assert metrics["last_queue_drop_at"] == stamp(50)
    a, b = feed.drain()
    assert (a["event_id"], b["event_id"]) == ("2", "3")
    assert kraken_live.ready_reason(b, *clock_at(2045)) is None
    assert kraken_live.ready_reason(b, *clock_at(2046)) == "STALE_AT_CONSUME"
    assert kraken_live.ready_reason(b, clock_at(97)[0], 1_000_000_000) == "LOCAL_CLOCK_STEP"


def test_unauthorized_adapter_never_opens_a_socket():
    def blocked(_):
        raise PermissionError("fixture")

    feed = kraken_live.KrakenTradeFeed(connector=lambda *_args, **_kwargs: pytest.fail("unexpected socket"), authorize=blocked)
    feed.start()
    try:
        end = time.monotonic() + 2.
        while feed.status()["state"] != "BLOCKED" and time.monotonic() < end:
            time.sleep(.01)
        assert feed.status()["state"] == "BLOCKED"
        assert feed.status()["reason"] == "KRAKEN_PERMISSION_OR_INSTRUMENT_UNAVAILABLE"
    finally:
        feed.close()


def test_single_connection_owner_sends_only_public_btc_trade_subscription():
    sockets = []

    class Socket:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def send(self, raw):
            assert json.loads(raw) == kraken_live.SUBSCRIBE
            assert json.loads(raw)["params"] == {"channel": "trade", "symbol": ["BTC/USD"], "snapshot": False}
            at = execution.clock()[0].isoformat()
            self.frames = iter([acknowledgement(at, at), update([("BTC/USD", 4, "69012.1", at)])])

        def recv(self, *, timeout, decode):
            assert timeout == 1. and decode is False
            if raw := next(self.frames, None):
                return raw
            time.sleep(.01)
            raise TimeoutError

    def connect_once(url, **options):
        assert url == "wss://ws.kraken.com/v2"
        assert options["max_queue"] == 8 and options["max_size"] == kraken_live.MAX_MESSAGE_BYTES
        assert "additional_headers" not in options
        socket = Socket()
        sockets.append(socket)
        return socket

    feed = kraken_live.KrakenTradeFeed(connector=connect_once, authorize=lambda _: None)
    feed.start().start()
    try:
        end = time.monotonic() + 2.
        while not feed.status()["queue_size"] and time.monotonic() < end:
            time.sleep(.01)
        point, = feed.drain()
        assert point["event_id"] == "4" and feed.status()["state"] == "STREAMING"
        assert len(sockets) == 1
    finally:
        feed.close()
