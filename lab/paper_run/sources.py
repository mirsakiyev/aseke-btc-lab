"""Bounded point observations on the existing KuCoin public market routes."""

from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from math import isfinite

import httpx
import pandas as pd

from lab.phase9.data import matching, timestamps
from lab.phase9.public import request, retry_boundary
from lab.paper_run.execution import clock


MARK = "KUCOIN_FUTURES_CURRENT_MARK"
SPOT = "KUCOIN_SPOT_TICKER_LAST_REFERENCE"
INTERVAL = pd.Timedelta(seconds=15)
MAX_BODY = 16 * 1024
KUCOIN = {
    MARK: ("XBTUSDTM", request("fresh-mark", "mark_now")),
    SPOT: ("BTC-USDT", request("fresh-spot", "ticker", tradeType="SPOT", symbol="BTC-USDT")),
}


def _decode(source, symbol, raw):
    payload = json.loads(raw, parse_float=Decimal)
    if payload.get("code") != "200000" or not isinstance(payload.get("data"), dict):
        raise ValueError("Unexpected KuCoin response code")
    data = payload["data"]
    if source == MARK:
        if data["symbol"] != symbol or data["granularity"] != 1000:
            raise ValueError("Unexpected mark symbol or snapshot cadence")
        value, at = data["value"], timestamps([data["timePoint"]], "ms")[0]
    else:
        ticker = matching(data, symbol, "SPOT")
        value, at = ticker["lastPrice"], timestamps([data["ts"]], "ns")[0]
    if isinstance(value, bool):
        raise ValueError("Invalid price type")
    price = Decimal(value)
    if not price.is_finite() or not isfinite(float(price)) or price <= 0:
        raise ValueError("Invalid price")
    return {"source": source, "symbol": symbol, "price": float(price), "price_text": str(price),
            "source_at": at.isoformat(), "event_id": at.isoformat(), "raw_sha256": sha256(raw).hexdigest()}


def observe(source, *, read_clock=clock, transport=None):
    symbol, spec = KUCOIN[source]
    requested, begin = read_clock()
    try:
        with httpx.Client(timeout=1.5, follow_redirects=False, transport=transport) as client:
            with client.stream("GET", spec["url"], params=spec["params"]) as response:
                raw = bytearray()
                for block in response.iter_bytes():
                    if len(raw) + len(block) > MAX_BODY:
                        return {"status": "RESPONSE_LIMIT"}
                    raw.extend(block)
                received, end = read_clock()
                if response.status_code == 429:
                    retry = retry_boundary(response.headers.get("Retry-After"), received)
                    return {"status": "RATE_LIMITED", "retry_at": max(retry, received + pd.Timedelta(minutes=1)).isoformat()}
                if response.status_code in (401, 403, 451):
                    return {"status": "ACCESS_RESTRICTED"}
                if response.status_code != 200:
                    return {"status": f"HTTP_{response.status_code}"}
    except httpx.TransportError:
        return {"status": "TRANSPORT_ERROR"}
    try:
        parsed = _decode(source, symbol, bytes(raw))
    except (KeyError, TypeError, ValueError, OverflowError, InvalidOperation):
        return {"status": "INVALID_EXCHANGE_RESPONSE"}
    return {**parsed, "status": "RECEIVED", "requested_at": requested.isoformat(), "received_at": received.isoformat(),
            "request_mono_ns": begin, "received_mono_ns": end, "elapsed_ns": end - begin}
