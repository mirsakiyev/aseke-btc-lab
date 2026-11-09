from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from lab.data import OHLCV
from lab.learning.design import SPOT, window
from lab.storage import file_hash, utc


class DataBlocked(ValueError):
    pass


@dataclass
class Market:
    trade: pd.DataFrame
    mark: pd.DataFrame
    funding: dict
    step: pd.Timedelta
    scope: str

    def __post_init__(self):
        self.index = self.trade.index
        self.times = self.index.as_unit("ns").asi8
        self.trade_prices = self.trade[list(OHLCV)].to_numpy()
        self.mark_prices = self.mark.reindex(self.index)[list(OHLCV[:4])].to_numpy()


def load_file(asset, columns, right, max_rows):
    path = Path(asset["file"])
    if file_hash(path) != asset["sha256"]:
        raise DataBlocked(f"Snapshot checksum changed for {path.name}")
    if pq.ParquetFile(path).metadata.num_rows > max_rows:
        raise DataBlocked(f"Native row budget exceeded for {path.name}")
    filters = [(columns[0], "<", right.to_pydatetime())]
    if "close_at" in columns:
        filters.append(("close_at", "<=", right.to_pydatetime()))
    frame = pq.read_table(path, columns=columns, filters=filters, use_threads=False).to_pandas()
    timestamps = frame[columns[0]]
    if not isinstance(timestamps.dtype, pd.DatetimeTZDtype) or str(timestamps.dt.tz) != "UTC":
        raise DataBlocked(f"{path.name} requires timezone-aware UTC timestamps")
    frame[columns[0]] = timestamps.dt.as_unit("ns")
    frame = frame.set_index(columns[0]).sort_index()
    if frame.empty or not frame.index.is_unique or frame.index[-1] >= right:
        raise DataBlocked(f"Empty, duplicated, or out-of-window timestamps in {path.name}")
    return frame


def validate_prices(frame, step):
    values = frame[list(OHLCV)]
    if ((frame.index.as_unit("ns").asi8 % step.value != 0).any()
            or not frame.close_at.eq(frame.index + step).all()
            or not np.isfinite(values).all().all()
            or (values[["open", "high", "low", "close"]] <= 0).any().any()
            or (values.volume < 0).any()
            or (values.high < values[["open", "low", "close"]].max(axis=1)).any()
            or (values.low > values[["open", "high", "close"]].min(axis=1)).any()):
        raise DataBlocked("Misaligned or invalid OHLCV")


def load_market(design, profile_id):
    settings = design["settings"]
    profile = settings["profiles"][profile_id]
    source = design["data"][profile_id]
    if source["status"] == "BLOCKED_DATA":
        raise DataBlocked(source["reason"])
    left, right = window(settings["evaluation"]["train_start"], design["available_at_cutoff_exclusive"], design)
    step = pd.Timedelta(seconds=profile["execution_seconds"])
    columns = ["open_at", *OHLCV, "close_at"]
    max_rows = settings["budget"]["max_native_rows"]
    spec = settings["instruments"][profile["instrument"]]
    if profile_id == SPOT:
        trade = load_file({"file": source["prices"], "sha256": source["sha256"]}, columns, right, max_rows)
        validate_prices(trade, step)
        return Market(trade, trade, {}, step, "COINBASE_SPOT_DEVELOPMENT_REPLAY"), spec
    trade = load_file(source["assets"]["trade"], columns, right, max_rows)
    mark = load_file(source["assets"]["mark"], columns, right, max_rows)
    validate_prices(trade, step)
    validate_prices(mark, step)
    if (not source["hypothetical_current_rules"]
            and (utc(source["rule_effective_from"]) > trade.index[0]
                 or utc(source["rule_effective_through"]) < trade.close_at.iloc[-1])):
        raise DataBlocked("Dated rules do not cover the full trade archive")
    settlements = load_file(source["assets"]["funding"], ["settlement_at", "rate"], right, max_rows)
    interval = pd.Timedelta(minutes=source["funding_interval_minutes"])
    if interval < step or interval % step or not np.isfinite(settlements.rate).all():
        raise DataBlocked("Unsupported funding cadence or invalid rates")
    anchor = utc(source["funding_anchor_utc"])
    first = max(left, trade.index[0])
    offset = (first - anchor) % interval
    expected = pd.date_range(first if not offset else first + interval - offset, trade.index[-1], freq=interval)
    relevant = settlements.loc[first:trade.index[-1], "rate"]
    if not expected.equals(relevant.index):
        raise DataBlocked("Funding settlements missing or off the declared historical grid")
    return Market(trade, mark, relevant.to_dict(), step, "HYPOTHETICAL_REPLAY"), source["rules"]


def market_coverage(market):
    index = market.trade.index
    expected = int((index[-1] - index[0]) / market.step) + 1
    return {"first_open": index[0], "last_close": market.trade.close_at.iloc[-1], "trade_bars": len(index),
            "missing_native_bars": expected - len(index),
            "mark_bars_on_trade_grid": int(np.isfinite(market.mark_prices).all(axis=1).sum()),
            "funding_settlements": len(market.funding), "native_seconds": int(market.step.total_seconds()),
            "timestamp_units": "UTC timezone-aware; normalized to nanoseconds",
            "scope": market.scope}


def capabilities(design, observed):
    result = {}
    for profile, source in design["data"].items():
        if profile == SPOT:
            result[profile] = {"venue_instrument": source["source"], "availability": observed.get(profile),
                               "retrieval": source["published_at"], "source_sha256": source["sha256"],
                               "funding_margin_liquidation": "Not applicable to 1x spot",
                               "limits": "Hourly spot OHLCV and hypothetical taker fills; intrabar order and real quotes unknown."}
        else:
            result[profile] = {
                "venue_instrument": source.get("source", "MEXC BTC_USDT linear perpetual, intended preference only"),
                "availability": observed.get(profile, source["status"]), "retrieval": source.get("retrieved_at"),
                "trade_candles": "Provide permitted UTC one-minute native trade OHLCV with gap/provenance metadata.",
                "mark_candles": "Provide aligned one-minute fair/mark OHLC; finer event timestamps or trades improve ordering.",
                "index_price": "Needed for venue fair-price reproduction and diagnostics; simulator consumes supplied mark directly.",
                "funding": "Provide timestamped settled rates and complete historical settlement schedule, including changes.",
                "rules": "Provide dated contract size, base/contract units, ticks, lots, min/max, margin tier leverage and "
                         "maintenance, liquidation costs and handling. First engine covers a fixed tier only.",
                "fee_channel": "API research scenario 6 bp maker and 8 bp taker per fill, published effective "
                               "2026-06-01 08:00 UTC, unverified for the account. Maker fills are outside the simulation.",
                "restrictions": "Canadian context remains restricted. No venue requests, private access or account verification.",
                "assumption": "OHLC cannot verify IOC quantity, queue depth or execution order. Any supplied OHLC replay is hypothetical.",
            }
    return result
