"""VectorBT adapter for explicit fractional orders and candle-close account marks."""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Fill:
    bar_open: pd.Timestamp
    units: float  # positive buy, negative sell
    price: float  # already includes adverse price proxy


def replay(candles: pd.DataFrame, fills: list[Fill], initial_cash: float, fee: float) -> pd.DataFrame:
    # Each observed bar has ordered slots for buy, sell, and close valuation.
    # An entry-bar stop needs two order slots. Explicit prices prevent close-price lookahead.
    import vectorbt as vbt

    mark_slots = np.arange(len(candles)) * 3 + 2
    marks = pd.Series(candles.close.to_numpy(), index=mark_slots)
    order_slots = []
    for fill in fills:
        index = candles.index.get_loc(fill.bar_open)
        order_slots.append(index * 3 + int(fill.units < 0))
    prices = pd.Series([fill.price for fill in fills], index=order_slots, dtype=float)
    sizes = pd.Series([fill.units for fill in fills], index=order_slots, dtype=float)
    close = pd.concat([marks, prices]).sort_index() if fills else marks
    if not close.index.is_unique:
        raise ValueError("Multiple orders of the same side in one execution bar")
    result = vbt.Portfolio.from_orders(
        close, size=sizes.reindex(close.index), price=prices.reindex(close.index),
        size_type="amount", direction="longonly", fees=fee, min_size=0,
        init_cash=initial_cash, allow_partial=False, raise_reject=True,
    )
    if len(result.orders.records) != len(fills):
        raise ValueError("Engine did not execute every planned order")
    return pd.DataFrame({
        "cash": result.cash().loc[mark_slots].to_numpy(),
        "units": result.assets().loc[mark_slots].to_numpy(),
        "equity": result.value().loc[mark_slots].to_numpy(),
    }, index=pd.DatetimeIndex(candles.close_at, name="close_at"))
