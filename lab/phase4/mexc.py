"""Offline MEXC Spot V3 request planning and response validation. No transport."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from collections.abc import Iterator

from lab.phase4.config import MarketIdentity

API_BASE = "https://api.mexc.com"
KLINES = "/api/v3/klines"
EXCHANGE_INFO = "/api/v3/exchangeInfo"
BOOK_TICKER = "/api/v3/ticker/bookTicker"
MAX_CANDLES = 500
INTERVAL_MS = {
    "5m": 300_000, "15m": 900_000, "60m": 3_600_000,
    "4h": 14_400_000, "1d": 86_400_000, "1W": 604_800_000,
}
PROFILE_INTERVALS = {
    "position_spot": {"decision": "1d", "context": ("1W",), "execution": "60m"},
    "swing": {"decision": "4h", "context": ("1d",), "execution": "60m"},
    "day": {"decision": "15m", "context": ("60m", "4h"), "execution": "5m"},
}


def mexc_market(market: MarketIdentity) -> None:
    if market.venue != "mexc":
        raise ValueError("This parser requires a distinct MEXC spot identity")


def epoch_ms(value: object) -> int:
    if type(value) is not int or not 10**12 <= value < 10**13:
        raise ValueError("Expected a 13-digit Unix millisecond timestamp")
    return value


def decimal_value(value: object, name: str, *, positive: bool = False) -> Decimal:
    if type(value) not in (str, int, Decimal):
        raise ValueError(f"{name} must be an exact decimal string or integer")
    try:
        amount = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} is not decimal") from exc
    if not amount.is_finite() or amount < 0 or (positive and amount == 0):
        raise ValueError(f"{name} is outside its numerical range")
    return amount


@dataclass(frozen=True)
class CandleWindow:
    market: MarketIdentity
    interval: str
    start_ms: int
    end_exclusive_ms: int
    anchor_ms: int

    @property
    def step(self) -> int:
        try:
            return INTERVAL_MS[self.interval]
        except KeyError as exc:
            raise ValueError(f"Unsupported MEXC interval: {self.interval}") from exc

    def __post_init__(self) -> None:
        mexc_market(self.market)
        if type(self.anchor_ms) is not int:
            raise ValueError("An explicit candle grid anchor is required")
        start, end = epoch_ms(self.start_ms), epoch_ms(self.end_exclusive_ms)
        if end <= start or (start - self.anchor_ms) % self.step or (end - self.anchor_ms) % self.step:
            raise ValueError("Expected an increasing range aligned to the declared candle grid")
        if self.count > MAX_CANDLES:
            raise ValueError("A Spot V3 page can request at most 500 candles")

    @property
    def count(self) -> int:
        return (self.end_exclusive_ms - self.start_ms) // self.step

    @property
    def parameters(self) -> dict:
        return {"symbol": self.market.symbol, "interval": self.interval, "startTime": self.start_ms,
                "endTime": self.end_exclusive_ms - 1, "limit": self.count}


def candle_windows(market: MarketIdentity, interval: str, start_ms: int, end_exclusive_ms: int,
                   *, anchor_ms: int, max_pages: int = 8) -> Iterator[CandleWindow]:
    if interval not in INTERVAL_MS:
        raise ValueError(f"Unsupported MEXC interval: {interval}")
    epoch_ms(start_ms)
    epoch_ms(end_exclusive_ms)
    if not 1 <= max_pages <= 8 or start_ms >= end_exclusive_ms:
        raise ValueError("Provide an increasing range and at most eight pages per probe")
    page_span = MAX_CANDLES * INTERVAL_MS[interval]
    if end_exclusive_ms - start_ms > max_pages * page_span:
        raise ValueError("Range exceeds the bounded page budget")
    for start in range(start_ms, end_exclusive_ms, page_span):
        yield CandleWindow(market, interval, start, min(start + page_span, end_exclusive_ms), anchor_ms)


@dataclass(frozen=True)
class Candle:
    open_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    close_ms: int
    quote_volume: Decimal


@dataclass(frozen=True)
class CandlePage:
    window: CandleWindow
    received_at_ms: int
    candles: tuple[Candle, ...]
    incomplete_open_ms: tuple[int, ...]
    missing_closed_open_ms: tuple[int, ...]
    duplicate_rows: int
    outside_range_rows: int


def parse_candles(payload: object, window: CandleWindow, received_at_ms: int) -> CandlePage:
    now = epoch_ms(received_at_ms)
    if not isinstance(payload, list) or len(payload) > MAX_CANDLES:
        raise ValueError("Expected at most 500 rows from MEXC klines")
    observed = {}
    duplicates, outside = 0, 0
    for row in payload:
        if not isinstance(row, list) or len(row) != 8:
            raise ValueError("Unexpected MEXC kline row shape")
        opened, closes = epoch_ms(row[0]), epoch_ms(row[6])
        if (opened - window.anchor_ms) % window.step or closes != opened + window.step:
            raise ValueError("Unexpected MEXC candle grid or close time")
        candle = Candle(opened, *(decimal_value(row[i], field, positive=True)
                                  for i, field in enumerate(("open", "high", "low", "close"), 1)),
                        decimal_value(row[5], "base volume"), closes, decimal_value(row[7], "quote volume"))
        if not candle.low <= min(candle.open, candle.close) <= max(candle.open, candle.close) <= candle.high:
            raise ValueError("Invalid MEXC OHLC relationship")
        if not window.start_ms <= opened < window.end_exclusive_ms:
            outside += 1
        elif opened in observed:
            if observed[opened] != candle:
                raise ValueError("Conflicting MEXC candle duplicates")
            duplicates += 1
        else:
            observed[opened] = candle
    closed = tuple(c for _, c in sorted(observed.items()) if c.close_ms <= now)
    incomplete = tuple(sorted(k for k, c in observed.items() if c.close_ms > now))
    missing = tuple(at for at in range(window.start_ms, window.end_exclusive_ms, window.step)
                    if at + window.step <= now and at not in observed)
    return CandlePage(window, now, closed, incomplete, missing, duplicates, outside)


def precision(value: object, field: str) -> int:
    if type(value) is not int or not 0 <= value <= 18:
        raise ValueError(f"Invalid decimal-place precision: {field}")
    return value


def enum_string(value: object, allowed: set[str], field: str) -> str:
    if type(value) not in (str, int) or str(value) not in allowed:
        raise ValueError(f"Unexpected MEXC {field}")
    return str(value)


@dataclass(frozen=True)
class SymbolInfo:
    market: MarketIdentity
    status: str
    spot_api_allowed: bool
    trade_sides: str
    order_types: tuple[str, ...]
    base_decimal_places: int
    price_decimal_places: int
    quote_asset_decimal_places: int
    minimum_base_quantity: Decimal
    minimum_limit_notional: Decimal
    maximum_limit_notional: Decimal | None
    minimum_market_notional: Decimal | None
    maximum_market_notional: Decimal | None
    public_maker_commission: Decimal | None
    public_taker_commission: Decimal | None
    price_band: tuple[Decimal, Decimal] | None


def parse_exchange_info(payload: object, market: MarketIdentity) -> SymbolInfo:
    mexc_market(market)
    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(symbols, list) or len(symbols) != 1 or not isinstance(symbols[0], dict):
        raise ValueError("Expected a single-symbol MEXC exchangeInfo response")
    row = symbols[0]
    if (row.get("symbol"), row.get("baseAsset"), row.get("quoteAsset")) != (
        market.symbol, market.base_currency, market.quote_currency,
    ):
        raise ValueError("MEXC metadata does not match the proposed symbol and currencies")
    spot, orders = row.get("isSpotTradingAllowed"), row.get("orderTypes")
    if type(spot) is not bool or not isinstance(orders, list) or not all(isinstance(kind, str) for kind in orders):
        raise ValueError("Malformed spot capability metadata")
    filters = row.get("filters", [])
    if not isinstance(filters, list) or not all(isinstance(item, dict) for item in filters):
        raise ValueError("Malformed MEXC filter metadata")
    band = next((item for item in filters if item.get("filterType") == "PERCENT_PRICE_BY_SIDE"), None)
    prices = tuple(decimal_value(band.get(name), name) for name in ("bidMultiplierUp", "askMultiplierDown")) if band else None

    def optional(field: str) -> Decimal | None:
        return None if row.get(field) is None else decimal_value(row[field], field)

    info = SymbolInfo(
        market, enum_string(row.get("status"), {"1", "2", "3"}, "symbol status"), spot,
        enum_string(row.get("tradeSideType"), {"1", "2", "3", "4"}, "trade side"), tuple(orders),
        precision(row.get("baseAssetPrecision"), "baseAssetPrecision"),
        precision(row.get("quotePrecision"), "quotePrecision"),
        precision(row.get("quoteAssetPrecision"), "quoteAssetPrecision"),
        decimal_value(row.get("baseSizePrecision"), "baseSizePrecision"),
        decimal_value(row.get("quoteAmountPrecision"), "quoteAmountPrecision"), optional("maxQuoteAmount"),
        optional("quoteAmountPrecisionMarket"), optional("maxQuoteAmountMarket"), optional("makerCommission"),
        optional("takerCommission"), prices,
    )
    for minimum, maximum in ((info.minimum_limit_notional, info.maximum_limit_notional),
                             (info.minimum_market_notional, info.maximum_market_notional)):
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError("MEXC minimum notional exceeds the maximum")
    return info


def offline_quantity_check(info: SymbolInfo, risk_quantity: Decimal, assumed_price: Decimal) -> tuple[Decimal, str]:
    """Estimate whether a risk-capped amount clears public market-order minima."""
    quantity = decimal_value(risk_quantity, "risk-capped quantity")
    price = decimal_value(assumed_price, "assumed price", positive=True)
    rounded = quantity.quantize(Decimal(1).scaleb(-info.base_decimal_places), rounding=ROUND_DOWN)
    notional = rounded * price
    if info.status != "1" or not info.spot_api_allowed or info.trade_sides != "1" or "MARKET" not in info.order_types:
        return rounded, "spot_market_capability_unavailable"
    if info.minimum_market_notional is None or info.maximum_market_notional is None:
        return rounded, "market_limits_unverified"
    if rounded == 0 or rounded < info.minimum_base_quantity or notional < info.minimum_market_notional:
        return rounded, "below_minimum"
    if notional > info.maximum_market_notional:
        return rounded, "above_maximum"
    return rounded, "passes_offline_metadata_checks"


@dataclass(frozen=True)
class BookTicker:
    market: MarketIdentity
    received_at_ms: int
    bid: Decimal
    bid_quantity: Decimal
    ask: Decimal
    ask_quantity: Decimal

    @property
    def observed_spread_bps(self) -> Decimal:
        return 20_000 * (self.ask - self.bid) / (self.ask + self.bid)


def parse_book_ticker(payload: object, market: MarketIdentity, received_at_ms: int) -> BookTicker:
    mexc_market(market)
    if not isinstance(payload, dict) or payload.get("symbol") != market.symbol:
        raise ValueError("MEXC quote symbol mismatch")
    book = BookTicker(market, epoch_ms(received_at_ms), decimal_value(payload.get("bidPrice"), "bid", positive=True),
                      decimal_value(payload.get("bidQty"), "bid quantity"),
                      decimal_value(payload.get("askPrice"), "ask", positive=True),
                      decimal_value(payload.get("askQty"), "ask quantity"))
    if book.ask < book.bid:
        raise ValueError("Crossed book ticker")
    return book
