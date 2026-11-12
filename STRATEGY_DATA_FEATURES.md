# Shared strategy data and features

**PAPER ONLY · MANUAL_OUTSIDE_LAB · No validated model.**

Strategy Lab components can use an independently reviewed local provider. The shared receipt and feature interfaces can be composed with frozen strategy versions, book-bound grants, and isolated paper ledgers. [Public source and execution guide](EMA_SYNTHETIC_PAPER_GUIDE.md)

Offline BTC/USD Spot adapters and a bounded incremental indicator cursor are available for independently authorized integrations. [Spot adapters](lab/strategy_lab/spot_sources.py) · [Technical features](lab/strategy_lab/technical_features.py)

## Components

| Component | Existing implementation reused | New capability |
| --- | --- | --- |
| Market data | KuCoin public request and receipt conventions, Strategy Lab's separate research admission, existing file hashing | A read-only provider interface for Spot and futures trade candles, futures mark candles, settled funding and dated contract metadata. The reader checks experiment grants, causal availability, coverage, deduplication and raw receipt hashes. |
| Chart candles | `lab.data.aggregate` and the lab's UTC origin | Complete 1m→15m, 1m→4h and 1h→1D candles with first open, maximum high, minimum low, last close and summed trade volume. Chart availability is the latest constituent handoff. Missing, duplicate, misaligned and unavailable inputs leave gaps. |
| Indicators | Existing Pandas EMA recurrence, Wilder smoothing and contiguous grouping | Shared EMA, Wilder RSI and ATR, SMA, and rolling extrema. One indicator result can serve multiple strategy recipes and aliases on the same authorized chart snapshot. |
| Warmup | Existing SHA-256 conventions, immutable storage and Windows writer lock | Approved exact seed range, content hash and indicator policy, complete timely forward bridge, restart-safe eligibility checkpoint and feature vectors for current and prior forward bars. |
| Strategy composition | Immutable specs, reviewed entry/exit evaluators, existing risk and execution settings, independent paper ledgers | Additive data and feature APIs. A reviewed successor evaluator can combine these APIs with the existing rules and ledger. The new modules have no paper, training or Telegram write path. |
| Futures contract | Existing isolated linear simulator `Contract` | An offline adapter for the documented XBTUSDTM payoff with reconciled Classic, UTA and isolated-tier metadata. |

[Market data](lab/strategy_lab/market_data.py) · [Chart and indicators](lab/strategy_lab/technical_features.py) · [Warmup](lab/strategy_lab/feature_warmup.py) · [Existing strategy evaluator](lab/strategy_lab/evaluator.py) · [KuCoin adapter](lab/strategy_lab/kucoin_contract.py)

An EMA crossover or future MACD recipe can use the same EMA primitive and cache. RSI mean reversion can reuse Wilder RSI. A future Bollinger Bands implementation can reuse the candle layer, SMA and feature identity, and would need a separately reviewed dispersion calculation and entry rule. VWAP and ADX have no implementation in this change. Continue to review and pin each strategy's entry, exit, risk and execution composition.

## Grants and provenance

The composition root supplies immutable `SourceGrant` records from reviewed configuration. A grant binds a source and its rights evidence hash to one book, one purpose, allowed observation types, UTC bounds, registration and approval clocks, and an approval reference. Its identifier hashes those fields. The reader checks them before invoking a provider. Protected and unverified provenance is rejected. The approved venue-use record and provider adapter must establish the original instrument, role, source clock, first provider receipt, local handoff and normalization from each hashed receipt. A raw hash verifies archived bytes. Legal rights and correct decoding require their own review.

Use separate warmup, forward decision and execution evidence grants. Day and Swing can point their independently granted reads at one futures receipt stream. Use a separate Spot stream for Position. The reader has no network client, grant writer, or preconfigured production adapter. Reuse one authorized collection and handoff path for each market. A current provider must preserve corrections with their actual availability and allow an as-of read to recover the same evidence on retry. Market data authorization is checked on each read, including when a feature cache is available. [Existing handoff format and scheduler](EMA_PAPER_RUNBOOK.md)

Feature cache entries include exact candle values, close and availability clocks, source/content snapshot, instrument, market, timeframe, input series, indicator length and name, smoothing/initialization definition and implementation version. An optional authorization partition prevents computational reuse where a source license requires isolation. The cache is bounded to 64 entries and 64 MiB. Source reads check a 100,000-row and 64 MiB receipt/normalized payload budget per call. Candle and indicator inputs have separate hard row limits. Forward history is checked in bounded blocks. Receipt and checkpoint updates are deduplicated. An extended or corrected chart snapshot uses bounded recomputation to retain exact batch semantics. Persistent incremental indicator state and throughput for 10–20 strategies per profile remain unverified. A future unattended integration should benchmark a single scheduler dispatch with shared source reads, exact-input cache reuse, and the existing resource guards before adding consumers.

## Feature-only warmup

1. Approve a historical source role, exact contiguous interval, and normalized content hash. Register an independent forward decision grant for the same venue market and instrument. A `WarmupPolicy` binds those approvals, the selected indicator definitions, timeframe, minimum bar count, and causal deadlines.
2. Call `freeze_seed` at an actual audit time. The returned artifact records the snapshot ID, last historical close, count, initialization clock, first and last provider receipt, last local availability, and finite indicator values. EMA Pullback uses a 200-bar minimum, EMA50 and EMA200, Wilder RSI14 and ATR14, volume SMA20 and five-bar highs and lows. Use all approved contiguous prehistory. EMA200 uses the first close as its seed. After 199 updates the initial seed still carries about 13.7% of its starting weight, so an extended approved history can change early values. Pin the exact start, prehistory count and definitions in each registration.
3. Call `record_eligibility` only after a complete new forward bar immediately after the seed, including timely futures mark coverage when required and the processing delay. Use the actual recorded readiness time. The next strictly future UTC chart boundary starts the proposed 90-day window. A missed bridge or new gap blocks this seed. Recovery reuses the originally frozen clock for the registration and verifies the checkpoint hash. Test fixtures can supply their own deterministic clocks.
4. Call `decision_features` on an eligible forward close within its publication window. It revalidates the seed and bridge, checks the new contiguous evidence and returns current and previous forward feature rows with a snapshot ID. The feature gate supplies no orders or fills. Before registering a successor, its reviewed dispatch must check separately granted execution evidence and dated metadata before fixing the actual experiment horizon. Reuse the existing publication, entry, risk, accounting and alert rules. Retain the 25 completed-trade evidence requirement.

## Independent execution evidence

A futures provider needs causally available trade and mark minutes, settled funding, and dated contract and margin terms. The KuCoin adapter validates XBTUSDTM’s BTC contract unit, USDT settlement, price and quantity increments, and isolated tier against reviewed payloads. Keep source-specific account approvals and actual historical receipt evidence in local stores. [KuCoin adapter](lab/strategy_lab/kucoin_contract.py)

## Verification

Synthetic tests cover book and purpose grant isolation, causal UTC aggregation, EMA and Wilder feature parity, and replay across restart. The execution tests cover contract math and keep paper and alert fixtures in temporary stores. [Market-data tests](tests/test_strategy_lab_market_data.py) · [Technical feature tests](tests/test_strategy_technical_features.py) · [Warmup tests](tests/test_strategy_feature_warmup.py) · [KuCoin adapter tests](tests/test_strategy_lab_kucoin_contract.py)
