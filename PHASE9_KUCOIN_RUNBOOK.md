# KuCoin reference, manual ideas, and research data

KuCoin is the reference for new configurations. The user reports that KuCoin is accessible and works for them. The report does not establish residence, account eligibility, or personal fees. Public access, instrument rules, dataset readiness, training, idea publication, and validation are separate states. Execution mode is `MANUAL_OUTSIDE_LAB`.

## Commands

```powershell
./phase9.ps1 status
./phase9.ps1 monitor on
./phase9.ps1 collect metadata
./phase9.ps1 collect probe
./phase9.ps1 collect extended
./phase9.ps1 collect current
./phase9.ps1 collect --resume <plan-id>
./phase9.ps1 collect history --profile swing --basis mark --start 2025-01-02T00:00:00Z --end 2025-01-02T12:00:00Z
./phase9.ps1 import --file C:\Research\trade.csv --descriptor C:\Research\trade.json
./phase9.ps1 run-once
./phase9.ps1 training on
./phase9.ps1 run-once --profile swing --bundle C:\Research\swing-bundle.json
./phase9.ps1 training off
./phase9.ps1 report
./phase9.ps1 ideas position on
./phase9.ps1 ideas swing on
./phase9.ps1 ideas day on
./phase9.ps1 decide --collect
./phase9.ps1 observe --collect
./phase9.ps1 ideas day off
./phase9.ps1 monitor off
```

`run-once` without a bundle audits the measured public coverage and starts no fit. A named bundle needs training enabled and the existing Phase 8 learner stopped. Training is operator-requested and shares the existing one-job lock, resource thresholds, two-thread limit, attempt cap and two-hour elapsed budget. A campaign pins one profile's source, label, rule and cost evidence. Repeat work reuses completed results. New futures imports do not enter Spot's identity. KuCoin controls persist in `data/phase9/controls.json`. They are also available in the dashboard's **Trade ideas** view.

The Coinbase worker uses its existing Phase 7 task. Phase 9 public acquisition and idea decisions are on demand. The enabled monitoring and idea switches permit explicit runs. They start no background service.

## Public data

The unified v2 candle, funding, instrument, and ticker routes use public GET requests. The currently documented Classic contract, mark and isolated-tier routes supply distinct fields. Candle timestamps use epoch seconds, settled funding and current mark use milliseconds, and unified ticker snapshots use nanoseconds. Unified trade candles store open, high, low, close, native volume and quote turnover. Spot native volume is BTC. Futures trade volume is contracts and is converted using the observed multiplier. Mark and index candles keep OHLC separate from trade volume. [Unified candles](https://www.kucoin.com/docs-new/v2/rest/ua/get-klines) · [Funding](https://www.kucoin.com/docs-new/v2/rest/ua/get-history-funding-rate) · [Instrument](https://www.kucoin.com/docs-new/v2/rest/ua/instrument) · [Ticker](https://www.kucoin.com/docs-new/v2/rest/ua/get-ticker) · [Futures contract](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol) · [Isolated tiers](https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-isolated-margin-risk-limit)

Each plan freezes its exact bounds and budget. The unified page limits are 1,500 spot or 200 futures bars. Phase 9 permits up to 24 requests per history plan, with a 350 ms minimum spacing, 10 s timeout, and 2 MiB response limit. Resuming skips accepted pages and observes `Retry-After`. A source access denial stops the plan. Raw hashes, receipt times, response status, missing grid rows and normalized import hashes stay local. The archive page advertises spot candles, trades and depth. It does not establish usable futures mark or funding archives. [Official history page](https://www.kucoin.com/markets/historydata)

The probes found four BTC-USDT spot hours on June 30, 2022. XBTUSDTM returned aligned 15-minute slices of minute trade, mark, and index data on December 30–31, 2024 and in 2025. The June 2022 and June 2024 futures probes were empty. This places the observed December samples earlier than the documentation's stated January 2025 boundary. It establishes no continuous range. Separate one-day funding probes returned three settlements each. The earlier and later sample days had different UTC settlement offsets. Archive coverage and the dated schedule remain necessary.

## Versioned costs and rules

The public BTC-USDT instrument reported Class A. Published VIP 0 decimal maker/taker references are `0.001/0.001` for that spot class and `0.0002/0.0006` for futures. Phase 9 uses taker on each side and a separate 0.0005 adverse-price proxy. Funding remains a separate settled cash flow. The fee assumption includes no discounts. [Published VIP table, updated July 16, 2026](https://www.kucoin.com/support/48142946141636)

XBTUSDTM is a linear USDT-settled perpetual. The observed multiplier was 0.001 BTC, lot and minimum quantity one contract, tick 0.1 USDT, and maximum market order one million contracts. The observed first isolated tier topped out at 250,000 USDT with 0.8% initial and 0.4% maintenance margin. The contract reported a 125x venue maximum and an eight-hour current funding cycle. These snapshots are current evidence. The lab keeps its existing 1x spot and 5x ordinary futures caps, with the parent 1x account-notional limit and 0.25% planned risk. The stress range stays within 1x–100x and the applicable tier maximum. The illustrative liquidation-cost proxy is 0.003 and is versioned separately from venue evidence.

## Local imports and research qualification

An import descriptor carries the actual venue, trade type, symbol, quote currency, kind, interval, timestamp unit, volume unit, source, permission evidence, retrieval time, start and exclusive end. CSV and Parquet trade candles need `open_at,open,high,low,close,volume`. Mark and index omit volume. Funding needs `settlement_at,rate`. Futures contract-volume files declare `base_per_contract`. Accepted timestamp units are `iso_utc`, `s`, `ms` and `ns`. The import scope ends no later than the preserved pre-holdout boundary, November 8, 2025 at 00:00 UTC. The holdout ends November 8, 2026 at 00:00 UTC. Current post-holdout ideas use the existing prospective feature-warmup exception and remain isolated from outcome labels.

```json
{
  "venue": "KuCoin",
  "trade_type": "FUTURES",
  "symbol": "XBTUSDTM",
  "quote": "USDT",
  "kind": "trade",
  "interval": "1min",
  "timestamp_unit": "iso_utc",
  "volume_unit": "contracts",
  "base_per_contract": 0.001,
  "source": "Identify the actual export and provenance here",
  "permission_evidence": "Identify the permitted source or export rights here",
  "retrieved_at": "2026-11-09T15:00:00Z",
  "start": "2024-01-01T00:00:00Z",
  "end_exclusive": "2025-03-01T00:00:00Z"
}
```

A bundle maps each kind to a list of imported asset IDs and names a saved metadata plan. A futures bundle also supplies the dated funding cadence as `funding_calendar` with `anchor`, `minutes`, and `basis`. Partition or select a window within one evidenced cadence for this version. Each expected settlement must appear. Set `rules_basis` to `OBSERVED_CURRENT_AS_HYPOTHETICAL` to acknowledge current-rule assumptions. This version reuses the single-tier hypothetical replay. Imports from alternatives retain their actual identity and cannot fill a KuCoin bundle. The existing 650,000-native-row limit per joined basis applies. A longer minute archive may need a separately reviewed, bounded partitioning approach.

The latest frozen fold starts outer evaluation January 1, 2025 and ends at the embargo. It still needs training before the July 1, 2024 inner start and enough subsequent labeled outcomes for inner validation. The two older folds require earlier history. Ridge and histogram training floors are 35 and 80 mature outcomes, with at least 10 inner and outer outcomes in each evaluated fold. The short futures probes qualified zero windows. A complete July 2024-to-embargo minute grid alone is 712,800 rows per basis before adding training history. Replaying that fold with a complete archive would also require partitioning the replay within the frozen 650,000-row budget. The KuCoin cost version changes labels before fitting, with the same frozen strategy/exit catalog and separate lineage.

The bounded Spot follow-up published 68,831 BTC-USDT hourly bars from January 2018 to the embargo, using 47 requests under a frozen 48-request ceiling. The missing candle opened at 17:00 UTC on January 4, 2018. The data supported all three unfiltered rule windows and eight fits across the two breakout exit variants. Only the 2025 window cleared the model evaluation floors. The study is a **HYPOTHETICAL_REPLAY** with insufficient evidence for model validation. The linked lineage erratum preserves the original manifest and identifies the KuCoin instrument and fee fields used by the trials. An identical bundle reuses that result.

## Manual idea workflow

At publication the lab uses completed native candles and higher-timeframe context from a real prospective snapshot. The rule is the existing closed-range breakout and fixed-R exit, labeled `EXPERIMENTAL / RULES_ONLY`. The validated section remains **No validated model**. Quote timestamps and local receipt timestamps must precede the decision. The lab permits one second of server/workstation skew and requires at most two minutes of age.

A qualified idea freezes a trigger, range, invalidation, stop basis, 100% fixed target, time or session exit, expiry, cost estimate, and example paper size. Quotes outside the range invalidate the original entry. The display keeps the original plan after expiry or revision. The existing Day UTC entry and flat boundaries still apply. Manual delay offsets of one and two native bars are preregistered as a diagnostic for later testing. They have no measured result in Phase 9.

An entry-condition observation is a separate market event. Historical hypothetical fills and outcomes have their own evidence, and the idea ledger can separately represent simulated and user-reported events. To attach a user-reported execution to an existing conditional idea, use `record-execution --idea <id> --file <json>` with `executed_at`, `price`, `quantity`, and `unit` set to `base` or `contracts`. The paper size is based on configured 10,000 USDT equity. The user chooses any real execution independently.
