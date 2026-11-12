# CoinAPI qualification for the original KuCoin EMA books

**2026-11-12 · PAPER ONLY · MANUAL_OUTSIDE_LAB · No validated model**

CoinAPI remains an unqualified candidate. The credential check found no CoinAPI key in the project, relevant Windows environment variables, or matching Credential Manager entries. This review used public official documentation. Actual authenticated samples, gap counts, earliest and latest observations, workstation latency, and billing usage are unmeasured. The verified Kraken snapshots are ready for feature initialization. A later lawful handoff requires a new verified Kraken snapshot ending at that seam and an append-only update that preserves the current snapshots.

## Coverage and evidence

| Required data | Public evidence | Qualification result |
|---|---|---|
| `KUCOIN` BTC-USDT Spot | CoinAPI publishes `KUCOIN_SPOT_BTC_USDT` in its exact-symbol trade example and advertises KuCoin trade data and historical and real-time OHLCV. | Documented ID. Confirm active native-symbol metadata, date range, live hourly and minute completeness, base volume, and dated Spot ticks, lots, minimum funds and order limits. |
| `KUCOINFTS` XBTUSDTM | CoinAPI documents a separate KuCoin futures exchange and support for USDT-margined perpetuals. No authoritative public row linking native `XBTUSDTM` to a CoinAPI symbol ID was located. | Confirm the exact ID via authenticated native-symbol metadata. Require native `XBTUSDTM`, perpetual, BTC base, USDT quote and USDT settlement. A naming convention is insufficient. |
| Futures trade minutes and volume | CoinAPI documents individual trades and OHLCV. Its FAQ says the normalized OHLCV method uses reception timestamps and can differ from venue-native candles. | Verify whether the bar boundaries and volume match the registered chart. Record the raw trade size unit and whether normalization already yields BTC. Apply `0.001 BTC/contract` only to contract-unit observations once the dated specification confirms it. |
| Futures venue marks | CoinAPI lists `DERIVATIVES_MARK_PRICE` for `KUCOINFTS` alongside a separate index-price metric. Metrics V2 Flat Files aggregate first, last, minimum, maximum and count in UTC minutes, published in hourly partitions. The documented standard WebSocket channel lists do not identify a KuCoin mark stream. | Exact-symbol coverage and timely availability are unverified. Ask for the supported live delivery route or measure first availability of REST mark-minute history. A historical file or current snapshot alone cannot qualify live mark-minute highs and lows. |
| Actual futures funding | CoinAPI’s KuCoin futures funding coverage article uses `DERIVATIVES_FUNDING_RATE_CURRENT` for historical and current values. The public metric list does not identify a separate KuCoin settlement series. | Actual settled rates and settlement clocks are unverified. Obtain a distinct finalized settlement feed or documented lineage to exchange-finalized records. Keep current-rate history out of the settlement role. |
| Contract terms and paper economics | CoinAPI symbol metadata can expose native ID, contract unit and precision. KuCoin’s published contract example documents USDT settlement, non-inverse structure, the multiplier and `isQuanto`. | Static documentation and optional fields do not establish effective-dated terms, isolated maintenance tiers, funding schedule, order limits or the actual terms at modeled entry. Require dated, raw-linked exchange evidence. Keep the frozen fees, adverse execution and margin rules. |

CoinAPI’s public exchange table reports exchange-level starts of 2018-01-24 for `KUCOIN` and 2022-03-28 for `KUCOINFTS`, with both rows ending 2026-01-23 in that table. Those are neither exact-symbol histories nor evidence of current forward delivery. No authenticated records were sampled. Counts, duplicates, gaps, row resolution, and current ranges remain unknown. [CoinAPI KuCoin integration](https://www.coinapi.io/products/market-data-api/exchanges/kucoin) · [Exchange metadata](https://www.coinapi.io/products/market-data-api/docs/metadata-tables/supported-exchanges/exchanges_K) · [Native symbol lookup](https://www.coinapi.io/products/market-data-api/docs/rest-api/metadata/symbols/exchange_id/by-exchange-symbol/exchange_symbol_id/get) · [OHLCV FAQ](https://www.coinapi.io/products/market-data-api/faq) · [Metrics by exchange](https://www.coinapi.io/products/market-data-api/docs/metadata-tables/metric_id) · [Funding coverage](https://www.coinapi.io/blog/historical-funding-rate-data-exchange-coverage) · [Metrics V2 Flat Files](https://www.coinapi.io/products/flat-files/docs/datasets/metrics-v2) · [KuCoin contract fields](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol)

## Permission and the minimum account

The CoinAPI Customer Agreement effective August 18, 2025 covers individual customers and grants internal use subject to the applicable order and service terms (§§1.1–1.3). The account and quota terms (§§2.1–2.2) and Cloud Service restrictions (§§4.1–4.2 and the AUP) apply to unattended use. CoinAPI’s usage guidance identifies internal analytics and quantitative research as ordinary internal use. Private feature calculations, rules-only paper simulation and internal P&L are consistent with that purpose under a covering order. The AUP restricts abuse, quota evasion and resale. It supplies no KuCoin-specific exception. [Customer Agreement and AUP](https://www.coinapi.io/legal) · [CoinAPI usage guidance](https://www.coinapi.io/usage-policy)

The unresolved part is §2.3, which leaves applicable external-source restrictions and any additional licenses with the customer. I found no published CoinAPI order term or KuCoin-specific upstream permission establishing coverage for an Ontario customer receiving the exact two instruments. KuCoin’s direct-service Ontario and extraction restrictions are relevant to that inquiry. They do not establish the answer for a separately licensed CoinAPI delivery. Applicable binding standard terms are sufficient if they cover the use. CoinAPI’s derived-output guidance permits non-reconstructable signals and alerts and restricts raw or near-raw redistribution. Confirm how exact price and receipt fields in owner-only alerts and long-term local receipt retention, including after service termination (§7.3), are treated under the applicable upstream terms. Sections 5.1 and 5.7 address data supplied by the customer. [Customer Agreement](https://www.coinapi.io/legal#customer-agreement) · [CoinAPI derived-data guidance](https://www.coinapi.io/blog/coinapi-data-commercial-use-policy) · [KuCoin terms](https://www.kucoin.com/legal/terms-of-use)

Suggested support question for the unresolved scope:

> Does the standard Market Data Pay As You Go order cover an Ontario individual receiving KuCoin BTC-USDT Spot and XBTUSDTM data solely through CoinAPI for unattended collection, private local receipt retention and rules-only prospective paper simulation? Please identify any applicable KuCoin-origin condition, including after-term audit retention and owner-only alerts containing isolated price or receipt-time fields. Does that order include an exact-symbol venue-mark feed and a separate actual settled-funding feed? Please identify the endpoints or provide representative schema samples.

After confirming the applicable scope, sign into the [API Bricks Console](https://console.apibricks.io/) and select **Market Data API → Pay As You Go**. The published plan has no fixed monthly fee and offers US$25 introductory usage credit with a verified payment method and generated key. Billing can apply to later usage. In **Billing → Spend Management**, check the spend cap and hard-stop behavior, leave automatic recharge disabled, and use a dedicated qualification key. Store it in the workstation’s secret manager or use the masked prompt below. Pass it only as an HTTPS request header to CoinAPI. Keep it out of URLs, the repository, console transcripts, and chat. The published first 1,000 daily REST credits cost US$5.26 per 1,000 before credit. A pilot of 32 one-credit requests would cost approximately US$0.17 before credit. Actual request weights, WebSocket traffic and required product access determine the bill. No recurring paid tier is justified yet. [Pricing](https://www.coinapi.io/products/market-data-api/pricing) · [Account and quota setup](https://www.coinapi.io/learn/academy/tutorials/coinapi-usage-free-credits-and-quota-explained) · [Support](https://support.coinapi.io/hc/en-us/requests/new)

## Bounded qualification after scope and key are available

1. Make one authenticated metadata lookup per exact native symbol. Record the unmodified `symbol_id`, exchange, native symbol, instrument type, quote, settlement evidence and data start/end. Use the returned identifier for subsequent calls. Also run the symbol metric listing for `KUCOINFTS` and filter to the exact returned ID. Stop if the exact contract mapping or required series cannot be established. [Native symbol lookup](https://www.coinapi.io/products/market-data-api/docs/rest-api/metadata/symbols/exchange_id/by-exchange-symbol/exchange_symbol_id/get) · [Metric discovery](https://www.coinapi.io/blog/historical-funding-rate-data-exchange-coverage)

   ```text
   GET /v1/symbols/KUCOIN/by-exchange-symbol/BTC-USDT
   GET /v1/symbols/KUCOINFTS/by-exchange-symbol/XBTUSDTM
   GET /v1/metrics/symbol/listing?exchange_id=KUCOINFTS
   ```

   PowerShell 7 metadata check for use after scope and billing are confirmed. It runs one lookup per native instrument and keeps the key out of command history.

   ```powershell
   $coinApiSecret = Read-Host 'CoinAPI qualification key' -AsSecureString
   $coinApiHeaders = @{}
   try {
       $coinApiHeaders['X-CoinAPI-Key'] = ConvertFrom-SecureString $coinApiSecret -AsPlainText
       foreach ($nativePath in 'KUCOIN/by-exchange-symbol/BTC-USDT', 'KUCOINFTS/by-exchange-symbol/XBTUSDTM') {
           Invoke-RestMethod -Uri "https://rest.coinapi.io/v1/symbols/$nativePath" -Headers $coinApiHeaders -TimeoutSec 10 |
               Select-Object exchange_id, symbol_id, symbol_id_exchange, symbol_type, asset_id_base, asset_id_quote,
                   future_contract_unit, future_contract_unit_asset, data_trade_start, data_trade_end
       }
   } finally {
       $coinApiHeaders.Clear()
       $coinApiSecret.Dispose()
   }
   ```

2. Compare one short completed UTC sample per required role. Target up to 20 trade minutes per symbol, 24 Spot hours, 20 futures mark minutes, up to three actual settlements and one effective-dated metadata sample per instrument. Record counts, gaps, duplicates, exact time bounds, trade size units, first/last timestamps, and request-cost headers. Keep historical observations separate from prospective first receipts. Validate every returned UTC bucket, including APIs that can supply nearby bars when a requested window is empty. Archive original hashes privately only within the verified retention scope. [Trade history and clocks](https://www.coinapi.io/products/market-data-api/docs/rest-api/trades/trades/symbol_id/history/get) · [OHLCV history](https://www.coinapi.io/products/market-data-api/docs/rest-api/ohlcv/ohlcv/symbol_id/history/get) · [KuCoin public settled-funding schema](https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history)
3. Pilot a bounded live session around a common UTC midnight and a separately verified funding event. Allow at most 12 discovery/history calls, up to 16 live probes across Spot and futures trade minutes, Spot hourly trades and futures mark minutes, plus up to four documented settlement probes. Use a response cap of 100 rows where supported, record actual credit headers, and stop on scope, quota or coverage errors. Share a single exact futures subscription across Day and Swing when available. Timestamp exchange events, CoinAPI reception or entry where provided, vendor publication when provided, and workstation arrival independently. Keep hypothetical ledger time separate and record no production trade during the pilot. For every completed minute and chart, measure time from the UTC close to first complete workstation receipt. WebSocket OHLCV updates can be partial, and its documented closure depends on a subsequent-period event. Minute OHLC bounds still leave stop/target ordering ambiguous. Use the existing ambiguity policy. [WebSocket subscriptions](https://www.coinapi.io/products/market-data-api/docs/websocket/general) · [WebSocket messages](https://www.coinapi.io/products/market-data-api/docs/websocket/messages) · [Metric clock definitions](https://www.coinapi.io/datasets/crypto-market-metrics) · [KuCoin final settlement message](https://www.kucoin.com/docs-new/3470088w0)
4. The frozen chart gate is 120 seconds from close. The strategy then needs 30 seconds of processing and a publication within 360 seconds from close. Execution uses trade and venue-mark minutes with the registered price receipt allowance of 1,020 seconds after close, a committed entry receipt cutoff 17 minutes after the modeled open on these books’ entry grids, and actual funding with a 7,200-second allowance after its minute close. Compare worst observed delay with each deadline and keep a missing or late interval out. The pilot establishes whether to proceed to full native window qualification. It cannot substitute for 20 complete native chart bars and five structural bars per book.

## Current readiness and protected studies

| Original book | Frozen Kraken candles | Eligible native volume / swing | CoinAPI blocker |
|---|---:|---:|---|
| Day · 15m | 720 | 0/20 · 0/5 | Entitlement and key absent. Exact futures ID, timely trades and marks, settled funding, and dated terms unverified. |
| Swing · 4H | 720 | 0/20 · 0/5 | Same futures evidence and entitlement gaps. Complete timely four-hour native windows are unverified. |
| Position · 1D | 719 | 0/20 · 0/5 | Entitlement and key absent. Active Spot mapping, timely hourly and minute receipts, and dated Spot terms unverified. |

The CoinAPI qualification ended without a reviewed source grant or measured live coverage. Phase 15’s operating collector and original study registration document a separate internal research scope. Its observations remain protected, and operation alone cannot establish entitlement for another consumer. [Data admission boundaries](DATA_ADMISSION_PLAN.md#inherited-permissions-and-exclusions)

The current EMA study uses the separately reviewed Kraken-reference synthetic protocol. [Synthetic paper guide](EMA_SYNTHETIC_PAPER_GUIDE.md)
