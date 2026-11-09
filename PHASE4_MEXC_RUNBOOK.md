# Phase 4 MEXC research

## Eligibility and independent statuses

MEXC is the chosen intended venue. BTC/USDT with API symbol `BTCUSDT` is provisional. Product scope is unlevered spot, long or cash. The Toronto context is recorded as Canadian. MEXC's October 13, 2026 User Agreement lists Canada among its prohibited jurisdictions. The source was reviewed November 8, 2026 Toronto time. A different or unknown residency remains unresolved pending factual evidence. Public documentation, endpoint reachability and account possession do not verify eligibility. [MEXC User Agreement](https://www.mexc.com/terms)

| State | Current value |
| --- | --- |
| Execution eligibility | RESTRICTED in the known Canadian context |
| Fee verification | PUBLISHED_STANDARD_ONLY |
| Market data | OFFLINE · INSUFFICIENT_DATA |
| Prospective protocol | DRAFT · activation BLOCKED |
| Local analysis | LOCAL_RESEARCH_ONLY |
| Approved recommendation lane | No validated model |

The MEXC adapter has no HTTP transport, credentials, account access or order submission. It ran against synthetic fixtures. No MEXC live endpoint or archive was tested. Earliest and latest actual candles, missing intervals, order capabilities and current symbol precision are unavailable. Coinbase collection and the original shadow experiment continue independently.

## Fee and execution evidence

- MEXC's November 7, 2026 guide gives standard spot maker 0 bp and taker 5 bp before MX deduction, with pair and regional variations. This is the published research assumption. The account, pair and channel schedule has not been verified. [MEXC standard fee guidance](https://www.mexc.com/learn/article/how-to-use-mx-for-trading-fee-discounts/1) · [Fee overview](https://www.mexc.com/fee)
- The January 28, 2026 announcement narrowed zero-fee eligibility to selected pairs from February 1 and excludes API users. The earlier BTC/USDT 3 bp taker offer ran June 11 to July 11, 2026 and did not change API rates. Phase 4 uses the standard 5 bp and assumes no promotion. [Zero-fee offer scope](https://www.mexc.com/announcements/article/update-on-ongoing-0-spot-fees-offer-17827791533330) · [Dated BTC/USDT offer](https://www.mexc.com/announcements/article/adjustment-to-btc-usdt-trading-fees-17827791536176)
- Immediate entries and exits use taker assumptions. A limit order that matches immediately removes liquidity. MX deduction and MX balance eligibility are disabled in the scenario. Record expiry, channel and qualifications separately if account evidence can later be obtained legitimately. [Maker, taker and MX conditions](https://www.mexc.com/learn/article/mexc-fees-explained-complete-trading-futures-withdrawal-fees-guide/1)
- 50 bp equals 0.50% or 0.005. Five bp equals 0.05% or 0.0005. Five bp taker plus five bp modeled adverse execution on each side is about 0.20% round trip before other costs. The proxy is inherited from the Coinbase scenario. Observed bid/ask spread is diagnostic and is never added to the combined proxy. Slippage, impact, execution delay and modeled fills remain separate from fee evidence. Track conversion, funding arrangements or withdrawals separately where relevant.
- The USD replay treats trading fees as a quote-value debit. MEXC describes BTC/USDT buy fees in BTC and sell fees in USDT in its general spot guide. A future venue study should record the actual commission asset and reconcile units. USD and USDT balances remain separate. [MEXC spot fee accounting](https://www.mexc.com/learn/article/must-read-for-new-users-11-mexc-spot-trading-faqs/1)

## Offline Spot V3 contract

The official market module exposes unauthenticated `GET /api/v3/klines`, `GET /api/v3/exchangeInfo` and `GET /api/v3/ticker/bookTicker`. This milestone plans and parses those response shapes offline. The authenticated symbol fee endpoint, `GET /api/v3/tradeFee`, requires `SPOT_ACCOUNT_READ` and signed account access. It was documented for possible future use and is absent from the adapter's public interface. [Public market module](https://www.mexc.com/api-docs/spot-v3/market-data-endpoints) · [Authenticated symbol commission](https://www.mexc.com/api-docs/spot-v3/spot-account-trade/query-symbol-commission)

| Existing profile | Decision interval | Context | Execution candles |
| --- | --- | --- | --- |
| Spot / Position | `1d` | `1W` | `60m` |
| Swing | `4h` | `1d` | `60m` |
| Day | `15m` | `60m`, `4h` | `5m` |

The candle parser follows the Spot V3 order of timestamp, OHLC, base volume, closing timestamp and quote volume. Requests use Unix milliseconds and a maximum of 500 candles. The local range is half-open, and the query ends one millisecond before its right boundary. The parser verifies the grid and the documented sample's close at the next boundary. It reports incomplete bars, missing closed bars, outside rows and exact duplicates. Conflicting rows fail validation. Day and week alignment require actual verification before use. An explicit grid anchor in a fixture does not establish exchange alignment. [Kline endpoint](https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/klinecandlestick-data) · [Interval enums](https://www.mexc.com/api-docs/spot-v3/public-api-definitions)

Symbol metadata supplies spot availability, order types, status, trade-side restrictions, decimal precision and size limits. The adapter treats `baseSizePrecision` as the documented minimum quantity and floors a risk-capped base quantity to the allowed decimal places. It checks market notional separately and rejects quantities below minima. It treats metadata commissions as indicative only. Verify any finer step, limit price rule, market-buy quote amount and protective-order capabilities for the eventual channel before integration. The API enums list `STOP_MARKET_ORDER` as query-only. The lab's simulated stop carries no exchange-native guarantee. [Exchange information](https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/exchange-information) · [Order parameters](https://www.mexc.com/api-docs/spot-v3/spot-account-trade/new-order) · [Order type enums](https://www.mexc.com/api-docs/spot-v3/public-api-definitions)

MEXC advertises spot archives from January 1, 2023. Actual BTC/USDT coverage and grid remain unverified. If access becomes permitted, check one bounded native probe and preserve source, receipt time, venue, product, symbol, currency, candle boundaries and immutable version separately. Reuse the existing single-writer service for any later approved collection. A venue evaluation needs its own frozen coverage and must meet the existing warmup and 24-month minimum-history rule. [Historical archive documentation](https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/download-historical-market-data)

## Local commands

```powershell
./phase4.ps1 validate
./phase4.ps1 sensitivity
./phase4.ps1 report
./phase4.ps1 register-draft
./phase4.ps1 study-state
./phase4.ps1 health
./.venv/Scripts/python.exe -m pytest -q
```

The sensitivity reuses the checksummed Phase 3 baseline ledger for fixed-quantity repricing. It runs one new unfiltered replay with fee-adjusted sizing on the identical eligible windows. Both analyses use the exact title **Coinbase BTC-USD price history with a hypothetical published-MEXC-fee scenario.** Both retain USD. Results, trade and execution ledgers, and equity are under `runs/phase4/swing-*/`. Rerunning the same design verifies the saved hashes and reuses it.

Draft registration hashes the content and stores an idempotent local record in `runs/phase4/protocols/`. It creates no account, activation date, endpoint or new worker. The future draft specifies a separate 10,000 USDT hypothetical account for the provisional pair, cash and passive 25% BTC comparators, unchanged Swing sizing and exits, and a 90-calendar-day first review. Its restart, outage, revision, fee amendment and last-valid-mark policies are in the registered protocol. The preserved Phase 3 baseline's 68 closes over its observed calendar give a rough scale of seven closes per 90 days. Actual MEXC accrual is unknown. The holdout stays sealed.

## Phase 6 draft amendment

The checklist in [DECISIONS_NEEDED.md](DECISIONS_NEEDED.md) and the machine-readable `draft-handoff.json` beside the Phase 6 report supplement the original registered protocol and its checksum. Activation stays blocked. MEXC lists Canada among its prohibited jurisdictions. If the Canadian context still applies, a different eligible venue is required before a venue-specific study. [MEXC User Agreement](https://www.mexc.com/terms)

Record residency and location eligibility, the final native spot pair, and manual web/app versus API. Verify dated account/pair/channel maker and taker fees and any qualifying discounts or promotions. Match price coverage and executable order constraints to that same venue, pair and channel. Paper risk, drawdown, and pause/resume preferences remain open inputs. Freeze the approved data, cost and strategy/execution versions at the actual activation time. The 90-day review can assess operation and accrual. At the preserved rate of 68 closes over 861 days, about seven closes in 90 days is a descriptive extrapolation.
