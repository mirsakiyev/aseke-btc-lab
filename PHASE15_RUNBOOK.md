# Phase 15 trade opportunities and paper comparison

Approval remains **No validated model**. Execution remains **MANUAL_OUTSIDE_LAB**. Published opportunities are experimental, rules-only, and have no model version.

## Operate

From the project root:

```powershell
.\dashboard.ps1
.\phase15.ps1 analyze
.\phase15.ps1 status
.\phase15.ps1 verify
.\phase15.ps1 report
```

In the dashboard, open **Trade ideas**. The **Operations** tab separates the public monitor, existing profile permission, Phase 15 analysis, publication and paper switches, frozen Phase 14, and training. **Setups & books** shows the completed-candle and trend gates, confirmation level, next eligible boundary, Day UTC session, annotated market chart, published cards and independent book accounts. Candles, quotes, and observations retain UTC timestamps. Cards and charts also show Toronto time.

```powershell
.\phase15.ps1 control analysis on
.\phase15.ps1 control publication off
.\phase15.ps1 control paper on
.\phase15.ps1 control spot_minutes on
.\phase10.ps1 status
.\phase14.ps1 technical-review
```

`publication off` stops new publications. `paper off` pauses accounting and new publications, keeping all stored state. Spot minute capture can be paused independently. Existing Phase 10 monitoring and per-profile permissions remain upstream controls. `analyze` and `verify` read current local sources. `cycle` runs one bounded operator cycle using the same single-writer lock. Use the managed cycle for unattended operation. `register` opens or reuses the single immutable prospective registration.

The already-running scheduled worker invokes Phase 15 through its authenticated public observation subprocess. Phase 15 depends on the existing shared feed's cadence and enabled control. The original observer and Phase 14 continue to own their stores. Phase 15 reads the shared futures store and only writes under `runs/phase15`. `verify` reports the same-launch heartbeat and the last managed Phase 15 receipt. The new experiment needs no scheduler registration.

## Catalog and time boundaries

| Independent book | Live recipe | Decision cadence | Maximum hold |
| --- | --- | --- | --- |
| Position fixed | `breakout_v1 / fixed_r_v1`, existing `kucoin-manual-breakout-v1` | Daily | 30 days |
| Swing fixed | `breakout_v1 / fixed_r_v1`, existing `kucoin-manual-breakout-v1` | 4 hours | 7 days |
| Swing trailing | `breakout_v1 / atr_trail_v1`, `kucoin-swing-breakout-atr-trail-v1` | 4 hours | 7 days |
| Day fixed | `breakout_v1 / fixed_r_v1`, existing `kucoin-manual-breakout-v1` | 15 minutes | 8 hours, subject to flat |

Day permits new entries before 22:00 UTC and requires flat at 23:55 UTC. The dashboard converts these to Toronto time on the applicable date. Position is Spot long-only. The frozen entry needs agreement among the higher-timeframe SMA regimes and a completed trade close strictly beyond the prior 20-bar range. Feature readiness is a prerequisite. The display uses confirmation distance as a price difference. Swing's new exit starts at the same initial ATR/pivot rule, then ratchets 2.5 ATR behind eligible completed four-hour closes after the actual input receipt and processing allowance. It only tightens and persists its current stop and last applied bar through restart.

This registration starts November 9, 2026 at 21:45 UTC, or 16:45 Toronto. New publication ends December 9, 2026 at 21:45 UTC exclusive. Existing positions have a fixed observation follow-through through January 9, 2027 at 21:45 UTC exclusive. The historical Day pullback, historical Swing trailing estimator and Phase 14 pins have separate identities and research scope.

## Accounting and evidence

An awaiting-confirmation item is a watchlist entry. It has no publication or paper fill. A published idea records its actual local publication, a strictly subsequent eligible minute, and immutable entry bounds and expiry. Published ideas may wait for a qualifying trade-minute open. Adverse-adjusted prices must remain within the frozen range. The open-book deadline is calculated from the modeled fill plus the maximum hold and the applicable Day flat cap. An occupied book has one pending idea or one position. A conflicting setup in the same book is rejected. The four independent 10,000 USDT books have separate cash, exposure and risk and are never aggregated as a fundable portfolio.

Paper entry and exit prices use the trade minute. Futures triggers use paired mark minutes and Spot triggers use trade prices. The model applies reference taker fees and a separate adverse price adjustment on both sides. Reported adverse cost is already included in modeled gross price P&L. For an opening gap the simulator uses the trade open, which can exceed planned loss. For a single intraminute futures hit it uses the adverse trade extreme. Spot single intraminute hits use the trigger level. Dual triggers or an unresolvable order of funding and exit events stop the book at the affected minute with exposure explicitly unresolved. A subsequent verified missing minute or settlement can resume the same cursor. No paper event implies an actual exchange fill or order.

Funding uses the dated public contract schedule and settled receipts. At-open funding applies to positions already open before the settlement instant. Off-grid events preserve the published instant and use the containing mark open as the hypothetical funding-price proxy. A zero rate is accepted only when published. If an expected event is absent, the book first waits and then reports missing evidence after two hours. Quotes are publication feasibility evidence. Legacy Phase 9/10 quote-proxy outcomes, native-minute paper events, and user-reported executions have separate records.

Phase 15 makes no extra futures requests. Spot minutes are fetched through the existing worker path while a Position idea or exposure requires them, plus one commissioning window. The budget is 6,000 public GETs, a five-second timeout, 350 ms minimum spacing, 180 minutes per catch-up request and 96 MiB retained raw evidence. Retry reservations, receipts and first-arrival data are durable. Rate limits and access denials are respected. [Public minute candles](https://www.kucoin.com/docs-new/v2/rest/ua/get-klines) · [Settled funding](https://www.kucoin.com/docs-new/v2/rest/ua/get-history-funding-rate)

Optional local user reporting takes JSON fields `executed_at`, `price`, `quantity`, and `unit` (`base` or, for futures, `contracts`). This event does not affect the paper ledger.

```powershell
.\phase15.ps1 record-execution --book swing/trail --idea <published-id> --file C:\path\execution.json
```

## Prospective futures mark-reference paper protocol

`lab.phase15.realtime_worker` is the independent futures entry prototype. The seven-slot current generation uses its exact-reference entry rules and adds forward-reference exits. Once the seven-slot generation retires the old run, the prototype is barred from admitting parallel entries.

`phase15-realtime-mark-reference-v1` reports as `REALTIME_MARK_REFERENCE_PAPER`. Day fixed, Swing fixed and Swing trailing receive new XBTUSDTM account IDs and a separate ledger under `runs/phase15-realtime-mark`. The registration seals a future start and a publication window with follow-through inside the reviewed shared futures coverage. The existing Phase 15 and EMA workers, account identities and notifications retain their existing paths.

The independent runner confirms a completed breakout with the Phase 15 analysis and commits the idea to its ledger. At or after the idea's first eligible minute, it requests a current mark from the already allowlisted public GET `https://api-futures.kucoin.com/api/v1/mark-price/XBTUSDTM/current`. The old publication quotes still serve the analysis gates. For entry the new request uses `data.timePoint` as exchange time in milliseconds and `data.value` as the XBTUSDTM mark. It records the raw response hash and request, local receipt, paper decision and paper entry commitment times. The entry timestamp is the local application time of the state transition saved atomically with the journal event. [KuCoin Get Mark Price](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-mark-price)

The request and exchange observation must both follow publication and meet the eligibility clock. The exchange time must be at or before receipt, and local request, receipt, decision and commitment times must proceed in order. The full response must arrive within two seconds of the request. The mark must be at most two seconds old at decision and entry commitment, and commitment must precede expiry. There is zero future-clock tolerance. A rejection is journaled with its reason, and an idea still unfilled at expiry records the last rejection or a missing-evidence reason. Historical OHLC has no entry path in this runner.

KuCoin documents a one-second snapshot cadence and public rate weight of three. A read-only check of 666 unique archived Day/Swing snapshots measured exchange-to-local receipt age of 0.547 s median and 1.288 s at p95. Sixty-two source timestamps led the local receipt clock and would fail this policy. The sample was collected by the existing monitor before Phase 15 confirmation. Production latency for the new path remains to be measured. The standalone runner checks local state every 15 seconds, with an eligible idea required for any request. It shares the response across eligible books and reserves each attempt before making the GET. It allows at most two requests per idea, spaces requests by at least 15 seconds, and allows 24 per rolling hour and 5,000 per registration. The per-operation HTTP timeout is 1.5 seconds, the receipt cap is 16 KiB, and requests stop below 2 GiB free disk space. The runner honors the existing monitor pause and backoff, backs off on its own rate limits, and stops its requests on access denial. [KuCoin Get Mark Price](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-mark-price)

Entry uses the exact mark including off-tick precision, with zero entry price adjustment. Quantity is rounded down to lot size and capped by the original order, current instrument rules and market maximum, available cash and margin, the modeled stop loss and funding allowance, and the liquidation buffer. The original stop remains fixed at entry. The fixed-R target uses the committed price and the timed exit uses the last minute open within the hold cap calculated from commitment. The published taker fee is charged separately. Exit monitoring uses the shared native mark/trade and funding path with the existing adverse exit proxy. The entry-containing minute is checked for potential stop, target or liquidation crossings, whose order relative to entry can be ambiguous. Such results remain provisional while subsequent protection continues. Mark is a paper reference. A bid/ask proxy needs fresh timestamped best ask for longs and best bid for shorts, sizes or depth consistent with the modeled quantity, and explicit latency and partial-fill rules. Actual fill verification needs an authenticated exchange fill receipt matched to the order ID, fills, quantities, timestamps and fees. [KuCoin market ticker](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-ticker) · [KuCoin mark-price methodology](https://www.kucoin.com/support/26684973273625)

After operator rollout approval, confirm the existing data-use scope covers the extra prospective requests and verify the workstation clock and shared protection feeds. Register the independent cohort with `python -m lab.phase15.realtime_worker register`. Registration leaves entries off. Use `status` or `report` under the same module to check account IDs, dates and balances. Start the standalone `run` process and explicitly set `entries on` when ready for a supervised initial window. Review the first observed timestamps, fees, protection receipts and reconciliation before expanding operation. To roll back entry collection, run `python -m lab.phase15.realtime_worker entries off`. The next cycle cancels pending ideas and continues processing open positions. Keep the standalone runner and shared protection feeds operating through reconciliation of the new accounts and retain their ledger for audit.

## Current seven-slot paper generation

`python -m lab.paper_run status` reads the current paper ledger and private archive pointer. After a verified cutover, the default dashboard and existing account readers show the four Breakout and three EMA successors. Each starts with a new 10,000-unit hypothetical endowment. The EMA ledger keeps the nominal USD-to-USDT parity convention. The study uses a future-only UTC quarter boundary and up to 30 days of entry opportunities, reduced if the reviewed source horizon needs more follow-through for the original hold cap. The registration records each slot's exclusive entry and follow-through ends.

Breakout futures use `data.value` and `data.timePoint` from the current XBTUSDTM mark endpoint. Breakout Position uses the BTC-USDT Spot row's `lastPrice` and the batch-level `data.ts` from the UTA Spot ticker. Its two-second budget measures the age of the ticker snapshot. The response does not supply the time of the underlying trade. EMA keeps the BTC/USD last-trade basis, chart inputs and 2.5 ATR confirmation brackets. Initial REST qualification used `/0/public/Trades?pair=XBTUSD&count=1`. The shared WebSocket v2 transport amendment uses `trade` updates with `snapshot:false`, one subscription across all three EMA books, and exact trade price, sequence and exchange timestamp. [KuCoin ticker documentation](https://www.kucoin.com/docs-new/v2/rest/ua/get-ticker) · [Kraken trade stream](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade)

All enabled books use exact accepted references at entry and exit. The recorded commitment clock follows the exchange and receipt times. The two-second age check includes monotonic processing time, and the runner checks workstation clock health. A stop, target, liquidation or time exit uses the next accepted forward reference that meets its condition, including a price beyond the threshold. KuCoin requests run at most every 15 seconds per source when eligible ideas or exposures need them. The bounded Kraken queue is consumed by the same runner, and history snapshots, duplicate IDs and stale queued points are excluded. Brief crossings between samples may be missed. A sustained gap or a Kraken disconnect or queue drop while exposed is recorded explicitly. Trading fees, verified KuCoin funding rates with forward mark valuation, synthetic EMA financing and modeled liquidation charges are accounted for separately. Missing costs defer final accounting and unresolved protection keeps results provisional.

An EMA source amendment preserves the current generation and all three book IDs. It records a future effective time and only permits signal boundaries at or after that time. The predecessor's 90-day EMA study remains in the archive. The successor registration fixed an up-to-30-day publication window, and elapsed source-blocked time remains part of that window.

Use the existing virtual environment for cutover and recovery.

```powershell
.\.venv\Scripts\python.exe -m lab.paper_run probe
.\.venv\Scripts\python.exe -m lab.paper_run status
.\.venv\Scripts\python.exe -m lab.paper_run verify-archive '<private archive directory>'
.\.venv\Scripts\python.exe -m lab.paper_run entries off
```

The authorized `cutover` command fences legacy writers, verifies SQLite backups and a scratch restore, retires their entry paths, and resumes safely from a prepared cutoff. The watchdog supervises the standalone writer. Use `entries off` to pause new entries while continuing protection. Keep the protected research and shared collectors available. Use `verify-archive` to validate historical records and read an isolated copy of an archived ledger for inspection. Legacy exposures end as administrative archival records and carry no synthetic exit or performance attribution.

## Challenger research

Phase 15 starts no fitting queue. The legacy learner, Phase 9 manual research and Phase 10 qualified-bundle queue retain their independent controls and one-job, two-thread and memory safeguards. Position retains its pinned saved Spot study. Swing and Day require a separately qualified futures bundle with documented trade, mark, funding and contract evidence. Permitted development inputs are provenance-checked pre-holdout development data and, after explicit review, separately admitted mature prospective paper outcomes. Phase 14 feature-only context, its frozen evaluation sample, unresolved outcomes and user reports are excluded from labels.

Require the parent training floors of 35 Ridge and 80 histogram mature labels, at least 10 mature labels in each inner and outer evaluation, and the existing 25-trade uncertainty floor. Freeze the attempt list and log all trials, failures and controls. Evaluate in chronological walk-forward order against matching rules, mean and cash. Keep completed challengers under new model identities and research-only approval. Publication into Phase 15 or adoption into a frozen cohort requires a separate explicit decision.
