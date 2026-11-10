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

## Challenger research

Phase 15 starts no fitting queue. The legacy learner, Phase 9 manual research and Phase 10 qualified-bundle queue retain their independent controls and one-job, two-thread and memory safeguards. Position retains its pinned saved Spot study. Swing and Day require a separately qualified futures bundle with documented trade, mark, funding and contract evidence. Permitted development inputs are provenance-checked pre-holdout development data and, after explicit review, separately admitted mature prospective paper outcomes. Phase 14 feature-only context, its frozen evaluation sample, unresolved outcomes and user reports are excluded from labels.

Require the parent training floors of 35 Ridge and 80 histogram mature labels, at least 10 mature labels in each inner and outer evaluation, and the existing 25-trade uncertainty floor. Freeze the attempt list and log all trials, failures and controls. Evaluate in chronological walk-forward order against matching rules, mean and cash. Keep completed challengers under new model identities and research-only approval. Publication into Phase 15 or adoption into a frozen cohort requires a separate explicit decision.
