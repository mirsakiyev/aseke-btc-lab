# Phase 14 feature context and amended KuCoin study

Approval remains **No validated model**. Execution remains **MANUAL_OUTSIDE_LAB**. Continuous training remains off. This phase initializes the frozen futures research features and commissions a separate receipt-based offline cohort. The original Phase 13 protocol, observation store, candidates, and research results remain available.

## Operator commands

```powershell
.\phase14.ps1 status
.\phase14.ps1 verify
.\phase14.ps1 technical-review
.\phase14.ps1 report
```

`verify` checks the current worker, writer lock, advancing managed Phase 13 checkpoints, seed hash and feature readiness. `technical-review` separates observed native coverage, funding, maturity, qualification and statistical evidence. `report` writes `runs/phase14/REPORT.md`. `plan`, `admit`, and `register` reopen the completed immutable records and preserve the request ledger and original start.

The scheduled worker collects observations automatically. The original collector supplies shared receipts up to its exclusive end. Its scheduled observer then uses a separate bounded Phase 14 store for the remainder. The Phase 13 and Phase 14 primary windows keep separate manifests and accounting boundaries. The commissioned worker advanced through the first amended source interval, November 9, 2026 from 20:00 to 20:15 UTC, with 15 paired native minutes.

Administrator verification confirmed the commissioned worker's Windows account, task ownership, and managed cycle. `verify` checks that the saved ownership evidence matches the current launch.

Final candidate scoring and economic evaluation are manual. Run the following after the amended horizon and once `technical-review` reports `QUALIFIED_FOR_OFFLINE_REPLAY`.

```powershell
.\phase14.ps1 evaluate
```

The evaluator seals checksum-addressed native execution inputs and a separate trade-only feature bridge, loads the admitted seed and frozen artifacts, and replays all eight Day and Swing lanes. It uses one job, two threads, the existing 640 MiB RSS guard and the frozen sample floors. A repeated command reuses the saved result. Phase 13's evaluation command remains for the original protocol.

## Exact feature dependencies

The basis is completed **KuCoin XBTUSDTM USDT-settled futures trade OHLCV**. The frozen features consume trade prices and base-BTC volume. Public candle timestamps are UTC opens. Every bucket is half open and closes at open plus its exact duration, on the existing frozen UTC grid. Daily buckets close at 00:00 UTC. Native contract volume is multiplied by the pinned 0.001 BTC per contract before features and parity comparisons. [KuCoin public klines](https://www.kucoin.com/docs-new/v2/rest/ua/get-klines)

| Profile | Decision series | Context series | Exact completed context |
| --- | --- | --- | --- |
| Day | 15 minutes | 1 hour and 4 hours | 51 bars at each resolution for SMA(50) and the previous SMA |
| Swing | 4 hours | 1 day | 201 daily bars for SMA(200) and the previous SMA |

The decision series needs at least 34 contiguous closed bars for MACD(12,26,9) and the full feature set. EMA(26) first exists on bar 26 and its slope on bar 27. The 9-period MACD signal starts from the first defined MACD and becomes defined on bar 34. One- and five-bar returns need 2 and 6 bars. The 20-prior range and volume means need 21 bars including the current bar. The 20 log returns need 21 closes. ATR(14) needs 14 and lagged ATR needs 15. RSI(14) needs 15 closes. Confirmed pivots use two closed neighbors on each side and first become knowable two bars after a candidate pivot. Both pivot anchors and a valid high-above-low Fibonacci range must exist, so 34 decision bars alone do not guarantee readiness. Recursive EMA and confirmed pivot state initialize at the declared contiguous seed start. Gaps reset the frozen state and invalidate incomplete higher-timeframe buckets.

The context slope divides the current completed-window SMA by the previous completed-window SMA and subtracts one. Two adjacent 200-bar windows span 201 distinct daily bars. The equivalent 50-bar dependency spans 51 bars. Higher context uses the latest complete close within the existing one-period tolerance. Neither context nor decision features consume mark prices or funding. Those inputs belong to post-start execution and outcomes. The post-start account uses contiguous paired native one-minute trade and mark candles, the dated funding schedule and published settled rates, and compatible dated contract and tier metadata. Coarser context is never expanded into minute execution data.

## Admission and study dates

The immutable `FEATURE_CONTEXT_ONLY` seed stores hashes and original receipt provenance separately from execution. The calendar overlap with the sealed dates is explicitly authorized for frozen feature initialization. The protected holdout bundle and performance evaluation remain sealed. Context provides no fitting, labels, selection, ranking, historical replay, or P&L. A receipt becomes available at its recorded time, subject to the admission cutoff. Subsequent features rebuild from the same immutable seed and first-received minute history. Corrections are retained separately for review.

| Cohort | Start UTC / Toronto | First review UTC / Toronto | End exclusive UTC / Toronto |
| --- | --- | --- | --- |
| Original | 2026-11-09 18:56 / 13:56 | 2026-11-16 18:56 / 13:56 | 2027-02-07 18:56 / 13:56 |
| Amended | 2026-11-09 20:00 / 15:00 | 2026-11-16 20:00 / 15:00 | 2027-02-07 20:00 / 15:00 |

The amended start is the first strictly future common 4-hour UTC boundary after registration and readiness. The 90-day window is fixed. Pre-start Phase 13 observations support commissioning and feature continuity. The amended replay begins its simulated account at the amended start. It carries positions across review dates and records unresolved exposure at the horizon. Frozen estimator pins are Day Ridge `58a718ac65e7936a737e53b1` and Swing histogram `90bd759026499a1a9656f599`. Rules, mean and cash controls retain their Phase 13 definitions.

## Next review

- Confirm paired minute coverage, receipt timing, both profiles' feature continuity, and correction handling.
- Audit funding against the recorded contract schedule. A short window can have no expected settlement. Expected, pending, missing, failed-query, and verified zero-rate cases are distinct.
- Keep outcomes pending until the primary horizon and the needed accounting evidence are available. Qualification and the 10-label and 25-trade floors are separate checks. The replay supplies research evidence from receipts. Task reboot and sign-out recovery require actual observation.
