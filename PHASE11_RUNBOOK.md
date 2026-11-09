# Phase 11 futures workload sizing

Phase 11 uses the local Phase 8-10 implementation and links to the frozen protocol. Research is offline and execution stays `MANUAL_OUTSIDE_LAB`. The approved section remains **No validated model**.

## Commands

Run these commands from the project root.

```powershell
.\phase11.ps1 status
.\phase11.ps1 report
.\phase11.ps1 plan
.\phase11.ps1 benchmark
.\phase11.ps1 amend
.\phase11.ps1 qualify
.\phase11.ps1 run-once
.\phase11.ps1 resume <campaign-id>
```

`run-once` evaluates immutable data readiness and reuses an existing result or starts the single bounded campaign. `resume` is available after a qualified campaign receives an ID. It uses the original deadline, consumed reservations, trial attempts, and completed evidence. Its completed result is reused on subsequent calls. The current admission attempt stopped before fitting, so it has a qualification ID and no fit-campaign deadline.

The completed archive steps are `acquire --preflight`, `acquire --batches 24`, `repair` and `daily-fallback`. Accepted objects, checksums, request receipts, and source limits are durable. The archive uses at most 19 GETs, 140 seconds and 32 MiB of response data in a batch, with 350 ms between requests. The independent campaign envelope is 463 GETs and 256 MiB retained dataset bytes. The original Phase 10 probe remains a separate immutable 19-request plan. This acquisition consumed 462 GETs in 27 batches. The current source result is in `runs/phase11/REPORT.md`.

Continuous training remains off. The existing Phase 8 learner and Phase 9 gate retain their earlier profile scopes. Enabling the Phase 10 Swing or Day flags applies to the Phase 10 queue and its 650,000-row profile. It does not automatically claim the Phase 11 campaign. The Phase 11 command is the explicit entry point for the amended research-only workload, and waits for a qualified bundle under the current evidence. Position keeps the saved Spot result.

## Resource interpretation and lineage

Phase 8 enforced 650,000 physical rows per source file before predicate push-down. Phase 9 summed the declared imported rows per basis before deduplication. Phase 10 summed admitted Parquet rows separately for trade and mark. Ordered nonoverlapping input made this equivalent to distinct source timestamps per stream. The count excluded combined streams, repeated execution visits, and the independent 50,000-row partition limit.

The 712,800 minutes are the latest fold's July 1, 2024 inner start through the November 8, 2025 embargo. The frozen January 1, 2018 training start remains. The logical range to the embargo contains 4,129,920 minutes. The archive work plan covers the September 2019 listing-month envelope through the embargo, allowing 3,254,400 observations each for trade and mark. The chosen Binance archive yielded its first paired files on January 1, 2020. September through December 2019 checksum URLs returned 404. The dataset identifies those unavailable observations separately from the period before listing.

The plan stores the shared interval union, all three inner and outer windows, the embargo, exit tails, warmup requirements, and maturity boundaries. It accounts separately for 12 label passes, 48 candidate account passes, equity and native clock scans, repeated trade-kernel visits, 52 candidates/controls, and up to 144 estimator fits. Funding has a finite cadence-derived upper bound. The simulator uses the published mark series directly, so index is outside its required inputs. The linked amendment preserves one active job, two threads, a 640 MiB child RSS cap, two attempts per candidate, a 256 MiB research output cap, 1,250 MiB available-RAM and 2 GiB free-disk guards, and the two-hour fit deadline. The trial order groups the existing controls by label lane for memory and runs Day before Swing. Fold order is chronological. Candidate, fee, sample, and validation policies are frozen.

The selected June 2022 benchmark processed 43,200 minutes in 12.34 seconds at 225.8 MiB peak RSS. It evaluated six Day label and unfiltered replay lanes as diagnostics and exercised two real off-grid funding postings across a partition. The full guarded estimate is about 70 minutes, 553 MiB peak RSS and 195 MiB compressed trial output. This is a projection with 50% time headroom. The runtime uses the original hard guards. The research-only historical simulation assumes the frozen single-tier rules and current published KuCoin reference cost of 0.06% taker plus 0.05% adverse price per side. Current Binance contract metadata confirms a BTC quantity with USDT settlement, but is not a historical margin schedule.

The concurrent 10.62-second repeat of the fixed benchmark overlapped the task-managed Day boundary. The Day checkpoint advanced from 17:00 to 17:15 UTC on November 9, 2026 with a timely decision. This verifies that boundary during the diagnostic. The full campaign resource estimate remains a projection.

## Observed qualification

Official checksums verified 225 of 237 planned objects. Eight samples were reused. The latest source candle opens November 7, 2025 at 23:59 UTC, before the sealed start. Trade covers 3,078,720 minutes from January 1, 2020. The exact-range official mark endpoint recovered 11,522 missing marks, leaving 3,078,667 verified mark observations. The two checksummed alternate daily files contained the same gaps. The exact remaining source shortfall is:

- **29** mark candles opened January 19, 2020 from **13:09 through 13:37 UTC**, inclusive.
- **24** mark candles opened December 17, 2020 from **07:32 through 07:55 UTC**, inclusive.

The funding schedule qualified across all observed 2020-to-embargo input, with 6,414 settlements on an eight-hour schedule and 2,788 source instants off minute opens. The largest nominal offset was 47 ms. The exact timestamp is retained for execution. At-open settlements precede opening exits for carried exposure. Off-grid events follow opening actions and precede modeled candle-close resolution, using the observed mark open as the execution proxy.

All inner and outer price windows are complete after repairs. The existing paired-mark gate still rejects all three folds because both gaps are in their common training prefix. Neither a fit nor an economic evaluation started. The staged next step is to obtain those exact same-instrument mark observations with verifiable provenance and qualifying use rights, retain the source bytes or checksums and normalized hashes, and requalify the amended bundle before calling `run-once`. No additional compute or storage allowance was required by the measured projection. The maximum remaining request budget for the current archive plan is one GET.

[Binance Vision terms](https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md) and [official USD-M market data reference](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).
