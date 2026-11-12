# Phase 13 frozen futures comparison

Approved section **No validated model**. Execution **MANUAL_OUTSIDE_LAB**. Phase 13 uses the completed Phase 12 futures study. Spot retains its saved study. The Phase 12 comparisons use Binance-source data with KuCoin reference costs and are descriptive development evidence. The operator chose the shortlist after reviewing Phase 12.

## Read the results

From the project root in Windows PowerShell:

```powershell
.\phase13.ps1 status
.\phase13.ps1 report
```

The report is written to `runs/phase13/REPORT.md`. It retains the original statuses of all 52 trial records and shows the exact account dates, economic decomposition, marked risk, matching forecasts, and two stresses for the eight frozen lanes. The registry contains the individual artifact paths and hashes, training cutoffs, activation boundaries, and source and use constraints. For each lane, the diagnostic directory contains `comparison.json`, the applicable `parity.json`, and stress records and order ledgers. The frozen walk-forward procedure used a separate model in each outer fold.

| Profile | Fixed-study selection | Artifact |
| --- | --- | --- |
| Day | Fixed-exit pullback Ridge | `58a718ac65e7936a737e53b1` |
| Swing | ATR-trailing breakout histogram | `90bd759026499a1a9656f599` |
| Swing | Matching unfiltered ATR-trailing breakout | Frozen strategy definition |

Both fitted versions use the existing 2025 outer-fold artifacts. Their training cutoff is January 1, 2025 at 00:00 UTC, exclusive. Each profile includes its matching unfiltered rule, frozen training-fold mean, and cash. The model output is net return per unit of planned risk in the frozen research scenario.

```powershell
.\phase13.ps1 freeze
.\phase13.ps1 diagnose
.\phase13.ps1 resume a67022bc0acf6a02115a9d4197d20eda01e123bd15db45502f8bcf72e2f953ec
```

The same diagnostic inputs reuse the completed result. Resume uses the original deadline and reservations. The campaign used one research job, two threads, the 640 MiB RSS guard, the 256 MiB output ceiling, and the 2 GiB free-disk guard. The two-hour clock starts when the job is admitted. There are at most 12 new continuous-account stress replays and 36 fold subrecords. The exact replays use saved predictions with zero historical fits.

The structured adapter accepts `.\phase13.ps1 infer day/estimator C:\path\fixture.json` with optional `--fold y2025` for consumed-fold testing. The input document contains `input_cutoff`, `decision_at`, `features` using the frozen schema, and `evidence` with `coverage_complete`, `latest_required_source_close`, and `latest_required_receipt`. Command-line evidence is tagged synthetic. The adapter returns research scope, feature readiness, the exact score and gates, and conditional setup and sizing details when eligible. It has no connection to Trade ideas publication.

## Registered KuCoin study

Registration `9e8ac6a44015ee11bb4c2b81d614c64a17475b932777223c53f5973f33056aed` was recorded November 9, 2026 at 18:55:18 UTC. The 90-day primary window starts November 9 at 18:56 UTC and ends February 7, 2027 at 18:56 UTC exclusive. First technical review is November 16, 2026 at 18:56 UTC. These three boundaries are 13:56 Toronto time. The evaluation is a future-data offline replay using actual receipt times.

```powershell
.\phase13.ps1 register
.\phase13.ps1 observe-once
.\phase13.ps1 technical-review
.\phase13.ps1 prepare-evaluation
.\phase13.ps1 evaluate
```

Registration reopens the same immutable protocol. Collection is controlled separately with `.\phase13.ps1 collection on` and `.\phase13.ps1 collection off`. It uses the public KuCoin `XBTUSDTM` trade and mark minute feeds, settled funding, and versioned public metadata from the existing monitor. Scheduled capture targets every 15 minutes after a 45-second close allowance and polls funding hourly with overlap. It allows at most 22,500 public GETs, three GETs per cycle, three attempts per boundary, 180 minutes of catch-up, and 192 MiB of retained observations. Raw checksums, UTC source times and receipts, reserved requests, correction records, and the checkpoint live in `runs/phase13/observations/<protocol-id>`. The historical acquisition ledger is separate.

Public API references are [KuCoin candles](https://www.kucoin.com/docs-new/v2/rest/ua/get-klines) and [KuCoin settled funding](https://www.kucoin.com/docs-new/v2/rest/ua/get-history-funding-rate).

`technical-review` checks paired minutes, funding slots, metadata freshness and rule changes. `prepare-evaluation` seals the qualified 90-day snapshot. `evaluate` runs the frozen fixed-artifact Day comparison and reports Swing's readiness shortfall. Scoring waits for a completed horizon and qualified evidence. Late data can support economic accounting with its original receipt but cannot support an earlier decision. Missing accounting evidence, incompatible rule changes, or corrections requiring reconciliation leave qualification blocked and preserve the observations. The account stays continuous across reporting periods. Residual positions at the cutoff remain unresolved. Day needs 51 complete four-hour bars plus frozen causal features. Swing needs 201 daily context bars, which exceed this study's horizon with the currently admitted inputs. There is no automatic extension or retraining.

## Activate the observation extension on Windows

The existing KuCoin task already owns a healthy worker. The first Phase 13 observation ran on demand. To load the extension, use the existing scheduler controls in the administrator Windows PowerShell already available to the operator:

```powershell
# Run from the repository root.
.\phase10.ps1 stop
.\phase10.ps1 start
.\phase10.ps1 verify
.\phase13.ps1 status
```

The stop/start reuses the registered task and its credentials. The scheduler write and process-owner checks need the elevated shell. `phase10 verify` confirms ownership when the new worker completes its first real monitoring cycle. `phase13 status` confirms a complete Phase 13 cycle whose launch ID matches the current scheduled worker. If either cycle is pending, run the same checks after the next due boundary. Reboot and sign-out recovery require separate observation.
