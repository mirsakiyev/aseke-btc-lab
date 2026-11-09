# ASEKE BTC Lab

ASEKE BTC Lab compares three independent USD 10,000 hypothetical accounts on Coinbase Exchange BTC-USD spot candles. Choose **Spot / Position**, **Swing**, or **Day** in the local dashboard. Each uses its own trend context, features, trade labels, fitted Ridge model, and results. Scalping is disabled.

The main recommendation stays **No validated model**. Development diagnostics and experimental forward shadow events are separate. The lab uses public market data and hypothetical fills. It has no exchange credential input, order submission, or automatic promotion.

## Start on Windows

The scripts call the project’s explicit Python executable. On the current workstation, the project already has a Python 3.12 virtual environment.

```powershell
./dashboard.ps1
```

Open [the local dashboard](http://127.0.0.1:8501). Press Ctrl+C in the launching terminal to stop a foreground instance. A second launch will encounter the existing listener if port 8501 is already in use.

To reproduce the environment in a fresh checkout, use Python 3.12 and install the pinned Windows x64 dependency set.

```powershell
py -3.12 -m venv .venv
./.venv/Scripts/python.exe -m pip install -r requirements.lock.txt
./.venv/Scripts/python.exe -m pip check
```

## Commands

Run these from the project folder. The first two backfill commands validate short ranges before larger requests. Each command accepts a request budget and resumes exact completed requests on rerun.

```powershell
./lab.ps1 diagnostics
./lab.ps1 backfill --seconds 3600 --start 2018-01-01 --end 2018-01-01T06:00:00Z --max-chunks 1
./lab.ps1 backfill --seconds 300 --start 2023-11-08 --end 2023-11-08T00:30:00Z --max-chunks 1
./lab.ps1 backfill --seconds 3600 --max-chunks 50
./lab.ps1 backfill --seconds 300 --max-chunks 100
./lab.ps1 validate
./lab.ps1 baseline all
./lab.ps1 train position_spot
./lab.ps1 train swing
./lab.ps1 train day
./lab.ps1 report
./.venv/Scripts/python.exe -m pytest -q
```

`train all` runs the three fits sequentially. Either research command also accepts a single profile ID. A repeated command on the same input snapshot, configuration, and code reuses its completed report. The latest training and baseline report pointers are separate.

Default backfill targets begin 2018-01-01 hourly and 2023-11-08 at five-minute resolution. Both end at the frozen 2026-11-08 UTC boundary. `pending_from` identifies where a request budget ended. A fully attempted backfill can still have gaps. For a targeted gap recheck, use a new aligned request range around the gap. Conflicting overlaps stop publication and leave the prior snapshot readable.

The twelve months from 2025-11-08 through the frozen cutoff are locked as final holdout. Historical development charts, indicators, labels, fills, and performance use pre-holdout prices only. The data-validation report includes holdout-period coverage counts as ingestion metadata. Phase 2 describes the restricted use of prior prices for post-holdout forward features.

## Local files and backups

| Location | Contents |
| --- | --- |
| `data/` | Raw public responses, request provenance and checkpoints, immutable Parquet snapshots, and the holdout lock |
| `models/<profile>/<model>/` | Local Ridge artifacts, identity and training metadata, and report references |
| `runs/<profile>/<run>/` | Run manifest, hypothetical labels, account equity and drawdown, trade journals, and monthly returns |
| `runs/summary.md` | Latest development results and common-date marks |
| `runs/data-validation.json` | Coverage, gaps, duplicates, and hourly versus five-minute reconciliation |
| `runs/environment.json` | Workstation and dependency diagnostics |
| `logs/` | Local dashboard process logs when launched with redirection |

Keep `data/`, `models/`, and `runs/` together in backups, including the holdout lock. Back up a settled writer state and keep a copy of the versioned code and configuration with the backup. Load model artifacts only from a trusted local registry.

- [Architecture and execution conventions](ARCHITECTURE.md)
- [Data dictionary](DATA_DICTIONARY.md)
- [Decisions needed and next operational phase](DECISIONS_NEEDED.md)
- [Dependency versions and licenses](DEPENDENCIES.md)

## Phase 2

The frozen Phase 1 checkpoint is indexed in `runs/phase2/checkpoint/evidence.json`. The audit command writes a new timestamped directory and leaves the original reports, models, and data intact.

```powershell
./phase2.ps1 audit
./phase2.ps1 status
./phase2.ps1 start --shadow
./phase2.ps1 stop
```

The dashboard shows collection and experimental shadow activity in separate sections. See the [collector and shadow runbook](PHASE2_RUNBOOK.md) for recovery, storage, process management, and the optional logon task. The [bounded next study](PHASE2_STUDY.md) records the development scope and holdout rules.

## Phase 3

The completed Swing-only study compares the existing baseline, Ridge filter, and training-fold mean control. Fold models are isolated under `runs/phase3/`. Existing shadow identities, the Day and Position historical studies, and the holdout boundary are preserved.

```powershell
./phase3.ps1 report
./phase3.ps1 health
./phase3.ps1 run --run-id 20261109T014235Z-a9572f71e8
```

The last command returns the completed result for the original frozen design. See the [study and command notes](PHASE3_STUDY.md) for outputs and reproduction.

## Phase 4 · MEXC research

MEXC is the intended venue and BTC/USDT is provisional. The current Canadian context restricts MEXC execution. The separate Spot V3 adapter runs offline, and the prospective Swing protocol remains a blocked draft. The Phase 4 fee sensitivity uses the preserved Coinbase BTC-USD development history in USD.

```powershell
./phase4.ps1 validate
./phase4.ps1 sensitivity
./phase4.ps1 report
./phase4.ps1 study-state
./phase4.ps1 health
```

See the [MEXC research runbook](PHASE4_MEXC_RUNBOOK.md) and [remaining factual inputs](DECISIONS_NEEDED.md).

## Phase 5 · Swing robustness

Coinbase BTC-USD historical research with a hypothetical published-MEXC-fee scenario. Phase 5 reuses the completed reference and lower-fee accounts and runs only two frozen stresses. The live observation command separates original receipt and decision timing from stored candle coverage.

```powershell
./phase5.ps1 run
./phase5.ps1 report
./phase5.ps1 observations
./phase2.ps1 status
```

See the [Phase 5 runbook and evidence map](PHASE5_RUNBOOK.md).

## Phase 6 risk and observation readiness

From the project directory in PowerShell

```powershell
./phase6.ps1 run
./phase6.ps1 report
./phase6.ps1 coverage
./phase6.ps1 coverage --decision-days 7 --json
```

`run` verifies and reuses the frozen audit after its first completion. `report` displays the risk reconciliation and Swing feature diagnosis. `coverage` reads current observation evidence and worker health, including full-day candle completeness, first-receipt timing since activation and scheduled decision readiness. The Phase 2 runbook describes the weekly review.

## Phase 7 collector operation

```powershell
./phase7.ps1 status
./phase7.ps1 coverage
./phase7.ps1 weekly
```

Use the [collector runbook](PHASE2_RUNBOOK.md) for unattended registration, explicit resume, persistent stop and rollback.

## Controlled learning

```powershell
./learning.ps1 status
./learning.ps1 run-once
./learning.ps1 report
```

Use the dashboard's **Research / Learning** view or the [learning runbook](LEARNING_RUNBOOK.md) for controls, data requirements, and evidence. The original observer stays separate.

## KuCoin reference and manual ideas

```powershell
./phase9.ps1 status
./phase9.ps1 decide --collect
./phase9.ps1 run-once
./phase9.ps1 report
```

The dashboard's **Trade ideas** view uses the same local controls. Use the [Phase 9 runbook](PHASE9_KUCOIN_RUNBOOK.md) for public downloads, imports, campaign gates, and the manual workflow.

## Background KuCoin monitoring

`./phase10.ps1 status` shows worker ownership, scheduled work, independent controls, and futures readiness. See [the Phase 10 runbook](PHASE10_RUNBOOK.md) for Windows commissioning, local operation, and research bundle requirements.

## Bounded historical futures campaign

Use `./phase11.ps1 status` and `./phase11.ps1 report` for the versioned workload, archive coverage, and qualification. The [Phase 11 runbook](PHASE11_RUNBOOK.md) explains the independent resource amendment and bounded run/resume commands.

## Complete-history futures amendment

Use `./phase12.ps1 status` and `./phase12.ps1 report` for the complete-history view, variant eligibility, and isolated historical run. The [Phase 12 runbook](PHASE12_RUNBOOK.md) describes the preregistered cutoff and resume conditions.

## Frozen futures comparison and new KuCoin evidence

Use `./phase13.ps1 status` and `./phase13.ps1 report` for the frozen Day and Swing comparisons and the separate KuCoin observation study. The [Phase 13 runbook](PHASE13_RUNBOOK.md) covers the exact pins, receipt-based offline replay, and observation controls.
