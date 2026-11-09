# Phase 3 Swing walk-forward

The Phase 2 proposal used separated test windows. The Phase 3 manifest instead froze six consecutive calendar blocks before evaluation. The first ran January–June 2023 and had 22 warmed-up training month buckets against a required 24. It remains in the record as insufficient. Continuous accounts start July 1, 2023 and run to the last pre-holdout mark at November 8, 2025 00:00 UTC. The final block is partial.

## Interpretation

At reference costs, the baseline returned −1.80%, Ridge −1.32%, and the training-fold mean control −2.39%. Ridge changed the orders and lost less than both alternatives in the reference replay. It had higher MAE and RMSE than the mean on the same 215 evaluable labels. All three methods lost money under both additional cost and timing scenarios. These dependent historical outcomes do not establish a net edge.

The original May–November 2025 checkpoint remains a separate result and overlaps part of the new schedule. It is excluded as a separate observation from every Phase 3 aggregate. The phase uses already viewed development history. The twelve-month holdout remains sealed.

## Reproduce or inspect

```powershell
./phase3.ps1 report
./phase3.ps1 health
./phase3.ps1 run --run-id 20261109T014235Z-a9572f71e8
./phase2.ps1 status
./.venv/Scripts/python.exe -m pytest -q
```

`report` reads the completed report. `health` queries the current worker status and a read-only SQLite transaction. Running an already completed ID reuses its artifacts. To reproduce the frozen design in a new directory, `./phase3.ps1 freeze` prints the directory and `./phase3.ps1 run --run-id <directory-name>` executes it. This uses one CPU research worker and the pinned Phase 1 hourly snapshot. Failed attempts retain their own directories.

| Output | Contents |
| --- | --- |
| `frozen-study.json` | Dates, profile and dependency identities, maturity rules, costs, benchmarks, and research budget |
| `SUMMARY.md` and `verification.json` | Verified summary, actual trade differences, evidence hashes and final worker check |
| `verification-tests.xml` | Test evidence, including the synthetic Day gate and open-position restart |
| `attempt-01/SUMMARY.md` | Readable window comparison, forecast error, attribution, and sensitivities |
| `attempt-01/study-results.json` | Fold metrics, forecast distributions, trade overlap, exposure, turnover, dependence and concentration |
| `attempt-01/reference-predictions.parquet` | Baseline signal membership, scores, threshold flags, and mature reference targets |
| `attempt-01/fold-models/` | Training sample membership and fold-specific isolated artifacts and metadata |
| `attempt-01/reference/` and stress directories | Actual hypothetical trade, execution, equity, and per-signal comparison ledgers |
| `attempt-01/reference-equity.html` | Offline equity and drawdown chart |
| `attempt-01/day-reconciliation.json` and Parquet | Exact Day label/score ID join and independent session gates |
| `attempt-01/operational-health.json` | Worker state captured at study execution |
| `attempt-01/complete.json` | SHA-256 checksums of the completed output files |

## Conventions

An evaluation signal close is included at the left edge and excluded at the right. An order or position formed before a fold boundary retains its originating model and rules. The cash, fees, and positions are carried between folds. Open BTC is marked on the last observed pre-holdout candle. Prediction errors use only outcomes resolved strictly before the fold’s right edge. Fold fitting was also reproduced from separately truncated price prefixes. Drawdowns use observed candle closes and can miss intrabar lows.

Reference costs were 50 bp fees plus a 5 bp adverse price proxy per side. The only fresh counterfactuals were 80 bp plus 10 bp and one extra hourly execution bar at reference costs. Models and thresholds stayed fixed. Same-trade attribution uses the exact recorded units and fills. The break-even fee divides gross P&L less modeled price drag by actual traded notional and keeps the 5 bp adverse proxy in place. It is a diagnostic for the same ledger.

Day’s 29 score-only IDs all failed the entry cutoff, and Position’s original reports and model were preserved. No new historical model evaluation was performed for either profile. Their public observation continues under the original policies.

## Next research question

Can the frozen unfiltered Swing strategy beat cash on a preregistered prospective sample at the intended venue’s verified all-in costs?
