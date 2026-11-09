# Bounded chronological development study

This is the preserved Phase 2 proposal. [Phase 3](PHASE3_STUDY.md) used the consecutive-window fallback in the later request and evaluated Swing only.

Prioritize Swing, with Spot / Position as the sparse comparison. Day stays under observation with its frozen candidate. This plan leaves the final holdout sealed and does not authorize a new model, optimization run, or promotion.

The existing audit covers an expanding training history ending before 2025-01-01 UTC, a January–April 2025 validation window, and a May–November 2025 development evaluation. Historical features and labels used before 2025 already influenced the frozen fits, and the 2025 validation and evaluation results have been viewed. A walk-forward rerun on those dates is development evidence. The original final holdout starts 2025-11-08 00:00 UTC and ends 2026-11-08 00:00 UTC.

## Fixed study budget

Use the original native hourly snapshot, warmup and causal features. Run at most three expanding-window fits for Swing and three for Position, one per evaluation block. Use the same standardization, Ridge alpha, zero threshold, breakout rules, reference costs, and accounts. Keep any insufficient or unsuccessful block.

| Evaluation block in UTC | Training eligibility |
| --- | --- |
| [2023-01-01, 2023-07-01) | All earlier warmed-up signals whose outcomes resolved strictly before 2023-01-01 |
| [2024-01-01, 2024-07-01) | All earlier warmed-up signals whose outcomes resolved strictly before 2024-01-01 |
| [2025-05-01, 2025-11-08) | All earlier warmed-up signals whose outcomes resolved strictly before 2025-05-01 |

Each block starts a separate hypothetical account with aligned cash, full BTC, and passive 25% benchmarks. Only prices from bars opening before the block’s right edge are available for accounting. Require mature labels for each prediction-error sample. Identify the training span, actual outcome overlap, entry attempts, exposures, gaps, and open marks for each block. Use the existing minimum counts and report a sparse or insufficient result if a block cannot support a fit or error estimate. The final block reuses a previously viewed period and must be labelled as such.

Assess whether the frozen filter actually changes executable Swing trades and whether the same-trade raw gross outcome covers fees and the adverse execution proxy. Compare the reference against the already declared fee and delay cases. For Position, inspect how clustered labels and market regimes drive the effective sample. Report per-block dispersion and the trade identity differences. Keep the existing 5-label minimum as a reporting floor. A separate future protocol must define sufficient independent evidence and acceptable paper drawdown.
