# Phase 5 robustness and prospective evidence

**Coinbase BTC-USD historical research with a hypothetical published-MEXC-fee scenario.**

## Commands

```powershell
./phase5.ps1 freeze
./phase5.ps1 run
./phase5.ps1 report
./phase5.ps1 observations
./phase5.ps1 observations --json
./phase2.ps1 status
./phase4.ps1 study-state
```

The run uses the project virtual environment and the pinned pre-holdout native hourly snapshot. The original Phase 3 reference, Phase 4 repricing and Phase 4 lower-fee replay are read from checksummed files. Only the two frozen stresses are executed. Each new replay records a start and a completion record. A repeated completed run verifies and reuses the evidence. An interrupted replay retains its partial evidence and cannot spend the same budget again automatically.

## Historical conventions and results

The Phase 5 manifest froze the original eligible evaluation, from 2023-07-01 00:00 to 2025-11-08 00:00 UTC. The right boundary is the last pre-holdout candle's close. The account elapsed span is 861 calendar days. The insufficient January–June 2023 block remains excluded. Equity and drawdown use observed hourly closes and leave five unavailable marks unfilled. The accounts reached the last common mark with no open strategy position. Drawdown can miss intrahour or unobserved lows. Total returns and their geometric annual equivalents use actual elapsed days, with 365.2425 days per year.

The two stress executions keep the signal and strategy rules frozen. One changes the combined adverse proxy to 10 bp per side at the 5 bp fee. The other waits one extra hourly execution bar with the same 5 bp fee and 5 bp proxy. The order size uses the original decision close and stop, scenario costs, and cash available at the decision. No delayed-bar high, low or close is an input to sizing. The entry and exit timing and holdout boundary were checked against each stress ledger.

The 5 bp reference-proxy replay returned +4.23% total, about +1.77% annualized. The two stresses returned +3.30% and +4.62%. Two of the five tradable windows were negative in all three lower-fee replays. The 25% passive benchmark retained its original buy-once definition and drifted to 42.22% average marked BTC exposure in the lower-fee case, versus 2.55% for Swing. It earned much more with a larger drawdown. Those exposure differences matter when interpreting the raw return gap.

The fixed-ledger fee difference accounts for USD 444.11. The same fills with recalculated quantities account for an additional USD 158.56 using full precision, or USD 158.57 when subtracting the separately rounded headline figures. The sizing file separates direct cost-sensitive sizing, the accumulated fee savings in decision equity, and further sizing compounding. Position quantities are fractional Coinbase-research quantities. Gap and proxy effects can push realized entry-to-stop risk or loss above the planned budget.

The lower-fee replay's five largest winners contributed USD 1,013.99 against total net USD 422.84. Any winner-exclusion arithmetic holds that replay's quantities fixed. The result is inconclusive as an edge claim. It supports further observation of the frozen rules, with concentration, hypothetical costs, and dependent outcomes still material.

## Prospective observations

The report reads the existing Coinbase reference-fee shadow through a read-only SQLite transaction. For each registered profile it enumerates actual decision closes after the original activation and holdout end. The 24-hour observation denominator counts a scheduled close when a record exists or its original modeled opening has passed. A still-pending deadline is displayed separately. Day decisions remain expected when new entry fills are barred by the session.

The audit retrieves the native decision-bucket candle receipts through the event's stored revision watermark. It uses the latest of the first receipts as the first local availability of the complete bucket. Selected revisions must have been received before feature reading and the logged decision. A timely record precedes its original modeled opening. A data block can itself be timely, while yielding no feature-ready decision. Late computations started causally before the deadline are separate from retrospective catch-up. Missing receipt or journal timing remains unknown. An intent also requires its own durable pre-open proof.

The 40-second minimum wait and the 30-second processing allowance are earliest scheduling gates. Neither is an operational latency target. Feed coverage may include catch-up and the health report's 120-second receipt count is descriptive. The 90-second fill observation grace applies separately to execution evidence. Existing UTC receipt, revision and journal timestamps were sufficient for this audit, so the worker needed no timestamp schema change or restart. The approved lane stays **No validated model**.

## Evidence

- [Frozen diagnostic and exact coverage](runs/phase5/swing-38dc720717c2d8/manifest.json)
- [Comparison and operational snapshot](runs/phase5/swing-38dc720717c2d8/SUMMARY.md)
- [Full metrics, yearly and window attribution, and concentration](runs/phase5/swing-38dc720717c2d8/results.json)
- [Sizing examples and overrun reconciliation](runs/phase5/swing-38dc720717c2d8/sizing.json)
- [Observed equity, drawdown and benchmark chart](runs/phase5/swing-38dc720717c2d8/equity-drawdown.html)
- [Prospective decision and original receipt evidence](runs/phase5/swing-38dc720717c2d8/prospective-observations.json)
- [Remaining execution facts](DECISIONS_NEEDED.md)
