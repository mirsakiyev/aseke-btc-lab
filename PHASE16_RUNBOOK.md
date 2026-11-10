# Phase 16 controlled challenger learning

Approval **No validated model**. Execution **MANUAL_OUTSIDE_LAB**. Phase 16 controls only this challenger route. The Phase 15 opportunities and four books, both collectors, and the frozen Phase 14 cohort operate independently.

## Commands

From the project root in Windows PowerShell:

```powershell
.\phase16.ps1 plan
.\phase16.ps1 run-once
.\phase16.ps1 status
.\phase16.ps1 report
.\phase16.ps1 pause
.\phase16.ps1 resume
.\phase16.ps1 stop
```

`plan` reopens the same frozen initial manifest. `run-once` explicitly runs that plan or reuses its saved result. `pause` interrupts only the challenger child. `resume` continues the same campaign, reservations, saved artifacts, completed cases, and original wall-clock deadline. Elapsed time keeps counting while paused. `stop` turns recurring checks off and leaves the checkpoint. `report` writes `runs/phase16/REPORT.md`. The dashboard's **Challenger learning** workspace reads the same status. Reload the existing dashboard on port 8501 to see the new workspace.

After an operator allocates a distinct admitted input, this exact command enables recurring eligibility checks:

```powershell
.\phase16.ps1 enable
```

Recurring checks run from the existing managed public observation cadence. A check with no new allocation starts no research. A distinct input may require one supervised qualification pass to count new mature setups. Training proceeds only when the maturity floor, interval, chronological folds, and shared workstation guards permit it. Phase 16 recurring checks and all legacy training are off at delivery.

## Initial frozen comparisons

| Profile | Operational recipe represented in the comparison | One challenger hypothesis | Incremental floor / interval |
| --- | --- | --- | --- |
| Day | Breakout / fixed R | Model cost relative to stop distance, volatility, relative volume and context | 40 new mature setups / 1 day |
| Swing | Breakout / ATR trail | Directional breakout in ATR units, momentum, relative volume and trend persistence | 20 / 7 days |
| Position | Long-only breakout / fixed R | Breakout strength and trend quality in admitted KuCoin Spot observations | 10 / 30 days |

Each profile uses the existing Ridge preprocessing with fixed alpha 10 and a small preregistered feature schema. Full-feature alpha-1 Ridge and histogram studies already exist. The Day cost feature is twice the reference per-side fee plus adverse-price rates, multiplied by the setup reference and divided by the initial stop distance. Directional breakout is the signed distance beyond the applicable prior range divided by setup ATR. All features are available at the setup decision. All targets are resolved modeled net R from eligible setups, including the same modeled trading costs and applicable funding used to generate the recipe-specific labels. The scaler and model fit inside each training partition. An outcome that matures on the split is excluded.

Futures use the admitted Phase 12 partitioned Binance development view and exact saved breakout label receipts. Position uses the Phase 9 hourly KuCoin Spot bundle and checked lineage erratum. These are consumed development windows. The campaign preserves the original chronology and uses a minimum of 35 mature Ridge training labels and 10 in each matched inner and outer assessment. It compares the matching full-risk rule, fold training mean, cash, and a fixed 50% risk-and-notional rule. Cash has zero return and drawdown. The other four lanes run under baseline, double execution costs, and a one-native-bar entry and exit delay with original time and session caps. The delay is one minute for futures and one hour for Spot. The 25-trade evidence floor and dependence-aware monthly uncertainty remain in effect. Forecast improvements alone do not qualify economic improvement.

The one-job guard, two threads, 640 MiB child RSS guard, two-hour deadline, free RAM, disk and output guards remain in force. The initial plan permits at most 20 estimator attempts and 42 continuous-account attempts including interrupted reservations. It makes no source requests.

## Future allocation

```powershell
.\phase16.ps1 allocate C:\Research\allocated-kucoin-development.json
.\phase16.ps1 status
.\phase16.ps1 run-once --campaign <allocation-id>
.\phase16.ps1 report --campaign <allocation-id>
```

The allocation report is written to that campaign's folder. The default report continues to show the initial campaign.

The allocation document needs `allocation_role` set to `CHALLENGER_DEVELOPMENT_ONLY`, `profile`, `start`, `inner_start`, `outer_start`, and `end_exclusive`. Supply `source` with `venue` KuCoin, `symbol` BTC-USDT for Position or XBTUSDTM for futures, `settlement` USDT, `scope` RESEARCH_ONLY, and explicit `permission`, `evidence`, and `terms`. Supply the public `contract`, `contract_evidence`, and `chunks` containing paths and SHA-256 for `trade` and, for futures, `mark`. Futures also need the Phase 10 `funding` descriptor with SHA-256, path, tolerance, and complete dated settlement regimes. Spot requires a contiguous UTC hourly trade grid including `close_at` and OHLCV. Futures use paired minute partitions and the existing native row cap. All timestamp boundaries must align to the native grid. The archive must already exist, including the feature warm-up prefix.

Use an independent local source outside the lab's protected stores. The window starts after the latest protected holdout, Phase 14 and Phase 15 follow-through boundary. Its outer interval follows the previous evaluation. The worker computes labels from all eligible setups with resolved mature outcomes. New mature progress counts decisions at or after the prior evaluation cutoff, so corrections to an earlier prefix cannot manufacture an increment. Unresolved positions, user-reported executions, and feature-only history supply no labels. Only one allocation per profile can await completion. Identical native inputs reuse the same identity. A new allocation cannot grant more rights to its data or artifacts.

Entry into a separately registered prospective experiment would require explicit permission for that use, complete causal feature and execution evidence, a fresh qualified forward evaluation, the profile label floors and at least 25 resolved trades under the primary and stress comparisons. Net return must clear the frozen controls under realistic costs and delay, with adequate risk and expectancy evidence. A Binance-derived research-only model stays in that scope. A separately permitted KuCoin-native candidate needs its own model version, publication time, protocol and paper book. Experiment admission and validation are separate decisions.

Phase 15's publication ends December 9, 2026 at 21:45 UTC and its follow-through ends January 9, 2027 at 21:45 UTC, both exclusive. A future continuation requires an operator-approved new immutable registration, fresh prospective start and new book identities, with explicit source coverage through its holding window. `phase15 register` reopens the original registration. It does not extend the dates or move open positions into a new book.
