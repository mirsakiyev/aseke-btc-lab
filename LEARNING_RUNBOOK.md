# Phase 8 controlled learning

The learner uses its own SQLite control and trial ledger in `data/learning` and writes versioned evidence to `runs/learning`. The existing Coinbase worker and Phase 7 scheduled task manage observation. The dashboard's **Research / Learning** view and the PowerShell commands use the same learner controls.

## Operate on Windows

```powershell
./learning.ps1 status
./learning.ps1 freeze
./learning.ps1 run-once
./learning.ps1 report
./learning.ps1 start
./learning.ps1 pause
./learning.ps1 resume
./learning.ps1 stop
./learning.ps1 profile day_perp_research_v1 off
./learning.ps1 profile day_perp_research_v1 on
```

`freeze` checks and reuses the manifest. `run-once` waits for a single campaign. The dashboard queues the same campaign asynchronously. `start` enables daily eligibility checks in a detached process for the current Windows session. Repeated requests on identical inputs reuse the existing campaign. The first campaign is fixed to the pre-holdout development windows. New data require a reviewed source revision before they can contribute labels.

`pause` transitions through PAUSING while the child checkpoints at trial or fit boundaries. An unresponsive child is stopped after a four-second grace. The ledger retains completed trials and marks unfinished configurations. `resume` reuses the job, attempt counts and original deadline. `stop` persists OFF through restart. Profile flags apply to the next campaign and can be inspected at any time. The collector continues independently.

A campaign has one below-normal-priority child, at most two compute threads, a two-hour elapsed deadline, two attempts per interrupted candidate, and a monitored 640 MiB child-tree resident-memory limit. The supervisor checks resource and collector health before admitting work and during the run. It requires at least 1,250 MiB available RAM and 2 GiB free disk. The learner output cap is 256 MiB and the source limit is 650,000 native rows per file. These are workstation controls. The cap is monitored at bounded intervals. A separate scheduled learner task is unnecessary for this local milestone. Continuous learning is off after verification.

## What the study uses

| Profile | Decisions and context | Initial execution convention |
| --- | --- | --- |
| `position_spot_research_v1` | Daily, completed weekly SMA20 | Coinbase BTC-USD, hourly execution, long/cash at 1x |
| `swing_perp_research_v1` | Four hours, completed daily SMA200 | Linear USDT perpetual, minute trade and mark data, long/short |
| `day_perp_research_v1` | Fifteen minutes, completed hourly and four-hour SMA50 | Same contract family and minute requirement, long/short |

Day uses the existing 22:00 UTC entry cutoff, 23:55 UTC flat deadline and eight-hour maximum as research conventions. Swing and Position have seven- and thirty-day maximums. The original observer's features and rules remain pinned separately. UTC windows derive from the archived manifest and holdout lock. The first campaign admits source bars strictly before **2025-11-08 00:00 UTC**. The holdout ends **2026-11-08 00:00 UTC**. Arrow applies the cutoff to every market input. A supported online SQLite backup records operational continuity and supplies no feature or outcome data.

The catalog freezes three entries, two exits, and three variants, with up to 18 primary configurations per profile. Breakout uses the previous 20-bar extremes and trend context. Pullback requires a prior retracement of at least half an ATR from EMA26 and a current return reversal with the trend. Range reversion uses a low-trend regime and a 1.25 ATR distance with an RSI gate. The complete formulas and settings are in `learning.yaml`, `feature-registry.json`, and the manifest. Pivot and Fibonacci features first use a pivot two completed bars after its center. Higher timeframes admit only complete buckets. The optional entitled, timestamped external signal slot is disabled.

The external-signal contract reserves the fields `instrument`, `signal_at_utc`, `available_at_utc`, `value`, `provider_version`, `source_digest`, and `entitlement_evidence`. A reviewed future adapter needs to validate those fields, establish rights to use the supplied values, and admit each row only after its recorded availability. This milestone ships the disabled contract and uses transparent local features.

Both exit policies use the same ATR/confirmed-structure stop and time barrier. Fixed R uses a 2R target. The other policy ratchets a 2.5 ATR trail from later completed bars, effective at the next eligible open. Label orders use the frozen USD/USDT 10,000 reference account independently of occupancy. Executable replays use each account's available equity and one position. Positive predictions must exceed the fixed zero net-R threshold. Cash, training mean, matching unfiltered rules, and fee-inclusive passive 25% spot are controls. Spot's 50 bp fee plus 5 bp adverse execution is primary. The 5 bp fee plus 5 bp adverse diagnostic is a separate hypothetical spot scenario.

Ridge scales inputs inside each training window. The shallow histogram model uses fixed parameters and disables internal early stopping. Chronological inner and outer windows and adequacy floors are recorded before the run. Outcome maturity must be strictly before the split; the executable path also excludes entries with a known time deadline reaching the right boundary. Gaps, ambiguous outcomes, rejected orders, and failed work stay in the evidence. Error metrics concern outcome prediction. Account metrics concern simulated trading. Moving three-month blocks describe uncertainty when there are enough trades and months. These intervals are unadjusted for selection across the catalog. The campaign used previously inspected development periods and can nominate zero challengers under its frozen selection rule. [Backtest selection risk](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf) · [Multiple testing and Sharpe](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)

Day can refit after one day and 40 new mature outcomes, Swing after seven days and 20, and Position after 30 days and 10. A reviewed data revision can be frozen with `./learning.ps1 freeze --reviewed-new` while the learner is off and the prior campaign is closed. The code checks prior fitted trials and deduplicates unchanged work. The archive and cumulative counts remain intact. The first version still enforces the original pre-holdout calendar boundary.

## Futures data and execution scope

The initial venue-specific futures comparisons are **BLOCKED_DATA**. Supply lawful local Parquet exports and provenance through a reviewed `perp_sources` profile entry before running a futures campaign. The descriptor names the source, permission evidence, retrieval time, UTC units, trade/mark/funding paths, funding cadence and UTC anchor, rule evidence and effective dates, and versioned contract rules. Trade and mark files use timezone-aware UTC `open_at`, `close_at`, and numeric OHLCV on the one-minute grid. Funding uses timezone-aware UTC `settlement_at` and signed `rate`. A missing expected settlement blocks loading. A reviewed descriptor may declare current rules used hypothetically. Exact archival availability, matching historical rules, and fill detail require separate evidence. [Contract specification](https://www.mexc.com/api-docs/futures/market-endpoints/get-contract-info) · [Fair-price candles](https://www.mexc.com/api-docs/futures/market-endpoints/get-fair-price-candles) · [Funding history](https://www.mexc.com/api-docs/futures/market-endpoints/get-funding-rate-history)

The isolated simulator handles one linear long or short in a fixed supported margin tier. It reserves collateral separately from expenses and charges fees on filled quote notional. Positive funding is debited to longs and credited to shorts at the recorded settlement, before an existing position's same-timestamp opening exits. It uses mark for triggers and separate trade observations for fills. In a shared intrabar range it prioritizes liquidation, stop, then target and records the ambiguity. An exposed data gap or unsupported margin-tier transition stays unresolved. The full-liquidation diagnostic tracks any modeled deficit separately. Cross margin, inverse, hedge, margin additions, venue-tier transitions, partial liquidation, and full ADL are outside this engine. [MEXC liquidation mechanism](https://www.mexc.com/support/article/faq-on-liquidation-for-futures-trading-8123281969561) · [Funding mechanics](https://www.mexc.com/support/article/mexc-futures-funding-rate-305432020820705280)

An IOC order fixes quantity, price bound and quote capacity before execution. Its remainder expires. The simulated opening quote uses adverse execution and tick rounding. Quote-cap matching and partial capacity are frozen hypothetical assumptions. Reduce-only exits clamp to the residual position and cancel siblings; a remaining quantity is retried. Intrabar perpetual exits use the adverse trade extreme because mark and trade event order is unavailable in OHLC. Actual quote and fillability require timestamped trades or book observations. The stress grid runs 1x through 100x at fixed planned risk and can reject infeasible buffers or margins. Ordinary futures use the 0.25% risk budget, 1x account-notional cap and 5x selected-leverage ceiling. Higher settings are isolated diagnostics. [MEXC TP/SL mechanics](https://www.mexc.com/support/article/mexc-futures-take-profit-and-stop-loss-faq-350096811661177856)

The published MEXC futures API research scenario uses 8 bp taker plus a 5 bp adverse proxy per fill. The fee table effective June 1, 2026 and the newer guide agree on 6 bp maker and 8 bp taker. The older announcement example quotes the previous rates. The fees are **PUBLISHED_UNVERIFIED_FOR_ACCOUNT**. Synthetic paths use a fictional contract specification and are marked **SIMULATOR_DIAGNOSTIC**. The known Canadian eligibility restriction and blocked MEXC draft remain in effect. [API fee announcement](https://www.mexc.com/announcements/article/updates-to-api-futures-trading-fees-jun-1-2026-17827791535742) · [Newer fee guide](https://www.mexc.com/learn/article/mexc-fees-explained-complete-trading-futures-withdrawal-fees-guide/1) · [MEXC User Agreement](https://www.mexc.com/terms)

## Evidence and handoff

Read `manifest.json` for policy and code identities, `all-trials.jsonl` for append-only campaign events, `report.json` and `REPORT.md` for the comparison, `data-capabilities.json` for prerequisites, and `simulator-diagnostics.json` for mechanical stresses. Label and order files give causal cutoffs, sides, planned risk, leverage, fills, and explicit skip/unknown reasons. Fitted artifacts are research-only. The existing observer and approved lane remain separate. The open venue decisions are in [DECISIONS_NEEDED.md](DECISIONS_NEEDED.md).
