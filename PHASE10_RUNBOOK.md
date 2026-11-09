# Phase 10 KuCoin monitoring and futures research

KuCoin is the public prospective reference. The worker operates in `MANUAL_OUTSIDE_LAB`. The approved lane reads **No validated model**. The Coinbase task and its experiment retain their existing owner, journal, and lock. The historical MEXC draft remains blocked.

## Windows operations

Run commands from the project root. The status view separates enabled controls, a verified live process and lock, Task Scheduler registration, and task-managed collection evidence. The dashboard shows the same worker controls and reads the durable records.

```powershell
.\phase10.ps1 status
.\phase10.ps1 install
.\phase10.ps1 verify
.\phase10.ps1 start-local
.\phase10.ps1 stop
.\phase10.ps1 monitor off
.\phase10.ps1 monitor on
.\phase10.ps1 ideas day off
.\phase10.ps1 ideas day on
.\phase10.ps1 timezone America/Toronto
```

`start-local` starts a detached process in the current signed-in Windows session. It can outlive the invoking terminal and dashboard. A session worker does not establish boot or sign-out recovery. `stop` persists an intentional pause. The task entry point respects that pause, and a start explicitly clears it. The OS lock under `data/phase10` prevents a second worker. The scheduler has a separate `IgnoreNew` policy and at most three restarts two minutes apart. The task runs with limited privileges and starts one minute after boot.

After the local checks, run the following in an **interactive administrator Windows PowerShell** under the account that owns the project. The separate task uses the Windows-native password prompt. The credential is stored by Windows Task Scheduler.

```powershell
Set-Location -LiteralPath 'C:\Users\16477\Projects\aseke-btc-lab'
.\phase10.ps1 install -Register
.\phase10.ps1 start
.\phase10.ps1 verify
```

`start` validates the registered action and operating policy and checks scheduler write access before draining a local worker. If the new launch fails, it attempts to restore the local worker. `verify` requires a running validated task, live process and lock, matching native owner and Session 0 action, and a real public collection record under that same launch ID. If a bar is not yet due, check again after the next due time displayed by `status`. Boot and sign-out recovery require separate observations.

## Scheduling and storage

Decisions use the frozen UTC boundaries of one day for Position, four hours for Swing, and 15 minutes for Day. The worker waits 40 seconds after close and polls every 15 seconds. Source closes and actual receipt times are saved separately. A decision made by 120 seconds is timely. A decision made within five minutes is delayed. A boundary older than five minutes is catch-up only. A recorded decision with unavailable inputs is unknown. Older missed boundaries are counted separately. One deterministic identity per profile, pinned strategy and boundary survives restart. Missing or stale quotes, candles or features receive a reason, and eligible market structure may result in `NO_TRADE`.

The startup cache can reuse compatible Phase 9 warmup evidence. Later requests fill new or missing ranges at the provider page limits. Each cycle has at most 20 GETs, 350 ms minimum request spacing and five-second HTTP timeouts. A retry obeys any provider `Retry-After`, with local backoff; access restrictions trigger a longer pause. The rolling cache holds the configured warmup plus eight extra bars per profile. Raw response evidence has a visible 96 MiB ceiling. Worker logs rotate at 1 MiB with three backups. Research uses separate output and a separate process under the parent wall, thread, RSS, disk, and shared admission limits.

The worker owns Phase 10 cache and decisions. It mirrors immutable ideas into the existing local idea ledger. While it holds the lock, the Phase 9 PowerShell prospective commands defer to it. Legacy historical collection/import and user-reported execution commands still use Phase 9. Monitoring off pauses new market fetches and decisions. Existing ideas can expire by clock. Ideas off for a profile stops new publication and lets already published ideas continue through lifecycle handling when data arrive.

An observed entry condition can create an explicitly hypothetical quote-based fill. A later quote may resolve a modeled stop, target, or time exit. Spot results report the quoted spread and reference trading fees. Futures outcomes keep quoted gross and fee reference with net unresolved when funding evidence is absent. This is a separate prospective proxy from the frozen research simulator. User-reported executions remain independent.

## Research controls

```powershell
.\phase10.ps1 requirements
.\phase10.ps1 probe-archive
.\phase10.ps1 queue swing C:\Research\futures-bundle.json
.\phase10.ps1 training swing on
.\phase10.ps1 training swing off
.\phase10.ps1 training day on
.\phase10.ps1 training day off
```

An enabled profile can run only a qualified queued futures bundle. The worker launches a lower-priority child separately from prospective work. The Phase 8 legacy learner (`learning.ps1 start`) and Phase 9 manual research gate (`phase9.ps1 training on`) remain separate entry points. Phase 10 uses the same shared supervisor and fit locks, and yields admission while the legacy learner is enabled. Position uses the pinned Phase 9 Spot result and has no Phase 10 training job. Candidate completion never changes the live strategy or approval. At commissioning, both futures profiles wait for a qualified bundle, and all training gates are off.

A bundle descriptor contains the following fields. Paths can be relative to the descriptor.

- `source` contains `venue`, `symbol`, `settlement` (`USDT`), `scope` (`RESEARCH_ONLY`), `terms` and source/permission `evidence`.
- `chunks` is a chronological list. Each entry contains `trade`, `mark`, `trade_sha256` and `mark_sha256`. Each Parquet file has UTC `open_at` and OHLC. Trade additionally contains native volume normalized to base units. Each price partition permits at most 50,000 rows. The existing 650,000 joined rows per basis limit still applies.
- `funding` contains Parquet `path`, `sha256`, declared `tolerance` below one minute, and `schedule`. Funding files contain UTC `settlement_at` and decimal `rate`. Each adjacent schedule regime contains `start`, `end_exclusive`, `anchor`, integer `minutes`, and dated `evidence`. Slots must match settlements one to one. A recorded zero remains a valid observation.
- `rules_basis` is `HYPOTHETICAL_SINGLE_TIER`. Supply `rules_evidence` and a `contract` using the parent simulator fields (family, quote, base multiplier, quantity step, tick, minimum contracts, tier notional, leverage cap, maintenance and liquidation rates). KuCoin reference fees applied to another venue's inputs are hypothetical.

The replay preserves complete derived buckets across native partitions, then computes the original features across the derived series. A compact global clock and two native price partitions support continuous positions, partial fills, pending exits, labels and trailing state. Gaps during exposure produce unresolved exposure. At-open funding goes to positions already open before that timestamp, before opening exits. Off-grid funding retains the published timestamp and applies after opening actions and before modeled intrabar resolution, with the containing mark open as the hypothetical price proxy. Funding events and economic outcomes stay in the replay evidence.

## Observed source qualification

The bounded Binance Vision probe used 17 requests, 6,100,274 bytes and 12.61 seconds. Checksums and schemas passed for both price bases in June 2022 (43,200 aligned minute bars each), July 2024 (44,640 each), and November 7, 2025 (1,440 each). June and July had 90 and 93 funding rows respectively, including 61 and 13 off-minute timestamps. These samples do not establish continuous funding or trade/mark history. Their terms and hashes are saved under `runs/phase10/binance-probe`. Data and derivatives are restricted to this offline research lane. [Binance Vision dataset terms](https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md)

June 2022 also passed the real partition alignment and funding checks as five price partitions. Its 90 settlements matched the observed eight-hour schedule within 25 ms, retaining the published instants. Both profile gates rejected the sample for insufficient date coverage. The qualification evidence is in `runs/phase10/binance-probe/june-2022-qualification/result.json`.

The latest frozen fold needs paired minute history and settlements before July 1, 2024 for training and through November 8, 2025 at 00:00 UTC for inner and outer windows. The earlier inner starts are July 1, 2022 and July 1, 2023. Their outer windows are calendar 2023 and 2024. Even the latest inner and outer interval alone is 712,800 minute rows per basis. Phase 8's 650,000 limit originally protected per-file reads. Phase 9 applied it to the joined input; Phase 10 retains that joined workload ceiling alongside the lower partition limit. Full earlier folds need 790,560 and 792,000 rows per basis before their training prefix. The frozen model floors remain 35 Ridge and 80 histogram mature training outcomes, with 10 each in inner and outer evaluation. Maturity is strictly before the applicable boundary. Day uses the parent 8-hour maximum holding period and Swing 7 days, with the original 30-second processing allowance. Swing's 200-day context also needs the previous completed rolling window for its slope.

Result is `WAITING_FOR_QUALIFIED_FUTURES_BUNDLE`, zero eligible frozen folds and zero futures fits. The sealed BTC period remains excluded. Partition tests are simulator diagnostics.
