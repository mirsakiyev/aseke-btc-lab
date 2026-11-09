# Phase 2 collector and shadow runbook

## Commands

Run these in PowerShell from the project folder. Each command uses the project’s existing Python virtual environment.

```powershell
./phase7.ps1 status
./phase7.ps1 start
./phase7.ps1 stop
./phase7.ps1 coverage
./phase7.ps1 weekly
```

- `start` explicitly resumes the original shadow and collector. It uses the registered unattended task when available and otherwise uses the detached launcher in the current Windows session. An already-running matching worker is reused.
- `stop` writes a persistent pause, stops through the worker's graceful token interface, and disables the owned task if registered. A boot or retry cannot resume the paused worker. Positions and pending intents remain durable.
- `status` checks the verified PID and creation time, executable, working directory, owner, project lock, and scheduler state. It reports heartbeat staleness, missing or stalled candles, process failure, and intentional stop separately. `status -Json` includes raw health evidence and account state. The log is `logs/research-collector.log`.
- `coverage` shows active-period receipt denominators, decision readiness and timing, and Swing warmup. `weekly` uses a seven-day decision window with the same since-activation candle denominators.
- Legacy `./phase2.ps1 start --shadow` and `./phase2.ps1 stop` now delegate to Phase 7. The legacy `start` also resumes shadow. `./phase2.ps1 status` remains the raw worker status. Foreground `collect --once` is available when the task is absent, the pause is clear, and the project lock is free. It performs one collection pass.
- `./phase3.ps1 health` adds a read-only 24-hour tally of observed and timely candle receipts, checkpoint and decision lag, experiment identity, and recent logged interruptions. Bulk catch-up can complete the candles in a lookback without demonstrating 24 hours of timely observation.
- `./phase5.ps1 observations` adds a read-only decision timing audit. It uses the pinned decision schedule and original receipt and journal evidence, excludes scheduled time before activation, and reports pending, blocked, late, missing, and unknown separately. Use `--json` for the evidence behind each decision.
- The dashboard is read-only. Its approved recommendation lane stays **No validated model**.

### Register the unattended task

```powershell
./phase7.ps1 install
./phase7.ps1 verify
```

`install` prepares and validates `runs/phase7/task.xml`. To register the boot trigger, open PowerShell as Administrator under `ASEKESTATION\16477`, then run the following from the project directory.

```powershell
./phase7.ps1 install -Register
./phase7.ps1 start
./phase7.ps1 verify
```

Enter the Windows account password directly at the native `schtasks.exe` prompt. Registration stages a disabled task. `start` snapshots the durable state, stops the current worker gracefully, and starts the same cohort through Task Scheduler. It verifies the PID, owner, lock, experiment, prior journal, and live Coinbase heartbeat. A successful REST batch after the new PID's start confirms scheduled candle access. If the next eligible candle has yet to arrive, inspect the next due time with `verify`. A failed handoff disables the task and restores the prior manual launch when one was running. Durable evidence remains in SQLite. [Windows boot trigger](https://learn.microsoft.com/en-us/windows/win32/taskschd/boottrigger) · [Windows task logon modes](https://learn.microsoft.com/en-us/windows/win32/api/taskschd/ne-taskschd-task_logon_type)

The task is `\ASEKE BTC Lab - Forward Research`, under the existing `ASEKESTATION\16477` identity with password logon and least privilege. It runs `.venv\Scripts\python.exe` directly, with the absolute project directory as its working directory and `-X utf8 -m lab.phase2 collect --shadow --managed`. It starts one minute after boot, ignores overlapping task instances, and uses the same OS worker lock as manual collection. Its execution limit is `PT0S`, meaning unlimited. It starts without idle or network gates and allows operation on battery. Collection requires an awake machine and usable connectivity. [Windows execution time limit](https://learn.microsoft.com/en-us/windows/win32/taskschd/tasksettings-executiontimelimit)

Task Scheduler retries an unsuccessful worker exit up to three times at two-minute intervals. Ordinary network failures use the collector's existing capped REST retry of 120 seconds and WebSocket reconnect backoff of 60 seconds. A live process with a stale heartbeat or stalled collection remains visible in status. Task Scheduler reacts to process exit and cannot detect those conditions. Intentional stop and the storage-budget stop are clean exits. RDP disconnect, Windows sign-out, reboot recovery, and uninterrupted uptime are separate checks.

Phase 4 keeps this Coinbase collector and shadow cohort in place. The separate MEXC adapter is offline and has no collector or scheduler. Inspect the existing worker with the same Phase 2 commands or the Phase 4 health command. [MEXC research runbook](PHASE4_MEXC_RUNBOOK.md)

```powershell
./phase7.ps1 uninstall
./phase7.ps1 start
```

For rollback, `uninstall` gracefully removes the verified project task and keeps the data. Run `start` to return to the detached current-session launcher. If an older project task was replaced, its exported definition is in `runs/phase7/previous-task.xml` for review and optional local restoration after uninstall. The legacy `collector-task.ps1` commands delegate to the same manager.

## Public collection

The Exchange BTC-USD heartbeat WebSocket supplies event timestamps, sequence, and last trade ID. The runner holds only the latest heartbeat and bounds the client queue to eight messages. It reconnects on timeout with exponential backoff up to 60 seconds. The heartbeat establishes stream health and never supplies candle prices or volume. [Coinbase Exchange heartbeat documentation](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)

Native Coinbase five-minute and hourly candles are reconciled with REST once per newly eligible close, after a 40-second grace. A normal pass requests the new candle and up to two previous intervals for late arrivals or revisions. Recovery uses at most four requests per resolution per cycle with the adapter’s 299-interval ceiling and bounded retries. No ticker or trade-derived volume is mixed in. A scanned range can still contain missing candles. [Coinbase Exchange candle endpoint](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)

The audit treats the sixth candle field as base-asset volume and quotes OHLC in USD per BTC. Coinbase identifies BTC as the base and USD as the quote for BTC-USD. The candle page names the field as trading volume, and the product stats reference explicitly uses base-currency volume. The audit compares like-for-like native fields without multiplying volume by price. [Product metadata](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-single-product) · [Product stats units](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-stats)

The forward SQLite database is `data/forward/observation.sqlite3`. A WAL transaction publishes the compressed raw response, SHA-256, request bounds, actual receipt, immutable changed candle versions, and per-resolution scan cursor together. A malformed response retains raw evidence and does not advance the cursor. Duplicate observations do not create a second candle revision. The database starts at the holdout end. Earlier canonical Parquet data remain separate.

An OS lock allows one forward runner. Collection also takes the same data writer lock as the existing backfill. Backfill and collection serialize or retry. The dashboard reads a consistent SQLite transaction and immutable Parquet. A published revision applies to later decisions only. The exact revision IDs, receipt times, and hashes remain available for existing decisions and executions.

The forward database and WAL have a combined 256 MiB operating budget. The collector stops on reaching the budget rather than deleting evidence. The log rotates at 1 MiB with four backups. For backup, stop the runner and copy the entire `data/forward/` directory together with the frozen dataset, model registry, and run reports. A database snapshot or SQLite backup is another option if a writer must remain active.

## Experimental shadow

Activation records the current UTC instant and creates separate USD 10,000 accounts once. A restart resumes the same activation, orders, positions, marks, and event keys. Collection can run without shadow. A compatible frozen Phase 1 model is required for each profile’s experimental entries, and model age remains visible via the training cutoff.

The first eligible decision is a completed strategy candle at or after both activation and the registered holdout end. Only one current post-holdout feature row can be scored per decision. Fixed feature calculations use past closed prices, including reserved prices when necessary for warmup. No reserved strategy outcome is trained, scored, selected, or exposed. The event and experiment records state whether reserved prices were used for features. Fitted scalers, weights and the zero threshold remain frozen.

The modeled execution open is the first execution bar strictly after decision availability, with the profile’s configured delay. Before that open, the worker durably commits quantity, stop, fee/proxy policy, and the known maximum holding or session exit. It records a post-commit timestamp before the open as proof that the intent was precommitted. If that proof is missing after a restart, no fill is attributed. An entry is resolved only from an observed closed execution bar. The journal records the modeled event time, actual candle receipt, and actual resolution separately. These are delayed hypothetical fills.

Closed bars resolve stop and scheduled exit rules under the Phase 1 convention. A stop inside a candle uses the candle close as its conservative resolution timestamp. Day keeps its 22:00 UTC fill cutoff and 23:55 UTC flat target. A missing execution bar after a 90-second observation grace freezes the affected account with its last known position or uncertain pending entry. The journal records unresolved deadlines. Later price revisions do not silently recast earlier fills or remove uncertainty.

Day displays the next execution opening separately from a hypothetical fill. The independent session gate reports whether that opening is before the UTC cutoff in the signal’s session, even if the baseline condition failed. A true session gate still requires the other entry conditions. Exit monitoring stays active after the entry cutoff.

Catch-up decisions that missed the modeled open are logged without a new order. A previously committed intent can be reconstructed when its closed candle later arrives. The evidence class distinguishes those events from timely observation, and the dashboard identifies reconstructed or uncertain closed trades separately. A missed dynamic exit is flagged, and a precommitted stop or time exit remains in force where candle evidence exists. Research budgets do not replace user-approved paper drawdown or pause rules.

Planned risk is fixed at sizing time. A later modeled fill above the sizing reference can increase realized loss even when the original stop rule is followed. Phase 6 reconciled the saved examples. Reopen that question only if new observations contradict the accounting.

## Weekly observation review

```powershell
./phase7.ps1 weekly
./phase7.ps1 weekly -Json
./phase7.ps1 status
```

1. Check the experiment identity and pinned models, live PID, heartbeat, startup time, and any stops or errors. Inspect any existing position or pending intent before intervening.
2. Save the dated coverage output for comparison with the prior review. Table A describes full-day candle completeness including catch-up. Table B keeps every elapsed close-plus-120-second horizon since activation. Compare the new due, late, catch-up, missing, and unknown counts with the prior snapshot. Use the journal and worker logs to investigate exceptions. The 120-second threshold is descriptive.
3. Use table C to review the last seven days of scheduled decisions, timing, feature readiness and outcomes separately. Use the live Swing warmup estimate and inspect the most recent journaled decision. A native gap can move the estimate. Verify readiness on a new saved decision.
4. If an intent exists, verify its durable pre-open acknowledgement and execution-quality flag. Keep reconstructed fills distinct from timely closed-bar resolution. Review data or execution issues independently of strategy evaluation. This review cadence supplies no promotion threshold.

Phase 6 added append-only input load timestamps, feature-gate values, and code/effective-version metadata to new decisions. The compatible restart retained the original activation and model/configuration identities. The exact effective boundary and before/after checks are in `worker-restart.json` beside the Phase 6 report. An existing journal written before the boundary keeps its original metadata. Logoff and reboot persistence remain unverified.
