# Current PAPER operations

Run from the canonical Lab checkout using `.venv\Scripts\python.exe`. The financial runner and dashboard use this checkout. The financial ledger, published runtime snapshots and generation controls live under `runs/paper-run`. The collectors keep their existing market data under `data/phase10` and `runs/ema_kraken_reference`. Record the working directory and source hashes of any scheduled collector that uses a pinned runtime.

## Inspect the running system

```powershell
$python = '.\.venv\Scripts\python.exe'
& $python -X utf8 -m lab.paper_run status
& $python -X utf8 -m lab.paper_run clock_quality
& $python -X utf8 -m lab.paper_run protection report --slot ema/day
& $python -X utf8 -m lab.health task-status
& $python -X utf8 -m lab.health dashboard-status
```

Check the current generation and build against the managed runner heartbeat. The existing **ASEKE BTC Lab - Health Watchdog** supervises the paper owner through `lab.health check-once --scheduled`. Match its executable and working directory to the canonical checkout. Keep the existing task account, logon type and settings. Use the native Windows Task Scheduler dialog for any password required to save its action.

The dashboard is served on [localhost port 8501](http://127.0.0.1:8501). If its child needs a source refresh, use `lab.health recover-dashboard` to request a recycle of the verified supervised child. Check the rendered paper account table after the supervisor starts its successor. The status reader copies its ledger inputs under a short read transaction and performs its projections after releasing that connection. Paper and KuCoin status receipts use immutable snapshots with legacy-file read compatibility. KuCoin research persistence and trailing inputs run outside the financial protection writer lease.

The paper status shows source readiness, account occupancy, protection incidents and prospective entry permission. `WAITING` and `NO_TRADE` are normal when a boundary is in the future or the unchanged strategy finds no setup. Prices used for execution and protection need the existing two-second freshness and workstation clock qualification. Kraken stream continuity and the individual delivery checkpoints provide separate evidence. KuCoin mark and Spot references are sampled and follow their source-specific cadence.

## Stop and initialize an authorized successor

The reset terminates a PAPER generation administratively. Its archive preserves all remaining positions, pending work, costs and uncertainty as recorded at retirement. Market data, model files and frozen specifications are reused by the new generation.

1. Identify the active generation, database, owner PID and creation time, launcher and watchdog. Fence any legacy or private deployment watchdog that could still relaunch the predecessor. Keep compatible market-data collection running.
2. Ask the exact generation to stop, then allow a bounded interval for its owner to exit. Verify process exit and `.runner.lock` availability. A predecessor that predates this command needs its original shutdown mechanism or a specifically authorized stop of the verified Windows process after relaunch is fenced.

   ```powershell
   & $python -X utf8 -m lab.paper_run stop --generation $oldGeneration
   ```

3. Save a fresh data-only Kraken reconnect qualification to a private JSON file. `kraken_live.measure(125, reconnect=True)` uses the existing bounded measurement helper. Run it independently from any open paper position. The reset command also checks the current KuCoin qualification and clock.

   ```powershell
   & $python -X utf8 -c "from pathlib import Path; from sys import argv; from lab.paper_run.kraken_live import measure; from lab.storage import write_json; write_json(Path(argv[1]), measure(125, reconnect=True))" $krakenMeasurement
   & $python -X utf8 -m lab.paper_run reset --generation $oldGeneration --archive-parent $privateArchiveParent --kraken-measurement $krakenMeasurement
   ```

4. Keep the same retired generation ID and archive parent on any interrupted reset retry. Once the preparation has been recorded, a retry can use the sealed qualification already saved for that successor. The raw retiring database and its applicable sidecars stay in the private archive. Examine duplicate copies of the raw database when SQLite recovery is needed.
5. Start the existing watchdog against the canonical checkout. A scheduled one-shot check with `--no-send` can verify dispatcher startup without sending test notifications. Verify a single owner, the new generation's heartbeat and source evidence, and all seven books at 10,000 paper units in their configured currencies with zero exposure and pending entries. Confirm the frozen EMA sources match their registered checksums and the status reader can run concurrently with advancing ledger commits.
6. While entries are paused, inspect the protection report. Activate the incident policy on the flat verified generation if needed, using the generation-bound request and preview in the [protection incident policy](PROTECTION_INCIDENT_POLICY.md). Record any actual current source restriction before entry release.
7. Preview and review the pause identity. Release that exact marker and observe a genuine eligible EMA Day evaluation at a normal future boundary.

   ```powershell
   & $python -X utf8 -m lab.paper_run entries on --generation $newGeneration
   & $python -X utf8 -m lab.paper_run entries on --generation $newGeneration --expected-pause-sha256 $reviewedPauseSha256 --approve
   ```

## Maintain an existing generation

Use `lab.paper_run entries off` to hold new entries during maintenance. Pending ideas follow the normal paused-cycle handling. Existing positions continue through their regular protection. Source changes for an existing exposed generation require a reviewed handoff that preserves its live protection and journal. Retain any fault, timing uncertainty and research exclusions that occurred during the handoff. Preserve the ledger before intervention and keep the newest legitimate economic events authoritative.

For an individually authorized supported administrative PAPER exit, preview `lab.paper_run operator-close --request $operatorCloseRequest`. The request identifies `generation`, `slot`, `book_id`, `position_id`, `request_id` and `reason`. Application uses `--apply --approve` and requires an exact matching entry pause and fresh post-request Kraken reference. Natural stop, target and time processing retain priority. Incomplete funding stays visible until genuine evidence settles it. Use the generation reset only for a specifically authorized administrative retirement.

See the [capability contract](STRATEGY_CAPABILITY_CONTRACT.md), [funding policy](FUNDING_RECONCILIATION_POLICY.md), [health runbook](HEALTH_MONITORING_RUNBOOK.md) and [KuCoin worker runbook](PHASE10_RUNBOOK.md) for subsystem details.
