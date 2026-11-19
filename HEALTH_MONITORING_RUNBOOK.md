# ASEKE BTC Lab operational monitoring

The watchdog reads existing local collector, scheduler, research and paper-accounting evidence. It checks dashboard health over loopback and uses Telegram HTTPS for delivery. The dashboard only reads the watchdog receipt. Both collectors keep their existing tasks and controls.

## Windows startup and handoff

From an administrator PowerShell signed in as the existing lab Windows account:

```powershell
# Run from the repository root.
.\health.ps1 install
.\health.ps1 task-status
.\health.ps1 dashboard-status
.\health.ps1 status
```

The installer prompts for the existing Windows password using `Get-Credential`. It registers only **ASEKE BTC Lab - Dashboard** and **ASEKE BTC Lab - Health Watchdog**. Both use the same account, limited privileges, password logon, the pinned `.venv`, the project working directory, and `IgnoreNew`. The dashboard starts at boot and supervises one Streamlit child. The watchdog starts at boot and repeats every three minutes. Each watchdog check has a two-minute Scheduler limit. Verify a fresh receipt with mode `scheduled`, then wait for a second scheduled receipt. Check the new task definitions and results:

```powershell
$names = 'ASEKE BTC Lab - Dashboard','ASEKE BTC Lab - Health Watchdog'
Get-ScheduledTask -TaskName $names -TaskPath '\' |
    Select-Object TaskName, State, Principal, Actions, Triggers
foreach ($name in $names) { Get-ScheduledTaskInfo -TaskName $name -TaskPath '\' }
```

The dashboard binds to [localhost port 8501](http://127.0.0.1:8501/). An existing listener stays in place. The supervisor waits for the port to become free when it cannot verify the listener or when the expected dashboard is already running. An identified conflicting listener produces an incident. Once the supervisor owns a Streamlit child, it makes up to three retries after consecutive short runs. The task also allows up to two Scheduler restarts of the supervisor. A child running at least five minutes resets the consecutive short-run count. The app log rotates under `runs/health/dashboard.log`.

On November 10, 2026, Windows denied unattended registration with `0x80070005` from the unelevated session. The detached dashboard supervisor and watchdog serve as a bridge until installation. Confirm an actual scheduled run and test reboot and sign-out recovery separately. To start or stop the detached bridge manually:

```powershell
.\health.ps1 dashboard-local
.\health.ps1 watchdog-local
.\health.ps1 stop-local
```

After installation, `.\health.ps1 start` starts the two registered tasks. `.\health.ps1 recover-dashboard` stops only a live listener whose executable, working directory, arguments, creation time and ancestry match the active lab supervisor. Use it to verify replacement after the dashboard is task-managed. A refusal means operator inspection is required. To roll back the installation, stop and unregister the two tasks named above from administrator PowerShell. The dashboard and watchdog have separate locks, and the watchdog state remains on disk.

## Inspect and control notifications

```powershell
.\health.ps1 status
.\health.ps1 alerts
.\health.ps1 check-once --no-send
.\health.ps1 check-once
.\health.ps1 test-telegram
.\health.ps1 mute on
.\health.ps1 mute off
.\health.ps1 category health on
.\health.ps1 category ideas on
.\health.ps1 category paper_entry on
.\health.ps1 category paper_no_entry on
.\health.ps1 category paper_exit on
.\health.ps1 category open_position off
.\health.ps1 position-interval day 30
.\health.ps1 category training_completion off
.\health.ps1 audit $boundaryUtc
```

Health, published ideas, paper entry, terminal unfilled ideas and paper exits are on by default. Training completion and open-position updates are opt-in. The initial update intervals are 30 minutes for Day, 60 for Swing and 240 for Position. Mute pauses automatic delivery while checks, incident tracking and the queue continue. An explicit test can run during mute and respects Telegram rate limits. The Streamlit **Operational health** view refreshes the latest check and alert history every 30 seconds. `runs/health/last.json` is the atomic check receipt and `runs/health/watchdog.sqlite3` holds durable incident, publication and lifecycle identities. Keep both during upgrades.

The installed watchdog loads updated Python source on its next scheduled run. Check `phase15.notifications` in that receipt to verify the lifecycle cursor. `check-once --no-send` still records local monitoring and queues eligible events. Use an isolated ledger and outbox for historical notification tests.

After the three EMA paper books are registered, scheduled checks also record `ema_paper` and `ema_paper_notifications`. EMA reads its independently authorized receipt handoff and queues only committed prospective lifecycle events under separate identities. [EMA paper runbook](EMA_PAPER_RUNBOOK.md)

The notifier reuses the existing DPAPI credentials and authorized destination. The watchdog can send while Codex is closed. Keep the credential file restricted to the Windows account running the watchdog, SYSTEM and Administrators. The token is used only to call the official HTTPS Bot API. [Telegram Bot API](https://core.telegram.org/bots/api)

## Current-generation operational alerts

`lab.health.fresh_paper_health` is a health-only reader. It discovers the sealed current registration and, when installed, the supported `membership.lanes(record, db=source)` amendment projection. It never changes paper state, supervises a runner, acquires the financial writer lease or sends Telegram. Installing this reader does not change financial `RUNNER_BUILD`.

Its separate durable per-book operational cursor follows committed protection and operational events independently from trade-notification cursors. First commissioning baselines historical events and immediately reports a currently blocked book. Later checks report changed reasons, distinct incidents, accepted protection recovery and actual prospective entry availability. A future-dated approved resume is labelled **FUTURE ADMISSION PENDING**. Source recovery does not imply entry restoration. An occupied ACTIVE book is not available for an overlapping entry; historical uncertainty may remain after a valid resume.

Positive eligibility notices require a drained incident/event backlog, a consistent current snapshot, current clock/source/control and qualification gates, a flat book with no pending entry, and a valid resume newer than any relevant historical restriction. A stale snapshot, revoked old permission or new incident cannot announce recovery. Shared-source events are matched by explicit source-scope identity. A newly registered flat consumer can inherit a current source restriction without a fictitious exposure incident. Missing or malformed registered-book evidence is isolated and reported; healthy books continue to be scanned.

Commission from the project root using operator mode:

```powershell
.\health.ps1 check-once --no-send
.\health.ps1 alerts
.\health.ps1 check-once
.\health.ps1 status
```

Do not add `--scheduled` to manual commissioning. Operator-mode checks do not dispatch financial-runner supervision. `--no-send` still writes health state, outbox rows and cursors; inspect the new `ema-private:fresh-health:` rows before delivery. The full receipt contains `fresh_paper_operational_health`, version `fresh-paper-operational-health-v1`, per-book cursors, backlog and snapshot-consistency evidence. `status.current.fresh_paper_operational_monitor` reports a missing, stale or inaccessible scanner receipt explicitly. Its 420-second stale threshold describes monitoring evidence and does not alter any trading deadline.

If no genuine current restriction exists, an explicitly authorized commissioning TEST can be queued through the same guarded private health route:

```powershell
# Use one unique request identity, and retain it for any interrupted retry.
.\health.ps1 test-operational-alert --request-id $operationalTestRequestId
.\health.ps1 alerts
.\health.ps1 check-once
```

This command queues only. It says **OPERATIONAL NOTIFICATION TEST** and **No real incident or trade occurred**. Repeating the same request does not duplicate the outbox row. It remains category `health`, so existing mute, category, cooldown, retry and ambiguity policies apply. Do not send another connectivity test when the authorization permits only one test. No production feed should be disconnected and no false protection incident should be created for commissioning.

Operational rows have no trade `idea_id`, so a failed older entry message cannot hold them behind its per-idea sequence. They use the existing private-route verification, shared `.check.lock` and single durable sender. The ordinary scheduled watchdog loads updated health source on its next run. Verify a receipt with mode `scheduled` and the new collector version while the financial runner's PID/build and open-position protection continue unchanged. Leave the watchdog schedule intact.

The registered cadence remains one full check every three minutes. Full-check work, the five-message batch limit, older eligible health messages, network calls and backoff add queue latency. The outbox and `outbox_attempt` retain enqueue, dispatch, request start/completion, API acceptance and Telegram message IDs. Report those measured timestamps; API acceptance does not prove phone-display time. A faster scanner/service requires its own coordinated release and is not activated by this reader.

On November 18, 2026, operator `--no-send` commissioning enqueued a genuine **EMA Day BLOCKED â€” FUTURE ADMISSION PENDING** notice at **18:26:14.652887 EST**. The ordinary scheduled watchdog loaded the new scanner at **18:26:28.731615 EST** and delivered that notice through the verified private route. Telegram accepted message **379** at **18:26:30.325631 EST**, **15.673 seconds** after enqueue. The request ran from **18:26:29.558822** to **18:26:30.325631 EST**, approximately **0.767 seconds**. The scheduled process completed successfully at **18:26:30.345701 EST**. The subsequent operator check completed at **18:26:38.832997 EST**; it was not the sender of that already-delivered message. No TEST was needed or sent. These receipts establish API acceptance and ordinary-schedule pickup, not phone-display time or a permanent latency guarantee. The financial runner remained PID **16784**, build `e0d54dea9782c6caaa56e330d3b2eb1b439afad2511c77a5b77b12bbabb8036b`, and both open Swing books retained their protection.

To roll back only this bounded release, restore the reviewed preceding health reader/integration source. Preserve `watchdog.sqlite3`, its operational cursors, existing outbox and delivery attempts. Do not reset notification history, change scheduler registration or restart a financial runner as part of a health-only rollback.

Each check attempts at most five messages. Definite connection and Telegram rejections retry with exponential backoff and honor Telegram's requested delay, up to six attempts by default. The watchdog saves the in-flight attempt before sending. If it loses the response or crashes after Telegram may have accepted the request, the next check marks that attempt `UNCERTAIN` and leaves it for review. Telegram and the local SQLite outbox have no shared exactly-once transaction. This workstation cannot notify while powered off, offline, or unable to run the watchdog. An independent off-host heartbeat service would be needed to detect a full host outage.

## Phase 15 paper lifecycle

Use `.phase15.ps1 status` for the current books and their recent saved events. Use `.health.ps1 alerts` for delivery history, or pass the full idea ID to see that idea's saved timestamps and each attempt:

```powershell
.phase15.ps1 status
.health.ps1 alerts
.\health.ps1 alerts --idea $ideaId
```

| Category | Authoritative source | Example |
| --- | --- | --- |
| `ideas` | Committed `PUBLISHED` | **EXPERIMENTAL IDEA** Â· Day SHORT Â· frozen entry range, stop, illustrative 2R target and entry expiry |
| `paper_entry` | Committed `HYPOTHETICAL_FILL` | **PAPER ENTRY CONFIRMED â€” DELAYED** Â· modeled UTC time, ledger receipt time and position size Â· fill-based target |
| `paper_no_entry` | Committed `EXPIRED` or `INVALIDATED` for an unfilled idea | **NO PAPER ENTRY** Â· EXPIRED Â· entry window elapsed |
| `paper_exit` | Committed `CLOSED` | **PAPER TRADE CLOSED â€” DELAYED** Â· exit reason, net P&L and updated equity |
| `open_position` | Current saved nonflat book and its last accounted price | **OPEN PAPER POSITION UPDATE** Â· last accounted mark time Â· stale or unverified at check Â· unrealized P&L and remaining holding time |

`ENTRY_OBSERVED` alone generates no entry alert. A fill suppresses a pending contradictory unfilled alert. Exit messages use the saved gross, fees, funding, liquidation costs and net. Adverse execution is already included in gross. When saved closing equity cannot be reconciled, the alert includes an accounting warning. Updated events add notification evidence to the normal ledger commit. The watchdog opens a read-only snapshot and does no accounting.

The first lifecycle scan saves a ledger row baseline and lists any positions already open. Those positions can still generate later close alerts or opt-in updates. The cursor follows committed SQLite insert order and the durable queue deduplicates each book, idea and transition. A delayed commit with an older market timestamp can still qualify. Entries precede closes in the queue for the same idea. A retry blocks later alerts for that idea until resolved or exhausted. If a later alert is sent while an earlier category is disabled, the earlier pending alert is superseded. Open-position updates are superseded when flat and carry the price source time and freshness. The original publication baseline remains separate.

Entry and exit alerts say **DELAYED** when ledger recording trails modeled occurrence by at least one minute. The saved ledger recording time precedes its transaction commit, and the exact commit instant is unavailable. The watchdog persists outbox creation, local request start, local completion, local observation of API success and the Telegram server's message date. The request-start line is included in the message. Older receipts show the historical check-start timestamp under `delivered` and have no per-attempt acceptance evidence. Telegram API success proves acceptance and provides a message ID. Phone delivery time is outside this evidence. [Telegram Bot API](https://core.telegram.org/bots/api)

Futures minutes arrive in 15-minute captures. A healthy watchdog adds up to one three-minute polling interval after ledger commit before sending, plus request time. The first two Day paper fills took about 13m 53s and 14m 56s from modeled entry to ledger record. Their exits took about 3m 01s and 11m 53s. The November 11 publication reached its watchdog check after about 90 seconds. These are measured examples. A faster ledger scan would only trim post-commit delay. Earlier genuine confirmation would require a separately designed source-observation path.

## Initial thresholds

| Signal | Warning | Critical |
| --- | ---: | ---: |
| Coinbase heartbeat | 45 s | 120 s |
| KuCoin heartbeat | 90 s | 240 s |
| Coinbase five-minute feed age | 20 min | 35 min |
| Coinbase and KuCoin Spot hourly feed age | 85 min | 135 min |
| KuCoin futures capture age | 30 min | 50 min |
| KuCoin missing trade or mark minutes in the recent hour | 3 | 12 |
| KuCoin receipt lag beyond the 15-minute capture window | 4 min | 12 min |
| KuCoin reported backoff | 8 min | 20 min |
| Managed eligibility check age | 35 min | 60 min |
| Available RAM | below 1,250 MiB | below 750 MiB |
| Free disk | below 2,048 MiB | below 1,024 MiB |

The defaults use three warning checks, two critical checks, two good checks for recovery, and a four-hour unresolved-incident reminder. Verified severe events such as a stopped worker or permanently blocked book can alert immediately. During the first three minutes after Windows boots, stopped-worker findings use the ordinary two-check threshold. Futures funding has a two-hour receipt grace. Ordinary BTC-USDT Spot requires trades only. Idea alerts start at watchdog commissioning and use saved Phase 15 `PUBLISHED` events with stable idea IDs, actual published prices and experimental classification.

To tune watchdog thresholds, edit the corresponding keys from `.\health.ps1 status` in `runs/health/controls.json`. Use an optional boolean `recurring_expected` if the intended recurring-learning setting changes. That setting describes what the watchdog should expect. Change learning itself through the existing learning controls. The 1,250 MiB memory and 2,048 MiB disk warnings match the research reserves. Alert thresholds have no effect on collection, risk policy, training admission, or order execution.


Operational alert commissioning on November 18 recorded genuine private Telegram messages 379 and 380. Message 379 was accepted 15.673 seconds after enqueue; the restored-eligibility message 380 took 181.845 seconds. These are enqueue-to-API-acceptance measurements, not phone-display times. The three-minute watchdog schedule remains unchanged. The latter notice missed same-check destination verification because the query compared its fractional due time to the integer check-start epoch. The installed health-only correction compares due time with the actual dispatch-time clock; tests/test_health_due_time.py verifies same-check routing without weakening mute, retry, backoff, uncertain-send or shared-lock rules. This fix removes that avoidable extra scan, not the normal polling delay. No additional operational TEST was sent.

 After the correction, the ordinary 19:02 EST scheduled check queued two genuine Swing funding-evidence restrictions at 00:02:28.934518 UTC. Telegram accepted messages 381 and 382 at 00:02:30.551437 and 00:02:31.323129 UTC, respectively: 1.617 and 2.389 seconds after enqueue in that same check. These were real current accounting restrictions, not operational TESTs. Detection still follows the unchanged three-minute schedule.


The November 18 Swing funding audit added a health-only legacy clearance observer. It first baselines the exact open position and unresolved costs, then requires new committed matching FUNDING receipts, the applied settlement markers, finite reconciled nominal accounts, a consistent snapshot and drained backlog. Its notice says FUNDING RESTRICTION CLEARED, position OPEN, entry OCCUPIED; it never treats fresh prices as financial evidence or announces entry restored for an occupied book. It is disabled for versioned funding projections, whose canonical transitions own the notification. The financial runner is not restarted by commissioning.
