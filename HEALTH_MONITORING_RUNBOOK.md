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
| `ideas` | Committed `PUBLISHED` | **EXPERIMENTAL IDEA** · Day SHORT · frozen entry range, stop, illustrative 2R target and entry expiry |
| `paper_entry` | Committed `HYPOTHETICAL_FILL` | **PAPER ENTRY CONFIRMED — DELAYED** · modeled UTC time, ledger receipt time and position size · fill-based target |
| `paper_no_entry` | Committed `EXPIRED` or `INVALIDATED` for an unfilled idea | **NO PAPER ENTRY** · EXPIRED · entry window elapsed |
| `paper_exit` | Committed `CLOSED` | **PAPER TRADE CLOSED — DELAYED** · exit reason, net P&L and updated equity |
| `open_position` | Current saved nonflat book and its last accounted price | **OPEN PAPER POSITION UPDATE** · last accounted mark time · stale or unverified at check · unrealized P&L and remaining holding time |

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
