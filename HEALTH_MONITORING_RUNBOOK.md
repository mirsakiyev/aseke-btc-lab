# ASEKE BTC Lab operational monitoring

The watchdog reads existing local collector, scheduler, research and paper-accounting evidence. It checks dashboard health over loopback and uses Telegram HTTPS for delivery. The dashboard only reads the watchdog receipt. Both collectors keep their existing tasks and controls.

## Windows startup and handoff

From an administrator PowerShell signed in as the existing lab Windows account:

```powershell
Set-Location -LiteralPath 'C:\Users\16477\Projects\aseke-btc-lab'
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
.\health.ps1 category training_completion off
.\health.ps1 audit 2026-11-10T02:00:00Z
```

Health and eligible published ideas are on by default. Training completion is opt-in. Mute pauses automatic delivery while checks, incident tracking and the queue continue. An explicit test can run during mute and respects Telegram rate limits. The Streamlit **Operational health** view refreshes the latest check and alert history every 30 seconds. `runs/health/last.json` is the atomic check receipt and `runs/health/watchdog.sqlite3` holds durable incident and publication IDs. Keep both during upgrades.

The notifier reuses the existing Codex user's DPAPI credentials and destination. The watchdog can send while Codex is closed. Keep the credential file restricted to the existing Windows account, SYSTEM and Administrators. The token is used only to call the official HTTPS Bot API. [Telegram Bot API](https://core.telegram.org/bots/api)

Each check attempts at most five messages. Definite connection and Telegram rejections retry with exponential backoff and honor Telegram's requested delay. A request that may have reached Telegram is marked `UNCERTAIN` and held for manual review to prevent duplicates. `DELIVERED` records Telegram API acceptance and its message ID. This workstation cannot notify while powered off, offline, or unable to run the watchdog. An independent off-host heartbeat service would be needed to detect a full host outage.

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
