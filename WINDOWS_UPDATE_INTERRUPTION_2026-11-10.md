# ASEKE BTC Lab interruption on November 10, 2026

The two planned Windows restarts delayed collection and live analysis. Both collectors recovered after the second boot. KuCoin subsequently supplied every trade and mark minute spanning the interruption. All eight outstanding Phase 14 trade minutes had already been omitted from KuCoin responses on November 9. All 52 Phase 10 UNKNOWN decisions also predated the restarts.

Times in this report are Toronto EST (UTC−05:00) unless marked UTC. The Phase 14 evidence cutoff is November 10 at 21:00 EST, and the latest watchdog receipt inspected was at 21:05:33 EST.

## Timeline and root cause

| November 10 EST | Event and evidence |
| --- | --- |
| 18:46:05.161 | Last Phase 14 capture before the restart completed the quarter ending 18:45 under KuCoin PID 17476. |
| 18:55:43.112 | Coinbase PID 16132 recorded its last pre-restart five-minute REST receipt, batch 646. |
| 19:00:03.446 | System Event 1074, record 2650. Windows Update's `MoUsoCoreWorker.exe` requested a planned service-pack restart as SYSTEM, reason `0x80020010`. |
| 19:01:58.439 → 19:02:14.500 | First OS shutdown and subsequent boot, Kernel-General records 2666 and 2667. |
| 19:02:58–19:02:59 | The watchdog first observed both former worker PIDs without their writer locks. The dashboard log recorded a listener on port 8501 at 19:02:59.178. |
| 19:03:18.029 | System Event 1074, record 2793. `TrustedInstaller.exe` requested a planned OS-upgrade restart as SYSTEM, reason `0x80020003`. The dashboard began stopping at 19:03:18.225. |
| 19:03:20.029 → 19:03:35.500 | Second OS shutdown and subsequent boot, Kernel-General records 2805 and 2806. |
| 19:03:46 → 19:04:08.866 | The dashboard task launched. Supervisor PID 6468 started the Streamlit child tree and PID 7628 listened on 8501. The watchdog delivered four collector outage notices at 19:04:08, Telegram IDs 18–21. |
| 19:04:46 → 19:04:49 | Task Scheduler launched both collectors after their one-minute boot delay. Coinbase resumed as PID 7132, KuCoin as PID 6896. |
| 19:04:50.124 / 19:04:50.455 | Coinbase received the five-minute and hourly candles closing at 19:00, batches 647 and 648, with zero missing candles or revisions. The five-minute candle closing at 19:05 arrived at 19:05:45.781, batch 649. |
| 19:04:53.610–19:04:54.791 | Phase 10 recorded the 19:00 Day, Swing and Position boundaries as `DELAYED / NO_TRADE`. Day returned to `TIMELY / NO_TRADE` at the 19:15 boundary. |
| 19:05:00.167 / 19:05:00.730 | Phase 14 received all 15 trade and all 15 mark minutes for 18:45–19:00. The cycle finalized at 19:05:01.297 on attempt 1. |
| 19:05:58.884 | System Event 19, record 2894, recorded successful installation of security update KB5132061, build 26300.9974. |
| 19:08:28 | The watchdog delivered task and worker recovery notices, Telegram IDs 22–25, after confirming both writer locks and running task states. |
| 19:11:28 | The watchdog reported elevated trade and mark receipt lag, Telegram IDs 26–27. |
| 19:16:02.361 / 19:16:02.990 | Phase 14 received all 15 trade and all 15 mark minutes for 19:00–19:15, including the five source opens from 19:00 through 19:04. |
| 20:00:59.418 | The 19:00 futures funding settlement arrived within the registered two-hour funding grace. |
| 20:05:28 | The watchdog delivered recovery for both rolling receipt-lag findings, Telegram IDs 28–29. |
| 21:01:09.874 | A separate Phase 17.1 Position label check exited with a pandas `TypeError`. Telegram reported it at 21:02:28, ID 30. |

The OS log establishes the update-related restart sequence. Task Scheduler's Operational channel was disabled. Scheduler last-run times, worker logs and receipts, process creation times, and watchdog records establish recovery. The audit found a dashboard launch and watchdog check between the two restarts. Collector startup is evidenced after the second boot. The watchdog recorded the old collector runs as terminated (`267014`, `0x00041306`) while waiting for their new runs. [Task Scheduler result codes](https://learn.microsoft.com/en-us/windows/win32/taskschd/task-scheduler-error-and-success-constants)

## Phase 14 futures minutes and recovery

The reported 1,777/1,785 trade and 1,785/1,785 mark counts reconcile exactly at the 20:45 EST cutoff on November 10. At the 21:00 cutoff the counts were 1,792/1,800 trade and 1,800/1,800 mark, with zero duplicate rows and zero source or feature-bridge corrections.

| Missing trade minute open, EST | Same minute open, UTC | Quarter close, EST | Trade request IDs | Matching mark received, EST |
| --- | --- | --- | --- | --- |
| Nov 9 16:25 | Nov 9 21:25 | Nov 9 16:30 | 24, 26, 27 | Nov 9 16:30:55.292097 |
| Nov 9 17:24 | Nov 9 22:24 | Nov 9 17:30 | 35, 37, 38 | Nov 9 17:30:54.976638 |
| Nov 9 17:52 | Nov 9 22:52 | Nov 9 18:00 | 41, 44, 45 | Nov 9 18:01:00.631734 |
| Nov 9 19:42 | Nov 10 00:42 | Nov 9 19:45 | 59, 61, 62 | Nov 9 19:45:55.652221 |
| Nov 9 19:44 | Nov 10 00:44 | Nov 9 19:45 | 59, 61, 62 | Nov 9 19:45:55.652221 |
| Nov 9 21:14 | Nov 10 02:14 | Nov 9 21:15 | 75, 77, 78 | Nov 9 21:16:00.701656 |
| Nov 9 21:37 | Nov 10 02:37 | Nov 9 21:45 | 81, 83, 84 | Nov 9 21:45:51.804789 |
| Nov 9 23:56 | Nov 10 04:56 | Nov 10 00:00 | 103, 106, 107 | Nov 10 00:00:50.699181 |

All 21 listed trade requests returned HTTP 200. Re-parsing the checksum-verified raw responses gave the same omissions on all three attempts for each affected quarter. Six quarters contained 14/15 trade minutes and one contained 13/15. All seven ended `PARTIAL_FINAL` with complete mark data. KuCoin documents that kline data can be incomplete and omits intervals with no ticks. The receipts establish that the endpoint omitted the rows. They do not establish whether those minutes had no trades. [KuCoin Get Klines](https://www.kucoin.com/docs-new/v2/rest/ua/get-klines)

Requests 276–278 closed 18:45–19:00 on the first attempt under the new KuCoin launch, and requests 279–280 closed 19:00–19:15 on their first attempt. The restart caused a late receipt for the entire earlier quarter, 300.167 seconds after its boundary for trade and 300.730 seconds for mark. Each basis has three individual rows tagged `CATCH_UP`, based on minute-close age. All 15 rows on each basis missed the separate 120-second quarter-boundary allowance. The later quarter arrived within that allowance. The four settlements due before the audit cutoff were verified within funding grace.

The collector resumes from its latest finalized checkpoint with a 180-minute catch-up cap. It does not revisit the seven finalized partial windows. Genuine late native evidence can support the offline economic path under the frozen protocol at its actual receipt time, subject to qualification. It cannot make an earlier decision timely. This audit used the existing receipts and left the eight gaps in place.

## Consequences for features, execution and accounts

- **Phase 14 features.** The eight trade gaps invalidate seven 15-minute buckets and any enclosing 1-hour, 4-hour, or daily bucket that needs those minutes. Day's 15-minute rolling indicators and 1-hour/4-hour context, and Swing's 4-hour indicators and daily context, inherit missing buckets and restart their contiguous calculations. The frozen seed still reproduces commissioning readiness. A feature-only check over actual cohort data through 21:00 found both profiles already unready at 19:00 on November 10. The recovered 19:00 Day and Swing feature inputs also arrived beyond the two-minute gate. Mark coverage supplies execution evidence, while feature inputs still require native trades.
- **Phase 14 execution and validation.** The eight unpaired minutes leave entry prices, stops, targets, liquidation fills and account marks unverifiable wherever a hypothetical path depends on them. No candidate or control lane was scored. `PRIMARY_HORIZON_OPEN`, `economic_path_complete=false`, `scoring_availability=AFTER_QUALIFICATION`, and zero future evaluations were confirmed. The first technical review is November 16 at 15:00 EST. The frozen horizon ends February 7, 2027 at 15:00 EST. At close, complete native coverage, verified funding, compatible dated metadata and the correction checks must clear before any economic replay. The current gaps alone would fail qualification if still present then. No performance or account outcome can be assigned from this audit.
- **Phase 15 paper accounts.** All four books are flat, with complete accounting and no permanent block. The Day book's sole hypothetical position closed November 9 at 23:28, before the restart. No publication, fill, exit or unresolved accounting event occurred across the restart window. Native-minute capture delay could temporarily delay an exposed book's cursor, but the ledger shows no open exposure or pending idea at the interruption.
- **Coinbase shadow continuity.** The worker reopened experiment `923a3a6822ac93632c3cb0d9` and advanced the saved five-minute and hourly checkpoints. The journal shows the three 19:00 shadow decisions before their modeled opening, with no order intents, fills or exits during the inspected interruption window. All three accounts were flat with no pending intents at audit.

## Phase 10 UNKNOWN decisions

The ledger contains 52 records with `timing=UNKNOWN`, all on November 10 before the first Event 1074. Each is `DATA_NOT_READY` with the reason `Fresh timestamped venue quote unavailable` and with ready candle features. Each saved quote has an exchange timestamp, a local receipt, and a raw payload hash. The exchange timestamps were 0.005–0.238 seconds ahead of their corresponding local decision times. The strict Phase 9 causality gate used by Phase 10 rejects a quote timestamp later than the decision, even when the configured one-second quote-clock skew bound is satisfied. The saved records establish that ordering. They do not identify whether venue timestamp behavior, workstation clock offset, or both produced it.

| Phase 10 decision boundary, EST | Day UNKNOWN | Swing UNKNOWN | Position UNKNOWN |
| --- | ---: | ---: | ---: |
| Before 11:00 | 22 | 0 | 0 |
| 11:00–14:59 | 15 | 1 | 0 |
| 15:00–18:59 | 13 | 1 | 0 |
| Interruption and after collector recovery through 21:00 | 0 | 0 | 0 |

The earliest rejected decision was recorded at 04:32:05.624 and the latest at 18:47:05.593. The two Swing boundaries were 11:00 and 15:00. The 19:00 boundaries recorded after reboot were delayed decisions and distinct from this cumulative count. The audit applied the saved quote and timing rules as written.

## Scheduled services

| Scheduled service | Final observed state | Recovery evidence and configuration |
| --- | --- | --- |
| ASEKE BTC Lab - Forward Research | Running, PID 7132 in session 0, writer lock held, heartbeat current | Boot delay 1 minute, unlimited execution, `IgnoreNew`, three failure restarts 2 minutes apart, Password logon and Limited run level. REST checkpoints advanced under the original experiment. XML export SHA-256 `44FB65D8F4ECC8D404697000F807450E35A36CC7CA04E65DD3C7EDFC77E9821E` matches the pre-incident export. |
| ASEKE BTC Lab - KuCoin Monitoring | Running, PID 6896 in session 0, writer lock held, heartbeat current | Same collector recovery policy. A new managed launch owns the advanced Phase 14 checkpoint. The 20:56 elevated verification for this launch confirmed Windows account ownership and managed cycle identity. XML export SHA-256 `AA09E46080C007C42DA98B9EAC55B62A4894660B5C18B9BC0435E43ECA90AF8E` matches the pre-incident export. |
| ASEKE BTC Lab - Dashboard | Running, supervisor PID 6468 and HTTP listener PID 7628 in session 0 | Started on both observed boots. Boot trigger, unlimited execution, two failure restarts 2 minutes apart, Password logon, Limited run level and `IgnoreNew`. The scheduled watchdog verified the listener identity and HTTP health. |
| ASEKE BTC Lab - Health Watchdog | Ready between runs, last result 0, fresh `scheduled` receipt | Boot trigger and 3-minute repetition, 2-minute execution limit, Password logon, Limited run level and `IgnoreNew`. Boot-period incident rows and Telegram delivery IDs establish recovery independently of the dashboard. |
| Codex Telegram Notifications | Running, last start 20:54:48 | Separate Interactive logon task, with five one-minute failure restarts. The lab watchdog sent the boot alerts. |

Direct Windows owner lookup for the session-0 processes returned access denied in this audit shell. The recovery evidence above uses the registered actions, process creation times, locks and advancing receipts, plus the earlier elevated KuCoin verification for the same current launch.

## Operational correction and prevention

The first Phase 17.1 Position daily check had 24 Spot trade hours. It crashed because the seeded Spot label path first built an unused unseeded feature set, which had no complete weekly context and triggered a pandas index-type error. The seeded path now invokes its admitted feature builder directly. The failed run's exact saved input completed in a read-only reproduction with 24 trade hours, zero missing hours, zero setups and zero mature labels. A one-day regression test exercises the seeded label path. The full suite passed 147 tests. The next managed retry remains pending. The existing 30-minute retry interval makes it eligible after 21:31:02 EST, subject to the next managed check and resource admission. The existing failure alert remains open until the watchdog observes recovery. Recurring eligibility checks remain enabled.

Keep automatic security updates enabled. In **Settings → Windows Update → Advanced options**, choose manual **Active hours** that include the evening analysis window, for example 08:00–22:00 Toronto time, and enable restart-required notifications. When Windows offers **Schedule the restart**, choose a monitored maintenance window outside active hours, just after confirming the hourly and futures-quarter receipts. Leave time for a second boot. Active hours reduce automatic interruptions. Applicable update deadlines and an explicitly scheduled restart can still determine the restart time. The audit found no populated local Windows Update policy keys or readable explicit active-hours values. [Microsoft active hours](https://support.microsoft.com/en-us/windows/deployment/updates-lifecycle/keep-your-pc-up-to-date-with-active-hours) · [Microsoft restart policy and deadlines](https://learn.microsoft.com/en-us/windows/deployment/update/waas-restart)

After the maintenance window, check `.\phase7.ps1 status`, `.\phase10.ps1 verify`, `.\phase14.ps1 technical-review`, `.\health.ps1 status`, and `.\health.ps1 alerts`. Enable Task Scheduler history if more detailed launch and exit records are needed for future incidents. Check workstation time synchronization if future-stamped quotes recur. `W32Time` was stopped when queried, while the System log recorded a successful synchronization at 04:17:52. The clock evidence does not establish the cause of the earlier subsecond quote offset.

No collector restart or task change was needed. The Phase 14 protocol, Phase 15 books, Phase 16/17 records, and the sealed holdout were inspected in place. Approval is **No validated model** and execution is **MANUAL_OUTSIDE_LAB**.

## Local evidence

- Windows System events and the five Task Scheduler task exports.
- `logs/research-collector.log` and the Coinbase response and shadow-event ledgers.
- `runs/phase13/observations/9e8ac6a44015ee11bb4c2b81d614c64a17475b932777223c53f5973f33056aed/observations.sqlite3` and the request-linked `raw` files.
- `runs/phase10/decisions`, `runs/phase10/managed-verification.json`, the Phase 15 paper ledger, and `runs/phase17/pilot/pilot-check/position/20261111T000000/worker.log`.
- `runs/health/dashboard.log`, `runs/health/dashboard-supervisor.json`, `runs/health/last.json`, and `runs/health/watchdog.sqlite3`.
