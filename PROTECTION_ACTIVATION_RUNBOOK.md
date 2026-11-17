# Protection audit and operator deployment procedure

This procedure prepares generation `caf235480b6362124546d17c9a77719572bc551c13b17da82d89374a007438e9` for a separately approved deployment. The audit did not change production controls, deploy code, activate policy, apply adjudications or resume publication. The affected EMA Day position remains protected by the existing runner. Its recorded deadline is November 17 at 19:00:00.274724 EST. A deadline is not evidence of a completed exit.

## Evidence package

Private package root is `C:/Users/16477/AppData/Local/ASEKE-BTC-Lab/protection-audit/activation-preparation-20261117T181600Z`. The `prepare_requests.py` helper creates a fresh request directory and previews every outstanding known incident using the actual local command implementation. It never applies a request. An unfamiliar incident is reported as requiring new evidence. Already adjudicated incidents are skipped.

The original two incidents use `ema-day-chart-audit-20261117T172713840614Z.json`, SHA256 `84ad5610a2c6733e7720c42b8e79bd5aefe62d28d2df1fa45c23e6b59663af6f`.

Incident 3 uses `incident3-20261117T181312537322Z/native-candle-audit.json`, SHA256 `23418e0ade213ee604f850bd6bee5fff122a0e42c5057863738b2e83f37e7453`. Its last accepted receipt is exactly `17:51:41.168994 UTC`. A valid protector had resumed by `17:51:54.020419 UTC`, inferred from repeated rejection transitions. The exact first recovery trade remains unknown. Subscription acknowledgement at `17:51:46.570846 UTC` provides connection evidence only.

The third audit contains three complete native Kraken BTC/USD minute bars covering `[17:50,17:53) UTC`. Boundary minutes encompass the configured 250 ms clock margin. Raw response hashes, authorized SQLite receipts and final candle observations reconcile. Historical worst-case UTC uncertainty is not certified by the local clock labels. The incident minute ranges from 63,822.20 to 63,871.10, within the frozen stop 65,541.76 and target 63,316.32. Liquidation is modeled at approximately 84,062.5984. No financing or stop change was effective during the interval. The supported disposition is `NO_EXIT_THRESHOLD_CROSSING_SUPPORTED` with `historical_coverage=UNRESOLVED`.

The consistent baseline backup is `activation-preservation-20261117T181233792452Z/paper.sqlite3` under the same private audit tree. Consult that folder's manifest for the exact backup filename, SHA256 and logical journal watermark 124. Compare only preserved prefixes when production appends legitimate events. Use fresh snapshots before the approved handoff. Do not restore a baseline backup over later economic events.

## Commands safe during preparation

Run from the active Lab project with its interpreter.

```powershell
Set-Location 'C:/Users/16477/Projects/aseke-btc-lab'
$generation = 'caf235480b6362124546d17c9a77719572bc551c13b17da82d89374a007438e9'
$auditRoot = Join-Path $env:LOCALAPPDATA 'ASEKE-BTC-Lab/protection-audit'
$packageRoot = Join-Path $auditRoot 'activation-preparation-20261117T181600Z'
& .\.venv\Scripts\python.exe -m lab.paper_run status
& .\.venv\Scripts\python.exe -m lab.paper_run protection report --slot ema/day
& .\.venv\Scripts\python.exe (Join-Path $packageRoot 'prepare_requests.py') --output $packageRoot
& .\.venv\Scripts\python.exe -m lab.paper_run entries on --generation $generation
```

The final command is a read-only preview. It does not remove a pause or create an authorization. The request generator prints the new request directory. Set `$requests` to that exact path after inspecting its manifest. A candidate adjudication may preview successfully before policy activation. Activation and resume drafts currently fail because exposure is open. Their later prerequisites must be rechecked.

```powershell
$requests = 'COPY_THE_FRESH_REQUEST_DIRECTORY_PRINTED_BY_THE_HELPER'
$incident = '9eb0de8803fe9641b932797e54f75ff8f77bf6c073b55817339acf5d54a219d1'
& .\.venv\Scripts\python.exe -m lab.paper_run protection adjudicate --request (Join-Path $requests "adjudicate-$incident.json")
```

Review tokens are bound to account state and the incident projection. Regenerate after a close, new incident, activation/import, applied adjudication or other material account change. Each applied adjudication changes the token. Regenerate the remaining requests before considering the next one. Request identity binds the operation and request content, so retain an already applied file for idempotent retry instead of editing it.

## Ordered procedure requiring separate operator approval

1. Capture a fresh status, protection report, source continuity and complete source-file manifest. Pin the staged full runner build and the separate `lab/paper_run/__main__.py` SHA256. Its CLI code is outside the runner fingerprint list. Discover current process identities from the fresh managed heartbeat and launch record.
2. Approve a temporary global entry pause if a deployment window is desired. This prevents another book opening between checks. The existing pause also cancels pending ideas through the normal cycle, so approve that effect explicitly. It leaves open-position protection active. Do not force an exit to obtain flatness.
3. Wait for genuine `EXIT_OBSERVED` and `CLOSED` events and complete cost reconciliation. Recheck all seven books, including no open or pending state, finite reconciled balances, per-position fill/close consistency and no unknown or deferred economic outcome. Historical uncertainty and provisional research status are permitted. They are not financial reconciliation failures.
4. Hold the existing launch and writer leases for the fresh paused preflight. Make a consistent SQLite online backup and hash manifest there. The prepared guarded-handoff helper performs the same safety checks, then invokes the existing `reload.if_needed` identity verification. It discovers the live PID and repeats flatness under the writer lease. This session cannot inspect the Session 0 runner owner, cwd or command line, so the actual handoff needs an authorized same-account administrator. Never substitute a raw `Stop-Process` command.
5. If the guarded helper reports `RUNNER_RELOADED`, use the existing registered Health Watchdog to launch the replacement in Session 0. Do not start an interactive second runner. Verify one logical runner owner, held runner lease, fresh matching-generation heartbeat, exact reviewed build, current chart and trade collection, source permissions, clock qualification, fresh advancing trade IDs and replay protection. Stop the procedure if any check fails.
6. Regenerate the activation request and preview it. Activation requires all seven books flat, no pending ideas, finite nominal reconciliation and the reviewed managed build. Apply only the individually approved activation request. Activation imports legacy incident records for the selected slot exactly once, preserving original gap events and unknown recovery values.
7. Regenerate the incident report. Confirm every legacy event is represented once, then regenerate adjudication candidates. Preview and review each request separately. Apply one approved adjudication, verify its journal event, then regenerate before the next. An additional incident requires additional evidence. A healthy subscription does not supply historical coverage proof.
8. When every incident has a disposition, regenerate and preview prospective resume. It requires a flat affected book, no pending entry or active protection obligation, complete nominal reconciliation, a qualified source and clock, streaming transport and a fresh qualifying reference. A stale quote may make this command fail safely. Obtain a new report and retry a preview after fresh evidence arrives.
9. Apply only the explicitly approved resume. Verify `BOOK_PROSPECTIVE_RESUME`, its operator, authorization time, capital dependency and strictly future `effective_at`. The historical problem and provisional close remain in the journal. Validated statistics continue excluding affected and dependent results.
10. Separately approve release of the global entry pause. A prospective resume does not remove the pause marker. Verify the generation-specific entry-control authorization and marker removal. At the recorded future boundary, verify a new eligible decision under unchanged strategy windows. If approval or unpause occurs after that effective boundary, observe the next normal future boundary. Never replay a missed signal or alter study dates.

## Approval-only command forms

The following commands are reference forms for the separately approved procedure. None was executed during this audit.

```powershell
& .\.venv\Scripts\python.exe -m lab.paper_run entries off

# After approved pause, actual flatness and fresh backup, pin these from the reviewed manifest.
$deploymentManifest = Get-Content -Raw (Join-Path $packageRoot 'final-review.json') | ConvertFrom-Json -ErrorAction Stop
$reviewedBuild = $deploymentManifest.staged_build
$reviewedCliSha = $deploymentManifest.operator_cli_sha256
$handoffHelper = Join-Path $auditRoot 'deployment-helper-20261117T181648785Z/guarded_handoff.py'
& .\.venv\Scripts\python.exe $handoffHelper --expected-generation $generation --expected-build $reviewedBuild --expected-cli-sha $reviewedCliSha

# Only after the helper's checks pass and the operator approves the handoff.
$handoffText = (& .\.venv\Scripts\python.exe $handoffHelper --expected-generation $generation --expected-build $reviewedBuild --expected-cli-sha $reviewedCliSha --handoff) -join "`n"
if ($LASTEXITCODE -ne 0) { throw 'The guarded handoff was blocked' }
$handoff = $handoffText | ConvertFrom-Json -ErrorAction Stop
if ($handoff.state -eq 'RUNNER_RELOADED') {
    Start-ScheduledTask -TaskName 'ASEKE BTC Lab - Health Watchdog'
} elseif ($handoff.state -ne 'RUNNER_ALREADY_ADOPTED') {
    throw 'Inspect the unexpected handoff result before proceeding'
}

# Recreate requests and recheck between every applied operation.
& .\.venv\Scripts\python.exe -m lab.paper_run protection activate --request (Join-Path $requests 'activate-draft.json')
& .\.venv\Scripts\python.exe -m lab.paper_run protection activate --request (Join-Path $requests 'activate-draft.json') --apply --approve
& .\.venv\Scripts\python.exe -m lab.paper_run protection adjudicate --request (Join-Path $requests "adjudicate-$incident.json")
& .\.venv\Scripts\python.exe -m lab.paper_run protection adjudicate --request (Join-Path $requests "adjudicate-$incident.json") --apply --approve
& .\.venv\Scripts\python.exe -m lab.paper_run protection resume --request (Join-Path $requests 'resume-draft.json')
& .\.venv\Scripts\python.exe -m lab.paper_run protection resume --request (Join-Path $requests 'resume-draft.json') --apply --approve
& .\.venv\Scripts\python.exe -m lab.paper_run entries on --generation $generation
& .\.venv\Scripts\python.exe -m lab.paper_run entries on --generation $generation --approve
```

The helper is a safety wrapper around the existing guarded reload. It does not create a new runner. Its SHA256 is `3913363f2eeccec28bb93e8449a1fca670229618cd50bfa8b709d060230cd943`. Its explicit handoff mode creates a fresh consistent private backup under both leases before retirement. Check its parsed result before requesting the watchdog. A matching already-adopted build needs no second retirement.

## Corrections found during preparation

The prior CLI exposed `entries off` without a supported reverse. The staged CLI now supplies generation-checked `entries on`, preview by default and explicit approval for release. It records and fsyncs a separate immutable control authorization before removing the exact matching marker. It never invokes the runner supervisor. Production controls were not changed.

The prior closed-position reconstruction rejected all financing or stop/tier events, even those after the reviewed interval. A natural financing settlement near this position's deadline could therefore make later adjudication impossible. The staged reviewer now evaluates static-model changes against the reviewed half-open interval. Changes before its end still require a segmented favorable review. A post-interval change does not invalidate the prior incident's static model. An explicit insufficiency disposition does not require a favorable static-model claim.

## Interruption and rollback

Keep the entry pause during any unresolved deployment interruption while protection continues. If the old owner cannot be verified or termination is denied, leave it running. If it has exited, the registered supervisor may launch one replacement through its launch/runner leases. Recheck the current owner before retrying a handoff. A busy or changed owner fails safely.

Incident application is transactional. If interrupted, inspect the journal by request ID. Retry the identical request when no material state changed, or regenerate against the new token. A committed import or adjudication is retained. An entry-release interruption after its audit write leaves either the original marker or a completed release. Read the control and authorization before retrying.

There is no policy-deactivation or analytical-promotion command. Before activation, a separately reviewed flat handoff can return to compatible prior code while entries remain paused. After activation or prospective resume, use an approved entry pause and compatibility review before any binary rollback. Older reporting could include provisional results, so preserve research quarantine. Never restore stale balances, delete incident events, reset the generation or launch a duplicate writer.

## Authorized administrative paper exit

An explicit operator authorization can target one generation, slot, book and position for an early paper exit. It does not permit a strategy-trigger substitution, a different position, or an exchange order. The preview-first command is:

```powershell
& .\.venv\Scripts\python.exe -m lab.paper_run operator-close --request $operatorCloseRequest
# Only for the explicitly authorized exact target, after tests and consistent backup:
& .\.venv\Scripts\python.exe -m lab.paper_run operator-close --request $operatorCloseRequest --apply --approve
```

The JSON request identifies `generation`, `slot`, `book_id`, `position_id`, `request_id` and the operator's `reason`. The action uses the existing writer lease and reloads current state in each transaction. A matching global maintenance pause is required for application. It records a durable request before waiting boundedly for a new actual Kraken WebSocket observation. It does not subscribe again or request an additional HTTP price. The observed event must follow the request timestamp and sequence watermark, pass the existing source, clock, monotonic and two-second checks, and match or advance the current accepted protection reference. Equality is permitted only for the exact fully matching new reference already accepted by normal protection; it cannot use a prior request-time mark.

The normal financing and close-accounting functions record the remaining quantity, fees and any due financing. `OPERATOR_REQUESTED_PAPER_EXIT` remains distinct from a natural stop, target or time exit. The request and exit are idempotent, including interruption and a racing natural exit. A final evidence check precedes transaction commit; failure rolls back the candidate exit. The writer lease is released between acquisition attempts so normal protection continues.

An administrative outcome carries `operator_intervened=true`, preserves existing uncertainty, and is excluded from validated strategy results along with dependent outcomes. An observed exit with missing financing remains an open accounting obligation until genuine evidence completes it. Do not invoke the all-flat handoff until actual terminal events and financial reconciliation pass.

For the November 17 explicitly authorized operation, do not keep unrelated research paused while waiting indefinitely for evidence, process elevation, or another book's exposure to disappear. If the bounded operation or guarded handoff cannot proceed and the existing protective owner is healthy, use the supported generation-specific `entries on` preview and approved release, then report the unfinished stage and final control state. This operator instruction supersedes retaining the deployment pause indefinitely. Never release entries into an unverified owner transition.
