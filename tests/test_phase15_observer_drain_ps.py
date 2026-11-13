import os
from pathlib import Path
import subprocess

import pytest


PROJECT = Path(__file__).resolve().parents[1]


HARNESS = r"""
$ErrorActionPreference = 'Stop'
if ($PSVersionTable.PSVersion.Major -ne 5 -or $PSVersionTable.PSVersion.Minor -ne 1) {
    throw 'Expected Windows PowerShell 5.1.'
}

$tokens = $null
$errors = $null
$tree = [System.Management.Automation.Language.Parser]::ParseFile(
    $env:ASEKE_DRAIN_TEST_SCRIPT, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'phase10.ps1 failed parsing.' }
$definition = $tree.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
    $node.Name -eq 'Complete-KuCoinRepairDrain'
}, $false)
if (-not $definition) { throw 'Missing Complete-KuCoinRepairDrain.' }
. ([scriptblock]::Create($definition.Extent.Text))

$case = $env:ASEKE_DRAIN_TEST_CASE
$name = 'Fixture KuCoin task'
$taskPath = '\'
$pythonPaths = @('C:\fixture\python.exe')
$runtime = [pscustomobject]@{
    pid = 6896
    created = 1794355486.635744
    stopped_at = if ($case -in 'orphan-retry','writer-lock-held','start-failed','observer-still-alive') {
        '2026-11-13T16:52:50+00:00'
    } else { $null }
    launch_id = '4f5a8831c8524f8d88423838e7829305'
    launcher = 'scheduled'
    drain_guard_version = if ($case -eq 'legacy-stalled-worker') { $null } else { 1 }
}
$worker = [pscustomobject]@{ProcessId = 6896; CreationDate = [datetime]'2026-11-10T19:04:46'}
$observer = [pscustomobject]@{ProcessId = 1916; ParentProcessId = 1912; CreationDate = [datetime]'2026-11-13T10:38:52'}
$wrapper = [pscustomobject]@{ProcessId = 1912; ParentProcessId = 6896; CreationDate = [datetime]'2026-11-13T10:37:56'}
$script:workerAlive = -not $runtime.stopped_at -and $case -notin 'force-retry','unproven-stopped'
$script:paused = -not $script:workerAlive
$script:forced = @()
$script:trace = @()
$script:started = 0
$script:blocked = $false
$script:lease = $false
$script:orphans = @($observer, $wrapper)
$script:flatChecks = 0
$script:repair = if ($case -eq 'force-retry') {
    [pscustomobject]@{state = 'BLOCKED'; launch_id = $runtime.launch_id}
} elseif ($case -eq 'already-restarted') {
    [pscustomobject]@{state = 'START_REQUESTED'; launch_id = '55555555555555555555555555555555'}
} else { $null }

function Assert-Phase15Flat {
    $script:flatChecks++
    $script:trace += 'flat'
    if ($case -in 'open-position','pending-idea' -and $script:paused) { throw "FIXTURE_BLOCKED_$case" }
}
function Read-LabTask {
    [pscustomobject]@{State = if ($script:workerAlive) { 'Running' } else { 'Ready' }; Settings = @{Priority=4}}
}
function Get-ScheduledTask { Read-LabTask }
function Wait-KuCoinDrain($Runtime) {
    if ($Runtime.launch_id -ne $runtime.launch_id -or -not $script:paused) { throw 'UNSAFE_DRAIN' }
    $script:trace += 'wait-drain'
    if ($case -eq 'graceful') {
        $script:workerAlive = $false
        $script:orphans = @()
        return $true
    }
    return (-not $script:workerAlive -and $case -ne 'writer-lock-held')
}
function Wait-KuCoinTaskReady {
    if ($script:workerAlive) { throw 'FIXTURE_TASK_NOT_READY' }
    Read-LabTask
}
function Start-ScheduledTask {
    param($TaskName, $TaskPath)
    if ($TaskName -ne $name -or $script:lease -or $script:workerAlive -or $script:orphans.Count -or $script:paused) {
        throw 'UNSAFE_START'
    }
    $script:started++
    $script:trace += 'start'
    if ($case -eq 'start-failed') { throw 'FIXTURE_START_FAILED' }
}
function Get-RepairProcess([int]$ProcessId) {
    if ($ProcessId -eq $worker.ProcessId) {
        if ($script:workerAlive) { $worker }
    } else { $script:orphans | Where-Object ProcessId -eq $ProcessId }
}
function Get-KuCoinRepairOwner($Runtime, $Paths) {
    if ($case -eq 'wrong-owner') { throw 'FIXTURE_OWNER_MISMATCH' }
    if (-not $script:workerAlive) { throw 'FIXTURE_OWNER_EXITED' }
    $worker
}
function Set-RepairProcessNormal($Expected) {
    if ($Expected.ProcessId -ne $worker.ProcessId) { throw 'UNSAFE_PRIORITY_TARGET' }
    $script:trace += 'normal'
}
function Get-LegacyObservers($Runtime, $Paths) {
    if ($case -eq 'reused-observer') { throw 'FIXTURE_PID_REUSED' }
    $script:orphans
}
function Assert-LegacyObserverLeaseProtocol($Runtime, $Observers) {
    if ($case -eq 'unproven-observer') { throw 'FIXTURE_OBSERVER_LEASE_PROTOCOL_UNPROVEN' }
}
function Get-VerifiedRepairProcess($Expected, [switch]$AllowExited) {
    Get-RepairProcess $Expected.ProcessId
}
function Invoke-RepairGuard($Action, $Reason) {
    $script:trace += $Action
    if ($Action -eq 'pause') { $script:paused = $true }
    if ($Action -eq 'status') {
        return [pscustomobject]@{
            worker_lock_held = ($script:workerAlive -or $case -eq 'writer-lock-held')
            paused = $script:paused
            repair = $script:repair
        }
    }
    if ($Action -eq 'resume') {
        if ($script:orphans.Count -or $script:workerAlive) { throw 'UNSAFE_RESUME' }
        $script:paused = $false
    }
    if ($Action -eq 'block') {
        if (-not $Reason) { throw 'MISSING_BLOCK_REASON' }
        $script:paused = $true
        $script:blocked = $true
    }
}
function Enter-RepairQuiescence([switch]$AllowWorker) {
    if ($case -eq 'journal-write') { throw 'FIXTURE_ACTIVE_JOURNAL' }
    if (($script:workerAlive -and -not $AllowWorker) -or $case -eq 'writer-lock-held') {
        throw 'FIXTURE_WRITER_LOCK_HELD'
    }
    $script:lease = $true
    $script:trace += 'lease'
    [pscustomobject]@{Fixture = $true; HasExited = $false}
}
function Exit-RepairQuiescence($Guard) {
    if (-not $Guard.Fixture) { throw 'BAD_GUARD' }
    $script:lease = $false
    $script:trace += 'release'
}
function Stop-RepairObserver($Expected) {
    if (-not $script:lease -or -not $script:paused -or -not $script:flatChecks) { throw 'UNSAFE_TERMINATE' }
    if (-not (Get-RepairProcess $Expected.ProcessId)) { return }
    $script:forced += $Expected.ProcessId
    if ($Expected.ProcessId -eq $worker.ProcessId) { $script:workerAlive = $false }
    if ($case -ne 'observer-still-alive') {
        $script:orphans = @($script:orphans | Where-Object ProcessId -ne $Expected.ProcessId)
    }
}
function Start-Sleep {}

$failure = $null
try {
    $null = Complete-KuCoinRepairDrain (Read-LabTask) $runtime $pythonPaths
} catch {
    $failure = $_.Exception.Message
}
switch ($case) {
    'graceful' {
        if ($failure -or $script:forced.Count -or $script:started -ne 1) { throw "Graceful drain failed: $failure" }
    }
    {$_ -in 'orphan-retry','force-retry'} {
        if ($failure -or ($script:forced -join ',') -ne '1916,1912' -or $script:started -ne 1) {
            throw "Paused orphan continuation failed: $failure"
        }
    }
    'already-restarted' {
        if ($failure -or $script:forced.Count -or $script:started -or $script:paused) {
            throw "Idempotent retry changed the replacement: $failure / $script:trace"
        }
    }
    'guarded-worker-force' {
        if ($failure -or (@($script:forced | Sort-Object) -join ',') -ne '1912,1916,6896' -or $script:started -ne 1) {
            throw "Guarded worker termination failed: $failure / $script:trace / $script:forced"
        }
    }
    'start-failed' {
        if (-not $failure -or $script:started -ne 1 -or -not $script:paused -or -not $script:blocked) {
            throw "Failed start did not persist its paused state: $failure / $script:trace"
        }
    }
    'observer-still-alive' {
        if (-not $failure -or -not $script:forced.Count -or $script:started -or -not $script:paused -or -not $script:blocked) {
            throw "Surviving observer did not prevent restart: $failure / $script:trace"
        }
    }
    default {
        if (-not $failure -or $script:forced.Count -or $script:started) {
            throw "Unsafe fixture did not block: $case / $failure / $script:trace"
        }
    }
}
if ($script:lease) { throw 'Fixture guard leaked.' }
Write-Output 'DRAIN_REPAIR_VERIFIED'
"""


LEASE_PROOF_HARNESS = r"""
$ErrorActionPreference = 'Stop'
$tokens = $null
$errors = $null
$tree = [System.Management.Automation.Language.Parser]::ParseFile(
    $env:ASEKE_DRAIN_TEST_SCRIPT, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'phase10.ps1 failed parsing.' }
$definition = $tree.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
    $node.Name -eq 'Assert-LegacyObserverLeaseProtocol'
}, $false)
if (-not $definition) { throw 'Missing Assert-LegacyObserverLeaseProtocol.' }
$functions = Join-Path $PSScriptRoot 'observer lease functions.ps1'
Set-Content -LiteralPath $functions -Value $definition.Extent.Text -Encoding UTF8
. $functions

$python = 'C:\fixture\venv\python.exe'
$runtime = [pscustomobject]@{launch_id = '4f5a8831c8524f8d88423838e7829305'}
$observers = @(
    [pscustomobject]@{ProcessId = 1912; ExecutablePath = $python; CreationDate = '2026-11-13T15:37:56Z'},
    [pscustomobject]@{ProcessId = 1916; ExecutablePath = 'C:\fixture\base\python.exe'; CreationDate = '2026-11-13T15:38:52Z'}
)
function Get-VerifiedRepairProcess($Expected, [switch]$AllowExited) { $Expected }
$mode = $env:ASEKE_DRAIN_TEST_CASE
$directory = Join-Path $PSScriptRoot 'runs\phase10'
$null = New-Item -ItemType Directory -Force -Path $directory
if ($mode -ne 'no-stage-proof') {
    $launch = if ($mode -eq 'wrong-launch') { 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' } else { $runtime.launch_id }
    $at = if ($mode -eq 'reused-pid') { '2026-11-13T15:37:01Z' } else { '2026-11-13T15:52:20Z' }
    $entry = '{{"at":"{0}","pid":1916,"launch":"{1}","stage":"ownership","state":"STARTED"}}' -f $at,$launch
    Set-Content -LiteralPath (Join-Path $directory 'observer.jsonl') -Value $entry -Encoding ASCII
}
if ($mode -eq 'current-worker') { $runtime | Add-Member NoteProperty drain_guard_version 1 }
$failure = $null
try { Assert-LegacyObserverLeaseProtocol $runtime $observers }
catch { $failure = $_.Exception.Message }
if ($mode -in 'proven-legacy','current-worker') {
    if ($failure) { throw "Unexpected block: $failure" }
} elseif ($failure -notlike '*Cannot prove*') {
    throw "Unverified observer escaped: $failure"
}
Write-Output 'OBSERVER_LEASE_VERIFIED'
"""


@pytest.mark.parametrize("case", [
    "graceful", "orphan-retry", "guarded-worker-force", "legacy-stalled-worker", "writer-lock-held",
    "journal-write", "open-position", "pending-idea", "wrong-owner", "reused-observer", "start-failed",
    "observer-still-alive", "force-retry", "unproven-stopped", "already-restarted", "unproven-observer",
])
def test_observer_drain_state_machine_in_windows_powershell_51(tmp_path, case):
    powershell = Path(os.environ["WINDIR"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    harness = tmp_path / "observer drain fixtures.ps1"
    harness.write_text(HARNESS, encoding="ascii")
    result = subprocess.run(
        [str(powershell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(harness)],
        env={**os.environ, "ASEKE_DRAIN_TEST_SCRIPT": str(PROJECT / "phase10.ps1"), "ASEKE_DRAIN_TEST_CASE": case},
        capture_output=True, text=True, timeout=55,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DRAIN_REPAIR_VERIFIED" in result.stdout


@pytest.mark.parametrize("case", ["proven-legacy", "current-worker", "no-stage-proof", "wrong-launch", "reused-pid"])
def test_legacy_observer_stage_proof_in_windows_powershell_51(tmp_path, case):
    powershell = Path(os.environ["WINDIR"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    harness = tmp_path / "observer lease fixture.ps1"
    harness.write_text(LEASE_PROOF_HARNESS, encoding="ascii")
    result = subprocess.run(
        [str(powershell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(harness)],
        env={**os.environ, "ASEKE_DRAIN_TEST_SCRIPT": str(PROJECT / "phase10.ps1"), "ASEKE_DRAIN_TEST_CASE": case},
        capture_output=True, text=True, timeout=55,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OBSERVER_LEASE_VERIFIED" in result.stdout
