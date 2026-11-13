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

$case = $env:ASEKE_CIM_TEST_CASE
if ($case -eq 'native-read-and-normal-priority') {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$PID"
    $owner = Invoke-CimMethod -InputObject $process -MethodName GetOwnerSid
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if ($process.ProcessId -ne $PID -or -not $process.CreationDate -or -not $process.ExecutablePath -or
            $owner.ReturnValue -ne 0 -or $owner.Sid -ne $sid) {
        throw 'Native CIM did not identify the fixture shell.'
    }
    $result = Invoke-CimMethod -InputObject $process -MethodName SetPriority -Arguments @{Priority = [int32]32}
    if ($result.ReturnValue -ne 0) { throw "Native SetPriority returned $($result.ReturnValue)." }
    Write-Output 'CIM_REPAIR_VERIFIED'
    exit 0
}

$tokens = $null
$errors = $null
$tree = [System.Management.Automation.Language.Parser]::ParseFile(
    $env:ASEKE_CIM_TEST_SCRIPT, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'phase10.ps1 failed parsing.' }
$definitions = foreach ($name in 'Get-RepairProcess', 'Get-VerifiedRepairProcess', 'Set-RepairProcessNormal',
        'Stop-RepairObserver', 'Get-KuCoinRepairOwner') {
    $definition = $tree.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $false)
    if (-not $definition) { throw "Missing function $name." }
    $definition.Extent.Text
}
$fixtureFunctions = Join-Path $PSScriptRoot 'process repair functions.ps1'
Set-Content -LiteralPath $fixtureFunctions -Value $definitions -Encoding UTF8
. $fixtureFunctions

$identity = [pscustomobject]@{User = [pscustomobject]@{Value = 'S-1-5-21-123'}}
$expectedFields = @{
    ProcessId = 3210; ParentProcessId = 4321; SessionId = 0
    CreationDate = [datetime]'2026-11-13T12:30:25'
    ExecutablePath = 'C:\fixture with spaces\python.exe'
    CommandLine = '"C:\fixture with spaces\python.exe" -X utf8 -m lab.phase10 worker --managed'
}
$expected = [pscustomobject]$expectedFields.Clone()
$runtime = [pscustomobject]@{
    pid = 3210; created = ([DateTimeOffset]$expected.CreationDate).ToUnixTimeMilliseconds() / 1000.0
    launcher = 'scheduled'; launch_id = '0123456789abcdef0123456789abcdef'
}
if ($case -eq 'runtime-reuse') { $runtime.created += 1 }
$runtimeDir = Join-Path $PSScriptRoot 'data\phase10'
$null = New-Item -ItemType Directory -Path $runtimeDir -Force
$launch = if ($case -eq 'runtime-changed') { 'fedcba9876543210fedcba9876543210' } else { $runtime.launch_id }
$timestamp = $runtime.created.ToString('R', [Globalization.CultureInfo]::InvariantCulture)
$runtimeJson = '{{"pid":3210,"created":{0},"launcher":"scheduled","launch_id":"{1}"}}' -f $timestamp,$launch
Set-Content -LiteralPath (Join-Path $runtimeDir 'runtime.json') -Value $runtimeJson -Encoding ASCII
$script:cimCalls = @()
$script:flatChecks = 0
$script:attempts = 0
$script:terminated = $false
$script:disposed = 0
$script:waits = 0

function Get-Process { throw 'LEGACY_PROCESS_CMDLET_CALLED' }
function Stop-Process { throw 'LEGACY_PROCESS_CMDLET_CALLED' }
function Get-ScheduledTask { throw 'SCHEDULER_ACCESS' }
function Set-ScheduledTask { throw 'SCHEDULER_ACCESS' }
function Start-ScheduledTask { throw 'SCHEDULER_ACCESS' }
function Open-RepairProcessHandle([int]$ProcessId) {
    if ($ProcessId -ne $expected.ProcessId) { throw 'UNEXPECTED_HANDLE_REQUEST' }
    if ($case -eq 'handle-denied') { throw 'HANDLE_DENIED' }
    $handle = [pscustomobject]@{ HasExited = $false }
    $handle | Add-Member -MemberType ScriptMethod -Name Dispose -Value { $script:disposed++ }
    $handle | Add-Member -MemberType ScriptMethod -Name WaitForExit -Value {
        param([int]$Timeout)
        $script:waits++
        if ($Timeout -ne 3000) { throw 'INVALID_WAIT_BUDGET' }
        $script:terminated -and $case -ne 'stop-still-running'
    }
    $handle
}
function Get-KuCoinTaskEngineId {
    if ($case -eq 'wrong-task-engine') { return 101 }
    $expected.ProcessId
}
function Assert-Phase15Flat {
    $script:flatChecks++
    if ($case -eq 'not-flat') { throw 'FIXTURE_PREFLIGHT_BLOCKED' }
}
function Get-CimInstance {
    [CmdletBinding()]
    param($ClassName, $Filter)
    if ($ClassName -ne 'Win32_Process' -or $Filter -ne 'ProcessId=3210') { throw 'UNEXPECTED_CIM_QUERY' }
    $script:attempts++
    if ($case -eq 'missing') { return }
    $fields = $expectedFields.Clone()
    switch ($case) {
        'pid-reuse' { $fields.CreationDate = $fields.CreationDate.AddSeconds(5) }
        'wrong-executable' { $fields.ExecutablePath = 'C:\different\python.exe' }
        'wrong-command' { $fields.CommandLine = 'python.exe -m another.command' }
        'wrong-parent' { $fields.ParentProcessId = 1000 }
        'wrong-session' { $fields.SessionId = 2 }
        'stop-reused-before-terminate' { $fields.CreationDate = $fields.CreationDate.AddSeconds(5) }
    }
    [pscustomobject]$fields
}
function Invoke-CimMethod {
    [CmdletBinding()]
    param($InputObject, $MethodName, $Arguments)
    $script:cimCalls += $MethodName
    if ($InputObject.ProcessId -ne $expected.ProcessId -or $InputObject.CreationDate -ne $expected.CreationDate) {
        throw 'UNVERIFIED_PROCESS_USED'
    }
    switch ($MethodName) {
        'GetOwnerSid' {
            $sid = if ($case -eq 'wrong-owner') { 'S-1-5-21-999' } else { $identity.User.Value }
            [pscustomobject]@{ReturnValue = 0; Sid = $sid}
        }
        'SetPriority' {
            if ($Arguments.Priority -ne 32 -or $script:flatChecks -lt 1) { throw 'UNSAFE_PRIORITY_MUTATION' }
            $rc = if ($case -eq 'priority-denied') { 2 } else { 0 }
            [pscustomobject]@{ReturnValue = $rc}
        }
        'Terminate' {
            if ($Arguments.Reason -ne 1) { throw 'INVALID_TERMINATION_REASON' }
            $rc = if ($case -eq 'terminate-denied') { 2 } else { 0 }
            $script:terminated = $rc -eq 0
            [pscustomobject]@{ReturnValue = $rc}
        }
        default { throw 'UNEXPECTED_CIM_MUTATION' }
    }
}

$failure = $null
try {
    if ($case.StartsWith('stop-') -or $case -eq 'terminate-denied') {
        Stop-RepairObserver $expected
    } elseif ($case -in 'verified-task-owner', 'runtime-reuse', 'runtime-changed', 'wrong-task-engine') {
        $owner = Get-KuCoinRepairOwner $runtime @($expected.ExecutablePath)
        if ($case -eq 'verified-task-owner' -and $owner.ProcessId -ne $expected.ProcessId) {
            throw 'Expected the verified task owner.'
        }
    } else {
        Set-RepairProcessNormal $expected
    }
} catch { $failure = $_.Exception.Message }

$priorities = @($script:cimCalls | Where-Object { $_ -eq 'SetPriority' }).Count
$terminations = @($script:cimCalls | Where-Object { $_ -eq 'Terminate' }).Count
switch ($case) {
    'normal' {
        if ($failure -or $priorities -ne 1 -or $terminations -ne 0 -or $script:disposed -ne 1) {
            throw "Normal priority failed: $failure"
        }
    }
    'verified-task-owner' {
        if ($failure -or $priorities -or $terminations) { throw "Owner validation failed: $failure" }
    }
    'stop-normal' {
        if ($failure -or $terminations -ne 1 -or $priorities -ne 0 -or $script:waits -ne 1 -or $script:disposed -ne 1) {
            throw "Bounded held-handle termination verification failed: $failure"
        }
    }
    'stop-still-running' {
        if (-not $failure -or $terminations -ne 1 -or $script:waits -ne 1 -or $script:disposed -ne 1) {
            throw 'A still-running process did not block safe cleanup.'
        }
    }
    'priority-denied' {
        if (-not $failure -or $priorities -ne 1 -or $terminations -ne 0) { throw 'Missing priority error.' }
    }
    'terminate-denied' {
        if (-not $failure -or $terminations -ne 1 -or $priorities -ne 0) { throw 'Missing termination error.' }
    }
    default {
        if (-not $failure -or $priorities -or $terminations) { throw "Unsafe fixture did not block: $failure" }
    }
}
Write-Output 'CIM_REPAIR_VERIFIED'
"""


@pytest.mark.parametrize("case", [
    "native-read-and-normal-priority", "normal", "not-flat", "missing", "pid-reuse", "wrong-executable", "wrong-command", "wrong-parent",
    "wrong-session", "wrong-owner", "priority-denied", "handle-denied", "stop-normal", "stop-reused-before-terminate",
    "stop-still-running", "terminate-denied", "verified-task-owner", "runtime-reuse", "runtime-changed", "wrong-task-engine",
])
def test_observer_repair_process_management_in_windows_powershell_51(tmp_path, case):
    powershell = Path(os.environ["WINDIR"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    harness = tmp_path / "observer CIM checks.ps1"
    harness.write_text(HARNESS, encoding="ascii")
    result = subprocess.run(
        [str(powershell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(harness)],
        env={**os.environ, "ASEKE_CIM_TEST_SCRIPT": str(PROJECT / "phase10.ps1"), "ASEKE_CIM_TEST_CASE": case},
        capture_output=True, text=True, timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CIM_REPAIR_VERIFIED" in result.stdout
