$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw 'Project .venv Python is missing.' }
if (-not $args.Count) { throw 'Use .\phase10.ps1 status, or see PHASE10_RUNBOOK.md.' }
$action = $args[0]
if ($action -notin @('install', 'verify', 'start', 'stop', 'status')) {
    & $python -X utf8 -m lab.phase10 @args
    exit $LASTEXITCODE
}
if ($args.Count -gt 1 -and -not ($args.Count -eq 2 -and $action -eq 'install' -and $args[1] -eq '-Register')) {
    throw 'Only install accepts -Register.'
}

$name = 'ASEKE BTC Lab - KuCoin Monitoring'
$taskPath = '\'
$argument = '-X utf8 -m lab.phase10 worker --managed'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$isElevated = [Security.Principal.WindowsPrincipal]::new($identity).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)
$evidence = Join-Path $PSScriptRoot 'runs\phase10'
$xmlPath = Join-Path $evidence 'task.xml'

function Invoke-Lab {
    $out = & $python -X utf8 -m lab.phase10 @args
    if ($LASTEXITCODE -ne 0) { throw "Phase 10 exited with code $LASTEXITCODE." }
    $out
}

function Test-TaskDefault($Parent, [string]$Field, [string]$XmlValue, [string]$Effective, [string]$Expected) {
    if (-not $Parent -or $Effective -cne $Expected) { return $false }
    $nodes = $Parent.SelectNodes("*[local-name()='$Field']")
    return ($nodes.Count -eq 0 -or ($nodes.Count -eq 1 -and
        $nodes[0].NamespaceURI -ceq $Parent.NamespaceURI -and
        $nodes[0].InnerText -ceq $XmlValue -and -not $nodes[0].SelectSingleNode('*')))
}

function Read-LabTask {
    try { $task = Get-ScheduledTask -TaskPath $taskPath -TaskName $name -ErrorAction Stop }
    catch {
        if ($_.FullyQualifiedErrorId -like 'CmdletizationQuery_NotFound*' -and
            $_.CategoryInfo.Category -eq 'ObjectNotFound') { return }
        throw
    }
    $definition = ([xml](Export-ScheduledTask -TaskPath $taskPath -TaskName $name)).Task
    $exec = $definition.Actions.Exec
    $account = $definition.Principals.Principal
    $settings = $definition.Settings
    $checks = [ordered]@{
        'Actions.Count' = @($definition.Actions.ChildNodes).Count -eq 1
        'Actions.Exec.Command' = $exec.Command -eq $python
        'Actions.Exec.WorkingDirectory' = $exec.WorkingDirectory -eq $PSScriptRoot
        'Actions.Exec.Arguments' = $exec.Arguments -ceq $argument
        'Principals.Count' = @($definition.Principals.ChildNodes).Count -eq 1
        'Principal.UserId' = $account.UserId -in @($identity.User.Value, $identity.Name)
        'Principal.LogonType' = $account.LogonType -ceq 'Password' -and $task.Principal.LogonType -eq 'Password'
        'Principal.RunLevel' = (Test-TaskDefault $account 'RunLevel' 'LeastPrivilege' $task.Principal.RunLevel 'Limited')
        'Triggers.Count' = @($definition.Triggers.ChildNodes).Count -eq 1
        'Triggers.BootTrigger.Delay' = $definition.Triggers.BootTrigger.Delay -ceq 'PT1M'
        'Triggers.BootTrigger.Enabled' = $definition.Triggers.BootTrigger.Enabled -ne 'false'
        'Settings.MultipleInstancesPolicy' = (Test-TaskDefault $settings 'MultipleInstancesPolicy' 'IgnoreNew' $task.Settings.MultipleInstances 'IgnoreNew')
        'Settings.ExecutionTimeLimit' = ($settings.ExecutionTimeLimit -ceq 'PT0S' -and $task.Settings.ExecutionTimeLimit -ceq 'PT0S')
        'Settings.StartWhenAvailable' = $settings.StartWhenAvailable -eq 'true'
        'Settings.AllowStartOnDemand' = $settings.AllowStartOnDemand -ne 'false'
        'Settings.RunOnlyIfIdle' = $settings.RunOnlyIfIdle -ne 'true'
        'Settings.RunOnlyIfNetworkAvailable' = $settings.RunOnlyIfNetworkAvailable -ne 'true'
        'Settings.DisallowStartIfOnBatteries' = $settings.DisallowStartIfOnBatteries -eq 'false'
        'Settings.StopIfGoingOnBatteries' = $settings.StopIfGoingOnBatteries -eq 'false'
        'Settings.RestartOnFailure.Interval' = $settings.RestartOnFailure.Interval -ceq 'PT2M'
        'Settings.RestartOnFailure.Count' = $settings.RestartOnFailure.Count -ceq '3'
    }
    $failed = @($checks.Keys | Where-Object { -not $checks[$_] })
    if ($failed.Count) { throw "Registered KuCoin task failed validation for $($failed -join ', ')." }
    $task
}

function Test-Definition([string]$Xml) {
    $service = New-Object -ComObject 'Schedule.Service'
    $service.Connect()
    $definition = $service.NewTask(0)
    $definition.XmlText = $Xml
}

function Write-Definition {
    $null = New-Item -ItemType Directory -Force -Path $evidence
    $escape = [Security.SecurityElement]
    $command = $escape::Escape($python)
    $working = $escape::Escape($PSScriptRoot)
    $sid = $identity.User.Value
    $xml = @"
<?xml version="1.0"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>ASEKE BTC Lab public KuCoin monitoring and manual ideas</Description></RegistrationInfo>
  <Triggers><BootTrigger><Enabled>true</Enabled><Delay>PT1M</Delay></BootTrigger></Triggers>
  <Principals>
    <Principal id="Monitor"><UserId>$sid</UserId><LogonType>Password</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>false</Enabled>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure><Interval>PT2M</Interval><Count>3</Count></RestartOnFailure>
  </Settings>
  <Actions Context="Monitor">
    <Exec><Command>$command</Command><Arguments>$argument</Arguments><WorkingDirectory>$working</WorkingDirectory></Exec>
  </Actions>
</Task>
"@
    Test-Definition $xml
    [IO.File]::WriteAllText($xmlPath, $xml, [Text.UTF8Encoding]::new($false))
}

function Show-Status {
    $task = Read-LabTask
    $worker = Invoke-Lab status | ConvertFrom-Json
    $process = $null
    $owner = $null
    if ($worker.runtime -in @('RUNNING','STALE') -and $worker.identity.pid) {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId=$($worker.identity.pid)"
        if ($process) { $owner = Invoke-CimMethod -InputObject $process -MethodName GetOwner }
    }
    $owned = $task -and $task.State -eq 'Running' -and $worker.runtime -eq 'RUNNING' -and
        $worker.identity.launcher -eq 'scheduled' -and $process.SessionId -eq 0 -and $owner.ReturnValue -eq 0 -and
        "$($owner.Domain)\$($owner.User)" -eq $identity.Name -and
        $process.CommandLine -match '\s-X utf8 -m lab\.phase10 worker --managed\s*$'
    $cycle = $false
    if ($owned) {
        foreach ($profile in $worker.profiles.PSObject.Properties.Value) {
            if ($profile.last_cycle -and $profile.last_cycle -match '^[0-9a-f]{64}$') {
                $path = Join-Path $evidence "cycles\$($profile.last_cycle).json"
                if (Test-Path -LiteralPath $path) {
                    $row = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
                    $cycle = $cycle -or ($row.launcher -ceq 'scheduled' -and $row.launch_id -ceq $worker.identity.launch_id)
                }
            }
        }
    }
    $worker | Add-Member -NotePropertyName scheduler -NotePropertyValue ([ordered]@{
        task = "$taskPath$name"
        elevated = $isElevated
        definition = if (Test-Path -LiteralPath $xmlPath) { $xmlPath } else { $null }
        registration = if ($task) { 'validated' } else { 'not_registered' }
        task_state = if ($task) { [string]$task.State } else { $null }
        owner_query = if ($owner) { $owner.ReturnValue } else { $null }
        session = if ($process) { $process.SessionId } else { $null }
        task_worker_identity_verified = [bool]$owned
        task_managed_cycle_verified = [bool]$cycle
        boot_recovery_observed = $false
        signout_recovery_observed = $false
    })
    if ($Action -eq 'verify' -and $owned -and $cycle) {
        [IO.File]::WriteAllText((Join-Path $evidence 'managed-verification.json'),
            ($worker | ConvertTo-Json -Depth 25), [Text.UTF8Encoding]::new($false))
    }
    $worker | ConvertTo-Json -Depth 25
}

$operationLock = $null
if ($action -in @('install','start','stop')) {
    $state = Join-Path $PSScriptRoot 'data\phase10'
    $null = New-Item -ItemType Directory -Force -Path $state
    $operationLock = [IO.File]::Open((Join-Path $state '.scheduler-operation.lock'), 'OpenOrCreate', 'ReadWrite', 'None')
}
try {
switch ($action) {
    'install' {
        $registered = Read-LabTask
        Write-Definition
        if ($args.Count -eq 2 -and -not $registered) {
            if (-not $isElevated) { throw 'Open administrator Windows PowerShell for boot-task registration.' }
            if ([Console]::IsInputRedirected) { throw 'Open a local interactive terminal for the native Windows password prompt.' }
            & (Join-Path $env:SystemRoot 'System32\schtasks.exe') /Create /TN "$taskPath$name" /XML $xmlPath /RU $identity.Name /RP '*' /HRESULT
            if ($LASTEXITCODE -ne 0) { throw ('Registration failed (0x{0:X8}).' -f $LASTEXITCODE) }
            $registered = Read-LabTask
            if (-not $registered) { throw 'Registered KuCoin task did not validate.' }
        }
        Show-Status
    }
    'verify' {
        if (Test-Path -LiteralPath $xmlPath) { Test-Definition ([IO.File]::ReadAllText($xmlPath)) }
        Show-Status
    }
    'start' {
        $task = Read-LabTask
        if (-not $task) { throw 'Run .\phase10.ps1 install -Register in administrator PowerShell. Use start-local for a session worker.' }
        $worker = Invoke-Lab status | ConvertFrom-Json
        if ($task.State -eq 'Running' -and $worker.identity.launcher -eq 'scheduled' -and $worker.runtime -eq 'RUNNING') {
            Show-Status
            break
        }
        # Confirm scheduler write access before draining a working session process.
        $null = Enable-ScheduledTask -InputObject $task
        $wasLocal = $worker.identity.launcher -eq 'local' -and $worker.runtime -ne 'STOPPED'
        try {
            $null = Invoke-Lab stop-local
            $null = Invoke-Lab allow-managed
            Start-ScheduledTask -TaskPath $taskPath -TaskName $name
            $running = $false
            for ($i = 0; $i -lt 20; $i++) {
                $state = Invoke-Lab status | ConvertFrom-Json
                if ($state.runtime -eq 'RUNNING' -and $state.identity.launcher -eq 'scheduled') { $running = $true; break }
                Start-Sleep -Seconds 1
            }
            if (-not $running) { throw 'Task did not launch a healthy KuCoin worker.' }
        } catch {
            $message = $_.Exception.Message
            $null = Invoke-Lab stop-local
            if ($wasLocal) { $null = Invoke-Lab start-local }
            throw $message
        }
        Show-Status
    }
    'stop' { $null = Invoke-Lab stop-local; Show-Status }
    'status' { Show-Status }
}
} finally {
    if ($operationLock) { $operationLock.Dispose() }
}
