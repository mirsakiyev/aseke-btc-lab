[CmdletBinding()]
param(
    [Parameter(Position=0, Mandatory)]
    [ValidateSet('install','verify','start','stop','status','coverage','weekly','uninstall')]
    [string]$Action,
    [switch]$Register,
    [switch]$Json
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
$taskName = 'ASEKE BTC Lab - Forward Research'
$taskPath = '\'
$arguments = '-X utf8 -m lab.phase2 collect --shadow --managed'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
$isElevated = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$evidence = Join-Path $PSScriptRoot 'runs\phase7'
$xmlPath = Join-Path $evidence 'task.xml'
$priorTaskPath = Join-Path $evidence 'previous-task.xml'
$utf8 = [Text.UTF8Encoding]::new($false)
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw 'Project .venv Python is missing.' }
if ($Register -and $Action -ne 'install') { throw '-Register is only valid with install.' }

function Invoke-LabPython {
    $out = & $python -X utf8 @args
    if ($LASTEXITCODE -ne 0) { throw "Lab command failed with exit code $LASTEXITCODE. Check the collector log." }
    $out
}

function Save-Evidence($Name, $Record) {
    $null = New-Item -ItemType Directory -Force -Path $evidence
    $path = Join-Path $evidence $Name
    $tmp = Join-Path $evidence ('.' + [Guid]::NewGuid() + '.tmp')
    try {
        [IO.File]::WriteAllText($tmp, ($Record | ConvertTo-Json -Depth 35), $utf8)
        Move-Item -LiteralPath $tmp -Destination $path -Force
    } finally {
        if (Test-Path -LiteralPath $tmp) { Remove-Item -LiteralPath $tmp }
    }
}

function Test-TaskDefault($Parent, [string]$Field, [string]$XmlValue, [string]$Effective, [string]$Expected) {
    if (-not $Parent -or $Effective -cne $Expected) { return $false }
    $nodes = $Parent.SelectNodes("*[local-name()='$Field']")
    return ($nodes.Count -eq 0 -or ($nodes.Count -eq 1 -and
        $nodes[0].NamespaceURI -ceq $Parent.NamespaceURI -and
        $nodes[0].InnerText -ceq $XmlValue -and -not $nodes[0].SelectSingleNode('*')))
}

function Get-LabTaskKind($Task, $Definition, $Identity, $Executable, $WorkingDirectory, $Arguments) {
    $action = $definition.Actions.Exec
    $expectedPrincipal = $definition.Principals.Principal.UserId -in @($identity.User.Value, $identity.Name)
    $checks = [ordered]@{
        'Actions.Count' = @($definition.Actions.ChildNodes).Count -eq 1
        'Actions.Exec.Command' = $action.Command -eq $Executable
        'Actions.Exec.WorkingDirectory' = $action.WorkingDirectory -eq $WorkingDirectory
        'Principal.UserId' = $expectedPrincipal
        'Principal.RunLevel' = (Test-TaskDefault $definition.Principals.Principal 'RunLevel' 'LeastPrivilege' $Task.Principal.RunLevel 'Limited')
    }
    $kind = 'unattended'
    if ($action.Arguments -in @('-m lab.phase2 collect', '-m lab.phase2 collect --shadow') -and
        $definition.Principals.Principal.LogonType -eq 'InteractiveToken' -and
        $definition.Triggers.LogonTrigger) {
        $kind = 'legacy'
    } else {
        $settings = $definition.Settings
        $checks['Actions.Exec.Arguments'] = $action.Arguments -ceq $Arguments
        $checks['Principal.LogonType'] = $definition.Principals.Principal.LogonType -eq 'Password'
        $checks['Triggers.Count'] = @($definition.Triggers.ChildNodes).Count -eq 1
        $checks['Triggers.BootTrigger.Delay'] = $definition.Triggers.BootTrigger.Delay -eq 'PT1M'
        $checks['Triggers.BootTrigger.Enabled'] = $definition.Triggers.BootTrigger.Enabled -ne 'false'
        $checks['Settings.ExecutionTimeLimit'] = ($settings.ExecutionTimeLimit -ceq 'PT0S' -and
                                                 $Task.Settings.ExecutionTimeLimit -ceq 'PT0S')
        $checks['Settings.MultipleInstancesPolicy'] = (Test-TaskDefault $settings 'MultipleInstancesPolicy' 'IgnoreNew' $Task.Settings.MultipleInstances 'IgnoreNew')
        $checks['Settings.StartWhenAvailable'] = $settings.StartWhenAvailable -eq 'true'
        $checks['Settings.AllowStartOnDemand'] = $settings.AllowStartOnDemand -ne 'false'
        $checks['Settings.RunOnlyIfIdle'] = $settings.RunOnlyIfIdle -ne 'true'
        $checks['Settings.RunOnlyIfNetworkAvailable'] = $settings.RunOnlyIfNetworkAvailable -ne 'true'
        $checks['Settings.DisallowStartIfOnBatteries'] = $settings.DisallowStartIfOnBatteries -eq 'false'
        $checks['Settings.StopIfGoingOnBatteries'] = $settings.StopIfGoingOnBatteries -eq 'false'
        $checks['Settings.RestartOnFailure.Interval'] = $settings.RestartOnFailure.Interval -eq 'PT2M'
        $checks['Settings.RestartOnFailure.Count'] = $settings.RestartOnFailure.Count -eq '3'
    }
    $failed = @($checks.Keys | Where-Object { -not $checks[$_] })
    if ($failed.Count) {
        throw "Registered Phase 7 task failed validation for $($failed -join ', ')."
    }
    return $kind
}

function Read-LabTask {
    try {
        $task = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName -ErrorAction Stop
    } catch {
        if ($_.FullyQualifiedErrorId -like 'CmdletizationQuery_NotFound*' -and
            $_.CategoryInfo.Category -eq 'ObjectNotFound') { return }
        throw
    }
    # Export by registered name and path. Exporting the CIM object can reserialize the account and defaults.
    $xml = Export-ScheduledTask -TaskPath $taskPath -TaskName $taskName
    $definition = ([xml]$xml).Task
    $kind = Get-LabTaskKind $task $definition $identity $python $PSScriptRoot $arguments
    [pscustomobject]@{ Item = $task; Kind = $kind; Xml = $xml; Definition = $definition }
}

function Test-TaskXml([string]$Xml) {
    $scheduler = New-Object -ComObject 'Schedule.Service'
    $scheduler.Connect()
    $validation = $scheduler.NewTask(0)
    $validation.XmlText = $Xml
}

function Write-TaskDefinition {
    $null = New-Item -ItemType Directory -Force -Path $evidence
    $escape = [Security.SecurityElement]
    $command = $escape::Escape($python)
    $workDir = $escape::Escape($PSScriptRoot)
    $sid = $identity.User.Value
    $xml = @"
<?xml version="1.0"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>ASEKE BTC Lab public Coinbase collector and pinned experimental shadow</Description></RegistrationInfo>
  <Triggers><BootTrigger><Enabled>true</Enabled><Delay>PT1M</Delay></BootTrigger></Triggers>
  <Principals>
    <Principal id="Collector"><UserId>$sid</UserId><LogonType>Password</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal>
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
  <Actions Context="Collector">
    <Exec><Command>$command</Command><Arguments>$arguments</Arguments><WorkingDirectory>$workDir</WorkingDirectory></Exec>
  </Actions>
</Task>
"@
    Test-TaskXml $xml
    if (-not (Test-Path -LiteralPath $xmlPath) -or [IO.File]::ReadAllText($xmlPath) -cne $xml) {
        [IO.File]::WriteAllText($xmlPath, $xml, $utf8)
    }
}

function Get-WorkerHealth {
    Invoke-LabPython -m lab.phase7 health | ConvertFrom-Json
}

function Show-Status {
    $task = Read-LabTask
    $health = Get-WorkerHealth
    $prepared = Test-Path -LiteralPath $xmlPath -PathType Leaf
    if ($Action -eq 'verify' -and $prepared) {
        Test-TaskXml ([IO.File]::ReadAllText($xmlPath))
    }
    $summary = [ordered]@{
        task_name = $taskName
        task_path = $taskPath
        installer_elevated = $isElevated
        intended_account = $identity.Name
        prepared_xml = if ($prepared) { $xmlPath } else { $null }
        registration = if ($task) { $task.Kind } else { 'not_registered' }
        task_state = if ($task) { [string]$task.Item.State } else { $null }
        logon_mode = if ($task) { $task.Definition.Principals.Principal.LogonType } else { $null }
        execution_time_limit = if ($task) { $task.Definition.Settings.ExecutionTimeLimit } else { $null }
        task_info = if ($task) { $task.Item | Get-ScheduledTaskInfo |
            Select-Object LastRunTime, LastTaskResult, NumberOfMissedRuns, NextRunTime } else { $null }
        owned_by_task = ($task -and $task.Kind -eq 'unattended' -and $task.Item.State -eq 'Running' -and
                         $health.worker.launcher -eq 'scheduled' -and $health.process.expected_launch_identity -and
                         $health.process.owner -eq $identity.Name -and $health.worker_lock_held)
        worker_health = $health
    }
    if ($Json) { $summary | ConvertTo-Json -Depth 35; return }
    "Task $taskPath$taskName is $($summary.registration). State $($summary.task_state). Worker owned by task $($summary.owned_by_task)."
    if ($task) {
        "Account $($identity.Name), $($summary.logon_mode) logon, execution limit $($summary.execution_time_limit), last result $($summary.task_info.LastTaskResult)."
    } elseif ($prepared) {
        "Prepared definition $xmlPath. Effective settings await local registration and verification."
    }
    "Worker PID $($health.worker.pid), $($health.process_state), project lock held $($health.worker_lock_held), heartbeat $($health.heartbeat_state)."
    "Owner $($health.process.owner). Experiment $($health.experiment_id). Uptime seconds $($health.uptime_seconds)."
    foreach ($feed in $health.feeds.PSObject.Properties) {
        $v = $feed.Value
        "Feed $($feed.Name)s is $($v.progress). Scanned through $($v.scanned_through). Latest close $($v.last_observed_close). Successful REST batches under this worker $($v.successful_batches_since_worker_start). Next due $($v.next_collection_due_utc)."
    }
    "Forward store $($health.forward_bytes) / $($health.forward_budget_bytes) bytes. Log $($health.worker.log)."
}

function Stop-LabWorker($Task) {
    $null = Invoke-LabPython -m lab.phase2 stop
    if ($Task -and $Task.Item.Settings.Enabled) {
        $null = Disable-ScheduledTask -InputObject $Task.Item
    }
    for ($i = 0; $i -lt 10; $i++) {
        $current = Get-WorkerHealth
        if (-not $current.worker.process_alive -and -not $current.worker_lock_held) { return }
        Start-Sleep -Seconds 1
    }
    throw 'The persistent pause is set. Worker or lock release is still pending.'
}

function Start-LabWorker {
    $task = Read-LabTask
    if (-not $task) {
        $null = Invoke-LabPython -m lab.phase2 start --shadow
        Show-Status
        return
    }
    if ($task.Kind -ne 'unattended') { throw 'The legacy interactive task must be upgraded with install -Register.' }
    $old = Get-WorkerHealth
    if ($old.worker.process_alive -and (-not $old.process.expected_launch_identity -or
                                      $old.process.owner -ne $identity.Name -or -not $old.worker_lock_held)) {
        throw 'The current worker identity or lock could not be verified. No handoff was attempted.'
    }
    if ($old.worker.process_alive -and $old.worker.launcher -eq 'scheduled' -and -not $old.worker.intentionally_paused) {
        if ($task.Item.State -ne 'Running') { throw 'Scheduled worker and Task Scheduler state disagree. Inspect status.' }
        Show-Status
        return
    }
    $before = Invoke-LabPython -m lab.phase7 checkpoint | ConvertFrom-Json
    $started = [DateTimeOffset]::UtcNow
    $priorManual = $old.worker.process_alive -and $old.worker.launcher -ne 'scheduled'
    # Test scheduler write access before stopping the current collector. A
    # disabled task can be enabled again after the persistent pause is set.
    $null = Disable-ScheduledTask -InputObject $task.Item
    $task = Read-LabTask
    try {
        Stop-LabWorker $task
        $boundary = Invoke-LabPython -m lab.phase7 checkpoint | ConvertFrom-Json
        $null = Invoke-LabPython -m lab.phase2 allow-scheduled-start
        $null = Enable-ScheduledTask -InputObject $task.Item
        Start-ScheduledTask -TaskPath $taskPath -TaskName $taskName
        $ready = $false
        for ($i = 0; $i -lt 24; $i++) {
            $health = Get-WorkerHealth
            $running = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName
            $ready = ($running.State -eq 'Running' -and $health.worker.launcher -eq 'scheduled' -and
                      $health.process.expected_launch_identity -and $health.process.owner -eq $identity.Name -and
                      $health.worker_lock_held -and -not $health.worker.runner_stale -and
                      $health.worker.shadow.state -eq 'observing_experimentally' -and $health.heartbeat_state -eq 'healthy')
            if ($ready) { break }
            Start-Sleep -Seconds 1
        }
        if (-not $ready) { throw 'Scheduled worker identity, shadow, and live Coinbase heartbeat were not verified in the handoff window.' }
        $after = Invoke-LabPython -m lab.phase7 checkpoint --watermark $boundary.event_watermark | ConvertFrom-Json
        $sameExperiment = ($after.experiment | ConvertTo-Json -Compress -Depth 20) -eq
                          ($before.experiment | ConvertTo-Json -Compress -Depth 20)
        if (-not $sameExperiment -or $after.journal_prefix_sha256 -ne $boundary.journal_prefix_sha256) {
            throw 'Experiment or existing journal changed across handoff.'
        }
        Save-Evidence 'handoff.json' ([ordered]@{
            started_utc = $started; verified_utc = [DateTimeOffset]::UtcNow
            status = 'scheduled_launch_observed'; before_worker_pid = $old.worker.pid; after_worker_pid = $health.worker.pid
            before = $before; stopped_checkpoint = $boundary; after = $after
            account_state_unchanged_at_handoff = (($after.accounts | ConvertTo-Json -Compress -Depth 25) -eq
                                                   ($boundary.accounts | ConvertTo-Json -Compress -Depth 25))
            journal_prefix_preserved = $true; experiment_preserved = $true
            live_network_heartbeat_verified = $true; scheduled_feed_progress = $health.feeds
            last_scheduled_batches = $health.worker.last_batches
            rdp_disconnect_observed = $false; windows_signout_observed = $false; reboot_observed = $false
        })
    } catch {
        $failure = $_.Exception.Message
        Stop-LabWorker (Read-LabTask)
        if ($priorManual) { $null = Invoke-LabPython -m lab.phase2 start --shadow }
        Save-Evidence 'handoff-failure.json' @{ at = [DateTimeOffset]::UtcNow; reason = $failure; before = $before
                                              manual_launch_restored = $priorManual }
        throw "Scheduled handoff failed. The registered task is disabled. Prior manual launch restored $priorManual. $failure"
    }
    Show-Status
}

if ($Action -in @('coverage', 'weekly')) {
    if ($Json) { Invoke-LabPython -m lab.phase7 $Action --json } else { Invoke-LabPython -m lab.phase7 $Action }
    return
}
if ($Action -in @('verify', 'status')) { Show-Status; return }

$state = Join-Path $PSScriptRoot 'data\forward'
$null = New-Item -ItemType Directory -Force -Path $state
$operationLock = [IO.File]::Open((Join-Path $state '.supervisor.lock'), 'OpenOrCreate', 'ReadWrite', 'None')
try {
    switch ($Action) {
        'install' {
            $task = Read-LabTask
            if ($task -and $task.Kind -eq 'unattended') { Show-Status; break }
            Write-TaskDefinition
            if ($task -and -not (Test-Path -LiteralPath $priorTaskPath)) {
                [IO.File]::WriteAllText($priorTaskPath, ($task.Xml -replace 'encoding="UTF-16"', 'encoding="UTF-8"'), $utf8)
            }
            if (-not $Register) {
                "Task definition validated with Windows Task Scheduler and prepared at $xmlPath."
                'Registration requires an elevated local terminal and the account password in the native Windows prompt.'
                break
            }
            if (-not $isElevated) { throw 'Windows requires elevation to register the boot trigger. Use an elevated local terminal.' }
            if ([Console]::IsInputRedirected) { throw 'Open a local interactive terminal for the native password prompt.' }
            if ($task -and $task.Item.State -eq 'Running') { throw 'Gracefully stop the legacy scheduled instance before replacing its task.' }
            $registrationArgs = @('/Create','/TN',"$taskPath$taskName",'/XML',$xmlPath,'/RU',$identity.Name,'/RP','*','/HRESULT')
            if ($task) { $registrationArgs += '/F' }
            & (Join-Path $env:SystemRoot 'System32\schtasks.exe') @registrationArgs
            if ($LASTEXITCODE -ne 0) {
                throw ('Native task registration failed (0x{0:X8}). The existing collector was not stopped.' -f $LASTEXITCODE)
            }
            $registered = Read-LabTask
            if (-not $registered -or $registered.Kind -ne 'unattended') {
                throw 'The imported task did not verify as the expected unattended definition.'
            }
            if ($registered.Item.Settings.Enabled) {
                $null = Disable-ScheduledTask -InputObject $registered.Item
                if ((Read-LabTask).Item.Settings.Enabled) { throw 'The registered task could not be staged disabled.' }
            }
            Show-Status
        }
        'start' { Start-LabWorker }
        'stop' { Stop-LabWorker (Read-LabTask); Show-Status }
        'uninstall' {
            $task = Read-LabTask
            if ($task) {
                if ($task.Item.State -eq 'Running' -or (Get-WorkerHealth).worker.launcher -eq 'scheduled') {
                    Stop-LabWorker $task
                }
                Unregister-ScheduledTask -InputObject $task.Item -Confirm:$false
            }
            Show-Status
        }
    }
} finally {
    $operationLock.Dispose()
}
