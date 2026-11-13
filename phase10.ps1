$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw 'Project .venv Python is missing.' }
if (-not $args.Count) { throw 'Use .\phase10.ps1 status, or see PHASE10_RUNBOOK.md.' }
$action = $args[0]
if ($action -notin @('install', 'verify', 'start', 'stop', 'status', 'repair-observer')) {
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
    <Priority>4</Priority>
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

function Assert-Phase15Flat {
    $preflight = Join-Path $PSScriptRoot 'lab\phase15\restart_preflight.py'
    & $python -X utf8 $preflight
    if ($LASTEXITCODE -ne 0) { throw 'Could not confirm four flat Phase 15 accounts.' }
}

function Get-RepairProcess([int]$ProcessId) {
    if ($ProcessId -le 0) { throw 'Invalid process ID in the observer repair.' }
    $rows = @(Get-CimInstance -ClassName Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop)
    if ($rows.Count -gt 1 -or ($rows.Count -eq 1 -and $rows[0].ProcessId -ne $ProcessId)) {
        throw "Conflicting CIM evidence for process $ProcessId."
    }
    if ($rows.Count -eq 1) { $rows[0] }
}

function Get-VerifiedRepairProcess($Expected, [switch]$AllowExited) {
    $current = Get-RepairProcess $Expected.ProcessId
    if (-not $current -and $AllowExited) { return }
    if (-not $current -or -not $Expected.CreationDate -or -not $Expected.ExecutablePath -or
            -not $Expected.CommandLine -or $null -eq $Expected.SessionId -or $null -eq $Expected.ParentProcessId -or
            $current.CreationDate -ne $Expected.CreationDate -or
            $current.ExecutablePath -ne $Expected.ExecutablePath -or
            $current.CommandLine -cne $Expected.CommandLine -or
            $current.SessionId -ne $Expected.SessionId -or $current.ParentProcessId -ne $Expected.ParentProcessId) {
        throw "Process $($Expected.ProcessId) disappeared or its identity changed."
    }
    $sid = Invoke-CimMethod -InputObject $current -MethodName GetOwnerSid -ErrorAction Stop
    if ($null -eq $sid.ReturnValue -or $sid.ReturnValue -ne 0 -or $sid.Sid -ne $identity.User.Value) {
        throw "Process $($Expected.ProcessId) is not verified as owned by the task account."
    }
    $current
}

function Open-RepairProcessHandle([int]$ProcessId) {
    $process = [Diagnostics.Process]::GetProcessById($ProcessId)
    try {
        # An open handle prevents Windows from recycling this PID between
        # the identity check and the CIM method, even if the process exits.
        if ($process.SafeHandle.IsInvalid) { throw "Cannot hold a handle to process $ProcessId." }
        $process
    } catch {
        $process.Dispose()
        throw
    }
}

function Set-RepairProcessNormal($Expected) {
    Assert-Phase15Flat
    $handle = Open-RepairProcessHandle $Expected.ProcessId
    try {
        $current = Get-VerifiedRepairProcess $Expected
        $result = Invoke-CimMethod -InputObject $current -MethodName SetPriority -Arguments @{Priority=[int32]32} -ErrorAction Stop
        if ($null -eq $result.ReturnValue -or $result.ReturnValue -ne 0) {
            throw "Could not set Normal priority for process $($Expected.ProcessId). CIM result $($result.ReturnValue)."
        }
    } finally { $handle.Dispose() }
}

function Stop-RepairObserver($Expected) {
    if (-not (Get-VerifiedRepairProcess $Expected -AllowExited)) { return }
    $handle = Open-RepairProcessHandle $Expected.ProcessId
    try {
        $current = Get-VerifiedRepairProcess $Expected -AllowExited
        if (-not $current) { return }
        $result = Invoke-CimMethod -InputObject $current -MethodName Terminate -Arguments @{Reason=[uint32]1} -ErrorAction Stop
        if ($null -eq $result.ReturnValue -or $result.ReturnValue -ne 0) {
            if ($handle.HasExited) { return }
            throw "Could not stop observer process $($Expected.ProcessId). CIM result $($result.ReturnValue)."
        }
        if (-not $handle.WaitForExit(3000)) { throw "Verified observer process $($Expected.ProcessId) did not exit." }
    } finally { $handle.Dispose() }
}

function Get-KuCoinTaskEngineId {
    $service = New-Object -ComObject 'Schedule.Service'
    $service.Connect()
    $instances = $service.GetFolder($taskPath).GetTask($name).GetInstances(0)
    if ($instances.Count -ne 1) { throw 'Expected exactly one running KuCoin task instance.' }
    $instance = $instances.Item(1)
    if ($instance.State -ne 4 -or $instance.CurrentAction -ne $python -or $instance.EnginePID -le 0) {
        throw 'Cannot verify the running KuCoin task action and engine process.'
    }
    [int]$instance.EnginePID
}

function Get-KuCoinRepairOwner($Runtime, [string[]]$PythonPaths) {
    $currentRuntime = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'data\phase10\runtime.json') -Raw | ConvertFrom-Json
    if ($currentRuntime.pid -ne $Runtime.pid -or $currentRuntime.created -ne $Runtime.created -or
            $currentRuntime.launcher -ne 'scheduled' -or $currentRuntime.launch_id -cne $Runtime.launch_id) {
        throw 'The scheduled KuCoin ownership record changed.'
    }
    $worker = Get-RepairProcess ([int]$Runtime.pid)
    if (-not $worker -or -not $worker.CreationDate -or $worker.SessionId -ne 0 -or
            $worker.ExecutablePath -notin $PythonPaths -or $worker.CommandLine -notmatch '\s-X utf8 -m lab\.phase10 worker --managed\s*$' -or
            $null -eq $Runtime.created) {
        throw 'Cannot verify the session-zero KuCoin worker executable, command and creation time.'
    }
    $created = ([DateTimeOffset]$worker.CreationDate).ToUnixTimeMilliseconds() / 1000.0
    if ([math]::Abs($created - [double]$Runtime.created) -gt 0.001) {
        throw 'The KuCoin worker creation time differs from its runtime record.'
    }
    $engineId = Get-KuCoinTaskEngineId
    if ($engineId -ne $worker.ProcessId) {
        if ($engineId -ne $worker.ParentProcessId) { throw 'The worker is outside the registered task process tree.' }
        $engine = Get-RepairProcess $engineId
        if (-not $engine -or $engine.SessionId -ne 0 -or $engine.ExecutablePath -ne $python -or
                $engine.CommandLine -notmatch '\s-X utf8 -m lab\.phase10 worker --managed\s*$' -or
                $engine.CreationDate -gt $worker.CreationDate) {
            throw 'The registered task launcher identity could not be verified.'
        }
        $null = Get-VerifiedRepairProcess $engine
    }
    Get-VerifiedRepairProcess $worker
}

function Invoke-RepairGuard([string]$Action, [string]$Reason) {
    $scriptPath = Join-Path $PSScriptRoot 'lab\phase10\repair_guard.py'
    if ($Reason) { $output = $Reason -replace '[\r\n]+',' ' | & $python -X utf8 $scriptPath $Action --reason-stdin }
    else { $output = & $python -X utf8 $scriptPath $Action }
    if ($LASTEXITCODE -ne 0) { throw "KuCoin repair guard failed during $Action." }
    $output | ConvertFrom-Json
}

function Enter-RepairQuiescence([switch]$AllowWorker) {
    $guardPath = Join-Path $PSScriptRoot 'lab\phase10\repair_guard.py'
    $info = New-Object Diagnostics.ProcessStartInfo
    $info.FileName = $python
    $info.Arguments = '-X utf8 "' + $guardPath + '" hold'
    if ($AllowWorker) { $info.Arguments += ' allow-worker' }
    $info.WorkingDirectory = $PSScriptRoot
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardInput = $true
    $info.RedirectStandardOutput = $true
    $guard = [Diagnostics.Process]::Start($info)
    try {
        $ready = $guard.StandardOutput.ReadLineAsync()
        if (-not $ready.Wait(15000) -or $ready.Result -ne 'QUIESCENT' -or $guard.HasExited) {
            throw 'Could not prove quiescence. A worker, observer, capture or journal writer may still be active.'
        }
        $guard
    } catch {
        Exit-RepairQuiescence $guard
        throw
    }
}

function Exit-RepairQuiescence($Guard) {
    try {
        $Guard.StandardInput.Close()
        if (-not $Guard.WaitForExit(10000)) { throw 'The quiescence guard did not release its locks.' }
    } finally { $Guard.Dispose() }
}

function Get-LegacyObservers($Runtime, [string[]]$PythonPaths) {
    $pattern = '\s-m lab\.phase13 _observe ' + $Runtime.launch_id + '\s*$'
    $created = [DateTimeOffset]::FromUnixTimeMilliseconds([long]([math]::Floor([double]$Runtime.created * 1000)))
    $rows = @(Get-CimInstance -ClassName Win32_Process -Filter "Name='python.exe' AND SessionId=0" -ErrorAction Stop)
    if ($rows | Where-Object { $_.ParentProcessId -eq $Runtime.pid -and -not $_.CreationDate }) {
        throw 'A Python child of the KuCoin worker has no creation-time evidence.'
    }
    $candidates = @($rows | Where-Object {
        $_.CreationDate -and ([DateTimeOffset]$_.CreationDate) -ge $created
    })
    $matching = @($candidates | Where-Object { $_.CommandLine -match $pattern })
    $parents = @([int]$Runtime.pid) + @($matching | ForEach-Object { [int]$_.ProcessId })
    foreach ($candidate in $candidates | Where-Object { $_.ParentProcessId -in $parents }) {
        if (-not $candidate.CommandLine -or -not $candidate.ExecutablePath) {
            throw 'A Python descendant of the KuCoin worker has incomplete identity evidence.'
        }
        if ($candidate.ParentProcessId -ne $Runtime.pid -and $candidate.CommandLine -notmatch $pattern) {
            throw 'A Python descendant of the KuCoin observer has conflicting identity evidence.'
        }
    }
    foreach ($candidate in $matching) {
        $wrapper = @($matching | Where-Object { $_.ProcessId -eq $candidate.ParentProcessId })
        $validParent = $candidate.ParentProcessId -eq $Runtime.pid -or
            ($wrapper.Count -eq 1 -and $wrapper[0].ParentProcessId -eq $Runtime.pid -and
             $wrapper[0].CreationDate -le $candidate.CreationDate)
        if (-not $validParent -or $candidate.ExecutablePath -notin $PythonPaths) {
            throw 'A KuCoin observer has conflicting ancestry or executable evidence.'
        }
        $candidate
    }
}

function Wait-KuCoinDrain($Runtime) {
    $clock = [Diagnostics.Stopwatch]::StartNew()
    do {
        $current = Get-RepairProcess ([int]$Runtime.pid)
        if (-not $current -and -not (Invoke-RepairGuard status).worker_lock_held) { return $true }
        Start-Sleep -Milliseconds 500
    } while ($clock.Elapsed.TotalSeconds -lt 60)
    return $false
}

function Wait-KuCoinTaskReady {
    $clock = [Diagnostics.Stopwatch]::StartNew()
    do {
        $current = Read-LabTask
        if ($current.State -eq 'Ready' -and $current.Settings.Priority -eq 4) { return $current }
        if ($current.State -ne 'Running') { break }
        Start-Sleep -Milliseconds 500
    } while ($clock.Elapsed.TotalSeconds -lt 20)
    throw 'The original KuCoin task did not return to Ready at Normal priority.'
}

function Assert-LegacyObserverLeaseProtocol($Runtime, $Observers) {
    if ($Runtime.drain_guard_version -eq 1 -or -not $Observers) { return }
    $audit = Join-Path $PSScriptRoot 'runs\phase10\observer.jsonl'
    $proof = @{}
    if (Test-Path -LiteralPath $audit -PathType Leaf) {
        foreach ($line in Get-Content -LiteralPath $audit -Tail 300) {
            $entry = $line | ConvertFrom-Json
            if ($entry.launch -ceq $Runtime.launch_id -and $entry.stage -eq 'ownership' -and $entry.state -eq 'STARTED') {
                $proof[[int]$entry.pid] = [DateTimeOffset]$entry.at
            }
        }
    }
    foreach ($observer in $Observers) {
        # The venv redirector never runs the Python observer body.
        if ($observer.ExecutablePath -eq $python) { continue }
        if (-not (Get-VerifiedRepairProcess $observer -AllowExited)) { continue }
        $stageAt = $proof[[int]$observer.ProcessId]
        if (-not $stageAt -or $stageAt -lt [DateTimeOffset]$observer.CreationDate) {
            throw 'Cannot prove that the legacy observer uses the journal and capture lease protocol.'
        }
    }
}

function Complete-KuCoinRepairDrain($Task, $Runtime, [string[]]$PythonPaths) {
    $began = $false
    $startRequested = $false
    try {
        $initial = Invoke-RepairGuard status
        if ($Task.State -eq 'Running' -and -not $initial.paused -and
                $initial.repair.state -in @('START_PENDING','START_REQUESTED') -and
                $initial.repair.launch_id -and $initial.repair.launch_id -cne $Runtime.launch_id) {
            $null = Get-KuCoinRepairOwner $Runtime $PythonPaths
            return
        }
        $owner = $null
        if ($Task.State -eq 'Running') {
            $owner = Get-KuCoinRepairOwner $Runtime $PythonPaths
        } elseif ($Task.State -ne 'Ready' -or -not $initial.paused -or
                $initial.worker_lock_held -or (Get-RepairProcess ([int]$Runtime.pid)) -or
                (-not $Runtime.stopped_at -and $initial.repair.launch_id -cne $Runtime.launch_id)) {
            throw 'Cannot resume the repair with this task state, ownership lock, or monitoring pause.'
        }
        $began = $true
        $null = Invoke-RepairGuard pause 'Draining the old scheduled KuCoin launch.'
        $prior = @(Get-LegacyObservers $Runtime $PythonPaths)
        if ($owner) { Set-RepairProcessNormal $owner }
        $drained = Wait-KuCoinDrain $Runtime
        if (-not $drained -and (-not $owner -or $Runtime.drain_guard_version -ne 1)) {
            throw 'Graceful drain expired. The legacy worker has no activity lease that can prove writes have stopped.'
        }
        $guard = Enter-RepairQuiescence -AllowWorker:(-not $drained)
        try {
            if ($guard.HasExited) { throw 'Quiescence was lost before process cleanup.' }
            Assert-Phase15Flat
            $observers = @($prior; Get-LegacyObservers $Runtime $PythonPaths) |
                Sort-Object -Property ProcessId,CreationDate -Unique
            Assert-LegacyObserverLeaseProtocol $Runtime $observers
            foreach ($observer in ($observers | Sort-Object CreationDate -Descending)) {
                if ($guard.HasExited) { throw 'Quiescence was lost before observer termination.' }
                Stop-RepairObserver $observer
            }
            if (-not $drained) {
                if (Get-RepairProcess ([int]$Runtime.pid)) {
                    $null = Get-KuCoinRepairOwner $Runtime $PythonPaths
                    Assert-Phase15Flat
                    if ($guard.HasExited) { throw 'Quiescence was lost before worker termination.' }
                    Stop-RepairObserver $owner
                }
            }
            if ($guard.HasExited -or @(Get-LegacyObservers $Runtime $PythonPaths).Count -or
                    (Get-RepairProcess ([int]$Runtime.pid))) {
                throw 'Old KuCoin processes or incomplete quiescence still block restart.'
            }
        } finally { Exit-RepairQuiescence $guard }
        $null = Wait-KuCoinTaskReady
        if ((Invoke-RepairGuard status).worker_lock_held -or @(Get-LegacyObservers $Runtime $PythonPaths).Count) {
            throw 'An old writer or observer remains after drain.'
        }
        Assert-Phase15Flat
        $null = Invoke-RepairGuard resume 'Old KuCoin launch and observers exited; writer leases released.'
        Start-ScheduledTask -TaskPath $taskPath -TaskName $name
        $startRequested = $true
        $null = Invoke-RepairGuard started 'Start requested for the original scheduled task; await prospective evidence.'
    } catch {
        if ($began -and -not $startRequested) {
            $message = $_.Exception.Message
            try { $null = Invoke-RepairGuard block $message }
            catch { Write-Warning 'Could not persist the blocked repair status. Verify the current monitoring pause before retrying.' }
            Write-Warning 'KuCoin monitoring is paused. Re-run repair-observer after resolving the reported blocker.'
        } elseif ($startRequested) {
            Write-Warning 'The task start was requested. Verify its new owner and prospective receipts before retrying.'
        }
        throw
    }
}

function Repair-LabObserver {
    Assert-Phase15Flat
    $task = Read-LabTask
    if (-not $task -or $task.State -notin @('Running','Ready')) { throw 'Expected the registered KuCoin task to be Running or Ready.' }
    $runtime = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'data\phase10\runtime.json') -Raw | ConvertFrom-Json
    if ($runtime.launcher -ne 'scheduled' -or $runtime.launch_id -cnotmatch '^[0-9a-f]{32}$') {
        throw 'Cannot identify the current scheduled KuCoin worker.'
    }
    $venv = Get-Content -LiteralPath (Join-Path $PSScriptRoot '.venv\pyvenv.cfg')
    $base = @($venv | Where-Object { $_ -match '^executable\s*=' })
    if ($base.Count -ne 1) { throw 'Cannot verify the project virtual environment executable.' }
    $base = ($base[0] -split '=',2)[1].Trim()
    if (-not [IO.Path]::IsPathRooted($base) -or -not (Test-Path -LiteralPath $base -PathType Leaf)) {
        throw 'Cannot verify the base Python executable.'
    }
    $pythonPaths = @($python, $base)
    if ($task.Settings.Priority -ne 4) {
        $credential = Get-Credential -UserName $identity.Name -Message 'Update only the existing KuCoin task to Normal priority.'
        if (-not $credential -or ([Security.Principal.NTAccount]::new($credential.UserName)).Translate(
            [Security.Principal.SecurityIdentifier]).Value -ne $identity.User.Value) {
            throw 'Use the existing Windows task account.'
        }
        try {
            Assert-Phase15Flat
            if ($task.State -eq 'Running') { $null = Get-KuCoinRepairOwner $runtime $pythonPaths }
            $task.Settings.Priority = 4
            $change = @{TaskPath = $taskPath; TaskName = $name; Settings = $task.Settings; User = $identity.Name
                        Password = $credential.GetNetworkCredential().Password}
            Set-ScheduledTask @change | Out-Null
        } finally { Remove-Variable credential,change -ErrorAction SilentlyContinue }
        $task = Read-LabTask
        if ($task.Settings.Priority -ne 4) { throw 'The existing KuCoin task did not retain Normal priority.' }
    }
    Complete-KuCoinRepairDrain $task $runtime $pythonPaths
    [pscustomobject]@{Task = "$taskPath$name"; Priority = 4; PreviousPid = $runtime.pid
                      State = (Read-LabTask).State; ManagedVerification = 'Await next prospective cycle'}
}

$operationLock = $null
if ($action -eq 'repair-observer' -and -not $isElevated) {
    throw 'Open administrator PowerShell to update and restart the existing KuCoin task.'
}
if ($action -in @('install','start','stop','repair-observer')) {
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
        $repairPath = Join-Path $PSScriptRoot 'data\phase10\observer-repair.json'
        if (Test-Path -LiteralPath $repairPath -PathType Leaf) {
            $repair = Get-Content -LiteralPath $repairPath -Raw | ConvertFrom-Json
            if ($repair.state -in @('DRAINING','BLOCKED','START_PENDING')) {
                throw 'An observer repair is unfinished. Continue with .\phase10.ps1 repair-observer.'
            }
        }
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
    'repair-observer' { Repair-LabObserver }
    'status' { Show-Status }
}
} finally {
    if ($operationLock) { $operationLock.Dispose() }
}
