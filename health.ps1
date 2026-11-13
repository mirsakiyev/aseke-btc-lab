$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw 'Project .venv Python is missing.' }
$dashboard = 'ASEKE BTC Lab - Dashboard'
$watchdog = 'ASEKE BTC Lab - Health Watchdog'
$command = if ($args.Count) { $args[0] } else { 'status' }

if ($command -eq 'install') {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Windows requires an administrator PowerShell for these startup task registrations. Run .\health.ps1 install there.'
    }
    foreach ($name in $dashboard,$watchdog) {
        if (Get-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction SilentlyContinue) { throw "Existing $name requires inspection before any replacement." }
    }
    $credential = Get-Credential -UserName $identity.Name -Message 'Enter the existing Windows password for unattended ASEKE dashboard and watchdog tasks.'
    if (-not $credential) { throw 'Task credentials were not supplied.' }
    $sid = ([Security.Principal.NTAccount]::new($credential.UserName)).Translate([Security.Principal.SecurityIdentifier])
    if ($sid.Value -ne $identity.User.Value) { throw 'Use the same Windows account as the existing DPAPI Telegram configuration.' }
    $runAs = New-ScheduledTaskPrincipal -UserId $identity.Name -LogonType Password -RunLevel Limited
    $boot = New-ScheduledTaskTrigger -AtStartup
    $poll = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 3)
    $zero = New-TimeSpan -Seconds 0
    $dashboardSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit $zero -RestartCount 2 -RestartInterval (New-TimeSpan -Minutes 2) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    $watchSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -Priority 4 -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    $dashboardAction = New-ScheduledTaskAction -Execute $python -Argument '-X utf8 -m lab.health.dashboard_service --scheduled' -WorkingDirectory $PSScriptRoot
    $watchAction = New-ScheduledTaskAction -Execute $python -Argument '-X utf8 -m lab.health check-once --scheduled' -WorkingDirectory $PSScriptRoot
    $definitions = @(
        @{Name=$dashboard; Definition=(New-ScheduledTask -Action $dashboardAction -Trigger $boot -Principal $runAs -Settings $dashboardSettings)},
        @{Name=$watchdog; Definition=(New-ScheduledTask -Action $watchAction -Trigger @($boot,$poll) -Principal $runAs -Settings $watchSettings)}
    )
    $added = @()
    try {
        foreach ($item in $definitions) {
            Register-ScheduledTask -TaskName $item.Name -TaskPath '\' -InputObject $item.Definition -User $credential.UserName -Password $credential.GetNetworkCredential().Password | Out-Null
            $added += $item.Name
            $task = Get-ScheduledTask -TaskName $item.Name -TaskPath '\'
            $registeredSid = if ($task.Principal.UserId -like 'S-1-*') { $task.Principal.UserId } else {
                ([Security.Principal.NTAccount]::new($task.Principal.UserId)).Translate([Security.Principal.SecurityIdentifier]).Value
            }
            $action = $item.Definition.Actions[0]
            if ($registeredSid -ne $sid.Value -or $task.Principal.LogonType -ne 'Password' -or $task.Principal.RunLevel -ne 'Limited' -or
                $task.Settings.MultipleInstances -ne 'IgnoreNew' -or @($task.Actions).Count -ne 1 -or
                $task.Actions[0].Execute -ne $action.Execute -or $task.Actions[0].Arguments -cne $action.Arguments -or
                $task.Actions[0].WorkingDirectory -ne $action.WorkingDirectory) {
                throw "Registered $($item.Name) does not match the requested account, action or execution policy."
            }
        }
    } catch {
        foreach ($name in $added) { Unregister-ScheduledTask -TaskName $name -TaskPath '\' -Confirm:$false }
        throw
    } finally { Remove-Variable credential }
    & $python -X utf8 -m lab.health stop-local
    if ($LASTEXITCODE -ne 0) { throw 'Local handoff verification failed. Inspect task-status before starting another dashboard.' }
    Start-ScheduledTask -TaskName $dashboard -TaskPath '\'
    Start-ScheduledTask -TaskName $watchdog -TaskPath '\'
    & $python -X utf8 -m lab.health task-status
} elseif ($command -eq 'repair-watchdog-priority') {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Run .\health.ps1 repair-watchdog-priority in an administrator PowerShell as the existing watchdog account.'
    }
    $task = Get-ScheduledTask -TaskName $watchdog -TaskPath '\'
    $runAs = $task.Principal.UserId
    $sid = if ($runAs -like 'S-1-*') { $runAs } else {
        ([Security.Principal.NTAccount]::new($runAs)).Translate([Security.Principal.SecurityIdentifier]).Value
    }
    if ($sid -ne $identity.User.Value -or $task.Principal.LogonType -ne 'Password' -or
        $task.Principal.RunLevel -ne 'Limited' -or @($task.Actions).Count -ne 1 -or
        $task.Actions[0].Execute -ne $python -or $task.Actions[0].Arguments -cne '-X utf8 -m lab.health check-once --scheduled' -or
        $task.Actions[0].WorkingDirectory -ne $PSScriptRoot -or
        $task.Settings.MultipleInstances -ne 'IgnoreNew' -or $task.Settings.ExecutionTimeLimit -ne 'PT2M') {
        throw "Existing $watchdog differs from the expected unattended task."
    }
    if ($task.Settings.Priority -ne 4) {
        $credential = Get-Credential -UserName $identity.Name -Message 'Enter the existing watchdog Windows account password to update its task priority.'
        if (-not $credential) { throw 'Task credentials were not supplied.' }
        try {
            $credentialSid = ([Security.Principal.NTAccount]::new($credential.UserName)).Translate([Security.Principal.SecurityIdentifier]).Value
            if ($credentialSid -ne $sid) { throw 'Use the existing unattended watchdog account.' }
            $settings = $task.Settings
            $settings.Priority = 4
            Set-ScheduledTask -TaskName $watchdog -TaskPath '\' -Settings $settings -User $identity.Name -Password $credential.GetNetworkCredential().Password | Out-Null
        } finally { Remove-Variable credential }
    }
    $updated = Get-ScheduledTask -TaskName $watchdog -TaskPath '\'
    if ($updated.Settings.Priority -ne 4 -or $updated.Settings.ExecutionTimeLimit -ne 'PT2M' -or
        $updated.Settings.MultipleInstances -ne 'IgnoreNew' -or $updated.Principal.LogonType -ne 'Password' -or
        $updated.Principal.RunLevel -ne 'Limited') {
        throw 'Verify the unattended watchdog task settings in Task Scheduler.'
    }
    [pscustomobject]@{TaskName=$watchdog; Priority=$updated.Settings.Priority; ExecutionTimeLimit=$updated.Settings.ExecutionTimeLimit;
        MultipleInstances=$updated.Settings.MultipleInstances; NextRun=($updated | Get-ScheduledTaskInfo).NextRunTime}
} elseif ($command -eq 'start') {
    foreach ($name in $dashboard,$watchdog) { Start-ScheduledTask -TaskName $name -TaskPath '\' }
    & $python -X utf8 -m lab.health task-status
} elseif ($command -in 'dashboard-local','watchdog-local') {
    $name = if ($command -eq 'dashboard-local') { $dashboard } else { $watchdog }
    $task = Get-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction SilentlyContinue
    if ($task -and $task.State -ne 'Disabled') { throw "Use the registered $name task." }
    $module = if ($command -eq 'dashboard-local') { 'lab.health.dashboard_service' } else { 'lab.health.local_service' }
    $startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ShowWindow=[uint16]0}
    $created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine=('"'+$python+'" -X utf8 -m '+$module);CurrentDirectory=$PSScriptRoot;ProcessStartupInformation=$startup}
    if ($created.ReturnValue -ne 0) { throw "Windows process creation failed with code $($created.ReturnValue)." }
    [pscustomobject]@{Mode='Detached local fallback';ProcessId=$created.ProcessId} | ConvertTo-Json
} else {
    & $python -X utf8 -m lab.health @args
    exit $LASTEXITCODE
}
