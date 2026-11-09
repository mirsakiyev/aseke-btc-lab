from base64 import b64encode
from pathlib import Path
import subprocess
import sys

from lab.phase2 import __main__ as control, collector, store
from lab.storage import writer_lock


def test_stop_is_persistent_for_manual_and_managed_workers(tmp_path, monkeypatch):
    pause = tmp_path / "paused.json"
    monkeypatch.setattr(store, "FORWARD", tmp_path)
    monkeypatch.setattr(control, "PAUSE", pause)
    monkeypatch.setattr(collector, "PAUSE", pause)
    monkeypatch.setattr(control, "status", lambda: {"process_alive": False})
    control.stop()
    original = pause.read_bytes()
    control.stop()
    assert pause.read_bytes() == original
    for scheduled in (False, True):
        assert collector.run_worker(True, managed=scheduled) == {"state": "intentionally_paused"}
    control.allow_scheduled_start()
    assert not pause.exists()


def test_attached_worker_refuses_an_existing_writer_and_propagates_failure(tmp_path):
    program = """
from pathlib import Path
import sys
from lab.phase2 import __main__, collector, store
store.FORWARD = Path(sys.argv[1])
collector.PAUSE = store.FORWARD / "paused.json"
sys.argv[1:] = ["collect", "--shadow", "--managed"]
__main__.main()
"""
    with writer_lock(tmp_path / ".worker.lock"):
        result = subprocess.run([sys.executable, "-c", program, str(tmp_path)], capture_output=True, text=True, timeout=20)
    assert result.returncode != 0
    assert "Another writer owns" in result.stderr
    assert not (tmp_path / "worker.json").exists()


def test_task_validator_requires_effective_defaults_in_windows_powershell():
    script = r"""
$ErrorActionPreference = 'Stop'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Join-Path (Get-Location) 'phase7.ps1'), [ref]$tokens, [ref]$parseErrors)
if ($parseErrors) { throw ($parseErrors -join ', ') }
foreach ($name in 'Test-TaskDefault', 'Get-LabTaskKind') {
    $node = $ast.Find({
        param($candidate)
        $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $candidate.Name -eq $name
    }, $false)
    . ([scriptblock]::Create($node.Extent.Text))
}

$identity = [pscustomobject]@{ User = @{ Value = 'S-1-5-21-1-2-3-1000' }; Name = 'HOST\operator' }
$exe = 'C:\Lab\.venv\Scripts\python.exe'
$workdir = 'C:\Lab'
$arguments = '-X utf8 -m lab.phase2 collect --shadow --managed'

function Assert-Task($RunXml, $RunEffective, $InstancesXml, $InstancesEffective, $LimitXml, $LimitEffective,
                     [string]$ExpectedError = '') {
    $definition = ([xml]@"
<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Principals><Principal><UserId>$($identity.User.Value)</UserId><LogonType>Password</LogonType>$RunXml</Principal></Principals>
  <Triggers><BootTrigger><Delay>PT1M</Delay></BootTrigger></Triggers>
  <Settings>
    $InstancesXml$LimitXml
    <StartWhenAvailable>true</StartWhenAvailable>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <RestartOnFailure><Count>3</Count><Interval>PT2M</Interval></RestartOnFailure>
  </Settings>
  <Actions><Exec><Command>$exe</Command><Arguments>$arguments</Arguments><WorkingDirectory>$workdir</WorkingDirectory></Exec></Actions>
</Task>
"@).Task
    $registered = @{ Principal = @{ RunLevel = $RunEffective }
                     Settings = @{ MultipleInstances = $InstancesEffective; ExecutionTimeLimit = $LimitEffective } }
    try {
        $result = Get-LabTaskKind $registered $definition $identity $exe $workdir $arguments
    } catch {
        if ($ExpectedError -and $_.Exception.Message -eq "Registered Phase 7 task failed validation for $ExpectedError.") {
            return
        }
        throw
    }
    if ($ExpectedError -or $result -ne 'unattended') { throw "Unexpected validator result for $ExpectedError." }
}

$run = '<RunLevel>LeastPrivilege</RunLevel>'
$instances = '<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>'
$limit = '<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>'
Assert-Task '' Limited '' IgnoreNew $limit PT0S
Assert-Task $run Limited $instances IgnoreNew $limit PT0S
Assert-Task '' Highest $instances IgnoreNew $limit PT0S Principal.RunLevel
Assert-Task $run Highest $instances IgnoreNew $limit PT0S Principal.RunLevel
foreach ($value in 'Highest', 'HighestAvailable', 'Unknown', '', '<Value>LeastPrivilege</Value>') {
    Assert-Task "<RunLevel>$value</RunLevel>" Limited $instances IgnoreNew $limit PT0S Principal.RunLevel
}
foreach ($value in 'Parallel', 'Queue', 'StopExisting', 'Unknown') {
    Assert-Task $run Limited "<MultipleInstancesPolicy>$value</MultipleInstancesPolicy>" IgnoreNew $limit PT0S Settings.MultipleInstancesPolicy
    Assert-Task $run Limited '' $value $limit PT0S Settings.MultipleInstancesPolicy
}
Assert-Task $run Limited $instances Parallel $limit PT0S Settings.MultipleInstancesPolicy
Assert-Task '' $null '' $null $limit PT0S 'Principal.RunLevel, Settings.MultipleInstancesPolicy'
Assert-Task $run Limited $instances IgnoreNew '' PT72H Settings.ExecutionTimeLimit
Assert-Task $run Limited $instances IgnoreNew '' PT0S Settings.ExecutionTimeLimit
Assert-Task $run Limited $instances IgnoreNew $limit PT72H Settings.ExecutionTimeLimit
"""
    encoded = b64encode(script.encode("utf-16-le")).decode("ascii")
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
