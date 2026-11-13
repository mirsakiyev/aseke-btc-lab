from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from lab.config import fingerprint
from lab.phase15.catalog import POLICY
from lab.phase15.paper import initial


PROJECT = Path(__file__).resolve().parents[1]
LANES = ("day/fixed", "swing/fixed", "swing/trail", "position/fixed")
START = "2026-11-09T21:45:00+00:00"

# Run the real production PowerShell functions while making every scheduler or
# process call fail immediately. A flat fixture must reach the first task read.
HARNESS = r"""
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
if ($PSVersionTable.PSVersion.Major -ne 5 -or $PSVersionTable.PSVersion.Minor -ne 1) {
    throw 'Expected Windows PowerShell 5.1.'
}
Set-Location -LiteralPath $PSScriptRoot
$python = $env:ASEKE_PREFLIGHT_TEST_PYTHON
$tokens = $null
$errors = $null
$tree = [System.Management.Automation.Language.Parser]::ParseFile(
    $env:ASEKE_PREFLIGHT_TEST_SOURCE, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'phase10.ps1 failed parsing.' }
$definitions = foreach ($name in 'Assert-Phase15Flat', 'Repair-LabObserver') {
    $definition = $tree.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $false)
    if (-not $definition) { throw "Missing function $name." }
    $definition.Extent.Text
}
$fixtureFunctions = Join-Path $PSScriptRoot 'repair functions.ps1'
Set-Content -LiteralPath $fixtureFunctions -Value $definitions -Encoding UTF8
. $fixtureFunctions

$script:taskReads = 0
function Read-LabTask {
    $script:taskReads++
    throw 'FIXTURE_SCHEDULER_READ'
}
function Get-ScheduledTask { throw 'UNEXPECTED_SCHEDULER_ACCESS' }
function Set-ScheduledTask { throw 'UNEXPECTED_SCHEDULER_WRITE' }
function Start-ScheduledTask { throw 'UNEXPECTED_SCHEDULER_START' }
function Stop-ScheduledTask { throw 'UNEXPECTED_SCHEDULER_STOP' }
function Get-Process { throw 'UNEXPECTED_PROCESS_ACCESS' }
function Stop-Process { throw 'UNEXPECTED_PROCESS_STOP' }
function Invoke-Lab { throw 'UNEXPECTED_WORKER_COMMAND' }

$lock = $null
try {
    if ($env:ASEKE_PREFLIGHT_TEST_CASE -eq 'inaccessible_database') {
        $database = Join-Path $PSScriptRoot 'runs\phase15\paper.sqlite3'
        $lock = [IO.File]::Open($database, 'Open', 'Read', 'None')
    }
    $failure = $null
    try { Repair-LabObserver }
    catch { $failure = $_.Exception.Message }
    if ($env:ASEKE_PREFLIGHT_TEST_CASE -eq 'flat') {
        if ($failure -ne 'FIXTURE_SCHEDULER_READ' -or $script:taskReads -ne 1) {
            throw "Valid preflight did not reach the scheduler read. Failure: $failure"
        }
    } elseif ($script:taskReads -ne 0 -or $failure -ne 'Could not confirm four flat Phase 15 accounts.') {
        throw "Invalid preflight did not abort before task access. Failure: $failure"
    }
    Write-Output 'PREFLIGHT_GATE_VERIFIED'
} finally {
    if ($lock) { $lock.Dispose() }
}
"""


def _fixture(project, case):
    from lab.phase15 import restart_preflight

    phase = project / "runs" / "phase15"
    (phase / "registrations").mkdir(parents=True)
    lanes = {}
    for name in LANES:
        lane = {"profile": name.split("/")[0], "risk": {"starting_equity": 10_000}}
        lanes[name] = {**lane, "book_id": fingerprint([POLICY, name, lane, START])}
    body = {"policy": POLICY, "start": START, "lanes": lanes}
    ident = fingerprint(body)
    (phase / "registration.json").write_text(json.dumps({"id": ident}), encoding="utf-8")
    (phase / "registrations" / f"{ident}.json").write_text(json.dumps({"id": ident, "body": body}), encoding="utf-8")

    helper = project / "lab" / "phase15" / "restart_preflight.py"
    helper.parent.mkdir(parents=True)
    helper_code = Path(restart_preflight.__file__).read_text(encoding="utf-8")
    helper.write_text(helper_code.replace(restart_preflight.ORIGINAL_REGISTRATION_ID, ident, 1), encoding="utf-8")

    path = phase / "paper.sqlite3"
    if case == "missing_database":
        return path
    if case == "invalid_database":
        path.write_bytes(b"not a SQLite database")
        return path

    books = [initial(lane) for lane in lanes.values()]
    if case == "open_position":
        books[0]["position"] = {"entry_at": START}
    elif case == "pending_idea":
        books[0]["pending"] = {"id": "pending"}
    elif case == "missing_account":
        books.pop()
    elif case == "unexpected_account":
        books[0]["book_id"] = "f" * 64
    elif case == "duplicate_account":
        books[0] = books[1]
    rows = [(book["book_id"], json.dumps(book)) for book in books]
    if case == "malformed_account":
        rows[0] = (rows[0][0], "{invalid JSON")
    with closing(sqlite3.connect(path)) as db, db:
        identity = "TEXT" if case == "duplicate_account" else "TEXT PRIMARY KEY"
        db.execute(f"CREATE TABLE book (id {identity}, state TEXT NOT NULL)")
        db.execute("CREATE TABLE event (key TEXT PRIMARY KEY, book TEXT NOT NULL, kind TEXT NOT NULL, "
                   "idea TEXT NOT NULL, source_at TEXT NOT NULL, recorded_at TEXT NOT NULL, body TEXT NOT NULL)")
        db.executemany("INSERT INTO book VALUES (?, ?)", rows)
    return path


@pytest.mark.parametrize("case", [
    "flat", "open_position", "pending_idea", "missing_account", "unexpected_account", "duplicate_account",
    "missing_database", "invalid_database", "malformed_account", "inaccessible_database",
])
def test_phase15_preflight_gate_under_windows_powershell_51(tmp_path, case):
    powershell = Path(os.environ["WINDIR"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    project = tmp_path / "project with spaces"
    database = _fixture(project, case)
    before = database.read_bytes() if database.exists() else None
    harness = project / "repair gate.ps1"
    harness.write_text(HARNESS, encoding="ascii")
    result = subprocess.run(
        [str(powershell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(harness)],
        env={**os.environ, "ASEKE_PREFLIGHT_TEST_PYTHON": sys.executable,
             "ASEKE_PREFLIGHT_TEST_SOURCE": str(PROJECT / "phase10.ps1"), "ASEKE_PREFLIGHT_TEST_CASE": case},
        capture_output=True, text=True, timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PREFLIGHT_GATE_VERIFIED" in result.stdout
    assert (database.read_bytes() if database.exists() else None) == before
