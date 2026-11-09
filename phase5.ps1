$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) {
    throw 'Project .venv Python is missing. See README.md.'
}
$env:PYTHONUTF8 = '1'
& $python -m lab.phase5 @args
exit $LASTEXITCODE
