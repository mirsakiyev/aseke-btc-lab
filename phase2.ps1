$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) {
    throw 'Create the project .venv with Python 3.12. See README.md.'
}
if ($args.Count -gt 0 -and $args[0] -in @('start', 'stop')) {
    & (Join-Path $PSScriptRoot 'phase7.ps1') $args[0]
    return
}
if ($args.Count -gt 0 -and $args[0] -eq 'collect' -and (Get-ScheduledTask -TaskPath '\' |
    Where-Object TaskName -eq 'ASEKE BTC Lab - Forward Research')) {
    throw 'Use phase7.ps1 start to manage the registered collector.'
}
$env:PYTHONUTF8 = '1'
& $python -m lab.phase2 @args
exit $LASTEXITCODE
