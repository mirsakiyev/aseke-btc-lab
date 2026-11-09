$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw 'Project .venv Python is missing.' }
if ($args.Count -and ($args[0] -in @('decide','observe') -or
    ($args.Count -gt 1 -and $args[0] -eq 'collect' -and $args[1] -in @('current','quotes')))) {
    & $python -X utf8 -m lab.phase10 _manual @args
    exit $LASTEXITCODE
}
& $python -X utf8 -m lab.phase9 @args
exit $LASTEXITCODE
