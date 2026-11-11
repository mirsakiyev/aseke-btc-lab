$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw 'Project .venv Python is missing.' }
$env:OPENBLAS_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:NUMBA_NUM_THREADS = '1'
& $python -X utf8 -m lab.strategy_lab @args
exit $LASTEXITCODE
