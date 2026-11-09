param(
    [Parameter(Mandatory)]
    [ValidateSet('Install', 'Status', 'Remove')]
    [string]$Action,
    [switch]$Shadow,
    [switch]$Register
)
$ErrorActionPreference = 'Stop'
$phase7 = Join-Path $PSScriptRoot 'phase7.ps1'
switch ($Action) {
    'Install' { & $phase7 install -Register:$Register }
    'Status' { & $phase7 status }
    'Remove' { & $phase7 uninstall }
}
