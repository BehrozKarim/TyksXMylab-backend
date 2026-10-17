[CmdletBinding()]
param(
    [string]$Source,
    [string]$FrontendPath,
    [string]$Schema = 'aei_demo_live',
    [int]$BackendPort = 8000,
    [int]$FrontendPort = 5173,
    [int]$Days = 3,
    [int]$BatchSize = 20,
    [switch]$Restart,
    [switch]$SkipDocker,
    [switch]$NoBrowser,
    [switch]$ExitAfterFeed
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$demoPython = Join-Path $repositoryRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $demoPython)) {
    throw 'Create the backend .venv and install requirements.txt first; see DEMO.md.'
}

$demoArguments = @('-m', 'aei.demo', '--schema', $Schema,
    '--backend-port', "$BackendPort", '--frontend-port', "$FrontendPort",
    '--days', "$Days", '--batch-size', "$BatchSize")
if ($Source) { $demoArguments += @('--source', (Resolve-Path -LiteralPath $Source).Path) }
if ($FrontendPath) { $demoArguments += @('--frontend', (Resolve-Path -LiteralPath $FrontendPath).Path) }
if ($Restart) { $demoArguments += '--restart' }
if ($SkipDocker) { $demoArguments += '--skip-docker' }
if ($NoBrowser) { $demoArguments += '--no-browser' }
if ($ExitAfterFeed) { $demoArguments += '--exit-after-feed' }

Push-Location $repositoryRoot
try {
    & $demoPython @demoArguments
    $demoExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $demoExitCode
