#!/usr/bin/env pwsh
# test.ps1 — Test runner for the Graphene engine.
#
# Usage:
#   .\test.ps1              # run all unit tests with race detector
#   .\test.ps1 -Stress      # run stress tests
#   .\test.ps1 -Bench       # run benchmarks
#   .\test.ps1 -All         # unit + stress + benchmarks
#   .\test.ps1 -Package disk # run tests for a single package
#   .\test.ps1 -Verbose     # pass -v to go test
#   .\test.ps1 -Filter BFS  # filter tests by name pattern

param(
    [switch]$Stress,
    [switch]$Bench,
    [switch]$All,
    [switch]$Verbose,
    [string]$Package = "./...",
    [string]$Filter = "",
    [string]$BenchTime = "5s"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = $PSScriptRoot
Push-Location $root

function Run-Step([string]$Label, [string[]]$CmdArgs) {
    Write-Host ""
    Write-Host "==> $Label" -ForegroundColor Cyan
    & go @CmdArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $Label" -ForegroundColor Red
        Pop-Location
        exit $LASTEXITCODE
    }
}

# Resolve package path — allow short names like "disk" or "traversal"
if ($Package -ne "./..." -and -not $Package.StartsWith(".")) {
    $Package = "./graphene/$Package"
    if (-not (Test-Path (Join-Path $root $Package.TrimStart("./")))) {
        # Try as a direct subdirectory name
        $Package = "./$($Package.Split('/')[-1])"
    }
}

$verboseFlag = if ($Verbose) { @("-v") } else { @() }
$filterFlag = if ($Filter) { @("-run", $Filter) } else { @() }

# --- Unit tests (always run unless -Bench only) ---
if (-not $Bench -or $All) {
    $unitArgs = @("test", $Package, "-race", "-count=1") + $verboseFlag + $filterFlag
    Run-Step "Unit tests ($Package)" $unitArgs
}

# --- Stress tests ---
if ($Stress -or $All) {
    $stressFilter = if ($Filter) { $Filter } else { "TestStress" }
    $stressArgs = @("test", ".", "-tags=stress", "-race", "-count=1") + $verboseFlag + @("-run", $stressFilter)
    Run-Step "Stress tests" $stressArgs
}

# --- Benchmarks ---
if ($Bench -or $All) {
    $benchFilter = if ($Filter) { $Filter } else { "." }
    $benchArgs = @("test", ".", "-tags=stress", "-bench=$benchFilter", "-benchmem", "-benchtime=$BenchTime", "-run=^$")
    Run-Step "Benchmarks (benchtime=$BenchTime)" $benchArgs
}

Pop-Location
Write-Host ""
Write-Host "All steps passed." -ForegroundColor Green
