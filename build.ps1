#!/usr/bin/env pwsh
# build.ps1 — cross-compile the graphene CLI for every supported platform.
#
# Usage:
#   .\build.ps1                      every target into dist\
#   .\build.ps1 -Target linux/amd64  one target
#   .\build.ps1 -Version v1.2.3      override the version stamp
#   .\build.ps1 -Dist out            somewhere other than dist\
#
# Neither this nor build.sh is the source of truth for what gets released:
# .github/workflows/ci.yml invokes `go build` directly and is the definition.
# Both scripts exist to reproduce a release build locally, and a script here
# that disagrees with the workflow is a bug in the script.
#
# Git is read-only throughout. The three commands below describe the checkout
# and nothing more; nothing writes to the repository, the index, the working
# tree, or any remote. Each has a fallback, because a build from an archive with
# no .git must still produce a binary that says what it is.

param(
    [string]$Target = "",
    [string]$Version = "",
    [string]$Dist = "dist"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = $PSScriptRoot
Push-Location $root

try {
    $pkg = "./cmd/graphene"
    $bin = "graphene"

    # --- version metadata, read-only -------------------------------------
    function Test-GitAvailable {
        if (-not (Get-Command git -ErrorAction SilentlyContinue)) { return $false }
        git rev-parse --git-dir 2>$null | Out-Null
        return $LASTEXITCODE -eq 0
    }

    # Queries only. `git describe` gives the tag; the other two give the commit
    # and its timestamp. Nothing here can alter repository state.
    $haveGit = Test-GitAvailable
    if ($Version) {
        $version = $Version
    } elseif ($haveGit) {
        $version = (git describe --tags --always --dirty 2>$null)
        if ($LASTEXITCODE -ne 0 -or -not $version) { $version = "dev" }
    } else {
        $version = "dev"
    }

    if ($haveGit) {
        $commit = (git log -1 --format=%H 2>$null)
        if ($LASTEXITCODE -ne 0 -or -not $commit) { $commit = "unknown" }
        $epoch = (git log -1 --format=%ct 2>$null)
        if ($LASTEXITCODE -ne 0 -or -not $epoch) { $epoch = "0" }
    } else {
        $commit = "unknown"
        $epoch = "0"
    }

    # SOURCE_DATE_EPOCH is what reproducible-build tooling reads. Honour an
    # existing value rather than overwriting it: whoever set it knows something
    # this script does not.
    if (-not $env:SOURCE_DATE_EPOCH) { $env:SOURCE_DATE_EPOCH = $epoch }

    $targets = @(
        "linux/amd64", "linux/arm64",
        "darwin/amd64", "darwin/arm64",
        "windows/amd64", "windows/arm64"
    )
    if ($Target) { $targets = @($Target) }

    # -trimpath strips the build machine's paths out of the binary, and
    # -buildvcs=false stops the toolchain stamping its own VCS values over the
    # ones injected here — with both, two builds of the same commit produce
    # identical bytes. CGO is off because the engine is pure Go: there is
    # nothing to link against, and a static binary is the point of shipping one
    # file.
    $ldflags = "-s -w " +
        "-X main.version=$version " +
        "-X main.commit=$commit " +
        "-X main.buildTime=$epoch"

    New-Item -ItemType Directory -Force -Path $Dist | Out-Null

    Write-Host "graphene $version ($commit)"
    Write-Host ""

    # Saved and restored, because these are process-wide and this script is
    # sometimes dot-sourced from a shell somebody keeps using afterwards.
    $savedOS = $env:GOOS; $savedArch = $env:GOARCH; $savedCgo = $env:CGO_ENABLED

    foreach ($t in $targets) {
        $parts = $t.Split("/")
        if ($parts.Count -ne 2) { throw "target must be <os>/<arch>, got '$t'" }
        $goos, $goarch = $parts

        $out = Join-Path $Dist "${bin}_${goos}_${goarch}"
        if ($goos -eq "windows") { $out = "$out.exe" }

        Write-Host "==> $t" -ForegroundColor Cyan
        $env:GOOS = $goos; $env:GOARCH = $goarch; $env:CGO_ENABLED = "0"
        & go build -trimpath -buildvcs=false -ldflags $ldflags -o $out $pkg
        if ($LASTEXITCODE -ne 0) {
            throw "build failed for $t"
        }
    }

    $env:GOOS = $savedOS; $env:GOARCH = $savedArch; $env:CGO_ENABLED = $savedCgo

    Write-Host ""
    Write-Host "==> checksums" -ForegroundColor Cyan
    $sums = Get-ChildItem -Path $Dist -File |
        Where-Object { $_.Name -like "${bin}_*" } |
        Sort-Object Name |
        ForEach-Object {
            $h = (Get-FileHash -Algorithm SHA256 $_.FullName).Hash.ToLower()
            "$h  $($_.Name)"
        }
    # Written without a BOM and with LF endings, so the file is byte-identical
    # to the one build.sh produces and the two can be compared directly.
    $text = ($sums -join "`n") + "`n"
    [System.IO.File]::WriteAllText((Join-Path $Dist "SHA256SUMS"), $text,
        (New-Object System.Text.UTF8Encoding $false))
    $sums | ForEach-Object { Write-Host $_ }
}
finally {
    Pop-Location
}
