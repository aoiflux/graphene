#!/bin/sh
# build.sh — cross-compile the graphene CLI for every supported platform.
#
# Parity with build.ps1, for contributors who are not on Windows.
#
# Neither this nor build.ps1 is the source of truth for what gets released:
# .github/workflows/ci.yml invokes `go build` directly and is the definition.
# Both scripts exist to reproduce a release build locally, and a script here
# that disagrees with the workflow is a bug in the script.
#
# Git is read-only throughout. The three commands below describe the checkout
# and nothing more; nothing writes to the repository, the index, the working
# tree, or any remote. Each has a fallback, because a build from a tarball or a
# container with no .git must still produce a binary that says what it is.
#
#   ./build.sh                  every target into dist/
#   ./build.sh linux/amd64      one target
#   GOOS=linux GOARCH=arm64 ./build.sh   the same, by environment
#
# Variables:
#   DIST=dist        output directory
#   VERSION=v1.2.3   override the version stamp

set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$root"

DIST=${DIST:-dist}
PKG=./cmd/graphene
BIN=graphene

# --- version metadata, read-only ---------------------------------------------
#
# These are the only git invocations in the build, and all three are queries.
# `git describe` gives the tag; the other two give the commit and its timestamp.

git_ok() { command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; }

if [ -n "${VERSION:-}" ]; then
    version=$VERSION
elif git_ok; then
    version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)
else
    version=dev
fi

if git_ok; then
    commit=$(git log -1 --format=%H 2>/dev/null || echo unknown)
    epoch=$(git log -1 --format=%ct 2>/dev/null || echo 0)
else
    commit=unknown
    epoch=0
fi

# SOURCE_DATE_EPOCH is the convention reproducible-build tooling reads. Honour
# an existing value rather than overwriting it: whoever set it knows something
# this script does not.
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$epoch}
export SOURCE_DATE_EPOCH

TARGETS="linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64"
if [ $# -gt 0 ]; then
    TARGETS=$*
elif [ -n "${GOOS:-}" ] && [ -n "${GOARCH:-}" ]; then
    TARGETS="$GOOS/$GOARCH"
fi

# -trimpath strips the build machine's paths out of the binary, and
# -buildvcs=false stops the toolchain stamping its own VCS values over the ones
# injected below — with both, two builds of the same commit produce identical
# bytes. CGO is off because the engine is pure Go: there is nothing to link
# against, and a static binary is the whole point of shipping one file.
LDFLAGS="-s -w \
 -X main.version=$version \
 -X main.commit=$commit \
 -X main.buildTime=$epoch"

mkdir -p "$DIST"

echo "graphene $version ($commit)"
echo

for target in $TARGETS; do
    goos=${target%/*}
    goarch=${target#*/}
    out="$DIST/${BIN}_${goos}_${goarch}"
    [ "$goos" = windows ] && out="$out.exe"

    printf '==> %s\n' "$target"
    CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
        go build -trimpath -buildvcs=false -ldflags "$LDFLAGS" -o "$out" "$PKG"
done

echo
echo "==> checksums"
( cd "$DIST" && \
  if command -v sha256sum >/dev/null 2>&1; then
      sha256sum ${BIN}_* > SHA256SUMS
  else
      shasum -a 256 ${BIN}_* > SHA256SUMS
  fi )
cat "$DIST/SHA256SUMS"
