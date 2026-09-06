# Task runner for the Graphene engine.
#
# Parity with test.ps1, for contributors who are not on Windows. The engine is
# pure Go with no external dependencies, so before this the only documented entry
# point was a PowerShell script — a barrier to contribution that had nothing to do
# with the code.
#
# Neither this nor test.ps1 is the source of truth for what must pass:
# .github/workflows/ci.yml invokes `go` directly and is the definition. Both
# runners exist to reproduce CI locally, and a target here that disagrees with
# the workflow is a bug in the target.
#
#   make            same as `make check`
#   make check      what CI runs on every push: lint, test, stress
#   make build-all  cross-compile release binaries into dist/
#   make test       unit tests, race detector on
#   make stress     the build-tagged stress suite
#   make bench      benchmarks (see CONTRIBUTING.md before believing any number)
#   make allocprofile  rank allocation sites by count and by bytes
#   make fuzz       time-boxed fuzzing of every fuzz target in the tree
#   make lint       gofmt and go vet
#   make cover      coverage profile plus a per-package summary
#   make fmt        rewrite files that are not gofmt-clean
#   make clean      remove generated profiles
#
# Variables:
#   PKG=./disk/       restrict test/cover to one package      (default ./...)
#   FILTER=BFS        restrict to tests matching a pattern
#   BENCHTIME=5s      benchmark duration                      (default 5s)
#   FUZZTIME=60s      per-target fuzz duration                (default 30s)
#   ALLOCPROFILE_BENCHTIME=300x   iterations for make allocprofile     (default 300x)

# POSIX shell, stated rather than left to make's detection. This file is for
# Linux and macOS contributors; on Windows make defaults to cmd.exe and the
# shell-based targets below will not run — use test.ps1 there, which covers the
# same ground.
SHELL := /bin/sh

GO        ?= go
PKG       ?= ./...
FILTER    ?=
BENCHTIME ?= 5s
ALLOCPROFILE_BENCHTIME ?= 300x
FUZZTIME  ?= 30s

# Fuzz targets, as package:name pairs.
FUZZ_TARGETS := ./disk/:FuzzDeserialiseCSR \
                ./disk/:FuzzWALReplay \
                ./disk/:FuzzDecodeCatalogue \
                ./store/:FuzzParseNodeType \
                ./store/:FuzzParseEdgeType \
                ./merkle/:FuzzProofSoundness \
                ./merkle/:FuzzRootDistinguishesContent

ifdef FILTER
RUN_FLAG := -run $(FILTER)
else
RUN_FLAG :=
endif

.DEFAULT_GOAL := check
.PHONY: check build build-all test stress bench allocprofile fuzz lint fmt cover clean help

## check: lint, unit tests, stress — the same gates CI applies on a push.
check: lint test stress

## test: unit tests with the race detector.
test:
	$(GO) test $(PKG) -race -count=1 $(RUN_FLAG)

## stress: the stress suite, which lives behind a build tag and is therefore
## never compiled by `make test`. Like the rest of the engine's black-box
## tests it lives in ./tests/, not the root package.
stress:
	$(GO) test ./tests/ -tags=stress -race -count=1 -run $(if $(FILTER),$(FILTER),Test)

## bench: benchmarks. Read CONTRIBUTING.md first — a number from a single run on
## a loaded machine is not evidence. Interleave against a control.
bench:
	$(GO) test ./tests/ -tags=stress -bench=$(if $(FILTER),$(FILTER),.) -benchmem \
		-benchtime=$(BENCHTIME) -run='^$$'

## allocprofile: rank allocation sites for the benchmarks named by FILTER.
##
## Two rankings, deliberately: the largest allocation *count* and the largest
## allocated *bytes* are rarely the same site, and Phase 7's priority order
## ranks them differently — bytes predict GC and resident pressure, counts
## predict churn. Reading only one is how a phase optimises the wrong thing.
##
## -memprofilerate=1 is not optional. At the default rate Go samples small
## short-lived allocations, and the single-record marshal path — the largest win
## of Phase 7.1 — did not appear in the profile at all until this was set. A
## profile that cannot see what you are hunting is worse than no profile.
##
##   make allocprofile FILTER=BenchmarkIngest_AddNode_Disk
allocprofile:
	$(GO) test ./tests/ -tags=stress -run='^$$' -bench=$(if $(FILTER),$(FILTER),.) 		-benchmem -benchtime=$(ALLOCPROFILE_BENCHTIME) -test.memprofilerate=1 		-memprofile=alloc.prof
	@echo "==> by object count"
	@$(GO) tool pprof -alloc_objects -top -nodecount=20 alloc.prof
	@echo "==> by bytes"
	@$(GO) tool pprof -alloc_space -top -nodecount=20 alloc.prof
	@echo "==> line detail: go tool pprof -alloc_space -list='<func>' alloc.prof"

## fuzz: explore each target for FUZZTIME. This list must stay equal to what
## `grep -rn "^func Fuzz" --include=*.go .` reports — the two merkle targets sat
## outside it for a release, written and never run. Seed corpora and saved crash
## reproducers already run under `make test`; this looks for new ones.
##
## A failure writes the offending input to testdata/fuzz — that file is the bug
## report, and committing it makes it a permanent regression test.
fuzz:
	@for t in $(FUZZ_TARGETS); do \
		pkg=$${t%%:*}; name=$${t##*:}; \
		echo "==> $$name ($(FUZZTIME))"; \
		$(GO) test $$pkg -run=XXX -fuzz=$$name -fuzztime=$(FUZZTIME) || exit 1; \
	done

## lint: formatting and vet. gofmt is checked, not applied — use `make fmt`.
lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt-clean:"; echo "$$unformatted"; \
		echo "run 'make fmt'"; \
		exit 1; \
	fi
	$(GO) vet ./...

## fmt: rewrite files that are not gofmt-clean.
fmt:
	gofmt -w .

## cover: coverage profile and a per-package summary.
##
## -coverpkg is load-bearing. The root package's tests are black-box tests in
## ./tests/, so by default Go attributes their coverage to that package — which
## has no statements of its own — and reports the library as 0%. -coverpkg=./...
## measures the library, which is the number anyone running this wants.
cover:
	$(GO) test $(PKG) -coverprofile=coverage.out -covermode=atomic -coverpkg=./... $(RUN_FLAG)
	@$(GO) tool cover -func=coverage.out | tail -n 1
	@echo "full report: go tool cover -html=coverage.out"

## build: compile the library and the cmd/graphene inspector.
build:
	$(GO) build ./...

## build-all: cross-compile release binaries for every platform into dist/.
build-all:
	./build.sh

## clean: remove generated profiles and release binaries.
clean:
	rm -f coverage.out alloc.prof graphene graphene.exe
	rm -rf dist

## help: list targets.
help:
	@grep -E '^## [a-z]+:' $(MAKEFILE_LIST) | sed 's/^## /  /'
