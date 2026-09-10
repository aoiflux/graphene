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
#   make cpuprofile    rank functions by CPU time
#   make heapprofile   rank what a benchmark still holds when it finishes
#   make rssbench      process-residency baselines (see docs/benchmarks.md)
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
#   RSS_NODES=1500000             fixture size for make rssbench       (default 50000)
#   RSS_CYCLES=40                 rebuild cycles for make rssbench     (default 10)

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
RSS_NODES ?= 50000
RSS_CYCLES ?= 10
RSS_COUNT  ?= 3
FUZZTIME  ?= 30s

ifdef FILTER
RUN_FLAG := -run $(FILTER)
else
RUN_FLAG :=
endif

.DEFAULT_GOAL := check
.PHONY: check build build-all test stress bench allocprofile cpuprofile heapprofile rssbench fuzz fuzz-targets lint fmt cover clean help

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
	$(GO) test ./tests/ -tags=stress -run='^$$' -bench=$(if $(FILTER),$(FILTER),.) \
		-benchmem -benchtime=$(ALLOCPROFILE_BENCHTIME) -test.memprofilerate=1 \
		-memprofile=alloc.prof
	@echo "==> by object count"
	@$(GO) tool pprof -alloc_objects -top -nodecount=20 alloc.prof
	@echo "==> by bytes"
	@$(GO) tool pprof -alloc_space -top -nodecount=20 alloc.prof
	@echo "==> line detail: go tool pprof -alloc_space -list='<func>' alloc.prof"

## cpuprofile: rank functions by CPU time for the benchmarks named by FILTER.
##
## Separate from allocprofile because the default -memprofilerate=1 that target
## needs makes every allocation a sampled event, which distorts a CPU profile
## badly enough to rank the wrong functions.
##
##   make cpuprofile FILTER=BenchmarkPointLookupNode_Disk
cpuprofile:
	$(GO) test ./tests/ -tags=stress -run='^$$' -bench=$(if $(FILTER),$(FILTER),.) \
		-benchtime=$(BENCHTIME) -cpuprofile=cpu.prof
	@$(GO) tool pprof -top -nodecount=25 cpu.prof
	@echo "==> line detail: go tool pprof -list='<func>' cpu.prof"

## heapprofile: rank what a benchmark still holds when it finishes.
##
## The distinction from allocprofile is the whole point and is easy to lose:
## allocprofile ranks bytes *allocated*, which predicts GC pressure and churn;
## this ranks bytes *retained*, which is what a memory budget actually
## constrains. A path can top one ranking and be absent from the other.
##
## The default sampling rate is deliberate here — unlike allocprofile, which
## needs rate 1 to see short-lived allocations, retained structures are large
## and long-lived and the default rate finds them without distorting the run.
##
##   make heapprofile FILTER=BenchmarkRSS_Open
heapprofile:
	$(GO) test ./tests/ -tags=stress -run='^$$' -bench=$(if $(FILTER),$(FILTER),.) \
		-benchtime=$(BENCHTIME) -memprofile=heap.prof
	@echo "==> in-use bytes"
	@$(GO) tool pprof -inuse_space -top -nodecount=25 heap.prof
	@echo "==> in-use objects"
	@$(GO) tool pprof -inuse_objects -top -nodecount=25 heap.prof

## rssbench: the program's process-residency baselines.
##
## Reports what the operating system holds, split into anonymous and file-backed
## pages, which is the only measurement a RAM ceiling can be judged against —
## the heap figures the other targets report cannot see a memory-mapped image at
## all. Runs one fixture size per invocation on purpose: a process that builds
## several large fixtures reports the high-water mark of the largest.
##
##   make rssbench FILTER=RSS_Open RSS_NODES=1500000
rssbench:
	GRAPHENE_RSS_NODES=$(RSS_NODES) GRAPHENE_RSS_CYCLES=$(RSS_CYCLES) \
		$(GO) test ./tests/ -tags=stress -run='^$$' \
		-bench=$(if $(FILTER),$(FILTER),RSS_) -benchtime=1x -count=$(RSS_COUNT) -timeout=120m

## fuzz: explore every fuzz target in the tree for FUZZTIME each.
##
## The list comes from `make fuzz-targets`, which derives it from the tree. It
## used to be a variable in this file, and the two merkle targets sat outside it
## for a release — written, and never run. Seed corpora and saved crash
## reproducers already run under `make test`; this looks for new ones.
##
## A failure writes the offending input to testdata/fuzz — that file is the bug
## report, and committing it makes it a permanent regression test.
fuzz:
	@targets=$$($(MAKE) --no-print-directory fuzz-targets); \
	if [ -z "$$targets" ]; then echo "no fuzz targets found"; exit 1; fi; \
	for t in $$targets; do \
		pkg=$${t%%:*}; name=$${t##*:}; \
		echo "==> $$name ($(FUZZTIME))"; \
		$(GO) test $$pkg -run=XXX -fuzz=$$name -fuzztime=$(FUZZTIME) || exit 1; \
	done

## fuzz-targets: print every fuzz target in the tree as package:name.
##
## Derived, never written down. The list used to be a variable in this file and
## a second copy in the CI matrix, and both fell behind the tree: targets were
## added and went unfuzzed while `make fuzz` and the nightly job each went on
## reporting success. A list of what to test is the one list that must not be
## maintained by hand.
fuzz-targets:
	@for pkg in $$($(GO) list ./...); do \
		for name in $$($(GO) test -list '^Fuzz' $$pkg 2>/dev/null | grep '^Fuzz' || true); do \
			echo "$$pkg:$$name"; \
		done; \
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
