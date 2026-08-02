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
#   make test       unit tests, race detector on
#   make stress     the build-tagged stress suite
#   make bench      benchmarks (see CONTRIBUTING.md before believing any number)
#   make fuzz       time-boxed fuzzing of all four parser targets
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

# POSIX shell, stated rather than left to make's detection. This file is for
# Linux and macOS contributors; on Windows make defaults to cmd.exe and the
# shell-based targets below will not run — use test.ps1 there, which covers the
# same ground.
SHELL := /bin/sh

GO        ?= go
PKG       ?= ./...
FILTER    ?=
BENCHTIME ?= 5s
FUZZTIME  ?= 30s

# Fuzz targets, as package:name pairs.
FUZZ_TARGETS := ./disk/:FuzzDeserialiseCSR \
                ./disk/:FuzzWALReplay \
                ./store/:FuzzParseNodeType \
                ./store/:FuzzParseEdgeType

ifdef FILTER
RUN_FLAG := -run $(FILTER)
else
RUN_FLAG :=
endif

.DEFAULT_GOAL := check
.PHONY: check build test stress bench fuzz lint fmt cover clean help

## check: lint, unit tests, stress — the same gates CI applies on a push.
check: lint test stress

## test: unit tests with the race detector.
test:
	$(GO) test $(PKG) -race -count=1 $(RUN_FLAG)

## stress: the stress suite, which lives behind a build tag and is therefore
## never compiled by `make test`.
stress:
	$(GO) test . -tags=stress -race -count=1 -run $(if $(FILTER),$(FILTER),Test)

## bench: benchmarks. Read CONTRIBUTING.md first — a number from a single run on
## a loaded machine is not evidence. Interleave against a control.
bench:
	$(GO) test . -tags=stress -bench=$(if $(FILTER),$(FILTER),.) -benchmem \
		-benchtime=$(BENCHTIME) -run='^$$'

## fuzz: explore each parser for FUZZTIME. Seed corpora and saved crash
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
cover:
	$(GO) test $(PKG) -coverprofile=coverage.out -covermode=atomic $(RUN_FLAG)
	@$(GO) tool cover -func=coverage.out | tail -n 1
	@echo "full report: go tool cover -html=coverage.out"

## build: compile the library and the cmd/graphene inspector.
build:
	$(GO) build ./...

## clean: remove generated profiles.
clean:
	rm -f coverage.out graphene graphene.exe

## help: list targets.
help:
	@grep -E '^## [a-z]+:' $(MAKEFILE_LIST) | sed 's/^## /  /'
