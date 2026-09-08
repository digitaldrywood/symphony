SHELL := /bin/bash

BINARY_NAME := detent
BINARY_PATH := tmp/$(BINARY_NAME)
CMD_PACKAGE := ./cmd/detent
VERSION ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS ?= -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)
DEV_VERSION ?= dev
COVERPROFILE := tmp/coverage.out
COVERPROFILE_RAW := tmp/coverage.raw.out
COVERAGE_THRESHOLD := 70.0
PACKAGE_COVERAGE_FLOOR := 50
PACKAGE_COVERAGE_EXCEPTIONS := scripts/coverage-exceptions.txt
MODERNIZE_FIX_FLAGS ?= -newexpr=false
TEMPL ?= go run github.com/a-h/templ/cmd/templ@v0.3.1001
TAILWIND_INPUT ?= static/css/input.css
TAILWIND_OUTPUT ?= static/css/output.css
SQLC_CONFIG ?= sqlc/sqlc.yaml
MIGRATIONS_DIR ?= internal/store/migrations
GOOSE_DRIVER ?= sqlite3
DATABASE_URL ?= tmp/detent.db
NILAWAY_VERSION ?= v0.0.0-20260612163715-2d8907f431ca
NILAWAY ?= go run go.uber.org/nilaway/cmd/nilaway@$(NILAWAY_VERSION)
NILAWAY_INCLUDE_PKGS ?= github.com/digitaldrywood/detent
GOVULNCHECK_VERSION ?= v1.6.0
GOVULNCHECK ?= go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
# Upstream issue: https://github.com/securego/gosec/issues/1712
GOSEC_VERSION ?= v2.28.0
GOSEC_BINARY ?= tmp/gosec-$(GOSEC_VERSION)-deterministic
GOSEC_PATCH ?= scripts/gosec-v2.28.0-deterministic.patch
GOSEC_DETERMINISM_RUNS ?= 8
GO_TEST := env -u DETENT_API_TOKEN go test
HUB_RACE_TIMEOUT ?= 15m
HUB_RACE_PARALLEL ?= 2
GOLANGCI_LINT_VERSION_FILE := .golangci-version
GOLANGCI_LINT_VERSION := $(shell cat $(GOLANGCI_LINT_VERSION_FILE))
GOLANGCI_LINT_TOOLCHAIN := $(shell awk '/^toolchain / { print $$2 }' go.mod)
GOLANGCI_LINT_DIR := $(CURDIR)/tmp/tools/golangci-lint/$(GOLANGCI_LINT_VERSION)/$(GOLANGCI_LINT_TOOLCHAIN)
GOLANGCI_LINT := $(GOLANGCI_LINT_DIR)/golangci-lint
# G115: existing conversions are bounded or intentionally preserve platform/API widths.
# G301: shared runtime, service, and artifact directories intentionally require traversal access.
# G304: Detent intentionally reads operator-selected config, workflow, and workspace paths.
# G306: flagged files are non-secret configs, manifests, screenshots, and generated artifacts.
GOSEC_EXCLUDES ?= G115,G301,G304,G306
GOSEC_EXCLUDE_DIRS ?= .detent
GOSEC_EXCLUDE_DIR_FLAGS := $(addprefix -exclude-dir=,$(GOSEC_EXCLUDE_DIRS))
CHECK_LOCK_WAIT ?= 15m

.PHONY: dev generate check-migrations check-generated css css-watch build test test-race test-race-hub test-cover test-cover-packages soak visual-e2e visual-e2e-update lint vet gosec-build security-gosec-determinism security check check-unlocked modernize-check nilaway-audit release-snapshot sqlc db-migrate setup clean help

dev:
	@mkdir -p tmp
	@if [ -f tmp/air-combined.log ]; then \
		mv tmp/air-combined.log tmp/air-combined-$$(date +%Y%m%d-%H%M%S).log; \
	fi
	@ls -t tmp/air-combined-*.log 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true
	@ENV=dev LOG_LEVEL=debug DETENT_AIR_VERSION=$(DEV_VERSION) air 2>&1 | tee tmp/air-combined.log

generate:
	@go generate ./...
	@if [ -n "$$(git ls-files --others --exclude-standard -- '*.templ'; git ls-files -- '*.templ')" ]; then \
		$(TEMPL) generate; \
	else \
		echo "No templ files found; skipping templ generate."; \
	fi
	@$(MAKE) sqlc
	@$(MAKE) css

check-migrations:
	go run ./tools/migrationcheck

check-generated:
	go run ./internal/config/cmd/configdoc -root . -check

css:
	@if [ -f "$(TAILWIND_INPUT)" ]; then \
		if [ -f package-lock.json ] && [ ! -x node_modules/.bin/tailwindcss ]; then npm ci; elif [ -f package.json ] && [ ! -x node_modules/.bin/tailwindcss ]; then npm install; fi; \
		mkdir -p "$$(dirname "$(TAILWIND_OUTPUT)")"; \
		node_modules/.bin/tailwindcss -i "$(TAILWIND_INPUT)" -o "$(TAILWIND_OUTPUT)" --minify; \
	else \
		echo "No Tailwind input at $(TAILWIND_INPUT); skipping CSS build."; \
	fi

css-watch:
	@if [ -f "$(TAILWIND_INPUT)" ]; then \
		if [ -f package-lock.json ] && [ ! -x node_modules/.bin/tailwindcss ]; then npm ci; elif [ -f package.json ] && [ ! -x node_modules/.bin/tailwindcss ]; then npm install; fi; \
		mkdir -p "$$(dirname "$(TAILWIND_OUTPUT)")"; \
		node_modules/.bin/tailwindcss -i "$(TAILWIND_INPUT)" -o "$(TAILWIND_OUTPUT)" --watch; \
	else \
		echo "No Tailwind input at $(TAILWIND_INPUT); skipping CSS watch."; \
	fi

build: generate
	@mkdir -p tmp
	go build -ldflags "$(LDFLAGS)" -o $(BINARY_PATH) $(CMD_PACKAGE)

test:
	$(GO_TEST) ./...

test-race: test-race-hub
	@packages="$$(go list ./...)" && \
	packages="$$(printf '%s\n' "$$packages" | awk '$$0 != "github.com/digitaldrywood/detent/internal/hubserver"')" && \
	$(GO_TEST) -race $$packages

test-race-hub:
	env -u DETENT_API_TOKEN go run ./tools/testgate -race -parallel $(HUB_RACE_PARALLEL) -timeout $(HUB_RACE_TIMEOUT) -output tmp/hub-race-evidence ./internal/hubserver

test-cover:
	@mkdir -p tmp
	$(GO_TEST) -coverprofile=$(COVERPROFILE_RAW) ./...
	@awk 'NR == 1 || ($$1 !~ /_templ\.go:/ && $$1 !~ /\/internal\/store\/sqlc\// && $$1 !~ /\/internal\/database\/sqlc\//)' "$(COVERPROFILE_RAW)" > "$(COVERPROFILE)"
	@coverage="$$(go tool cover -func=$(COVERPROFILE) | awk '/^total:/ { gsub(/%/, "", $$3); print $$3 }')"; \
	awk -v coverage="$$coverage" -v threshold="$(COVERAGE_THRESHOLD)" 'BEGIN { \
		if (coverage + 0 < threshold + 0) { \
			printf "coverage %.1f%% is below %.1f%%\n", coverage, threshold; \
			exit 1; \
		} \
		printf "coverage %.1f%% meets %.1f%% threshold\n", coverage, threshold; \
	}'

test-cover-packages: test-cover
	go run ./tools/covercheck -profile $(COVERPROFILE) -floor $(PACKAGE_COVERAGE_FLOOR) -exceptions $(PACKAGE_COVERAGE_EXCEPTIONS)

soak:
	DETENT_RUN_SOAK_TESTS=1 $(GO_TEST) ./internal/orchestrator -run '^(TestSoak|TestDispatchParityAdversarialFixtures)' -count=1

visual-e2e: build
	@if [ ! -x node_modules/.bin/playwright ]; then npm ci; fi
	DETENT_BINARY="$(CURDIR)/$(BINARY_PATH)" node_modules/.bin/playwright test

visual-e2e-update: build
	@if [ ! -x node_modules/.bin/playwright ]; then npm ci; fi
	DETENT_BINARY="$(CURDIR)/$(BINARY_PATH)" node_modules/.bin/playwright test --update-snapshots

lint: $(GOLANGCI_LINT)
	GOTOOLCHAIN="$(GOLANGCI_LINT_TOOLCHAIN)" "$(GOLANGCI_LINT)" run --timeout=5m

$(GOLANGCI_LINT):
	@mkdir -p "$(GOLANGCI_LINT_DIR)"
	GOTOOLCHAIN="$(GOLANGCI_LINT_TOOLCHAIN)" GOBIN="$(GOLANGCI_LINT_DIR)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

vet:
	go vet ./...

gosec-build:
	./scripts/build-gosec.sh "$(GOSEC_VERSION)" "$(GOSEC_PATCH)" "$(GOSEC_BINARY)"

security-gosec-determinism: gosec-build
	./scripts/check-gosec-determinism.sh "$(GOSEC_BINARY)" "$(GOSEC_DETERMINISM_RUNS)"

security: security-gosec-determinism
	$(GOVULNCHECK) ./...
	$(GOSEC_BINARY) -quiet -exclude-generated -severity=medium -confidence=medium -exclude=$(GOSEC_EXCLUDES) $(GOSEC_EXCLUDE_DIR_FLAGS) ./...

nilaway-audit:
	$(NILAWAY) -include-pkgs=$(NILAWAY_INCLUDE_PKGS) ./...

check: check-migrations
	@common_dir="$$(git rev-parse --path-format=absolute --git-common-dir)" && \
	go run ./tools/checklock -lock "$$common_dir/detent-validation.lock" -wait-timeout "$(CHECK_LOCK_WAIT)" -- $(MAKE) check-unlocked

check-unlocked: check-migrations check-generated build lint vet nilaway-audit test-race test-cover test-cover-packages
	@echo "All checks passed."

modernize-check:
	go fix -diff $(MODERNIZE_FIX_FLAGS) ./...

release-snapshot:
	goreleaser release --snapshot --clean

sqlc:
	@if [ -f "$(SQLC_CONFIG)" ]; then \
		sqlc generate -f "$(SQLC_CONFIG)"; \
	else \
		echo "No sqlc config at $(SQLC_CONFIG); skipping sqlc generate."; \
	fi

db-migrate:
	@if [ -d "$(MIGRATIONS_DIR)" ]; then \
		mkdir -p "$$(dirname "$(DATABASE_URL)")"; \
		goose -dir "$(MIGRATIONS_DIR)" "$(GOOSE_DRIVER)" "$(DATABASE_URL)" up; \
	else \
		echo "No migrations directory at $(MIGRATIONS_DIR); skipping database migration."; \
	fi

setup: $(GOLANGCI_LINT)
	go install github.com/air-verse/air@latest
	go install github.com/a-h/templ/cmd/templ@v0.3.1001
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
	go install github.com/pressly/goose/v3/cmd/goose@latest
	@if [ -f package.json ]; then npm install; fi

clean:
	rm -rf tmp

help:
	@echo "Available targets:"
	@echo "  dev          Run Air with dev logging and combined log rotation"
	@echo "  generate     Run go generate, templ, sqlc, and Tailwind"
	@echo "  css          Build Tailwind CSS"
	@echo "  css-watch    Watch and rebuild Tailwind CSS"
	@echo "  build        Build $(BINARY_NAME)"
	@echo "  test         Run Go tests"
	@echo "  test-race    Run Go tests with the race detector"
	@echo "  test-race-hub  Run the complete Hub race suite with timing evidence"
	@echo "  test-cover   Run Go coverage with a $(COVERAGE_THRESHOLD)% minimum"
	@echo "  test-cover-packages  Run per-package coverage floor checks"
	@echo "  soak         Run opt-in orchestrator incident and adversarial soak tests"
	@echo "  visual-e2e   Run Playwright visual layout tests"
	@echo "  visual-e2e-update  Update Playwright visual baselines"
	@echo "  lint         Run golangci-lint"
	@echo "  security-gosec-determinism  Verify deterministic G705 caller traversal"
	@echo "  security     Run govulncheck and gosec security scans"
	@echo "  check        Run the local validation gate, including NilAway"
	@echo "  modernize-check  Run the Go modernizer diff check"
	@echo "  nilaway-audit  Run the local NilAway audit"
	@echo "  release-snapshot  Build local GoReleaser snapshot archives"
	@echo "  sqlc         Generate sqlc output"
	@echo "  db-migrate   Run goose migrations"
	@echo "  setup        Install development tools"
