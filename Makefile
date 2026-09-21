# Single entry point for everything. `make help` lists targets.
#
# .DEFAULT_GOAL and the help target exist so a newcomer running a bare `make`
# gets a menu rather than a build they did not ask for.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

MVN ?= mvn
CTL := ./bin/turnstilectl

.PHONY: help
help: ## show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nTurnstile\n\n"} \
	  /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 } \
	  END { printf "\n" }' $(MAKEFILE_LIST)

.PHONY: build
build: ## compile
	$(MVN) -q -B compile

.PHONY: test
test: test-java test-shell ## run every suite

.PHONY: test-java
test-java: ## run the JUnit suite
	$(MVN) -B test

.PHONY: test-shell
test-shell: ## run the shell suite
	./test/shell/run-tests.sh

.PHONY: prove
prove: ## simulate a contended sale and audit the log for oversells
	$(CTL) prove

.PHONY: doctor
doctor: ## check this machine can build and run the project
	$(CTL) doctor

.PHONY: lint
lint: ## shellcheck every script (requires shellcheck)
	@command -v shellcheck >/dev/null 2>&1 || { echo "shellcheck not installed"; exit 1; }
	shellcheck bin/turnstilectl scripts/*.sh scripts/lib/*.sh scripts/chaos/*.sh test/shell/*.sh

.PHONY: clean
clean: ## remove build output
	$(MVN) -q -B clean
	rm -rf target

.PHONY: mutate
mutate: ## break the code on purpose and check the tests notice (slow)
	$(CTL) mutate
