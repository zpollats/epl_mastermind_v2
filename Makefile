.PHONY: help sync ingest ingest-fpl ingest-football-data transform-dev transform-prod run test info ui fetchdf set-gh-secrets

SHELL := /usr/bin/env bash
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
ENV_FILE ?= $(ROOT)/.env
DEV ?= dev
QUERY ?=

define WITH_ENV
set -a; [[ -f "$(ENV_FILE)" ]] && . "$(ENV_FILE)"; set +a;
endef

help:
	@echo ""
	@echo "epl_mastermind — Premier League Analytics Lakehouse"
	@echo ""
	@echo "Setup:  make sync"
	@echo ""
	@echo "Ingest: make ingest | ingest-fpl | ingest-football-data"
	@echo "Transform: make transform-dev | transform-prod"
	@echo "Full run:  make run  (ingest + transform prod)"
	@echo ""
	@echo "Quality: make test"
	@echo "Explore: make ui | make fetchdf QUERY='...'"
	@echo ""

sync:
	cd "$(ROOT)" && uv sync

ingest-fpl:
	@$(WITH_ENV) cd "$(ROOT)/dlt" && uv run python fpl_pipeline.py

ingest-football-data:
	@$(WITH_ENV) cd "$(ROOT)/dlt" && uv run python football_data_pipeline.py

ingest: ingest-fpl ingest-football-data

transform-dev:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh plan "$(DEV)"

transform-prod:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh plan

sqlmesh-run:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh migrate && uv run sqlmesh run prod

run: ingest sqlmesh-run

test:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh test

info:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh info

ui:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh ui

fetchdf:
	@if [[ -z "$(strip $(QUERY))" ]]; then \
		echo "ERROR: QUERY is required. Usage: make fetchdf QUERY='select ...'"; \
		exit 2; \
	fi
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh fetchdf "$(QUERY)"

set-gh-secrets:
	cd "$(ROOT)" && ENV_FILE="$(ROOT)/.env" ./scripts/set_github_actions_secrets_from_env.sh
