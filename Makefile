.PHONY: help sync ingest ingest-fpl ingest-football-data plan plan-dev run test ui clean

SHELL := /usr/bin/env bash
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
ENV_FILE ?= $(ROOT)/.env

define WITH_ENV
set -a; [[ -f "$(ENV_FILE)" ]] && . "$(ENV_FILE)"; set +a;
endef

help:
	@echo ""
	@echo "epl_mastermind — FPL Analytics Pipeline"
	@echo ""
	@echo "  make sync              Install dependencies"
	@echo "  make ingest-fpl        Pull FPL API data (no key needed)"
	@echo "  make ingest-fd         Pull football-data.org data (needs API key)"
	@echo "  make ingest            Run both ingestion pipelines"
	@echo "  make plan-dev          SQLMesh plan in dev environment"
	@echo "  make plan              SQLMesh plan in prod"
	@echo "  make run               Full pipeline: ingest + plan prod"
	@echo "  make test              Run SQLMesh tests"
	@echo "  make ui                Launch SQLMesh web UI"
	@echo "  make marimo            Launch Marimo notebook server"
	@echo "  make clean             Delete local data"
	@echo ""

sync:
	cd "$(ROOT)" && uv sync

ingest-fpl:
	@$(WITH_ENV) cd "$(ROOT)/dlt" && uv run python fpl_pipeline.py

ingest-fd:
	@$(WITH_ENV) cd "$(ROOT)/dlt" && uv run python football_data_pipeline.py

ingest: ingest-fpl ingest-fd

plan-dev:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh plan dev

plan:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh plan

run: ingest plan

test:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh test

ui:
	@$(WITH_ENV) cd "$(ROOT)/sqlmesh" && uv run sqlmesh ui

marimo:
	@$(WITH_ENV) cd "$(ROOT)" && uv run marimo edit notebooks/

clean:
	rm -rf "$(ROOT)/data/"*.duckdb "$(ROOT)/data/"*.wal
	rm -rf "$(ROOT)/dlt/.dlt" "$(ROOT)/dlt/fpl_pipeline" "$(ROOT)/dlt/football_data_pipeline"