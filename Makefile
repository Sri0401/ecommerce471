# Run inside WSL2 Ubuntu from the repo root (ADR-007).
COMPOSE  := docker compose -f deploy/compose/compose.yml
LIVE_DIR ?= data/prep/rees46/split/live
SCENARIO ?= clean

.PHONY: help up-stream down nuke ps register replay test test-integration lint

help:  ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

up-stream:  ## start Kafka + Karapace and wait until healthy
	$(COMPOSE) --profile stream up -d --wait

down:  ## stop all containers (data volumes kept)
	$(COMPOSE) --profile stream down

nuke:  ## stop all containers AND delete Kafka data
	$(COMPOSE) --profile stream down -v

ps:  ## container status and memory
	$(COMPOSE) --profile stream ps -a
	docker stats --no-stream --format "{{.Name}}: {{.MemUsage}}"

register:  ## register the event contract in the schema registry
	uv run python -m shoppulse_contracts register

replay:  ## replay the live period, e.g. make replay SCENARIO=chaos
	uv run python -m shoppulse_replay --scenario services/replay/scenarios/$(SCENARIO).toml \
		--live-dir $(LIVE_DIR)

test:  ## unit tests (no containers needed)
	uv run pytest

test-integration:  ## tests against the running stream profile
	uv run pytest -m integration

lint:  ## ruff + mypy
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy
