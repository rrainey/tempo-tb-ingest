# tempo-tb-ingest development gates (see docs/implementation-plan.md)
#
#   make check        offline verification gate: lint + types + unit/integration
#   make test         pytest only (offline tiers)
#   make live         hardware tier: read-only, any Tempo-BT in range
#   make destructive  hardware tier: dev device + /SD:/testok marker required

.PHONY: check lint type test live destructive sync

sync:
	uv sync --extra dev

lint:
	uv run ruff check tempo_tb_ingest tests
	uv run ruff format --check tempo_tb_ingest tests

type:
	uv run mypy tempo_tb_ingest tests

test:
	uv run pytest

check: lint type test

live:
	uv run pytest -m live --override-ini addopts=

destructive:
	uv run pytest -m destructive --override-ini addopts=
