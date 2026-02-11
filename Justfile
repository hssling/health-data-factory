set shell := ["powershell", "-NoProfile", "-Command"]

setup:
	uv sync --all-extras

setup-min:
	uv sync

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy src apps connectors pipelines transforms validators exporters tests

test:
	uv run pytest -q

ci:
	uv run ruff check .
	uv run mypy src apps connectors pipelines transforms validators exporters tests
	uv run pytest -q

run dataset_id="demo_dataset" full_refresh="false":
	if ("{{full_refresh}}" -eq "true") { uv run hdb run {{dataset_id}} --full-refresh } else { uv run hdb run {{dataset_id}} }

run-all:
	uv run hdb run-all

validate dataset_id="demo_dataset":
	uv run hdb validate {{dataset_id}}

export-omop dataset_id="demo_dataset":
	uv run hdb export-omop {{dataset_id}}

export-fhir dataset_id="demo_dataset":
	uv run hdb export-fhir {{dataset_id}}

serve:
	uv run hdb serve-api
