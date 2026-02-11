# health-data-factory

Continuously build compliant, validated, versioned health datasets from open internet sources and publish analytics-ready artifacts with OMOP + FHIR exports.

## Why this repository
- Compliant ingestion by design: robots/allowlist policies are code-enforced.
- Reproducible data products: bronze/silver/gold Parquet pipeline with build manifests.
- Standards-ready outputs: canonical internal schema + OMOP subset + FHIR R4 bundle.
- Production workflows: FastAPI catalog, Prefect orchestration, GitHub Actions CI/CD, DVC versioning.

## Architecture
```text
connectors -> bronze -> transforms -> silver -> validators -> gold -> exporters -> manifests -> API
```

## Stack
- Python 3.11+
- `uv` package/dependency management
- Prefect 2.x orchestration
- pandera validation
- FastAPI API service
- DVC artifact versioning
- `just` task runner
- ruff + mypy + pytest quality gates

## Quickstart
```bash
just setup
just run demo_dataset
just serve
```

## CLI
```bash
uv run hdb list
uv run hdb run demo_dataset --full-refresh
uv run hdb validate demo_dataset
uv run hdb export-omop demo_dataset
uv run hdb export-fhir demo_dataset
uv run hdb publish-hf demo_dataset
uv run hdb publish-kaggle demo_dataset
uv run hdb publish-all demo_dataset
uv run hdb serve-api
```

## Platform Publishing
- Hugging Face Dataset + Model publication is supported in CI and CLI.
- Kaggle publication is supported for dataset artifacts and model bundle artifacts.
- Each build also produces a baseline model artifact in `data/gold/<dataset>/<timestamp>/models/`.

## CI/CD
- `ci.yml`: ruff, mypy, pytest on PRs and main.
- `nightly-build.yml`: scheduled dataset build + validation + exporters + artifact upload.
- `deploy.yml`: builds/pushes API and worker images to GHCR and supports optional webhook deployment.

## DVC
Default remote: local filesystem (`./dvc-remote`).

Switch to S3:
```bash
uv run dvc remote modify localremote url s3://my-bucket/health-data-factory
uv run dvc remote modify localremote endpointurl https://s3.amazonaws.com
```

Switch to Cloudflare R2:
```bash
uv run dvc remote modify localremote url s3://my-r2-bucket/health-data-factory
uv run dvc remote modify localremote endpointurl https://<accountid>.r2.cloudflarestorage.com
```

## Compliance + safety
- No illegal scraping, paywall bypass, or ToS evasion.
- HTML connectors require robots compliance and explicit allowlisting.
- PII heuristics can hard-fail builds (`pii_policy.block_if_suspected`).
- License metadata is propagated into manifests and dataset outputs.

## Documentation index
- `docs/README.md`: developer guide
- `docs/CONTRIBUTING.md`: collaboration and PR workflow
- `docs/COMPLIANCE.md`: policy and connector rules
- `docs/SECURITY.md`: security reporting and controls
- `docs/DEPLOYMENT.md`: CI/CD deployment playbook
- `docs/adrs/`: architecture decisions
