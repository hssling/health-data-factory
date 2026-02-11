# Developer Guide

`health-data-factory` is a monorepo for compliant and repeatable health dataset construction.

## Local setup
```bash
just setup
just ci
```

Run demo pipeline:
```bash
just run demo_dataset
```

Serve API:
```bash
just serve
```

## Key folders
- `connectors/`: source extractors with compliance checks and caching.
- `pipelines/`: Prefect flows and build engine.
- `transforms/`: source-specific normalization into canonical schema.
- `validators/`: pandera schema suites.
- `exporters/`: OMOP subset + FHIR bundle export logic.
- `datasets/registry.yaml`: declarative dataset registry.
- `apps/api/`: dataset catalog + latest manifest access.
- `manifests/`: build metadata, codebook, dictionary, license records.

## Build lifecycle
1. Resolve dataset config from `datasets/registry.yaml`.
2. Fetch source data through connector policies.
3. Persist bronze raw payload.
4. Normalize into canonical dataframe and write silver.
5. Validate with pandera and PII heuristics.
6. Write gold canonical parquet.
7. Export OMOP and FHIR artifacts.
8. Write manifest with provenance, hashes, row counts, validation results, and outputs.

## API endpoints
- `GET /health`
- `GET /datasets`
- `GET /datasets/{dataset_id}/latest-manifest`
- `GET /datasets/{dataset_id}/artifacts`

## CI/CD overview
- PR checks: ruff, mypy, pytest.
- Nightly schedule: run demo dataset, validate, export, publish workflow artifacts.
- Deploy workflow: build/push GHCR images and optional webhook deployment.

## Operations references
- Compliance: `docs/COMPLIANCE.md`
- Security: `docs/SECURITY.md`
- Deployment: `docs/DEPLOYMENT.md`
- ADRs: `docs/adrs/`

