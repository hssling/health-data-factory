# Contributing Guide

## Collaboration workflow
1. Create a feature branch from `main`.
2. Keep changes focused and small.
3. Run local quality checks before opening a PR.
4. Open a PR with:
   - problem statement
   - design notes
   - test evidence
   - compliance impact (if connector/ingestion changes)

## Local commands
```bash
just setup
just ci
```

## Dataset changes checklist
- Add/update dataset entry in `datasets/registry.yaml`.
- Ensure connector access type and compliance rules are respected.
- Add or update transform logic to canonical schema.
- Add/adjust pandera checks.
- Add tests for parser, validation, and outputs.
- Confirm PII gate behavior.

## Definition of done
- `ruff`, `mypy`, and `pytest` pass.
- Build manifest includes hashes, provenance, license, and validation results.
- Documentation reflects user-facing behavior changes.

