# ADR-0001: Use uv for dependency management

## Status
Accepted

## Decision
Use `uv` with `pyproject.toml` and `uv.lock` for consistent and fast dependency resolution.

## Consequences
- Fast environment setup in CI and local development.
- Locked dependency graph for reproducible builds.

