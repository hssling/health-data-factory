# ADR-0002: Use Prefect 2.x for orchestration

## Status
Accepted

## Decision
Use Prefect flows to orchestrate extraction, transformation, validation, export, and manifest writing.

## Consequences
- Observable flow runs with retries and task boundaries.
- Straightforward local execution without requiring a complex control plane.

