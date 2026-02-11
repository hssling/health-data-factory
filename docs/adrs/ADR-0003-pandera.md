# ADR-0003: Use pandera for data validation

## Status
Accepted

## Decision
Use pandera schemas and checks to enforce canonical data quality before publishing gold artifacts.

## Consequences
- Validation logic stays explicit and testable.
- Build failures occur before exports and API publication.

