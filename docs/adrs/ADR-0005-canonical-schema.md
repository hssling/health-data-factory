# ADR-0005: Canonical schema-first architecture

## Status
Accepted

## Decision
Normalize source records into a canonical parquet schema, then derive OMOP subset and FHIR R4 exports from canonical outputs.

## Consequences
- Consistent contract across diverse sources.
- Export logic remains isolated from source-specific connector behavior.

