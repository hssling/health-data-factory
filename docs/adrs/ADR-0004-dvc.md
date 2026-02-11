# ADR-0004: Use DVC for dataset versioning

## Status
Accepted

## Decision
Use DVC with a local remote by default to track data artifacts while keeping large outputs out of git history.

## Consequences
- Artifact lineage can be managed and synchronized independently of source code.
- Easy migration to S3/R2 by changing remote settings.

