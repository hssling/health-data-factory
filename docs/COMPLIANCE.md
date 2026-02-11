# Compliance Policy

## Core rules
- Never bypass paywalls, authentication gates, CAPTCHAs, or contractual restrictions.
- Respect source Terms of Service and robots directives.
- HTML scraping is blocked unless the source domain is explicitly allowlisted in `datasets/registry.yaml`.
- Send explicit user-agent metadata for all outbound requests.
- Propagate source license metadata into build manifests and dataset output folders.

## Connector enforcement
- Connectors must declare `access_type` (`api`, `rss`, `html`, `file`).
- For `html` access:
  - `robots.txt` is checked before fetching.
  - domain must be present in `html_allowlist`.
  - build fails on violation.

## PII controls
- PII heuristic checks run on transformed datasets.
- Builds fail when likely identifiers are detected and `block_if_suspected` is true.
- `declared_deidentified=true` does not bypass checks; it only documents source intent.

