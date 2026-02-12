from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from connectors.base import BaseConnector, ComplianceError, FetchResult


class LocalFileConnector(BaseConnector):
    name = "local_file"
    description = "Read local CSV/TSV file from disk for controlled offline ingestion."
    license = "Propagated from dataset registry"
    homepage_url = "local"
    access_type = "file"

    def fetch(
        self, params: dict[str, Any], run_dir: Path, html_allowlist: list[str]
    ) -> FetchResult:
        _ = html_allowlist
        source_path = Path(str(params["path"]))
        if not source_path.exists():
            raise ComplianceError(f"local source path does not exist: {source_path}")
        run_dir.mkdir(parents=True, exist_ok=True)
        output_path = run_dir / source_path.name
        output_path.write_bytes(source_path.read_bytes())
        return FetchResult(
            local_path=output_path,
            source_url=str(source_path),
            fetched_at=datetime.now(UTC).isoformat(),
            not_modified=False,
        )

