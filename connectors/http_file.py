from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from connectors.base import BaseConnector, ComplianceError, FetchResult


class HttpCsvConnector(BaseConnector):
    name = "http_csv"
    description = "Download CSV/TSV files over HTTP with caching and conditional requests."
    license = "Propagated from dataset registry"
    homepage_url = "https://github.com"
    access_type = "file"

    def fetch(
        self, params: dict[str, Any], run_dir: Path, html_allowlist: list[str]
    ) -> FetchResult:
        url = str(params["url"])
        access_type = str(params.get("access_type", self.access_type))
        expected_content_type = str(params.get("expected_content_type", ""))
        if access_type == "html":
            self._check_html_compliance(url, html_allowlist)
        elif access_type not in {"file", "api", "rss"}:
            raise ComplianceError(f"unsupported access_type: {access_type}")

        headers, metadata_path = self._headers_for(url)
        cache_prefix = self._cache_prefix(url)
        cache_data_path = self.cache_dir / f"{cache_prefix}.bin"
        response = self._get(url=url, headers=headers)

        if response.status_code == 304 and cache_data_path.exists():
            run_dir.mkdir(parents=True, exist_ok=True)
            output_path = run_dir / "raw_source.tsv"
            output_path.write_bytes(cache_data_path.read_bytes())
            return FetchResult(
                local_path=output_path,
                source_url=url,
                fetched_at=datetime.now(UTC).isoformat(),
                not_modified=True,
            )

        content_type = response.headers.get("Content-Type", "")
        accepted_types = [item.strip() for item in expected_content_type.split("|") if item.strip()]
        if accepted_types and not any(item in content_type for item in accepted_types):
            message = (
                "unexpected content type. "
                f"expected one of '{accepted_types}', got '{content_type}'"
            )
            raise ComplianceError(
                message
            )

        run_dir.mkdir(parents=True, exist_ok=True)
        output_path = run_dir / "raw_source.tsv"
        output_path.write_bytes(response.content)
        cache_data_path.write_bytes(response.content)
        self._write_metadata(metadata_path, response)

        return FetchResult(
            local_path=output_path,
            source_url=url,
            fetched_at=datetime.now(UTC).isoformat(),
            not_modified=False,
        )
