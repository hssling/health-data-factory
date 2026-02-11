from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from connectors.base import ComplianceError
from connectors.http_file import HttpCsvConnector


class FakeResponse:
    def __init__(
        self, status_code: int, content: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError("http error")


def test_connector_writes_cache(monkeypatch: Any, tmp_path: Path) -> None:
    connector = HttpCsvConnector(cache_dir=tmp_path / "cache", user_agent="ua")
    run_dir = tmp_path / "run"

    def fake_get(url: str, headers: dict[str, str]) -> FakeResponse:
        return FakeResponse(
            status_code=200,
            content=b"Entity\tCode\tYear\tLife expectancy\nA\tAAA\t2000\t70.0\n",
            headers={"Content-Type": "text/tab-separated-values", "ETag": "abc"},
        )

    monkeypatch.setattr(connector, "_get", fake_get)
    result = connector.fetch(
        {
            "url": "https://example.org/file.tsv",
            "expected_content_type": "text/tab-separated-values",
        },
        run_dir=run_dir,
        html_allowlist=[],
    )
    assert result.local_path.exists()
    assert any(path.suffix == ".bin" for path in (tmp_path / "cache").iterdir())


def test_connector_uses_cache_on_not_modified(monkeypatch: Any, tmp_path: Path) -> None:
    connector = HttpCsvConnector(cache_dir=tmp_path / "cache", user_agent="ua")
    run_dir = tmp_path / "run"
    cache_prefix = connector._cache_prefix("https://example.org/file.tsv")
    cache_data = connector.cache_dir / f"{cache_prefix}.bin"
    cache_data.parent.mkdir(parents=True, exist_ok=True)
    cache_data.write_bytes(b"Entity\tCode\tYear\tLife expectancy\nA\tAAA\t2000\t70.0\n")

    def fake_get(url: str, headers: dict[str, str]) -> FakeResponse:
        return FakeResponse(
            status_code=304, content=b"", headers={"Content-Type": "text/tab-separated-values"}
        )

    monkeypatch.setattr(connector, "_get", fake_get)
    result = connector.fetch(
        {
            "url": "https://example.org/file.tsv",
            "expected_content_type": "text/tab-separated-values",
        },
        run_dir=run_dir,
        html_allowlist=[],
    )
    assert result.not_modified is True
    assert result.local_path.read_text(encoding="utf-8").startswith("Entity")


def test_connector_blocks_disallowed_html(tmp_path: Path) -> None:
    connector = HttpCsvConnector(cache_dir=tmp_path / "cache", user_agent="ua")
    with pytest.raises(ComplianceError):
        connector.fetch(
            {"url": "https://forbidden.example/path", "access_type": "html"},
            run_dir=tmp_path / "run",
            html_allowlist=["allowed.example"],
        )
