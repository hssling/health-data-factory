from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from connectors.http_file import HttpCsvConnector
from pipelines.engine import run_dataset_build, validate_dataset_outputs


class FakeResponse:
    def __init__(self) -> None:
        self.status_code = 200
        self.content = (
            b"Entity\tCode\tYear\tLife expectancy\nIndia\tIND\t2000\t62.5\nIndia\tIND\t2001\t62.9\n"
        )
        self.headers = {"Content-Type": "text/tab-separated-values", "ETag": "x"}

    def raise_for_status(self) -> None:
        return None


def test_run_dataset_build_writes_manifest(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.setenv("HDB_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("HDB_MANIFEST_DIR", str(tmp_path / "manifests"))
    monkeypatch.setenv("HDB_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("HDB_REGISTRY_PATH", str(Path("datasets/registry.yaml").resolve()))

    def fake_get(self: HttpCsvConnector, url: str, headers: dict[str, str]) -> FakeResponse:
        return FakeResponse()

    monkeypatch.setattr(HttpCsvConnector, "_get", fake_get)
    manifest_path = run_dataset_build("demo_dataset", full_refresh=True)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "demo_dataset"
    assert payload["row_count"] == 2
    assert "omop" in payload["exporters"]


def test_validate_dataset_outputs(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.setenv("HDB_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("HDB_MANIFEST_DIR", str(tmp_path / "manifests"))
    monkeypatch.setenv("HDB_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("HDB_REGISTRY_PATH", str(Path("datasets/registry.yaml").resolve()))

    def fake_get(self: HttpCsvConnector, url: str, headers: dict[str, str]) -> FakeResponse:
        return FakeResponse()

    monkeypatch.setattr(HttpCsvConnector, "_get", fake_get)
    run_dataset_build("demo_dataset", full_refresh=True)
    result = validate_dataset_outputs("demo_dataset")
    assert result["valid"] is True
