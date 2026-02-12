from __future__ import annotations

import json
from pathlib import Path

from apps.api.main import app
from fastapi.testclient import TestClient
from pytest import MonkeyPatch


def test_tb_endpoints(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifests" / "tb_who_india_local" / "20260101T000000Z"
    manifest_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "dataset_id": "tb_who_india_local",
        "models": {
            "forecast": "data/gold/tb_who_india_local/20260101T000000Z/models/tb_forecast.parquet",
            "forecast_metrics": (
                "data/gold/tb_who_india_local/20260101T000000Z/models/"
                "tb_forecast_metrics.json"
            ),
        },
        "gold_outputs": [],
        "exporters": {},
        "codebook": {},
    }
    (manifest_root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setenv("HDB_MANIFEST_DIR", str(tmp_path / "manifests"))
    monkeypatch.setenv("HDB_REGISTRY_PATH", str(Path("datasets/registry.yaml").resolve()))

    client = TestClient(app)
    tb_list = client.get("/datasets/tb")
    assert tb_list.status_code == 200
    assert any(item["id"].startswith("tb_") for item in tb_list.json())

    forecast = client.get("/datasets/tb/tb_who_india_local/forecast")
    assert forecast.status_code == 200
    assert forecast.json()["forecast"].endswith("tb_forecast.parquet")
