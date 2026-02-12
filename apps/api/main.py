from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from fastapi import FastAPI, HTTPException

from apps.api.models import DatasetSummary
from hdb.registry import load_registry
from hdb.settings import get_settings

app = FastAPI(title="health-dataset-builder API", version="0.1.0")


def _latest_manifest_path(dataset_id: str, manifest_root: Path) -> Path | None:
    dataset_dir = manifest_root / dataset_id
    if not dataset_dir.exists():
        return None
    candidates = sorted([item for item in dataset_dir.iterdir() if item.is_dir()], reverse=True)
    for candidate in candidates:
        manifest = candidate / "manifest.json"
        if manifest.exists():
            return manifest
    return None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/datasets", response_model=list[DatasetSummary])
def datasets() -> list[DatasetSummary]:
    settings = get_settings()
    registry = load_registry(settings.registry_path)
    return [
        DatasetSummary(
            id=d.id, title=d.title, description=d.description, refresh_cron=d.refresh_cron
        )
        for d in registry.datasets
    ]


@app.get("/datasets/tb", response_model=list[DatasetSummary])
def tb_datasets() -> list[DatasetSummary]:
    return [item for item in datasets() if item.id.startswith("tb_")]


@app.get("/datasets/{dataset_id}/latest-manifest")
def latest_manifest(dataset_id: str) -> dict[str, Any]:
    settings = get_settings()
    path = _latest_manifest_path(dataset_id, settings.manifest_dir)
    if path is None:
        raise HTTPException(status_code=404, detail="No manifest found")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], payload)


@app.get("/datasets/{dataset_id}/artifacts")
def artifacts(dataset_id: str) -> dict[str, Any]:
    manifest = latest_manifest(dataset_id)
    return {
        "dataset_id": dataset_id,
        "gold": manifest.get("gold_outputs", []),
        "exporters": manifest.get("exporters", {}),
        "codebook": manifest.get("codebook", {}),
        "models": manifest.get("models", {}),
    }


@app.get("/datasets/tb/{dataset_id}/latest-manifest")
def tb_latest_manifest(dataset_id: str) -> dict[str, Any]:
    if not dataset_id.startswith("tb_"):
        raise HTTPException(status_code=400, detail="dataset_id must start with 'tb_'")
    return latest_manifest(dataset_id)


@app.get("/datasets/tb/{dataset_id}/forecast")
def tb_forecast(dataset_id: str) -> dict[str, Any]:
    if not dataset_id.startswith("tb_"):
        raise HTTPException(status_code=400, detail="dataset_id must start with 'tb_'")
    manifest = latest_manifest(dataset_id)
    models = manifest.get("models", {})
    return {
        "dataset_id": dataset_id,
        "forecast": models.get("forecast"),
        "forecast_metrics": models.get("forecast_metrics"),
    }
