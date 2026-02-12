from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, cast

from huggingface_hub import HfApi

from hdb.settings import get_settings


def _latest_manifest_path(dataset_id: str) -> Path:
    settings = get_settings()
    dataset_root = settings.manifest_dir / dataset_id
    if not dataset_root.exists():
        raise FileNotFoundError(f"No manifests found for dataset_id={dataset_id}")
    run_dirs = sorted([item for item in dataset_root.iterdir() if item.is_dir()], reverse=True)
    for run_dir in run_dirs:
        path = run_dir / "manifest.json"
        if path.exists():
            return path
    raise FileNotFoundError(f"No manifest file found for dataset_id={dataset_id}")


def _load_manifest(dataset_id: str) -> dict[str, Any]:
    path = _latest_manifest_path(dataset_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], payload)


def _bundle_publish_assets(dataset_id: str, target: str) -> tuple[Path, dict[str, Any], str]:
    settings = get_settings()
    manifest = _load_manifest(dataset_id)
    timestamp = str(manifest["timestamp"])
    bundle_dir = settings.cache_dir / "publish" / target / dataset_id / timestamp
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    files_to_copy: list[Path] = [Path(manifest["gold_outputs"][0])]
    files_to_copy.extend(
        [
            Path(manifest["codebook"]["json"]),
            Path(manifest["codebook"]["markdown"]),
            Path(manifest["exporters"]["fhir"]),
        ]
    )
    files_to_copy.extend(Path(path) for path in manifest.get("models", {}).values())
    files_to_copy.extend(Path(path) for path in manifest["exporters"]["omop"].values())
    files_to_copy.append(_latest_manifest_path(dataset_id))

    for file_path in files_to_copy:
        destination = bundle_dir / file_path.name
        destination.write_bytes(file_path.read_bytes())
    return bundle_dir, manifest, timestamp


def publish_to_huggingface(dataset_id: str) -> dict[str, str]:
    settings = get_settings()
    bundle_dir, manifest, timestamp = _bundle_publish_assets(dataset_id, "hf")
    token = os.getenv("HF_TOKEN", "")
    dataset_repo_id = settings.hf_dataset_repo_id
    model_repo_id = settings.hf_model_repo_id
    if not token:
        raise RuntimeError("HF_TOKEN is required for Hugging Face publishing")
    if not dataset_repo_id or not model_repo_id:
        raise RuntimeError("HDB_HF_DATASET_REPO_ID and HDB_HF_MODEL_REPO_ID are required")

    api = HfApi(token=token)
    api.create_repo(repo_id=dataset_repo_id, repo_type="dataset", exist_ok=True)
    api.create_repo(repo_id=model_repo_id, repo_type="model", exist_ok=True)

    api.upload_folder(
        folder_path=str(bundle_dir),
        repo_id=dataset_repo_id,
        repo_type="dataset",
        path_in_repo=f"{dataset_id}/{timestamp}",
        commit_message=f"Publish dataset artifacts for {dataset_id} ({timestamp})",
    )

    model_bundle = bundle_dir / "model_bundle"
    model_bundle.mkdir(exist_ok=True)
    for path in bundle_dir.iterdir():
        if path.is_file() and (
            path.name.endswith(".joblib")
            or path.name.endswith("_metrics.json")
            or path.name.endswith("_forecast.parquet")
            or path.name == "manifest.json"
            or path.name == "tb_forecast.parquet"
        ):
            (model_bundle / path.name).write_bytes(path.read_bytes())

    baseline_metrics = {}
    baseline_metrics_path = model_bundle / "baseline_metrics.json"
    if baseline_metrics_path.exists():
        baseline_metrics = json.loads(baseline_metrics_path.read_text(encoding="utf-8"))
    model_card = {
        "dataset_id": dataset_id,
        "timestamp": timestamp,
        "metrics": baseline_metrics,
        "license": manifest["license"],
    }
    (model_bundle / "model_card.json").write_text(
        json.dumps(model_card, indent=2), encoding="utf-8"
    )

    api.upload_folder(
        folder_path=str(model_bundle),
        repo_id=model_repo_id,
        repo_type="model",
        path_in_repo=f"{dataset_id}/{timestamp}",
        commit_message=f"Publish baseline model for {dataset_id} ({timestamp})",
    )
    return {
        "dataset_repo": dataset_repo_id,
        "model_repo": model_repo_id,
        "timestamp": timestamp,
    }


def _run_kaggle_publish(target_dir: Path, message: str) -> None:
    try:
        subprocess.run(
            ["kaggle", "datasets", "version", "-p", str(target_dir), "-m", message, "-q"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        subprocess.run(
            ["kaggle", "datasets", "create", "-p", str(target_dir), "-q"],
            check=True,
            capture_output=True,
            text=True,
        )


def publish_to_kaggle(dataset_id: str) -> dict[str, str]:
    settings = get_settings()
    if not os.getenv("KAGGLE_USERNAME") or not os.getenv("KAGGLE_KEY"):
        raise RuntimeError("KAGGLE_USERNAME and KAGGLE_KEY are required")
    if not settings.kaggle_dataset_slug or not settings.kaggle_model_slug:
        raise RuntimeError("HDB_KAGGLE_DATASET_SLUG and HDB_KAGGLE_MODEL_SLUG are required")

    bundle_dir, manifest, timestamp = _bundle_publish_assets(dataset_id, "kaggle")
    dataset_dir = bundle_dir / "dataset"
    model_dir = bundle_dir / "models"
    dataset_dir.mkdir(exist_ok=True)
    model_dir.mkdir(exist_ok=True)

    for name in ("canonical.parquet", "manifest.json", f"{dataset_id}_data_dictionary.json"):
        source = bundle_dir / name
        if source.exists():
            (dataset_dir / name).write_bytes(source.read_bytes())

    for name in (
        "baseline_regressor.joblib",
        "baseline_metrics.json",
        "manifest.json",
    ):
        source = bundle_dir / name
        if source.exists():
            (model_dir / name).write_bytes(source.read_bytes())

    dataset_metadata = {
        "title": f"health-data-factory-{dataset_id}",
        "id": settings.kaggle_dataset_slug,
        "licenses": [{"name": "CC-BY-4.0"}],
    }
    model_metadata = {
        "title": f"health-data-factory-{dataset_id}-models",
        "id": settings.kaggle_model_slug,
        "licenses": [{"name": "CC-BY-4.0"}],
    }
    (dataset_dir / "dataset-metadata.json").write_text(
        json.dumps(dataset_metadata, indent=2), encoding="utf-8"
    )
    (model_dir / "dataset-metadata.json").write_text(
        json.dumps(model_metadata, indent=2), encoding="utf-8"
    )

    _run_kaggle_publish(dataset_dir, f"Publish dataset artifacts for {dataset_id} ({timestamp})")
    _run_kaggle_publish(model_dir, f"Publish model artifacts for {dataset_id} ({timestamp})")

    return {
        "dataset_slug": settings.kaggle_dataset_slug,
        "model_slug": settings.kaggle_model_slug,
        "timestamp": timestamp,
        "license": str(manifest["license"]["name"]),
    }


def publish_all_targets(dataset_id: str) -> dict[str, dict[str, str]]:
    return {
        "huggingface": publish_to_huggingface(dataset_id),
        "kaggle": publish_to_kaggle(dataset_id),
    }
