from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    manifest_dir: Path
    registry_path: Path
    user_agent: str
    cache_dir: Path
    request_timeout: int
    kaggle_dataset_slug: str
    kaggle_model_slug: str
    hf_dataset_repo_id: str
    hf_model_repo_id: str
    continuous_failure_threshold: int
    auto_publish_tb: bool


def get_settings() -> Settings:
    return Settings(
        data_dir=Path(os.getenv("HDB_DATA_DIR", "./data")),
        manifest_dir=Path(os.getenv("HDB_MANIFEST_DIR", "./manifests")),
        registry_path=Path(os.getenv("HDB_REGISTRY_PATH", "./datasets/registry.yaml")),
        user_agent=os.getenv("HDB_USER_AGENT", "health-dataset-builder/0.1 (+https://example.org)"),
        cache_dir=Path(os.getenv("HDB_CACHE_DIR", ".cache/hdb")),
        request_timeout=int(os.getenv("HDB_REQUEST_TIMEOUT", "30")),
        kaggle_dataset_slug=os.getenv("HDB_KAGGLE_DATASET_SLUG", ""),
        kaggle_model_slug=os.getenv("HDB_KAGGLE_MODEL_SLUG", ""),
        hf_dataset_repo_id=os.getenv("HDB_HF_DATASET_REPO_ID", ""),
        hf_model_repo_id=os.getenv("HDB_HF_MODEL_REPO_ID", ""),
        continuous_failure_threshold=int(os.getenv("HDB_CONTINUOUS_FAILURE_THRESHOLD", "3")),
        auto_publish_tb=os.getenv("HDB_AUTO_PUBLISH_TB", "false").lower() == "true",
    )
