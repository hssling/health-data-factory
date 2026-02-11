from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path(os.getenv("HDB_DATA_DIR", "./data"))
    manifest_dir: Path = Path(os.getenv("HDB_MANIFEST_DIR", "./manifests"))
    registry_path: Path = Path(os.getenv("HDB_REGISTRY_PATH", "./datasets/registry.yaml"))
    user_agent: str = os.getenv(
        "HDB_USER_AGENT", "health-dataset-builder/0.1 (+https://example.org)"
    )
    cache_dir: Path = Path(os.getenv("HDB_CACHE_DIR", ".cache/hdb"))
    request_timeout: int = int(os.getenv("HDB_REQUEST_TIMEOUT", "30"))
    kaggle_dataset_slug: str = os.getenv("HDB_KAGGLE_DATASET_SLUG", "")
    kaggle_model_slug: str = os.getenv("HDB_KAGGLE_MODEL_SLUG", "")
    hf_dataset_repo_id: str = os.getenv("HDB_HF_DATASET_REPO_ID", "")
    hf_model_repo_id: str = os.getenv("HDB_HF_MODEL_REPO_ID", "")


def get_settings() -> Settings:
    return Settings()
