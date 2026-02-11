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


def get_settings() -> Settings:
    return Settings()
