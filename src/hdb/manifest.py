from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


@dataclass(frozen=True)
class FileDigest:
    path: str
    sha256: str


def build_digests(files: list[Path]) -> list[FileDigest]:
    return [FileDigest(path=str(path), sha256=sha256_file(path)) for path in files]


def write_manifest(manifest_path: Path, payload: dict[str, Any]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def now_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def license_record_text(name: str, url: str, attribution: str) -> str:
    return (
        f"License: {name}\n"
        f"URL: {url}\n"
        f"Attribution: {attribution}\n"
        "This license metadata is propagated from the dataset registry.\n"
    )


def serialize_digests(digests: list[FileDigest]) -> list[dict[str, str]]:
    return [asdict(item) for item in digests]
