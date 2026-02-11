from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipelines.engine import run_continuous_ingestion


def test_run_continuous_ingestion_runs_enabled_dataset(
    monkeypatch: Any, tmp_path: Path
) -> None:
    registry_path = tmp_path / "registry.yaml"
    manifest_root = tmp_path / "manifests"
    registry_path.write_text(
        """
html_allowlist: []
datasets:
  - id: ds1
    title: "DS1"
    description: "x"
    refresh_cron: "0 * * * *"
    continuous:
      enabled: true
      min_interval_minutes: 60
    license:
      name: "CC BY 4.0"
      url: "https://creativecommons.org/licenses/by/4.0/"
      attribution: "X"
    pii_policy:
      block_if_suspected: true
      declared_deidentified: true
    validations_suite: canonical_v1
    output_schemas:
      canonical: canonical_v1
      omop: omop_subset_v1
      fhir: fhir_r4_minimal_v1
    sources:
      - connector: http_csv
        params:
          url: "https://example.org/x.csv"
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("HDB_REGISTRY_PATH", str(registry_path))
    monkeypatch.setenv("HDB_MANIFEST_DIR", str(manifest_root))

    def fake_run(dataset_id: str, full_refresh: bool = False) -> Path:
        out = manifest_root / dataset_id / "20260101T000000Z"
        out.mkdir(parents=True, exist_ok=True)
        path = out / "manifest.json"
        path.write_text(json.dumps({"timestamp": "20260101T000000Z"}), encoding="utf-8")
        return path

    monkeypatch.setattr("pipelines.engine.run_dataset_build", fake_run)
    result = run_continuous_ingestion()
    assert result["selected_count"] == 1
    assert result["executed_count"] == 1
    assert result["results"][0]["status"] == "built"


def test_run_continuous_ingestion_skips_when_not_due(
    monkeypatch: Any, tmp_path: Path
) -> None:
    registry_path = tmp_path / "registry.yaml"
    manifest_root = tmp_path / "manifests"
    registry_path.write_text(
        """
html_allowlist: []
datasets:
  - id: ds1
    title: "DS1"
    description: "x"
    refresh_cron: "0 * * * *"
    continuous:
      enabled: true
      min_interval_minutes: 99999
    license:
      name: "CC BY 4.0"
      url: "https://creativecommons.org/licenses/by/4.0/"
      attribution: "X"
    pii_policy:
      block_if_suspected: true
      declared_deidentified: true
    validations_suite: canonical_v1
    output_schemas:
      canonical: canonical_v1
      omop: omop_subset_v1
      fhir: fhir_r4_minimal_v1
    sources:
      - connector: http_csv
        params:
          url: "https://example.org/x.csv"
""".strip(),
        encoding="utf-8",
    )
    latest_dir = manifest_root / "ds1" / "20990101T000000Z"
    latest_dir.mkdir(parents=True, exist_ok=True)
    (latest_dir / "manifest.json").write_text(
        json.dumps({"timestamp": "20990101T000000Z"}), encoding="utf-8"
    )

    monkeypatch.setenv("HDB_REGISTRY_PATH", str(registry_path))
    monkeypatch.setenv("HDB_MANIFEST_DIR", str(manifest_root))
    result = run_continuous_ingestion()
    assert result["selected_count"] == 0
    assert result["executed_count"] == 0

