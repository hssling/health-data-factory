from __future__ import annotations

import json
from pathlib import Path

from pytest import MonkeyPatch

from hdb.publish import _bundle_publish_assets


def test_bundle_publish_assets_collects_expected_files(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    manifest_root = tmp_path / "manifests" / "demo_dataset" / "20260101T000000Z"
    data_root = tmp_path / "data"
    cache_root = tmp_path / ".cache"
    manifest_root.mkdir(parents=True, exist_ok=True)
    (data_root / "gold").mkdir(parents=True, exist_ok=True)

    files = {
        "gold": data_root / "gold" / "canonical.parquet",
        "dict": manifest_root / "demo_dataset_data_dictionary.json",
        "codebook": manifest_root / "demo_dataset_codebook.md",
        "fhir": data_root / "gold" / "fhir_bundle.json",
        "model": data_root / "gold" / "baseline_regressor.joblib",
        "metrics": data_root / "gold" / "baseline_metrics.json",
        "omop_person": data_root / "gold" / "person.parquet",
        "omop_observation": data_root / "gold" / "observation.parquet",
        "omop_condition": data_root / "gold" / "condition_occurrence.parquet",
    }
    for path in files.values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    manifest = {
        "timestamp": "20260101T000000Z",
        "gold_outputs": [str(files["gold"])],
        "codebook": {"json": str(files["dict"]), "markdown": str(files["codebook"])},
        "exporters": {
            "fhir": str(files["fhir"]),
            "omop": {
                "person": str(files["omop_person"]),
                "observation": str(files["omop_observation"]),
                "condition_occurrence": str(files["omop_condition"]),
            },
        },
        "models": {"model": str(files["model"]), "metrics": str(files["metrics"])},
    }
    (manifest_root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setenv("HDB_MANIFEST_DIR", str(tmp_path / "manifests"))
    monkeypatch.setenv("HDB_CACHE_DIR", str(cache_root))
    bundle_dir, _, _ = _bundle_publish_assets("demo_dataset", "hf")

    assert (bundle_dir / "canonical.parquet").exists()
    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "baseline_regressor.joblib").exists()
