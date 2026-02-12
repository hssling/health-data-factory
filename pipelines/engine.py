from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from connectors import CONNECTOR_REGISTRY
from exporters.fhir import export_fhir_bundle
from exporters.omop import export_omop_subset
from transforms.canonical import (
    CANONICAL_SCHEMA_VERSION,
    transform_life_expectancy,
    transform_tb_resistance,
    transform_tb_who_india,
)
from validators.checks import validate_canonical

from hdb.codebook import generate_codebook
from hdb.manifest import (
    build_digests,
    license_record_text,
    now_timestamp,
    serialize_digests,
    write_manifest,
)
from hdb.pii import detect_pii
from hdb.registry import DatasetConfig, load_registry
from hdb.settings import get_settings
from pipelines.modeling import train_baseline_model

LOGGER = logging.getLogger(__name__)


def _get_dataset(dataset_id: str) -> tuple[DatasetConfig, list[str]]:
    settings = get_settings()
    registry = load_registry(settings.registry_path)
    dataset_map = registry.dataset_map()
    if dataset_id not in dataset_map:
        raise KeyError(f"Unknown dataset_id: {dataset_id}")
    return dataset_map[dataset_id], registry.html_allowlist


def _latest_manifest(dataset_id: str, manifest_dir: Path) -> Path:
    dataset_root = manifest_dir / dataset_id
    if not dataset_root.exists():
        raise FileNotFoundError(f"No manifests for dataset: {dataset_id}")
    run_dirs = sorted([p for p in dataset_root.iterdir() if p.is_dir()], reverse=True)
    for run_dir in run_dirs:
        manifest_path = run_dir / "manifest.json"
        if manifest_path.exists():
            return manifest_path
    raise FileNotFoundError(f"No manifest.json for dataset: {dataset_id}")


def run_dataset_build(dataset_id: str, full_refresh: bool = False) -> Path:
    settings = get_settings()
    dataset, html_allowlist = _get_dataset(dataset_id)
    timestamp = now_timestamp()

    bronze_dir = settings.data_dir / "bronze" / dataset_id / timestamp
    silver_dir = settings.data_dir / "silver" / dataset_id / timestamp
    gold_dir = settings.data_dir / "gold" / dataset_id / timestamp
    manifest_dir = settings.manifest_dir / dataset_id / timestamp

    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    source = dataset.sources[0]
    connector_cls = CONNECTOR_REGISTRY[source.connector]
    connector = connector_cls(  # type: ignore[abstract]
        cache_dir=settings.cache_dir / source.connector,
        user_agent=settings.user_agent,
        timeout_seconds=int(source.params.get("timeout_seconds", settings.request_timeout)),
    )
    if full_refresh:
        for item in (settings.cache_dir / source.connector).glob("*"):
            item.unlink(missing_ok=True)

    fetched = connector.fetch(
        params=source.params, run_dir=bronze_dir, html_allowlist=html_allowlist
    )
    raw_df = pd.read_csv(fetched.local_path, sep=None, engine="python")

    transform_key = str(source.params.get("transform", "life_expectancy"))
    if transform_key == "life_expectancy":
        canonical_df = transform_life_expectancy(
            raw_df, source_url=fetched.source_url, dataset_id=dataset_id
        )
    elif transform_key == "tb_resistance":
        canonical_df = transform_tb_resistance(
            raw_df, source_url=fetched.source_url, dataset_id=dataset_id
        )
    elif transform_key == "tb_who_india":
        canonical_df = transform_tb_who_india(
            raw_df, source_url=fetched.source_url, dataset_id=dataset_id
        )
    else:
        raise ValueError(f"Unsupported transform key: {transform_key}")
    pii_findings = detect_pii(canonical_df)
    if pii_findings and dataset.pii_policy.block_if_suspected:
        findings = [{"field": f.field, "reason": f.reason} for f in pii_findings]
        raise RuntimeError(f"PII suspected; build blocked: {json.dumps(findings)}")

    silver_path = silver_dir / "normalized.parquet"
    gold_path = gold_dir / "canonical.parquet"
    canonical_df.to_parquet(silver_path, index=False)

    validation = validate_canonical(canonical_df)
    canonical_df.to_parquet(gold_path, index=False)

    codebook_json, codebook_md = generate_codebook(
        canonical_df, dataset_id=dataset_id, output_dir=manifest_dir
    )

    omop_dir = gold_dir / "omop"
    omop_outputs = export_omop_subset(canonical_df, omop_dir)
    fhir_dir = gold_dir / "fhir"
    fhir_output = export_fhir_bundle(canonical_df, fhir_dir, dataset_id=dataset_id)
    model_dir = gold_dir / "models"
    model_outputs = train_baseline_model(canonical_df, model_dir)

    license_path = manifest_dir / "LICENSE.md"
    license_path.write_text(
        license_record_text(
            name=dataset.license.name,
            url=dataset.license.url,
            attribution=dataset.license.attribution,
        ),
        encoding="utf-8",
    )

    all_outputs = [
        silver_path,
        gold_path,
        codebook_json,
        codebook_md,
        license_path,
        Path(fhir_output),
        Path(model_outputs["model"]),
        Path(model_outputs["metrics"]),
    ]
    all_outputs.extend(Path(path) for path in omop_outputs.values())
    manifest_payload: dict[str, Any] = {
        "dataset_id": dataset_id,
        "timestamp": timestamp,
        "schema_version": CANONICAL_SCHEMA_VERSION,
        "row_count": int(len(canonical_df)),
        "hashes": serialize_digests(build_digests(all_outputs)),
        "provenance": [
            {
                "source_url": fetched.source_url,
                "fetch_time": fetched.fetched_at,
                "not_modified": fetched.not_modified,
            }
        ],
        "license": dataset.license.model_dump(),
        "validation": validation,
        "pii_findings": [{"field": f.field, "reason": f.reason} for f in pii_findings],
        "gold_outputs": [str(gold_path)],
        "codebook": {"json": str(codebook_json), "markdown": str(codebook_md)},
        "exporters": {"omop": omop_outputs, "fhir": str(fhir_output)},
        "models": model_outputs,
    }
    write_manifest(manifest_dir / "manifest.json", manifest_payload)
    LOGGER.info("build complete for dataset_id=%s rows=%s", dataset_id, len(canonical_df))
    return manifest_dir / "manifest.json"


def run_all_datasets() -> list[Path]:
    settings = get_settings()
    registry = load_registry(settings.registry_path)
    return [run_dataset_build(dataset.id, full_refresh=False) for dataset in registry.datasets]


def _parse_manifest_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def _dataset_due_for_continuous(dataset: DatasetConfig, now: datetime) -> bool:
    settings = get_settings()
    dataset_root = settings.manifest_dir / dataset.id
    if not dataset_root.exists():
        return True
    run_dirs = sorted([item for item in dataset_root.iterdir() if item.is_dir()], reverse=True)
    for run_dir in run_dirs:
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        last_ts = _parse_manifest_timestamp(str(manifest["timestamp"]))
        min_delta = timedelta(minutes=dataset.continuous.min_interval_minutes)
        return now - last_ts >= min_delta
    return True


def run_continuous_ingestion(dataset_id: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    registry = load_registry(settings.registry_path)
    now = datetime.now(UTC)
    selected: list[DatasetConfig] = []
    for dataset in registry.datasets:
        if dataset_id and dataset.id != dataset_id:
            continue
        if dataset.continuous.enabled and _dataset_due_for_continuous(dataset, now):
            selected.append(dataset)

    results: list[dict[str, str]] = []
    failures = 0
    for dataset in selected:
        try:
            manifest_path = run_dataset_build(dataset.id, full_refresh=False)
            results.append(
                {"dataset_id": dataset.id, "status": "built", "manifest": str(manifest_path)}
            )
            failures = 0
        except Exception as exc:
            failures += 1
            results.append({"dataset_id": dataset.id, "status": "failed", "error": str(exc)})
            if failures >= settings.continuous_failure_threshold:
                break

    return {
        "selected_count": len(selected),
        "executed_count": len(results),
        "failure_threshold": settings.continuous_failure_threshold,
        "results": results,
    }


def validate_dataset_outputs(dataset_id: str) -> dict[str, Any]:
    settings = get_settings()
    manifest_path = _latest_manifest(dataset_id, settings.manifest_dir)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    gold_path = Path(manifest["gold_outputs"][0])
    df = pd.read_parquet(gold_path)
    return validate_canonical(df)


def export_omop_for_dataset(dataset_id: str) -> dict[str, str]:
    settings = get_settings()
    manifest_path = _latest_manifest(dataset_id, settings.manifest_dir)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    gold_path = Path(manifest["gold_outputs"][0])
    df = pd.read_parquet(gold_path)
    output_dir = gold_path.parent / "omop"
    outputs = export_omop_subset(df, output_dir)
    return outputs


def export_fhir_for_dataset(dataset_id: str) -> Path:
    settings = get_settings()
    manifest_path = _latest_manifest(dataset_id, settings.manifest_dir)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    gold_path = Path(manifest["gold_outputs"][0])
    df = pd.read_parquet(gold_path)
    output_dir = gold_path.parent / "fhir"
    return export_fhir_bundle(df, output_dir, dataset_id=dataset_id)
