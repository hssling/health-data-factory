from __future__ import annotations

from pathlib import Path

from prefect import flow, task

from pipelines.engine import run_all_datasets, run_dataset_build


@task(name="run-dataset-build")
def run_dataset_build_task(dataset_id: str, full_refresh: bool) -> str:
    manifest = run_dataset_build(dataset_id, full_refresh=full_refresh)
    return str(manifest)


@flow(name="dataset-build-flow")
def dataset_build_flow(dataset_id: str, full_refresh: bool = False) -> str:
    return run_dataset_build_task(dataset_id, full_refresh)


@flow(name="run-all-datasets-flow")
def run_all_datasets_flow() -> list[str]:
    manifests: list[Path] = run_all_datasets()
    return [str(path) for path in manifests]
