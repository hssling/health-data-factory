from __future__ import annotations

import argparse
import json
from typing import Any

import uvicorn
from pipelines.engine import (
    export_fhir_for_dataset,
    export_omop_for_dataset,
    run_all_datasets,
    run_dataset_build,
    validate_dataset_outputs,
)

from hdb.logging_config import configure_logging
from hdb.publish import publish_all_targets, publish_to_huggingface, publish_to_kaggle
from hdb.registry import load_registry
from hdb.settings import get_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hdb")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list")

    run = sub.add_parser("run")
    run.add_argument("dataset_id")
    run.add_argument("--full-refresh", action="store_true")

    sub.add_parser("run-all")

    validate = sub.add_parser("validate")
    validate.add_argument("dataset_id")

    omop = sub.add_parser("export-omop")
    omop.add_argument("dataset_id")

    fhir = sub.add_parser("export-fhir")
    fhir.add_argument("dataset_id")

    publish_hf = sub.add_parser("publish-hf")
    publish_hf.add_argument("dataset_id")

    publish_kaggle = sub.add_parser("publish-kaggle")
    publish_kaggle.add_argument("dataset_id")

    publish_all = sub.add_parser("publish-all")
    publish_all.add_argument("dataset_id")

    sub.add_parser("serve-api")
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = get_settings()

    if args.command == "list":
        registry = load_registry(settings.registry_path)
        print(json.dumps([d.model_dump() for d in registry.datasets], indent=2))
        return 0
    if args.command == "run":
        run_dataset_build(args.dataset_id, full_refresh=args.full_refresh)
        return 0
    if args.command == "run-all":
        run_all_datasets()
        return 0
    if args.command == "validate":
        result: dict[str, Any] = validate_dataset_outputs(args.dataset_id)
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "export-omop":
        export_omop_for_dataset(args.dataset_id)
        return 0
    if args.command == "export-fhir":
        export_fhir_for_dataset(args.dataset_id)
        return 0
    if args.command == "publish-hf":
        result = publish_to_huggingface(args.dataset_id)
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "publish-kaggle":
        result = publish_to_kaggle(args.dataset_id)
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "publish-all":
        result = publish_all_targets(args.dataset_id)
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "serve-api":
        uvicorn.run("apps.api.main:app", host="0.0.0.0", port=8000, reload=False)
        return 0
    return 1
