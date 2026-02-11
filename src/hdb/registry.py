from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class LicenseConfig(BaseModel):
    name: str
    url: str
    attribution: str


class PiiPolicy(BaseModel):
    block_if_suspected: bool = True
    declared_deidentified: bool = False


class OutputSchemas(BaseModel):
    canonical: str
    omop: str | None = None
    fhir: str | None = None


class ContinuousPolicy(BaseModel):
    enabled: bool = False
    min_interval_minutes: int = 60


class SourceConfig(BaseModel):
    connector: str
    params: dict[str, Any] = Field(default_factory=dict)


class DatasetConfig(BaseModel):
    id: str
    title: str
    description: str
    refresh_cron: str
    license: LicenseConfig
    pii_policy: PiiPolicy
    validations_suite: str
    output_schemas: OutputSchemas
    continuous: ContinuousPolicy = Field(default_factory=ContinuousPolicy)
    sources: list[SourceConfig]


class RegistryConfig(BaseModel):
    html_allowlist: list[str] = Field(default_factory=list)
    datasets: list[DatasetConfig] = Field(default_factory=list)

    def dataset_map(self) -> dict[str, DatasetConfig]:
        return {dataset.id: dataset for dataset in self.datasets}


def load_registry(path: Path) -> RegistryConfig:
    content = yaml.safe_load(path.read_text(encoding="utf-8"))
    return RegistryConfig.model_validate(content)
