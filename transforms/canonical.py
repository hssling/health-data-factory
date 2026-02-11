from __future__ import annotations

import hashlib

import pandas as pd

CANONICAL_SCHEMA_VERSION = "canonical_v1"
CANONICAL_COLUMNS = [
    "record_id",
    "patient_id",
    "sex",
    "age_years",
    "condition_code",
    "condition_code_system",
    "observation_code",
    "observation_code_system",
    "observation_value_num",
    "observation_unit",
    "event_date",
    "source_dataset",
    "source_url",
    "deidentified",
]


def _stable_id(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def transform_life_expectancy(
    raw_df: pd.DataFrame, source_url: str, dataset_id: str
) -> pd.DataFrame:
    renamed = raw_df.rename(
        columns={
            "Entity": "entity",
            "Code": "code",
            "Year": "year",
            "Life expectancy": "life_expectancy",
        }
    )
    trimmed = renamed[["entity", "code", "year", "life_expectancy"]].copy()
    trimmed["year"] = trimmed["year"].astype(int)
    trimmed["life_expectancy"] = pd.to_numeric(trimmed["life_expectancy"], errors="coerce")
    trimmed = (
        trimmed.dropna(subset=["life_expectancy"])
        .sort_values(["entity", "year"])
        .reset_index(drop=True)
    )
    trimmed = trimmed.head(2000)

    canonical = pd.DataFrame(
        {
            "record_id": trimmed.apply(
                lambda row: _stable_id(f"{row['entity']}|{row['year']}|life_expectancy"), axis=1
            ),
            "patient_id": trimmed.apply(
                lambda row: _stable_id(f"{row['code']}|{row['year']}"), axis=1
            ),
            "sex": "unknown",
            "age_years": pd.Series([pd.NA] * len(trimmed), dtype="Int64"),
            "condition_code": "",
            "condition_code_system": "",
            "observation_code": "life_expectancy_years",
            "observation_code_system": "local",
            "observation_value_num": trimmed["life_expectancy"].astype(float),
            "observation_unit": "years",
            "event_date": pd.to_datetime(trimmed["year"].astype(str) + "-01-01", utc=False),
            "source_dataset": dataset_id,
            "source_url": source_url,
            "deidentified": True,
        }
    )
    return canonical[CANONICAL_COLUMNS]
