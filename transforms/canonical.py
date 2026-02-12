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


def transform_tb_resistance(
    raw_df: pd.DataFrame, source_url: str, dataset_id: str
) -> pd.DataFrame:
    required = {
        "date",
        "country",
        "state",
        "drug",
        "percent_resistant",
        "n_tested",
        "type",
    }
    missing = required.difference(set(raw_df.columns))
    if missing:
        raise ValueError(f"TB dataset missing required columns: {sorted(missing)}")

    tb = raw_df.copy()
    tb["percent_resistant"] = pd.to_numeric(tb["percent_resistant"], errors="coerce")
    tb["date"] = pd.to_datetime(tb["date"], errors="coerce")
    tb = tb.dropna(subset=["percent_resistant", "date"]).reset_index(drop=True)

    canonical = pd.DataFrame(
        {
            "record_id": tb.apply(
                lambda row: _stable_id(
                    f"{row['state']}|{row['drug']}|{row['type']}|{row['date']}"
                ),
                axis=1,
            ),
            "patient_id": tb.apply(
                lambda row: _stable_id(f"{row['state']}|{row['type']}|{row['date']}"), axis=1
            ),
            "sex": "unknown",
            "age_years": pd.Series([pd.NA] * len(tb), dtype="Int64"),
            "condition_code": "A15-A19",
            "condition_code_system": "ICD-10",
            "observation_code": tb["drug"].astype(str),
            "observation_code_system": "local",
            "observation_value_num": tb["percent_resistant"].astype(float),
            "observation_unit": "percent",
            "event_date": tb["date"],
            "source_dataset": dataset_id,
            "source_url": source_url,
            "deidentified": True,
        }
    )
    return canonical[CANONICAL_COLUMNS]


def transform_tb_who_india(
    raw_df: pd.DataFrame, source_url: str, dataset_id: str
) -> pd.DataFrame:
    required = {
        "year",
        "country",
        "mdr_new",
        "mdr_ret",
        "rr_new",
        "rr_ret",
        "dst_rlt_new",
        "dst_rlt_ret",
        "xdr",
    }
    missing = required.difference(set(raw_df.columns))
    if missing:
        raise ValueError(f"WHO TB dataset missing required columns: {sorted(missing)}")

    metrics = [
        "mdr_new",
        "mdr_ret",
        "rr_new",
        "rr_ret",
        "dst_rlt_new",
        "dst_rlt_ret",
        "xdr",
    ]
    tb = raw_df.copy()
    tb["year"] = pd.to_numeric(tb["year"], errors="coerce")
    tb = tb.dropna(subset=["year"]).copy()
    tb["year"] = tb["year"].astype(int)

    melted = tb.melt(
        id_vars=["year", "country"],
        value_vars=metrics,
        var_name="metric",
        value_name="value",
    )
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
    melted = melted.dropna(subset=["value"]).reset_index(drop=True)

    canonical = pd.DataFrame(
        {
            "record_id": melted.apply(
                lambda row: _stable_id(
                    f"{row['country']}|{row['metric']}|{int(row['year'])}"
                ),
                axis=1,
            ),
            "patient_id": melted.apply(
                lambda row: _stable_id(f"{row['country']}|{int(row['year'])}"), axis=1
            ),
            "sex": "unknown",
            "age_years": pd.Series([pd.NA] * len(melted), dtype="Int64"),
            "condition_code": "A15-A19",
            "condition_code_system": "ICD-10",
            "observation_code": melted["metric"].astype(str),
            "observation_code_system": "WHO_TB",
            "observation_value_num": melted["value"].astype(float),
            "observation_unit": "count",
            "event_date": pd.to_datetime(melted["year"].astype(str) + "-01-01"),
            "source_dataset": dataset_id,
            "source_url": source_url,
            "deidentified": True,
        }
    )
    return canonical[CANONICAL_COLUMNS]
