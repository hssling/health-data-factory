from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.api.checks import Check


def _observation_values_by_unit(df: pd.DataFrame) -> bool:
    percent_mask = df["observation_unit"] == "percent"
    years_mask = df["observation_unit"] == "years"
    other_mask = ~(percent_mask | years_mask)
    percent_ok = df.loc[percent_mask, "observation_value_num"].between(0, 100).all()
    years_ok = df.loc[years_mask, "observation_value_num"].between(0, 150).all()
    other_ok = (df.loc[other_mask, "observation_value_num"] >= 0).all()
    return bool(percent_ok and years_ok and other_ok)


canonical_v1_schema = pa.DataFrameSchema(
    {
        "record_id": pa.Column(str, nullable=False),
        "patient_id": pa.Column(str, nullable=False),
        "sex": pa.Column(str, nullable=False),
        "age_years": pa.Column("Int64", nullable=True),
        "condition_code": pa.Column(str, nullable=False),
        "condition_code_system": pa.Column(str, nullable=False),
        "observation_code": pa.Column(str, nullable=False),
        "observation_code_system": pa.Column(str, nullable=False),
        "observation_value_num": pa.Column(float, Check.ge(0), nullable=False),
        "observation_unit": pa.Column(str, nullable=False),
        "event_date": pa.Column(pa.DateTime, nullable=False),
        "source_dataset": pa.Column(str, nullable=False),
        "source_url": pa.Column(str, nullable=False),
        "deidentified": pa.Column(bool, nullable=False),
    },
    checks=[
        Check(_observation_values_by_unit, error="observations in range by unit"),
        Check(lambda df: df["record_id"].is_unique, error="record_id must be unique"),
    ],
    strict=True,
)
