from __future__ import annotations

import pandera.pandas as pa
from pandera.api.checks import Check

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
        Check(
            lambda df: df["observation_value_num"].between(0, 150).all(),
            error="observations in range",
        ),
        Check(lambda df: df["record_id"].is_unique, error="record_id must be unique"),
    ],
    strict=True,
)
