from __future__ import annotations

from pathlib import Path

import pandas as pd


def _to_person_id(patient_id: str) -> int:
    return int(patient_id[:12], 16)


def export_omop_subset(df: pd.DataFrame, output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    person = (
        df[["patient_id", "sex"]]
        .drop_duplicates()
        .assign(person_id=lambda x: x["patient_id"].map(_to_person_id), gender_concept_id=0)[
            ["person_id", "gender_concept_id"]
        ]
    )
    observation = df.assign(
        person_id=lambda x: x["patient_id"].map(_to_person_id),
        observation_source_value=lambda x: x["observation_code"],
        value_as_number=lambda x: x["observation_value_num"],
    )[["person_id", "observation_source_value", "value_as_number", "event_date"]]
    condition_occurrence = df[df["condition_code"] != ""].assign(
        person_id=lambda x: x["patient_id"].map(_to_person_id),
        condition_source_value=lambda x: x["condition_code"],
    )[["person_id", "condition_source_value", "event_date"]]

    person_path = output_dir / "person.parquet"
    observation_path = output_dir / "observation.parquet"
    condition_path = output_dir / "condition_occurrence.parquet"
    person.to_parquet(person_path, index=False)
    observation.to_parquet(observation_path, index=False)
    condition_occurrence.to_parquet(condition_path, index=False)
    return {
        "person": str(person_path),
        "observation": str(observation_path),
        "condition_occurrence": str(condition_path),
    }
