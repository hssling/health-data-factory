from pathlib import Path

import pandas as pd
from exporters.fhir import export_fhir_bundle
from exporters.omop import export_omop_subset


def _canonical_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "record_id": ["a", "b"],
            "patient_id": ["abc123abc1230000", "def123def1230000"],
            "sex": ["unknown", "unknown"],
            "age_years": [pd.NA, pd.NA],
            "condition_code": ["", ""],
            "condition_code_system": ["", ""],
            "observation_code": ["life_expectancy_years", "life_expectancy_years"],
            "observation_code_system": ["local", "local"],
            "observation_value_num": [70.2, 71.0],
            "observation_unit": ["years", "years"],
            "event_date": pd.to_datetime(["2000-01-01", "2001-01-01"]),
            "source_dataset": ["demo", "demo"],
            "source_url": ["https://x", "https://x"],
            "deidentified": [True, True],
        }
    )


def test_export_omop_subset(tmp_path: Path) -> None:
    outputs = export_omop_subset(_canonical_df(), tmp_path)
    assert set(outputs.keys()) == {"person", "observation", "condition_occurrence"}
    person = pd.read_parquet(outputs["person"])
    assert "person_id" in person.columns


def test_export_fhir_bundle(tmp_path: Path) -> None:
    bundle_path = export_fhir_bundle(_canonical_df(), tmp_path, dataset_id="demo")
    content = bundle_path.read_text(encoding="utf-8")
    assert '"resourceType": "Bundle"' in content
    assert '"resourceType": "Patient"' in content
    assert '"resourceType": "Observation"' in content
