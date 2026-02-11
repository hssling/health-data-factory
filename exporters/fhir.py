from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def export_fhir_bundle(df: pd.DataFrame, output_dir: Path, dataset_id: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    seen_patients: set[str] = set()
    for row in df.head(200).to_dict(orient="records"):
        patient_id = str(row["patient_id"])
        event_time = pd.Timestamp(str(row["event_date"])).isoformat()
        value = float(row["observation_value_num"])
        observation_code = str(row["observation_code"])
        observation_unit = str(row["observation_unit"])
        if patient_id not in seen_patients:
            seen_patients.add(patient_id)
            entries.append(
                {
                    "resource": {
                        "resourceType": "Patient",
                        "id": patient_id,
                        "identifier": [{"system": "urn:dataset", "value": patient_id}],
                    }
                }
            )
        entries.append(
            {
                "resource": {
                    "resourceType": "Observation",
                    "status": "final",
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "code": {"text": observation_code},
                    "effectiveDateTime": event_time,
                    "valueQuantity": {
                        "value": value,
                        "unit": observation_unit,
                    },
                }
            }
        )
    bundle = {"resourceType": "Bundle", "type": "collection", "id": dataset_id, "entry": entries}
    output_path = output_dir / "fhir_bundle.json"
    output_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    return output_path
