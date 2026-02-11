from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def generate_codebook(df: pd.DataFrame, dataset_id: str, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    dictionary = [
        {
            "column": column,
            "dtype": str(df[column].dtype),
            "non_null": int(df[column].notna().sum()),
            "null": int(df[column].isna().sum()),
        }
        for column in df.columns
    ]
    json_path = output_dir / f"{dataset_id}_data_dictionary.json"
    json_path.write_text(json.dumps(dictionary, indent=2), encoding="utf-8")

    md_lines = [
        f"# Codebook: {dataset_id}",
        "",
        "| Column | Dtype | Non-null | Null |",
        "|---|---|---:|---:|",
    ]
    for row in dictionary:
        md_lines.append(f"| {row['column']} | {row['dtype']} | {row['non_null']} | {row['null']} |")
    md_path = output_dir / f"{dataset_id}_codebook.md"
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return json_path, md_path
