from __future__ import annotations

from typing import Any

import pandas as pd

from validators.schemas import canonical_v1_schema


def validate_canonical(df: pd.DataFrame) -> dict[str, Any]:
    validated = canonical_v1_schema.validate(df, lazy=True)
    return {"suite": "canonical_v1", "rows": int(len(validated)), "valid": True}
