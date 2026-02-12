from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

PII_COLUMN_HINTS = {"name", "email", "phone", "mobile", "address", "ssn", "aadhaar"}
EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_PATTERN = re.compile(r"\b(?:\+?\d[\d\-\s]{9,}\d)\b")


@dataclass(frozen=True)
class PiiFinding:
    field: str
    reason: str


def detect_pii(df: pd.DataFrame) -> list[PiiFinding]:
    findings: list[PiiFinding] = []
    for column in df.columns:
        lowered = column.lower()
        if any(hint in lowered for hint in PII_COLUMN_HINTS):
            findings.append(PiiFinding(field=column, reason="column_name_hint"))
        is_text_like = pd.api.types.is_string_dtype(df[column]) or pd.api.types.is_object_dtype(
            df[column]
        )
        if not is_text_like:
            continue
        series = df[column].dropna()
        if series.empty:
            continue
        sample = series.astype(str).head(200)
        if sample.str.contains(EMAIL_PATTERN, regex=True).any():
            findings.append(PiiFinding(field=column, reason="email_pattern"))
        if sample.str.contains(PHONE_PATTERN, regex=True).any():
            findings.append(PiiFinding(field=column, reason="phone_pattern"))
    return findings
