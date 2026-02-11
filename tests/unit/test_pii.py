import pandas as pd

from hdb.pii import detect_pii


def test_detect_pii_by_column_name() -> None:
    df = pd.DataFrame({"email_address": ["a@example.com"]})
    findings = detect_pii(df)
    assert any(item.reason == "column_name_hint" for item in findings)


def test_detect_pii_by_pattern() -> None:
    df = pd.DataFrame({"note": ["Contact me: sample@example.com"]})
    findings = detect_pii(df)
    assert any(item.reason == "email_pattern" for item in findings)


def test_no_pii_detected() -> None:
    df = pd.DataFrame({"observation_value_num": [12.3, 13.2]})
    findings = detect_pii(df)
    assert findings == []
