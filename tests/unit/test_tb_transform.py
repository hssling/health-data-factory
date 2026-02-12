import pandas as pd
import pytest
from transforms.canonical import transform_tb_resistance, transform_tb_who_india


def test_transform_tb_resistance_happy_path() -> None:
    raw = pd.DataFrame(
        {
            "date": ["2017-01-01", "2018-01-01"],
            "country": ["India", "India"],
            "state": ["National", "National"],
            "drug": ["Rifampicin", "Rifampicin"],
            "percent_resistant": [4.1, 3.5],
            "n_tested": [100, 120],
            "type": ["new", "new"],
        }
    )
    out = transform_tb_resistance(raw, source_url="D:/data/tb_merged.csv", dataset_id="tb")
    assert len(out) == 2
    assert set(["condition_code", "observation_value_num"]).issubset(set(out.columns))
    assert out["condition_code"].iloc[0] == "A15-A19"


def test_transform_tb_resistance_missing_columns() -> None:
    raw = pd.DataFrame({"date": ["2017-01-01"]})
    with pytest.raises(ValueError):
        transform_tb_resistance(raw, source_url="x", dataset_id="tb")


def test_transform_tb_who_india_happy_path() -> None:
    raw = pd.DataFrame(
        {
            "year": [2017, 2018],
            "country": ["India", "India"],
            "mdr_new": [2000, 2200],
            "mdr_ret": [5000, 5200],
            "rr_new": [7000, 7100],
            "rr_ret": [21000, 22000],
            "dst_rlt_new": [50000, 90000],
            "dst_rlt_ret": [26000, 87000],
            "xdr": [400, 450],
        }
    )
    out = transform_tb_who_india(raw, source_url="D:/data/tb_raw/who_tb_india.csv", dataset_id="tb")
    assert len(out) > 0
    assert out["condition_code"].nunique() == 1
    assert "mdr_new" in set(out["observation_code"])


def test_transform_tb_who_india_missing_columns() -> None:
    raw = pd.DataFrame({"year": [2017], "country": ["India"]})
    with pytest.raises(ValueError):
        transform_tb_who_india(raw, source_url="x", dataset_id="tb")
