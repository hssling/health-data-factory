from pathlib import Path

import pandas as pd
from pipelines.modeling import train_baseline_model, train_tb_forecast_artifacts


def test_train_baseline_model_outputs(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "event_date": pd.to_datetime(["2000-01-01", "2001-01-01", "2002-01-01"]),
            "observation_value_num": [60.0, 61.0, 62.0],
        }
    )
    outputs = train_baseline_model(df, tmp_path)
    assert Path(outputs["model"]).exists()
    assert Path(outputs["metrics"]).exists()


def test_train_tb_forecast_artifacts_outputs(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "event_date": pd.to_datetime(
                [
                    "2017-01-01",
                    "2018-01-01",
                    "2019-01-01",
                    "2017-01-01",
                    "2018-01-01",
                    "2019-01-01",
                ]
            ),
            "observation_code": [
                "mdr_new",
                "mdr_new",
                "mdr_new",
                "rr_new",
                "rr_new",
                "rr_new",
            ],
            "observation_value_num": [2000.0, 2100.0, 2200.0, 7000.0, 7100.0, 7200.0],
        }
    )
    outputs = train_tb_forecast_artifacts(df, tmp_path, horizon_years=2)
    assert Path(outputs["forecast"]).exists()
    assert Path(outputs["forecast_metrics"]).exists()
