from pathlib import Path

import pandas as pd
from pipelines.modeling import train_baseline_model


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
