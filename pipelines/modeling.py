from __future__ import annotations

import json
from pathlib import Path

import joblib  # type: ignore[import-untyped]
import pandas as pd
from sklearn.linear_model import LinearRegression  # type: ignore[import-untyped]
from sklearn.metrics import mean_absolute_error, r2_score  # type: ignore[import-untyped]


def train_baseline_model(canonical_df: pd.DataFrame, output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    model_df = canonical_df.copy()
    model_df["year"] = pd.to_datetime(model_df["event_date"]).dt.year

    x = model_df[["year"]].astype(float)
    y = model_df["observation_value_num"].astype(float)

    model = LinearRegression()
    model.fit(x, y)
    preds = model.predict(x)

    metrics = {
        "mae": float(mean_absolute_error(y, preds)),
        "r2": float(r2_score(y, preds)),
        "samples": int(len(model_df)),
    }

    model_path = output_dir / "baseline_regressor.joblib"
    metrics_path = output_dir / "baseline_metrics.json"
    joblib.dump(model, model_path)
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    return {"model": str(model_path), "metrics": str(metrics_path)}
