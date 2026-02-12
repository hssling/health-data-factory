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


def train_tb_forecast_artifacts(
    canonical_df: pd.DataFrame, output_dir: Path, horizon_years: int = 3
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    tb_df = canonical_df.copy()
    tb_df["year"] = pd.to_datetime(tb_df["event_date"]).dt.year

    forecast_rows: list[dict[str, object]] = []
    metric_rows: list[dict[str, object]] = []
    for code, group in tb_df.groupby("observation_code"):
        group = group.sort_values("year")
        if group["year"].nunique() < 2:
            continue
        x = group[["year"]].astype(float)
        y = group["observation_value_num"].astype(float)
        model = LinearRegression()
        model.fit(x, y)
        pred = model.predict(x)
        metric_rows.append(
            {
                "observation_code": str(code),
                "samples": int(len(group)),
                "mae": float(mean_absolute_error(y, pred)),
                "r2": float(r2_score(y, pred)),
            }
        )

        max_year = int(group["year"].max())
        for year in range(max_year + 1, max_year + horizon_years + 1):
            predicted = float(model.predict(pd.DataFrame({"year": [float(year)]}))[0])
            forecast_rows.append(
                {
                    "observation_code": str(code),
                    "forecast_year": int(year),
                    "predicted_value": max(0.0, predicted),
                }
            )

    forecast_df = pd.DataFrame(forecast_rows)
    metrics_df = pd.DataFrame(metric_rows)

    forecast_path = output_dir / "tb_forecast.parquet"
    metrics_path = output_dir / "tb_forecast_metrics.json"
    forecast_df.to_parquet(forecast_path, index=False)
    metrics_path.write_text(
        json.dumps(metrics_df.to_dict(orient="records"), indent=2), encoding="utf-8"
    )
    return {"forecast": str(forecast_path), "forecast_metrics": str(metrics_path)}
