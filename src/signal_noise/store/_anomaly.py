"""Pure functions for anomaly detection using robust statistics."""
from __future__ import annotations

import pandas as pd


def detect_anomalies(
    new_values: pd.Series,
    new_timestamps: pd.Series,
    history: list[float],
    z_threshold: float = 4.0,
    min_history: int = 10,
) -> list[dict]:
    """Detect outliers in new_values against a historical baseline.

    Uses robust median/MAD instead of mean/std to prevent baseline
    contamination from prior outliers.

    Args:
        new_values: Series of new values to test.
        new_timestamps: Corresponding timestamps (same index as new_values).
        history: Recent historical values for baseline computation.
        z_threshold: Z-score threshold for outlier classification.
        min_history: Minimum history size needed to judge.

    Returns:
        List of anomaly dicts with timestamp, value, z_score, median, mad.
    """
    if new_values.empty or len(history) < min_history:
        return []

    hist = pd.Series(history, dtype=float)
    median = hist.median()
    mad = (hist - median).abs().median() * 1.4826  # MAD → std scale
    if mad == 0 or pd.isna(mad):
        return []

    anomalies = []
    for idx, val in new_values.items():
        z = abs((val - median) / mad)
        if z > z_threshold:
            ts = str(new_timestamps.loc[idx]) if idx in new_timestamps.index else ""
            anomalies.append({
                "timestamp": ts,
                "value": float(val),
                "z_score": round(float(z), 2),
                "median": round(float(median), 4),
                "mad": round(float(mad), 4),
            })
    return anomalies
