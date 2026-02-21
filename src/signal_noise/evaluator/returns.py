from __future__ import annotations

import pandas as pd

PERIOD_MAP = {
    "1h": 1,
    "4h": 4,
    "1d": 24,
    "1w": 168,
}


def compute_forward_returns(df: pd.DataFrame, periods: list[str]) -> pd.DataFrame:
    """Compute forward returns for given periods from an OHLCV DataFrame.

    Expects 'timestamp' and 'close' columns. Returns DataFrame with
    timestamp + ret_{period} columns.
    """
    result = df[["timestamp", "close"]].copy()
    for period_name in periods:
        shift = PERIOD_MAP.get(period_name)
        if shift is None:
            raise ValueError(f"Unknown period: {period_name}. Available: {list(PERIOD_MAP)}")
        result[f"ret_{period_name}"] = result["close"].pct_change(shift).shift(-shift)
    return result
