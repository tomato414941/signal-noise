import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def btc_hourly_df():
    n = 200
    np.random.seed(42)
    returns = np.random.normal(0, 0.01, n)
    prices = 50000.0 * np.cumprod(1 + returns)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="h"),
        "open": prices * 0.999,
        "high": prices * 1.005,
        "low": prices * 0.995,
        "close": prices,
        "volume": np.random.uniform(100, 1000, n),
    })


@pytest.fixture
def daily_signal_df():
    n = 30
    np.random.seed(123)
    return pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=n, freq="D"),
        "value": np.random.randn(n),
    })
