import numpy as np
import pandas as pd

from signal_noise.evaluator.pipeline import _align_signal_to_target


class TestAlignSignalToTarget:
    def test_hourly_alignment(self):
        target = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="h", tz="UTC"),
            "close": range(10),
        })
        signal = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="h", tz="UTC"),
            "value": np.arange(10, dtype=float),
        })
        result = _align_signal_to_target(signal, target)
        assert len(result) == 10
        assert not result.isna().all()

    def test_daily_to_hourly_alignment(self):
        target = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=48, freq="h", tz="UTC"),
            "close": range(48),
        })
        signal = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=3, freq="D", tz="UTC"),
            "value": [10.0, 20.0, 30.0],
        })
        result = _align_signal_to_target(signal, target)
        assert len(result) == 48
