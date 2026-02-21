import pandas as pd
import pytest

from signal_noise.evaluator.returns import PERIOD_MAP, compute_forward_returns


class TestComputeForwardReturns:
    def test_columns_created(self, btc_hourly_df):
        result = compute_forward_returns(btc_hourly_df, ["1h", "4h"])
        assert "ret_1h" in result.columns
        assert "ret_4h" in result.columns

    def test_1h_return_values(self):
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="h"),
            "close": [100.0, 110.0, 105.0, 115.0, 120.0],
        })
        result = compute_forward_returns(df, ["1h"])
        # pct_change(1) gives [NaN, 0.1, -0.0454, 0.0952, 0.0434]
        # shift(-1) gives [0.1, -0.0454, 0.0952, 0.0434, NaN]
        expected_0 = (110.0 - 100.0) / 100.0
        assert abs(result["ret_1h"].iloc[0] - expected_0) < 1e-10

    def test_last_values_are_nan(self, btc_hourly_df):
        result = compute_forward_returns(btc_hourly_df, ["1d"])
        assert result["ret_1d"].iloc[-1] != result["ret_1d"].iloc[-1]  # NaN check

    def test_unknown_period_raises(self, btc_hourly_df):
        with pytest.raises(ValueError, match="Unknown period"):
            compute_forward_returns(btc_hourly_df, ["2d"])

    def test_period_map_values(self):
        assert PERIOD_MAP["1h"] == 1
        assert PERIOD_MAP["4h"] == 4
        assert PERIOD_MAP["1d"] == 24
        assert PERIOD_MAP["1w"] == 168

    def test_preserves_timestamp_column(self, btc_hourly_df):
        result = compute_forward_returns(btc_hourly_df, ["1h"])
        assert "timestamp" in result.columns
        assert len(result) == len(btc_hourly_df)
