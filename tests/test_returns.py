import pandas as pd
import pytest

from signal_noise.evaluator.returns import compute_forward_returns


class TestForwardReturns:
    def test_basic_computation(self, btc_hourly_df):
        result = compute_forward_returns(btc_hourly_df, ["1h"])
        assert "ret_1h" in result.columns
        assert len(result) == len(btc_hourly_df)
        assert result["ret_1h"].iloc[-1] != result["ret_1h"].iloc[-1]  # NaN at end

    def test_multiple_periods(self, btc_hourly_df):
        result = compute_forward_returns(btc_hourly_df, ["1h", "4h", "1d"])
        assert "ret_1h" in result.columns
        assert "ret_4h" in result.columns
        assert "ret_1d" in result.columns

    def test_unknown_period_raises(self, btc_hourly_df):
        with pytest.raises(ValueError, match="Unknown period"):
            compute_forward_returns(btc_hourly_df, ["3m"])

    def test_return_values_reasonable(self, btc_hourly_df):
        result = compute_forward_returns(btc_hourly_df, ["1h"])
        valid = result["ret_1h"].dropna()
        assert valid.abs().max() < 0.5  # no 50%+ hourly moves in synthetic data
