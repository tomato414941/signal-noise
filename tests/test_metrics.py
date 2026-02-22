import numpy as np
import pandas as pd

from signal_noise.evaluator.metrics import (
    SignalMetrics,
    compute_ic,
    directional_accuracy,
    evaluate_signal,
    lagged_ic,
)


class TestComputeIC:
    def test_perfect_positive_correlation(self):
        signal = pd.Series(np.arange(100, dtype=float))
        returns = pd.Series(np.arange(100, dtype=float))
        ic, pval = compute_ic(signal, returns)
        assert ic > 0.99
        assert pval < 0.01

    def test_perfect_negative_correlation(self):
        signal = pd.Series(np.arange(100, dtype=float))
        returns = pd.Series(-np.arange(100, dtype=float))
        ic, pval = compute_ic(signal, returns)
        assert ic < -0.99

    def test_no_correlation(self):
        np.random.seed(42)
        signal = pd.Series(np.random.randn(1000))
        returns = pd.Series(np.random.randn(1000))
        ic, pval = compute_ic(signal, returns)
        assert abs(ic) < 0.1

    def test_insufficient_data(self):
        signal = pd.Series([1.0, 2.0, 3.0])
        returns = pd.Series([4.0, 5.0, 6.0])
        ic, pval = compute_ic(signal, returns)
        assert ic == 0.0
        assert pval == 1.0

    def test_handles_nan(self):
        signal = pd.Series([1.0, np.nan, 3.0, 4.0, 5.0] * 5)
        returns = pd.Series([2.0, 3.0, np.nan, 5.0, 6.0] * 5)
        ic, pval = compute_ic(signal, returns)
        assert isinstance(ic, float)


class TestDirectionalAccuracy:
    def test_perfect_alignment(self):
        signal = pd.Series([1.0, -1.0, 1.0, -1.0] * 5)
        returns = pd.Series([0.5, -0.5, 0.5, -0.5] * 5)
        da = directional_accuracy(signal, returns)
        assert da == 1.0

    def test_opposite_direction(self):
        signal = pd.Series([1.0, -1.0, 1.0, -1.0] * 5)
        returns = pd.Series([-0.5, 0.5, -0.5, 0.5] * 5)
        da = directional_accuracy(signal, returns)
        assert da == 0.0

    def test_random_around_half(self):
        np.random.seed(42)
        signal = pd.Series(np.random.randn(1000))
        returns = pd.Series(np.random.randn(1000))
        da = directional_accuracy(signal, returns)
        assert 0.4 < da < 0.6

    def test_insufficient_data(self):
        signal = pd.Series([1.0, 2.0])
        returns = pd.Series([1.0, 2.0])
        da = directional_accuracy(signal, returns)
        assert da == 0.5


class TestLaggedIC:
    def test_finds_lagged_signal(self):
        np.random.seed(42)
        n = 1000
        base_signal = pd.Series(np.random.randn(n))
        # returns[t] = signal[t-5] + noise -> shift(5) aligns them
        returns = base_signal.shift(5) + pd.Series(np.random.randn(n) * 0.1)
        returns = returns.fillna(0.0)
        best_lag, best_ic = lagged_ic(base_signal, returns, max_lag=10)
        assert best_lag == 5
        assert abs(best_ic) > 0.3


class TestEvaluateSignal:
    def test_returns_signal_metrics(self):
        np.random.seed(42)
        signal = pd.Series(np.random.randn(200))
        returns = pd.Series(np.random.randn(200))
        result = evaluate_signal(signal, returns, "test_source", "1h", max_lag=5)
        assert isinstance(result, SignalMetrics)
        assert result.collector_name == "test_source"
        assert result.period == "1h"
        assert result.n_observations == 200

    def test_correlated_signal_has_high_ic(self):
        np.random.seed(42)
        base = np.random.randn(200)
        signal = pd.Series(base)
        returns = pd.Series(base + np.random.randn(200) * 0.1)
        result = evaluate_signal(signal, returns, "corr_source", "1d", max_lag=5)
        assert abs(result.ic) > 0.5
        assert result.ic_pvalue < 0.01
