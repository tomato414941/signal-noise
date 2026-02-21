import numpy as np
import pandas as pd

from signal_noise.evaluator.metrics import (
    compute_ic,
    compute_pearson,
    directional_accuracy,
    evaluate_signal,
    lagged_ic,
)


class TestComputeIC:
    def test_perfect_positive(self):
        s = pd.Series(range(100), dtype=float)
        r = pd.Series(range(100), dtype=float)
        ic, pval = compute_ic(s, r)
        assert ic > 0.99
        assert pval < 0.001

    def test_perfect_negative(self):
        s = pd.Series(range(100), dtype=float)
        r = pd.Series(range(99, -1, -1), dtype=float)
        ic, pval = compute_ic(s, r)
        assert ic < -0.99

    def test_insufficient_data(self):
        s = pd.Series([1.0, 2.0])
        r = pd.Series([1.0, 2.0])
        ic, pval = compute_ic(s, r)
        assert ic == 0.0
        assert pval == 1.0

    def test_handles_nan(self):
        s = pd.Series([1, 2, np.nan, 4, 5, 6, 7, 8, 9, 10, 11, 12], dtype=float)
        r = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], dtype=float)
        ic, pval = compute_ic(s, r)
        assert ic > 0.9


class TestDirectionalAccuracy:
    def test_perfect_match(self):
        s = pd.Series([1, -1, 1, -1, 1, -1, 1, -1, 1, -1], dtype=float)
        r = pd.Series([1, -1, 1, -1, 1, -1, 1, -1, 1, -1], dtype=float)
        assert directional_accuracy(s, r) == 1.0

    def test_opposite(self):
        s = pd.Series([1, -1, 1, -1, 1, -1, 1, -1, 1, -1], dtype=float)
        r = pd.Series([-1, 1, -1, 1, -1, 1, -1, 1, -1, 1], dtype=float)
        assert directional_accuracy(s, r) == 0.0

    def test_insufficient_data(self):
        s = pd.Series([1.0, 2.0])
        r = pd.Series([1.0, 2.0])
        assert directional_accuracy(s, r) == 0.5


class TestLaggedIC:
    def test_finds_best_lag(self):
        np.random.seed(42)
        n = 200
        signal = pd.Series(np.random.randn(n))
        # Construct returns that correlate with signal lagged by 3
        returns = signal.shift(3) * 0.5 + pd.Series(np.random.randn(n)) * 0.1
        best_lag, best_ic = lagged_ic(signal, returns, max_lag=10)
        assert best_lag == 3
        assert best_ic > 0.5


class TestEvaluateSignal:
    def test_returns_all_fields(self):
        np.random.seed(42)
        s = pd.Series(np.random.randn(100))
        r = pd.Series(np.random.randn(100))
        m = evaluate_signal(s, r, "test_source", "1d", max_lag=5)
        assert m.source_name == "test_source"
        assert m.period == "1d"
        assert m.n_observations == 100
        assert 0 <= m.directional_accuracy <= 1
