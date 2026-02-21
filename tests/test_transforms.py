import numpy as np
import pandas as pd
import pytest

from signal_noise.transforms import (
    TRANSFORMS,
    _bollinger_pct,
    _diff,
    _ema_ratio,
    _log_return,
    _mean_reversion,
    _momentum,
    _rank_pct,
    _roc,
    _rolling_vol,
    _rsi,
    _sma_ratio,
    _z_score,
)


@pytest.fixture
def sample_series():
    np.random.seed(42)
    return pd.Series(100 + np.cumsum(np.random.randn(200)))


class TestTransformRegistry:
    def test_builds_expected_count(self):
        assert len(TRANSFORMS) == 22

    def test_all_have_unique_names(self):
        names = [t.name for t in TRANSFORMS]
        assert len(names) == len(set(names))

    def test_all_callable(self, sample_series):
        for t in TRANSFORMS:
            result = t.fn(sample_series)
            assert isinstance(result, pd.Series)
            assert len(result) == len(sample_series)


class TestZScore:
    def test_output_shape(self, sample_series):
        result = _z_score(sample_series, 20)
        assert len(result) == len(sample_series)

    def test_approximately_zero_mean(self, sample_series):
        result = _z_score(sample_series, 20)
        valid = result.dropna()
        assert abs(valid.iloc[-50:].mean()) < 2.0


class TestSmaRatio:
    def test_output_shape(self, sample_series):
        result = _sma_ratio(sample_series, 20)
        assert len(result) == len(sample_series)

    def test_ratio_around_one(self, sample_series):
        result = _sma_ratio(sample_series, 10)
        valid = result.dropna()
        assert 0.5 < valid.mean() < 1.5


class TestMomentum:
    def test_output_shape(self, sample_series):
        result = _momentum(sample_series, 5)
        assert len(result) == len(sample_series)

    def test_first_values_nan(self, sample_series):
        result = _momentum(sample_series, 5)
        assert result.iloc[:5].isna().all()


class TestRSI:
    def test_range(self, sample_series):
        result = _rsi(sample_series, 14)
        valid = result.dropna()
        assert valid.min() >= 0
        assert valid.max() <= 100

    def test_output_shape(self, sample_series):
        result = _rsi(sample_series, 14)
        assert len(result) == len(sample_series)


class TestRollingVol:
    def test_non_negative(self, sample_series):
        result = _rolling_vol(sample_series, 10)
        valid = result.dropna()
        assert (valid >= 0).all()


class TestLogReturn:
    def test_output_shape(self, sample_series):
        result = _log_return(sample_series)
        assert len(result) == len(sample_series)

    def test_first_nan(self, sample_series):
        result = _log_return(sample_series)
        assert np.isnan(result.iloc[0])


class TestBollingerPct:
    def test_range(self, sample_series):
        result = _bollinger_pct(sample_series, 20)
        valid = result.dropna()
        assert valid.min() > -1
        assert valid.max() < 2


class TestMeanReversion:
    def test_output_shape(self, sample_series):
        result = _mean_reversion(sample_series, 20)
        assert len(result) == len(sample_series)


class TestEmaRatio:
    def test_output_shape(self, sample_series):
        result = _ema_ratio(sample_series, 10)
        assert len(result) == len(sample_series)

    def test_ratio_around_one(self, sample_series):
        result = _ema_ratio(sample_series, 10)
        valid = result.dropna()
        assert 0.5 < valid.mean() < 1.5


class TestDiff:
    def test_output_shape(self, sample_series):
        result = _diff(sample_series, 5)
        assert len(result) == len(sample_series)


class TestRoc:
    def test_output_shape(self, sample_series):
        result = _roc(sample_series, 5)
        assert len(result) == len(sample_series)


class TestRankPct:
    def test_range(self, sample_series):
        result = _rank_pct(sample_series, 20)
        valid = result.dropna()
        assert valid.min() >= 0
        assert valid.max() <= 1
