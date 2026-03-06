from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from signal_noise.collector.alpha_factors import (
    _FACTOR_SPECS,
    _alpha_cache,
    _fetch_close_history,
    _make_alpha_factor_collector,
    _normalize_close_frame,
    get_alpha_factor_collectors,
)
from signal_noise.collector.base import CATEGORIES, DOMAINS


@pytest.fixture(autouse=True)
def _clear_cache():
    _alpha_cache.clear()
    yield
    _alpha_cache.clear()


def _mock_download_frame(data: dict[str, list[float]]) -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    columns = pd.MultiIndex.from_product([["Close"], list(data.keys())])
    values = list(zip(*data.values(), strict=False))
    return pd.DataFrame(values, index=index, columns=columns)


class TestAlphaFactorSpecs:
    def test_spec_count(self):
        assert len(_FACTOR_SPECS) == 10

    def test_no_duplicate_names(self):
        names = [spec.name for spec in _FACTOR_SPECS]
        assert len(names) == len(set(names))

    def test_domain_category_valid(self):
        for spec in _FACTOR_SPECS:
            assert spec.domain in DOMAINS
            assert spec.category in CATEGORIES


class TestHelpers:
    def test_normalize_close_frame_single_ticker(self):
        raw = pd.DataFrame(
            {"Close": [10.0, 11.0, 12.0]},
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        result = _normalize_close_frame(raw, ("SPY",))
        assert list(result.columns) == ["SPY"]
        assert result.index.tz is not None

    def test_normalize_close_frame_missing_close_raises(self):
        raw = pd.DataFrame(
            {"Open": [10.0, 11.0, 12.0]},
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        with pytest.raises(RuntimeError, match="missing Close"):
            _normalize_close_frame(raw, ("SPY",))

    @patch("signal_noise.collector.alpha_factors.yf.download")
    def test_fetch_close_history_uses_cache(self, mock_download):
        raw = _mock_download_frame({"^VIX": [12.0, 13.0, 14.0], "^VVIX": [80.0, 82.0, 84.0]})
        mock_download.return_value = raw

        first = _fetch_close_history(("^VVIX", "^VIX"))
        second = _fetch_close_history(("^VVIX", "^VIX"))

        assert mock_download.call_count == 1
        assert first.equals(second)


class TestAlphaFactorCollectors:
    @patch("signal_noise.collector.alpha_factors.yf.download")
    def test_ratio_factor_fetch(self, mock_download):
        mock_download.return_value = _mock_download_frame(
            {"^VVIX": [90.0, 99.0, 108.0], "^VIX": [15.0, 18.0, 18.0]}
        )

        cls = _make_alpha_factor_collector(_FACTOR_SPECS[0])
        df = cls().fetch()

        assert list(df["value"]) == pytest.approx([6.0, 5.5, 6.0])
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.alpha_factors.yf.download")
    def test_spread_factor_fetch(self, mock_download):
        spec = next(spec for spec in _FACTOR_SPECS if spec.name == "brent_wti_spread")
        mock_download.return_value = _mock_download_frame(
            {"BZ=F": [82.0, 84.5, 83.0], "CL=F": [79.0, 81.0, 80.5]}
        )

        df = _make_alpha_factor_collector(spec)().fetch()

        assert list(df["value"]) == pytest.approx([3.0, 3.5, 2.5])

    @patch("signal_noise.collector.alpha_factors.yf.download")
    def test_zero_denominator_is_dropped(self, mock_download):
        spec = next(spec for spec in _FACTOR_SPECS if spec.name == "hyg_lqd_ratio")
        mock_download.return_value = _mock_download_frame(
            {"HYG": [80.0, 82.0, 84.0], "LQD": [100.0, 0.0, 105.0]}
        )

        df = _make_alpha_factor_collector(spec)().fetch()

        assert len(df) == 2
        assert list(df["value"]) == pytest.approx([0.8, 0.8])

    def test_get_collectors_returns_all(self):
        collectors = get_alpha_factor_collectors()
        assert len(collectors) == len(_FACTOR_SPECS)
        assert "vvix_vix_ratio" in collectors
        assert "brent_wti_spread" in collectors

    def test_registration(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "vvix_vix_ratio",
            "skew_vix_spread",
            "hyg_lqd_ratio",
            "xly_xlp_ratio",
            "copper_gold_ratio",
            "brent_wti_spread",
        ]
        for name in expected:
            assert name in COLLECTORS
