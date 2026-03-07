from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS
from signal_noise.collector.sec_form4_factors import (
    _form4_factor_cache,
    get_sec_form4_factor_collectors,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    _form4_factor_cache.clear()
    yield
    _form4_factor_cache.clear()


def _history(ratio_day1: float, ratio_day2: float, tx_day1: float, tx_day2: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [
                pd.Timestamp("2026-03-04", tz="UTC"),
                pd.Timestamp("2026-03-05", tz="UTC"),
            ],
            "net_share_ratio": [ratio_day1, ratio_day2],
            "open_market_tx_count": [tx_day1, tx_day2],
        }
    )


_FORM4_HISTORIES = {
    "TSLA": _history(0.8, 0.0, 2.0, 0.0),
    "META": _history(-0.2, 0.7, 1.0, 1.0),
    "NVDA": _history(0.1, -0.1, 1.0, 1.0),
    "GS": _history(-0.5, -0.4, 1.0, 1.0),
    "JPM": _history(0.6, 0.3, 2.0, 1.0),
    "XOM": _history(1.0, 0.0, 2.0, 0.0),
    "CVX": _history(-0.3, -0.2, 1.0, 1.0),
    "DHI": _history(0.4, 0.9, 1.0, 2.0),
    "LEN": _history(0.2, 0.0, 1.0, 0.0),
    "CAT": _history(-0.1, -0.6, 1.0, 2.0),
    "DE": _history(0.5, 0.2, 1.0, 1.0),
}


class TestSECForm4Factors:
    @patch("signal_noise.collector.sec_form4_factors._fetch_ticker_history")
    def test_cross_sector_breadth(self, mock_history):
        mock_history.side_effect = lambda ticker: _FORM4_HISTORIES[ticker]

        collectors = get_sec_form4_factor_collectors()
        df = collectors["form4_cross_sector_net_buy_breadth"]().fetch()

        assert list(df["value"]) == pytest.approx([7.0 / 11.0, 4.0 / 11.0])

    @patch("signal_noise.collector.sec_form4_factors._fetch_ticker_history")
    def test_cluster_and_spread(self, mock_history):
        mock_history.side_effect = lambda ticker: _FORM4_HISTORIES[ticker]

        collectors = get_sec_form4_factor_collectors()

        cluster = collectors["form4_cross_sector_tx_cluster_count"]().fetch()
        assert list(cluster["value"]) == pytest.approx([11.0, 8.0])

        spread = collectors["form4_tech_financials_net_buy_spread"]().fetch()
        assert spread["value"].iloc[0] == pytest.approx((0.8 - 0.2 + 0.1) / 3 - (-0.5 + 0.6) / 2)

    @patch("signal_noise.collector.sec_form4_factors._fetch_ticker_history")
    def test_high_conviction_breadth(self, mock_history):
        mock_history.side_effect = lambda ticker: _FORM4_HISTORIES[ticker]

        collectors = get_sec_form4_factor_collectors()
        df = collectors["form4_cross_sector_high_conviction_breadth"]().fetch()

        assert list(df["value"]) == pytest.approx([5.0 / 11.0, 3.0 / 11.0])

    def test_meta_taxonomy_valid(self):
        collectors = get_sec_form4_factor_collectors()
        for cls in collectors.values():
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    def test_registration(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "form4_cross_sector_net_buy_breadth",
            "form4_energy_net_buy_breadth",
            "form4_tech_financials_net_buy_spread",
        ]
        for name in expected:
            assert name in COLLECTORS
