"""Tests for CFTC Commitments of Traders collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.cftc_cot import (
    CFTC_COMMODITIES,
    _METRICS,
    _make_cot_collector,
    get_cot_collectors,
)


COT_RESPONSE = [
    {
        "report_date_as_yyyy_mm_dd": "2024-01-16T00:00:00.000",
        "commodity_name": "BITCOIN",
        "open_interest_all": "25000",
        "noncomm_positions_long_all": "15000",
        "noncomm_positions_short_all": "8000",
        "comm_positions_long_all": "5000",
        "comm_positions_short_all": "12000",
    },
    {
        "report_date_as_yyyy_mm_dd": "2024-01-09T00:00:00.000",
        "commodity_name": "BITCOIN",
        "open_interest_all": "24000",
        "noncomm_positions_long_all": "14000",
        "noncomm_positions_short_all": "7500",
        "comm_positions_long_all": "4800",
        "comm_positions_short_all": "11500",
    },
]


class TestCOTFactory:
    @patch("signal_noise.collector.cftc_cot.requests.get")
    def test_fetch_open_interest(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = COT_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_cot_collector(
            "BITCOIN", "cot_btc", "COT Bitcoin",
            "oi", "Open Interest", "open_interest_all",
            "markets", "crypto",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[-1] == 25000.0
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.cftc_cot.requests.get")
    def test_fetch_net_noncommercial(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = COT_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_cot_collector(
            "BITCOIN", "cot_btc", "COT Bitcoin",
            "net_nc", "Net Non-Commercial", None,
            "markets", "crypto",
        )
        df = cls().fetch()
        assert len(df) == 2
        # 15000 - 8000 = 7000
        assert df["value"].iloc[-1] == 7000.0

    @patch("signal_noise.collector.cftc_cot.requests.get")
    def test_fetch_net_commercial(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = COT_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_cot_collector(
            "BITCOIN", "cot_btc", "COT Bitcoin",
            "net_c", "Net Commercial", None,
            "markets", "crypto",
        )
        df = cls().fetch()
        assert len(df) == 2
        # 5000 - 12000 = -7000
        assert df["value"].iloc[-1] == -7000.0

    @patch("signal_noise.collector.cftc_cot.requests.get")
    def test_empty_response_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_cot_collector(
            "BITCOIN", "cot_btc", "COT Bitcoin",
            "oi", "Open Interest", "open_interest_all",
            "markets", "crypto",
        )
        with pytest.raises(RuntimeError, match="No COT data"):
            cls().fetch()


class TestCOTMeta:
    def test_meta_domain_category(self):
        cls = _make_cot_collector(
            "GOLD", "cot_gold", "COT Gold",
            "oi", "Open Interest", "open_interest_all",
            "markets", "commodity",
        )
        assert cls.meta.domain == "markets"
        assert cls.meta.category == "commodity"
        assert cls.meta.update_frequency == "weekly"

    def test_meta_name(self):
        cls = _make_cot_collector(
            "EURO FX", "cot_eur", "COT Euro FX",
            "net_nc", "Net Non-Commercial", None,
            "markets", "forex",
        )
        assert cls.meta.name == "cot_eur_net_nc"
        assert "Net Non-Commercial" in cls.meta.display_name


class TestCOTRegistry:
    def test_commodity_count(self):
        assert len(CFTC_COMMODITIES) >= 25

    def test_metric_count(self):
        assert len(_METRICS) == 3

    def test_no_duplicate_names(self):
        collectors = get_cot_collectors()
        assert len(collectors) == len(CFTC_COMMODITIES) * len(_METRICS)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "cot_btc_oi", "cot_btc_net_nc", "cot_btc_net_c",
            "cot_gold_oi", "cot_gold_net_nc",
            "cot_wti_oi", "cot_es_oi",
            "cot_eur_net_nc", "cot_10y_oi",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_total_count(self):
        collectors = get_cot_collectors()
        assert len(collectors) >= 75
