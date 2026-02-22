"""Tests for GDELT collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.gdelt import (
    GDELT_TV_SERIES,
    GDELT_DOC_SERIES,
    _make_gdelt_tv_collector,
    _make_gdelt_doc_collector,
    get_gdelt_collectors,
)


GDELT_TV_RESPONSE = {
    "timeline": [
        {
            "series": "",
            "data": [
                {"date": "2024-01-15T00:00:00Z", "value": 2.5},
                {"date": "2024-01-16T00:00:00Z", "value": 3.1},
            ],
        }
    ]
}

GDELT_DOC_RESPONSE = {
    "timeline": [
        {
            "series": "",
            "data": [
                {"date": "2024-01-15T00:00:00Z", "value": 150},
                {"date": "2024-01-16T00:00:00Z", "value": 180},
            ],
        }
    ]
}


class TestGDELTTV:
    @patch("signal_noise.collector.gdelt.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = GDELT_TV_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_gdelt_tv_collector("bitcoin", "timelinevol", "test_btc", "Test BTC")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 2.5

    @patch("signal_noise.collector.gdelt.requests.get")
    def test_empty_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"timeline": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_gdelt_tv_collector("bitcoin", "timelinevol", "test_btc", "Test BTC")
        with pytest.raises(RuntimeError, match="No GDELT TV data"):
            cls().fetch()


class TestGDELTDoc:
    @patch("signal_noise.collector.gdelt.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = GDELT_DOC_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_gdelt_doc_collector("bitcoin", "test_btc_doc", "Test BTC Doc")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 150

    def test_meta(self):
        cls = _make_gdelt_doc_collector("bitcoin", "test_btc_doc", "Test BTC Doc")
        assert cls.meta.domain == "sentiment"
        assert cls.meta.category == "attention"


class TestGDELTRegistry:
    def test_tv_series_count(self):
        assert len(GDELT_TV_SERIES) >= 20

    def test_doc_series_count(self):
        assert len(GDELT_DOC_SERIES) >= 10

    def test_no_duplicates(self):
        names = [t[2] for t in GDELT_TV_SERIES] + [t[1] for t in GDELT_DOC_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_gdelt_collectors()
        assert len(collectors) == len(GDELT_TV_SERIES) + len(GDELT_DOC_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "gdelt_tv_bitcoin", "gdelt_tv_inflation", "gdelt_tv_fed",
            "gdelt_doc_bitcoin", "gdelt_doc_recession",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
