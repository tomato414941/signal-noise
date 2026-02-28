"""Tests for GDELT collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.gdelt import (
    GDELT_DOC_SERIES,
    _make_gdelt_doc_collector,
    get_gdelt_collectors,
)


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


class TestGDELTDoc:
    @patch("signal_noise.collector.gdelt.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = GDELT_DOC_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = '{"timeline": [{"series": "", "data": []}]}'
        mock_get.return_value = mock_resp

        cls = _make_gdelt_doc_collector("bitcoin", "test_btc_doc", "Test BTC Doc")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 150

    @patch("signal_noise.collector.gdelt.requests.get")
    def test_empty_response_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = ""
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_gdelt_doc_collector("bitcoin", "test_btc_doc", "Test BTC Doc")
        with pytest.raises(RuntimeError, match="empty response"):
            cls().fetch()

    @patch("signal_noise.collector.gdelt.requests.get")
    def test_no_data_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"timeline": []}
        mock_resp.text = '{"timeline": []}'
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_gdelt_doc_collector("bitcoin", "test_btc_doc", "Test BTC Doc")
        with pytest.raises(RuntimeError, match="No GDELT doc data"):
            cls().fetch()

    def test_meta(self):
        cls = _make_gdelt_doc_collector("bitcoin", "test_btc_doc", "Test BTC Doc")
        assert cls.meta.domain == "sentiment"
        assert cls.meta.category == "attention"


class TestGDELTRegistry:
    def test_doc_series_count(self):
        assert len(GDELT_DOC_SERIES) >= 10

    def test_no_duplicates(self):
        names = [t[1] for t in GDELT_DOC_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_gdelt_collectors()
        assert len(collectors) == len(GDELT_DOC_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = ["gdelt_doc_bitcoin", "gdelt_doc_recession", "gdelt_doc_ai"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
