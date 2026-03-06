"""Tests for EIA energy collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.eia_generic import (
    EIA_SERIES,
    _eia_cache,
    _make_eia_collector,
    get_eia_collectors,
)


# Batched response with multiple series identified by facet_key field
EIA_BATCH_RESPONSE = {
    "response": {
        "total": 4,
        "data": [
            {"period": "2024-01-15", "series": "RWTC", "value": "72.50"},
            {"period": "2024-01-16", "series": "RWTC", "value": "73.10"},
            {"period": "2024-01-15", "series": "RBRTE", "value": "77.00"},
            {"period": "2024-01-16", "series": "RBRTE", "value": "78.50"},
        ],
    }
}

EIA_PAGED_RESPONSE_1 = {
    "response": {
        "total": 4,
        "data": [
            {"period": "2024-01-15", "series": "RWTC", "value": "72.50"},
            {"period": "2024-01-16", "series": "RWTC", "value": "73.10"},
        ],
    }
}

EIA_PAGED_RESPONSE_2 = {
    "response": {
        "total": 4,
        "data": [
            {"period": "2024-01-17", "series": "RWTC", "value": "74.00"},
            {"period": "2024-01-18", "series": "RWTC", "value": "75.50"},
        ],
    }
}

EIA_EMPTY = {"response": {"total": 0, "data": []}}


@pytest.fixture(autouse=True)
def _clear_cache():
    """Clear EIA cache before each test."""
    _eia_cache.clear()
    yield
    _eia_cache.clear()


class TestEIABatchFetch:
    @patch("signal_noise.collector.eia_generic._get_eia_key", return_value="test_key")
    @patch("signal_noise.collector.eia_generic.requests.get")
    def test_fetch_parses(self, mock_get, mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EIA_BATCH_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_eia_collector(
            "petroleum/pri/spt/data", "series", "RWTC",
            "value", "daily",
            "test_wti", "Test WTI", "markets", "commodity",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 72.50
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.eia_generic._get_eia_key", return_value="test_key")
    @patch("signal_noise.collector.eia_generic.requests.get")
    def test_cache_prevents_duplicate_requests(self, mock_get, mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EIA_BATCH_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        wti_cls = _make_eia_collector(
            "petroleum/pri/spt/data", "series", "RWTC",
            "value", "daily",
            "test_wti", "Test WTI", "markets", "commodity",
        )
        brent_cls = _make_eia_collector(
            "petroleum/pri/spt/data", "series", "RBRTE",
            "value", "daily",
            "test_brent", "Test Brent", "markets", "commodity",
        )
        wti_df = wti_cls().fetch()
        brent_df = brent_cls().fetch()

        assert len(wti_df) == 2
        assert len(brent_df) == 2
        # Both share the same group, so only 1 API call
        assert mock_get.call_count == 1

    @patch("signal_noise.collector.eia_generic._get_eia_key", return_value="test_key")
    @patch("signal_noise.collector.eia_generic.requests.get")
    def test_fetch_pagination(self, mock_get, mock_key):
        mock_resp_1 = MagicMock()
        mock_resp_1.json.return_value = EIA_PAGED_RESPONSE_1
        mock_resp_1.raise_for_status = MagicMock()

        mock_resp_2 = MagicMock()
        mock_resp_2.json.return_value = EIA_PAGED_RESPONSE_2
        mock_resp_2.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_resp_1, mock_resp_2]

        cls = _make_eia_collector(
            "petroleum/pri/spt/data", "series", "RWTC",
            "value", "daily",
            "test_wti", "Test WTI", "markets", "commodity",
        )
        df = cls().fetch()
        assert len(df) == 4

    @patch("signal_noise.collector.eia_generic._get_eia_key", return_value="test_key")
    @patch("signal_noise.collector.eia_generic.requests.get")
    def test_empty_raises(self, mock_get, mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EIA_EMPTY
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_eia_collector(
            "petroleum/pri/spt/data", "series", "RWTC",
            "value", "daily",
            "test_wti", "Test WTI", "markets", "commodity",
        )
        with pytest.raises(RuntimeError, match="No data for EIA"):
            cls().fetch()

    def test_no_key_raises(self):
        cls = _make_eia_collector(
            "petroleum/pri/spt/data", "series", "RWTC",
            "value", "daily",
            "test_wti", "Test WTI", "markets", "commodity",
        )
        from signal_noise.collector import _auth
        _auth._cache.pop("EIA_API_KEY", None)
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(RuntimeError, match="EIA_API_KEY"):
                cls().fetch()


class TestEIAMeta:
    def test_requires_key(self):
        cls = _make_eia_collector(
            "petroleum/pri/spt/data", "series", "RWTC",
            "value", "daily",
            "test_wti", "Test WTI", "markets", "commodity",
        )
        assert cls.meta.requires_key is True

    def test_domain_category(self):
        cls = _make_eia_collector(
            "total-energy/data", "msn", "TETCBUS",
            "value", "monthly",
            "test_total", "Test Total", "economy", "economic",
        )
        assert cls.meta.domain == "economy"
        assert cls.meta.category == "economic"


class TestEIARegistry:
    def test_series_count(self):
        assert len(EIA_SERIES) >= 40

    def test_no_duplicates(self):
        names = [t[5] for t in EIA_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_eia_collectors()
        assert len(collectors) == len(EIA_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "eia_wti_spot", "eia_brent_spot", "eia_henryhub_spot",
            "eia_crude_stocks", "eia_gasoline_retail",
            "eia_total_consumption",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
