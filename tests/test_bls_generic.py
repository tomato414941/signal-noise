"""Tests for BLS collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.bls_generic import (
    BLS_SERIES,
    _bls_cache,
    _make_bls_collector,
    _parse_bls_items,
    get_bls_collectors,
)


MONTHLY_ITEMS = [
    {"year": "2024", "period": "M03", "value": "312.332"},
    {"year": "2024", "period": "M02", "value": "310.326"},
    {"year": "2024", "period": "M01", "value": "308.417"},
    {"year": "2024", "period": "M13", "value": ""},  # annual avg, skip
]

QUARTERLY_ITEMS = [
    {"year": "2024", "period": "Q01", "value": "112.5"},
    {"year": "2023", "period": "Q04", "value": "111.8"},
]

# Batched API response with multiple series
BATCH_RESPONSE = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {
        "series": [
            {"seriesID": "CUSR0000SA0", "data": MONTHLY_ITEMS},
            {"seriesID": "PRS85006092", "data": QUARTERLY_ITEMS},
        ]
    },
}

EMPTY_RESPONSE = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {"series": []},
}

RATE_LIMITED_RESPONSE = {
    "status": "REQUEST_NOT_PROCESSED",
    "message": ["daily threshold reached"],
    "Results": {},
}


@pytest.fixture(autouse=True)
def _clear_cache():
    """Clear BLS cache before each test."""
    _bls_cache.clear()
    yield
    _bls_cache.clear()


class TestParseBLSItems:
    def test_monthly(self):
        rows = _parse_bls_items(MONTHLY_ITEMS)
        assert len(rows) == 3  # M13 skipped
        values = {r["value"] for r in rows}
        assert 312.332 in values

    def test_quarterly(self):
        rows = _parse_bls_items(QUARTERLY_ITEMS)
        assert len(rows) == 2
        months = {r["date"].month for r in rows}
        assert 10 in months  # Q04 -> month 10
        assert 1 in months   # Q01 -> month 1

    def test_annual(self):
        items = [{"year": "2024", "period": "A01", "value": "100.0"}]
        rows = _parse_bls_items(items)
        assert len(rows) == 1
        assert rows[0]["date"].month == 1

    def test_empty_value_skipped(self):
        items = [{"year": "2024", "period": "M01", "value": ""}]
        rows = _parse_bls_items(items)
        assert len(rows) == 0

    def test_semiannual_skipped(self):
        items = [{"year": "2024", "period": "S01", "value": "100.0"}]
        rows = _parse_bls_items(items)
        assert len(rows) == 0


class TestBLSBatchFetch:
    @patch("signal_noise.collector.bls_generic.requests.post")
    def test_batch_fetches_all_series(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = BATCH_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        cls = _make_bls_collector(
            "CUSR0000SA0", "test_cpi", "Test CPI",
            "monthly", "economy", "inflation",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[-1] == 312.332
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.bls_generic.requests.post")
    def test_cache_prevents_duplicate_requests(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = BATCH_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        cpi_cls = _make_bls_collector(
            "CUSR0000SA0", "test_cpi", "Test CPI",
            "monthly", "economy", "inflation",
        )
        prod_cls = _make_bls_collector(
            "PRS85006092", "test_prod", "Test Productivity",
            "quarterly", "economy", "economic",
        )
        cpi_cls().fetch()
        prod_cls().fetch()

        # Both fetches should share the same batched request (cached)
        # Without key: 46 series / 25 per batch = 2 requests
        assert mock_post.call_count == 2

    @patch("signal_noise.collector.bls_generic.requests.post")
    def test_rate_limit_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = RATE_LIMITED_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        cls = _make_bls_collector(
            "CUSR0000SA0", "test_cpi", "Test CPI",
            "monthly", "economy", "inflation",
        )
        with pytest.raises(RuntimeError, match="BLS API rejected"):
            cls().fetch()

    @patch("signal_noise.collector.bls_generic.requests.post")
    def test_empty_series_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EMPTY_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        cls = _make_bls_collector(
            "CUSR0000SA0", "test_cpi", "Test CPI",
            "monthly", "economy", "inflation",
        )
        with pytest.raises(RuntimeError, match="No BLS data"):
            cls().fetch()


class TestBLSMeta:
    def test_domain_category(self):
        cls = _make_bls_collector(
            "LNS14000000", "test_unemp", "Test Unemployment",
            "monthly", "economy", "labor",
        )
        assert cls.meta.domain == "economy"
        assert cls.meta.category == "labor"
        assert cls.meta.requires_key is False


class TestBLSRegistry:
    def test_series_count(self):
        assert len(BLS_SERIES) >= 40

    def test_no_duplicates(self):
        names = [t[1] for t in BLS_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_bls_collectors()
        assert len(collectors) == len(BLS_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "bls_cpi_all", "bls_cpi_core", "bls_nfp_total",
            "bls_unemp_rate", "bls_jolts_openings",
            "bls_avg_hourly_priv", "bls_import_price",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
