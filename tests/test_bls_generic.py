"""Tests for BLS collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.bls_generic import (
    BLS_SERIES,
    _make_bls_collector,
    get_bls_collectors,
)


BLS_RESPONSE = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {
        "series": [
            {
                "seriesID": "CUSR0000SA0",
                "data": [
                    {"year": "2024", "period": "M03", "value": "312.332"},
                    {"year": "2024", "period": "M02", "value": "310.326"},
                    {"year": "2024", "period": "M01", "value": "308.417"},
                    {"year": "2024", "period": "M13", "value": ""},  # annual avg, skip
                ],
            }
        ]
    },
}

BLS_QUARTERLY_RESPONSE = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {
        "series": [
            {
                "seriesID": "PRS85006092",
                "data": [
                    {"year": "2024", "period": "Q01", "value": "112.5"},
                    {"year": "2023", "period": "Q04", "value": "111.8"},
                ],
            }
        ]
    },
}

BLS_EMPTY = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {"series": []},
}


def _mock_bls_post(primary_response):
    """Return primary response once, then empty for subsequent 20-year chunks."""
    empty = MagicMock()
    empty.json.return_value = BLS_EMPTY
    empty.raise_for_status = MagicMock()

    first = MagicMock()
    first.json.return_value = primary_response
    first.raise_for_status = MagicMock()

    return [first] + [empty] * 5  # enough for 2000-2026 in 20-year chunks


class TestBLSFactory:
    @patch("signal_noise.collector.bls_generic.requests.post")
    def test_fetch_monthly(self, mock_post):
        mock_post.side_effect = _mock_bls_post(BLS_RESPONSE)

        cls = _make_bls_collector(
            "CUSR0000SA0", "test_cpi", "Test CPI",
            "monthly", "macro", "inflation",
        )
        df = cls().fetch()
        assert len(df) == 3  # M13 is skipped
        assert df["value"].iloc[-1] == 312.332
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.bls_generic.requests.post")
    def test_fetch_quarterly(self, mock_post):
        mock_post.side_effect = _mock_bls_post(BLS_QUARTERLY_RESPONSE)

        cls = _make_bls_collector(
            "PRS85006092", "test_prod", "Test Productivity",
            "quarterly", "macro", "economic",
        )
        df = cls().fetch()
        assert len(df) == 2

    @patch("signal_noise.collector.bls_generic.requests.post")
    def test_empty_raises(self, mock_post):
        mock_post.side_effect = _mock_bls_post(BLS_EMPTY)

        cls = _make_bls_collector(
            "CUSR0000SA0", "test_cpi", "Test CPI",
            "monthly", "macro", "inflation",
        )
        with pytest.raises(RuntimeError, match="No BLS data"):
            cls().fetch()


class TestBLSMeta:
    def test_domain_category(self):
        cls = _make_bls_collector(
            "LNS14000000", "test_unemp", "Test Unemployment",
            "monthly", "macro", "labor",
        )
        assert cls.meta.domain == "macro"
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
