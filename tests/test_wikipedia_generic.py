"""Tests for generic Wikipedia pageview collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.wikipedia_generic import (
    WIKIPEDIA_PAGES,
    get_wiki_collectors,
    _make_wiki_collector,
)


WIKI_API_RESPONSE = {
    "items": [
        {"timestamp": "2024010100", "views": 5000},
        {"timestamp": "2024010200", "views": 6200},
        {"timestamp": "2024010300", "views": 4800},
    ]
}


class TestWikiGenericFactory:
    def test_page_count(self):
        assert len(WIKIPEDIA_PAGES) >= 24

    def test_no_duplicate_names(self):
        names = [t[1] for t in WIKIPEDIA_PAGES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_wiki_collector("Recession", "wiki_recession", "Wikipedia: Recession", "fear")
        assert cls.meta.name == "wiki_recession"
        assert cls.meta.data_type == "fear"

    def test_get_wiki_collectors_returns_dict(self):
        collectors = get_wiki_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(WIKIPEDIA_PAGES)

    def test_all_categories_present(self):
        data_types = {t[3] for t in WIKIPEDIA_PAGES}
        assert "fear" in data_types
        assert "safe_haven" in data_types
        assert "crypto_attention" in data_types
        assert "geopolitical" in data_types
        assert "greed" in data_types


class TestWikiGenericFetch:
    @patch("signal_noise.collector.wikipedia_generic.requests.get")
    def test_fetch_parses_pageviews(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = WIKI_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_wiki_collector("Recession", "wiki_test", "Test", "fear")
        df = cls().fetch()
        assert "date" in df.columns
        assert "value" in df.columns
        assert len(df) == 3
        assert df["value"].iloc[0] == 5000.0

    @patch("signal_noise.collector.wikipedia_generic.requests.get")
    def test_sorted_by_date(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = WIKI_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_wiki_collector("Gold", "wiki_gold", "Gold", "safe_haven")
        df = cls().fetch()
        assert df["date"].is_monotonic_increasing


class TestRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "wiki_recession", "wiki_inflation", "wiki_bank_run",
            "wiki_gold", "wiki_ethereum", "wiki_war",
            "wiki_bull_market", "wiki_bubble",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
