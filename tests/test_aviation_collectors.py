"""Tests for aviation collectors (OpenSky, FR24)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.aviation import (
    FR24TotalCollector,
    OpenSkyTotalCollector,
    OpenSkyUSCollector,
    _opensky_cache,
)
from signal_noise.collector.base import CollectorMeta


@pytest.fixture(autouse=True)
def _clear_opensky_cache():
    _opensky_cache.clear()
    yield
    _opensky_cache.clear()


class TestOpenSkyTotal:
    def test_meta(self):
        assert OpenSkyTotalCollector.meta.name == "opensky_total"
        assert OpenSkyTotalCollector.meta.category == "aviation"
        assert isinstance(OpenSkyTotalCollector.meta, CollectorMeta)

    @patch("signal_noise.collector.aviation.requests.get")
    def test_fetch_counts_all_states(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "time": 1700000000,
            "states": [
                ["abc", "UAL123", "United States", 0, 0, -73.0, 40.0, 10000, False, 200, 90, 0, None, 10000, None, False, 0],
                ["def", "BAW456", "United Kingdom", 0, 0, -0.5, 51.5, 11000, False, 250, 180, 0, None, 11000, None, False, 0],
                ["ghi", "DLH789", "Germany", 0, 0, 8.5, 50.0, 12000, False, 220, 270, 0, None, 12000, None, False, 0],
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = OpenSkyTotalCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 3.0

    @patch("signal_noise.collector.aviation.requests.get")
    def test_fetch_empty_states(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"time": 1700000000, "states": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = OpenSkyTotalCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 0.0


class TestOpenSkyUS:
    def test_meta(self):
        assert OpenSkyUSCollector.meta.name == "opensky_us"
        assert OpenSkyUSCollector.meta.category == "aviation"

    @patch("signal_noise.collector.aviation.requests.get")
    def test_fetch_filters_us_only(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "time": 1700000000,
            "states": [
                ["abc", "UAL123", "United States", 0, 0, -73.0, 40.0, 10000, False, 200, 90, 0, None, 10000, None, False, 0],
                ["def", "BAW456", "United Kingdom", 0, 0, -0.5, 51.5, 11000, False, 250, 180, 0, None, 11000, None, False, 0],
                ["ghi", "AAL789", "United States", 0, 0, -87.0, 41.0, 9000, False, 180, 45, 0, None, 9000, None, False, 0],
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = OpenSkyUSCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 2.0


class TestFR24Total:
    def test_meta(self):
        assert FR24TotalCollector.meta.name == "fr24_total"
        assert FR24TotalCollector.meta.category == "aviation"

    @patch("signal_noise.collector.aviation.requests.get")
    def test_fetch_reads_full_count(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "full_count": 19774,
            "version": 4,
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = FR24TotalCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 19774.0

    @patch("signal_noise.collector.aviation.requests.get")
    def test_fetch_missing_full_count(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"version": 4}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = FR24TotalCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 0.0


class TestAviationRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["opensky_total", "opensky_us"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_wiki_logistics_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "wiki_supply_chain", "wiki_containerization",
            "wiki_suez_canal", "wiki_panama_canal",
            "wiki_baltic_exchange", "wiki_aviation",
            "wiki_freight", "wiki_shipping_container",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_yahoo_shipping_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["bdry", "boat", "sblk", "zim", "dac", "matx"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_yahoo_aviation_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["jets", "dal", "ual", "luv", "aal", "ba", "airbus"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_fred_freight_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "fred_freight_index" in COLLECTORS
