"""Tests for new L1 collectors: NOAA CO-OPS, UK Carbon, BOC, BOE."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.boc_generic import (
    BOC_SERIES,
    _make_boc_collector,
    get_boc_collectors,
)
from signal_noise.collector.boe_generic import (
    BOE_SERIES,
    _make_boe_collector,
    get_boe_collectors,
)
from signal_noise.collector.noaa_coops import (
    NOAA_COOPS_SERIES,
    _make_coops_collector,
    get_coops_collectors,
)
from signal_noise.collector.uk_carbon_intensity import (
    UKCarbonActualCollector,
    UKCarbonForecastCollector,
    _carbon_cache,
)


# ── UK Carbon Intensity ──


class TestUKCarbonIntensity:
    def setup_method(self):
        _carbon_cache.clear()

    def test_actual_meta(self):
        assert UKCarbonActualCollector.meta.name == "uk_carbon_actual"
        assert UKCarbonActualCollector.meta.domain == "earth"
        assert isinstance(UKCarbonActualCollector.meta, CollectorMeta)

    def test_forecast_meta(self):
        assert UKCarbonForecastCollector.meta.name == "uk_carbon_forecast"
        assert UKCarbonForecastCollector.meta.domain == "earth"

    @patch("signal_noise.collector.uk_carbon_intensity.requests.get")
    def test_fetch_actual(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [
                {
                    "from": "2025-02-01T00:00Z",
                    "to": "2025-02-01T00:30Z",
                    "intensity": {"forecast": 201, "actual": 196, "index": "high"},
                },
                {
                    "from": "2025-02-01T00:30Z",
                    "to": "2025-02-01T01:00Z",
                    "intensity": {"forecast": 195, "actual": 190, "index": "moderate"},
                },
                {
                    "from": "2025-02-02T00:00Z",
                    "to": "2025-02-02T00:30Z",
                    "intensity": {"forecast": 180, "actual": 175, "index": "moderate"},
                },
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = UKCarbonActualCollector().fetch()
        assert len(df) == 2  # 2 days
        assert "date" in df.columns
        assert "value" in df.columns
        assert abs(df["value"].iloc[0] - 193.0) < 0.1

    @patch("signal_noise.collector.uk_carbon_intensity.requests.get")
    def test_fetch_forecast(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [
                {
                    "from": "2025-02-01T00:00Z",
                    "to": "2025-02-01T00:30Z",
                    "intensity": {"forecast": 200, "actual": 196, "index": "high"},
                },
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = UKCarbonForecastCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 200.0


# ── Bank of Canada ──


class TestBOCGeneric:
    def test_series_count(self):
        assert len(BOC_SERIES) >= 10

    def test_no_duplicate_names(self):
        names = [t[1] for t in BOC_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_boc_collector(
            "FXUSDCAD", "test_boc", "Test", "daily", "financial", "forex",
        )
        assert cls.meta.name == "test_boc"
        assert cls.meta.domain == "financial"
        assert isinstance(cls.meta, CollectorMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_boc_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(BOC_SERIES)
        assert "boc_usd_cad" in collectors
        assert "boc_target_rate" in collectors

    @patch("signal_noise.collector.boc_generic.requests.get")
    def test_fetch_fx(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "terms": {},
            "seriesDetail": {"FXUSDCAD": {"label": "USD/CAD"}},
            "observations": [
                {"d": "2025-02-03", "FXUSDCAD": {"v": "1.4603"}},
                {"d": "2025-02-04", "FXUSDCAD": {"v": "1.4295"}},
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_boc_collector(
            "FXUSDCAD", "test_boc_fx", "Test", "daily", "financial", "forex",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 1.4603

    @patch("signal_noise.collector.boc_generic.requests.get")
    def test_fetch_empty_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"observations": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_boc_collector(
            "FXUSDCAD", "test_boc_empty", "Test", "daily", "financial", "forex",
        )
        with pytest.raises(RuntimeError, match="No data"):
            cls().fetch()

    def test_all_series_valid_domain_category(self):
        for _, name, _, _, domain, category in BOC_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"


# ── Bank of England ──


class TestBOEGeneric:
    def test_series_count(self):
        assert len(BOE_SERIES) >= 5

    def test_no_duplicate_names(self):
        names = [t[1] for t in BOE_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_boe_collector(
            "IUDBEDR", "test_boe", "Test", "daily", "financial", "rates",
        )
        assert cls.meta.name == "test_boe"
        assert isinstance(cls.meta, CollectorMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_boe_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(BOE_SERIES)
        assert "boe_bank_rate" in collectors

    @patch("signal_noise.collector.boe_generic.requests.get")
    def test_fetch_bank_rate(self, mock_get):
        csv_text = "DATE,IUDBEDR\n02 Jan 2024,5.25\n03 Jan 2024,5.25\n01 Aug 2024,5.00\n"
        mock_resp = MagicMock()
        mock_resp.text = csv_text
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_boe_collector(
            "IUDBEDR", "test_boe_rate", "Test", "daily", "financial", "rates",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 5.25
        assert df["value"].iloc[2] == 5.00

    @patch("signal_noise.collector.boe_generic.requests.get")
    def test_fetch_handles_missing_values(self, mock_get):
        csv_text = "DATE,IUMALNPY\n31 Jan 2024,4.2152\n28 Feb 2024,\n31 Mar 2024,4.1500\n"
        mock_resp = MagicMock()
        mock_resp.text = csv_text
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_boe_collector(
            "IUMALNPY", "test_boe_yield", "Test", "monthly", "financial", "rates",
        )
        df = cls().fetch()
        assert len(df) == 2  # Feb row dropped

    @patch("signal_noise.collector.boe_generic.requests.get")
    def test_fetch_html_error(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "<!DOCTYPE html><html>Error</html>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_boe_collector(
            "IUDBEDR", "test_boe_html", "Test", "daily", "financial", "rates",
        )
        with pytest.raises(RuntimeError, match="HTML instead of CSV"):
            cls().fetch()

    def test_all_series_valid_domain_category(self):
        for _, name, _, _, domain, category in BOE_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"


# ── NOAA CO-OPS ──


class TestNOAACoops:
    def test_station_count(self):
        assert len(NOAA_COOPS_SERIES) >= 8

    def test_no_duplicate_names(self):
        names = [t[2] for t in NOAA_COOPS_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_coops_collector(
            "9414290", "water_level", "test_coops", "Test", "marine",
        )
        assert cls.meta.name == "test_coops"
        assert cls.meta.domain == "earth"
        assert isinstance(cls.meta, CollectorMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_coops_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(NOAA_COOPS_SERIES)
        assert "coops_wl_san_francisco" in collectors
        assert "coops_wt_battery_nyc" in collectors

    @patch("signal_noise.collector.noaa_coops.requests.get")
    def test_fetch_water_level(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "metadata": {"id": "9414290", "name": "San Francisco"},
            "data": [
                {"t": "2025-02-01 00:00", "v": "0.601", "s": "0.041", "f": "0,0,0,0", "q": "v"},
                {"t": "2025-02-01 01:00", "v": "0.812", "s": "0.035", "f": "0,0,0,0", "q": "v"},
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_coops_collector(
            "9414290", "water_level", "test_wl", "Test WL", "marine",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert "timestamp" in df.columns
        assert df["value"].iloc[0] == 0.601

    @patch("signal_noise.collector.noaa_coops.requests.get")
    def test_fetch_water_temp(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "metadata": {"id": "8518750"},
            "data": [
                {"t": "2025-02-01 00:00", "v": "3.1", "f": "0,0,0"},
                {"t": "2025-02-01 01:00", "v": "3.0", "f": "0,0,0"},
            ],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_coops_collector(
            "8518750", "water_temperature", "test_wt", "Test WT", "hydrology",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 3.1

    @patch("signal_noise.collector.noaa_coops.requests.get")
    def test_fetch_error_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "error": {"message": "No data was found"},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_coops_collector(
            "9414290", "water_level", "test_err", "Test", "marine",
        )
        with pytest.raises(RuntimeError, match="NOAA CO-OPS error"):
            cls().fetch()

    def test_all_series_valid_domain_category(self):
        for _, _, name, _, category in NOAA_COOPS_SERIES:
            assert "earth" in DOMAINS, f"{name}: earth not in DOMAINS"
            assert category in CATEGORIES, f"{name}: invalid category {category}"


# ── Registration ──


class TestNewL1Registration:
    def test_coops_registered(self):
        from signal_noise.collector import COLLECTORS

        for name in ["coops_wl_san_francisco", "coops_wl_battery_nyc", "coops_wt_battery_nyc"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_uk_carbon_registered(self):
        from signal_noise.collector import COLLECTORS

        for name in ["uk_carbon_actual", "uk_carbon_forecast"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_boc_registered(self):
        from signal_noise.collector import COLLECTORS

        for name in ["boc_usd_cad", "boc_target_rate", "boc_yield_10y"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_boe_registered(self):
        from signal_noise.collector import COLLECTORS

        for name in ["boe_bank_rate", "boe_long_yield"]:
            assert name in COLLECTORS, f"{name} not registered"
