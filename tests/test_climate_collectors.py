"""Tests for climate and weather collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.open_meteo_weather import (
    WEATHER_CITIES,
    get_weather_collectors,
    _make_weather_collector,
)
from signal_noise.collector.open_meteo_marine import (
    MARINE_POINTS,
    get_marine_collectors,
    _make_marine_collector,
)
from signal_noise.collector.open_meteo_air import (
    AIR_QUALITY_CITIES,
    get_air_collectors,
    _make_air_collector,
)
from signal_noise.collector.noaa_climate import (
    GlobalTempAnomalyCollector,
    LandTempAnomalyCollector,
    CO2DailyCollector,
    NASAGlobalTempCollector,
)
from signal_noise.collector.base import SourceMeta


class TestOpenMeteoWeather:
    def test_city_count(self):
        assert len(WEATHER_CITIES) >= 15

    def test_no_duplicate_names(self):
        names = [t[3] for t in WEATHER_CITIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_weather_collector(35.0, 139.0, "Test", "test_meteo", "Test Weather")
        assert cls.meta.name == "test_meteo"
        assert cls.meta.category == "weather"
        assert isinstance(cls.meta, SourceMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_weather_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(WEATHER_CITIES)
        assert "meteo_tokyo" in collectors
        assert "meteo_london" in collectors

    @patch("signal_noise.collector.open_meteo_weather.requests.get")
    def test_fetch_parses_daily(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "daily": {
                "time": ["2025-01-01", "2025-01-02", "2025-01-03"],
                "temperature_2m_mean": [5.2, 3.8, 7.1],
                "precipitation_sum": [0.0, 2.5, 0.0],
                "windspeed_10m_max": [15.0, 22.0, 10.0],
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_weather_collector(35.0, 139.0, "Test", "test_meteo", "Test")
        df = cls().fetch()
        assert len(df) == 3
        assert "date" in df.columns
        assert "value" in df.columns
        assert "precipitation" in df.columns
        assert df["value"].iloc[0] == 5.2


class TestOpenMeteoMarine:
    def test_point_count(self):
        assert len(MARINE_POINTS) >= 8

    def test_no_duplicate_names(self):
        names = [t[3] for t in MARINE_POINTS]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_marine_collector(30.0, 32.5, "Suez", "test_marine", "Test Marine")
        assert cls.meta.name == "test_marine"
        assert cls.meta.category == "marine"

    def test_get_collectors_returns_dict(self):
        collectors = get_marine_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(MARINE_POINTS)
        assert "marine_suez" in collectors

    @patch("signal_noise.collector.open_meteo_marine.requests.get")
    def test_fetch_parses_wave_data(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "daily": {
                "time": ["2025-01-01", "2025-01-02"],
                "wave_height_max": [2.5, 3.1],
                "wave_period_max": [8.0, 10.0],
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_marine_collector(30.0, 32.5, "Suez", "test_marine", "Test")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 2.5
        assert "wave_period" in df.columns


class TestOpenMeteoAir:
    def test_city_count(self):
        assert len(AIR_QUALITY_CITIES) >= 8

    def test_no_duplicate_names(self):
        names = [t[3] for t in AIR_QUALITY_CITIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_air_collector(39.9, 116.4, "Beijing", "test_air", "Test Air")
        assert cls.meta.name == "test_air"
        assert cls.meta.category == "air_quality"

    def test_get_collectors_returns_dict(self):
        collectors = get_air_collectors()
        assert isinstance(collectors, dict)
        assert "air_beijing" in collectors
        assert "air_delhi" in collectors

    @patch("signal_noise.collector.open_meteo_air.requests.get")
    def test_fetch_aggregates_hourly_to_daily(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "hourly": {
                "time": [
                    "2025-01-01T00:00", "2025-01-01T01:00",
                    "2025-01-01T02:00", "2025-01-02T00:00",
                ],
                "pm2_5": [50.0, 60.0, 70.0, 30.0],
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_air_collector(39.9, 116.4, "Beijing", "test_air", "Test")
        df = cls().fetch()
        assert len(df) == 2
        # Day 1 mean: (50+60+70)/3 = 60.0
        assert abs(df["value"].iloc[0] - 60.0) < 0.01
        # Day 2 mean: 30.0
        assert abs(df["value"].iloc[1] - 30.0) < 0.01


class TestNOAAClimate:
    def test_global_temp_meta(self):
        assert GlobalTempAnomalyCollector.meta.name == "noaa_global_temp"
        assert GlobalTempAnomalyCollector.meta.category == "climate"

    @patch("signal_noise.collector.noaa_climate.requests.get")
    def test_global_temp_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "description": {"title": "Test"},
            "data": {
                "202301": {"anomaly": 1.12},
                "202302": {"anomaly": 1.18},
                "202303": {"anomaly": 1.25},
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = GlobalTempAnomalyCollector().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 1.12

    def test_co2_meta(self):
        assert CO2DailyCollector.meta.name == "noaa_co2_daily"
        assert CO2DailyCollector.meta.category == "climate"

    @patch("signal_noise.collector.noaa_climate.requests.get")
    def test_co2_fetch(self, mock_get):
        csv_text = (
            "# comment line\n"
            "year,month,day,decimal,co2\n"
            "2025,1,1,2025.0014,427.5\n"
            "2025,1,2,2025.0041,428.1\n"
            "2025,1,3,2025.0068,-999.99\n"
        )
        mock_resp = MagicMock()
        mock_resp.text = csv_text
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = CO2DailyCollector().fetch()
        # -999.99 should be skipped
        assert len(df) == 2
        assert df["value"].iloc[0] == 427.5

    def test_nasa_giss_meta(self):
        assert NASAGlobalTempCollector.meta.name == "nasa_giss_temp"

    @patch("signal_noise.collector.noaa_climate.requests.get")
    def test_nasa_giss_fetch(self, mock_get):
        csv_text = (
            "Land-Ocean: Global Means\n"
            "Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D,D-N,DJF,MAM,JJA,SON\n"
            "2023,1.12,1.18,1.25,1.10,1.05,1.15,1.20,1.18,1.30,1.25,1.28,1.35,1.19,***,1.10,1.13,1.18,1.28\n"
        )
        mock_resp = MagicMock()
        mock_resp.text = csv_text
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = NASAGlobalTempCollector().fetch()
        assert len(df) == 12
        assert df["value"].iloc[0] == 1.12


class TestClimateRegistration:
    def test_noaa_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["noaa_global_temp", "noaa_land_temp", "noaa_co2_daily", "nasa_giss_temp"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_weather_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["meteo_nyc", "meteo_tokyo", "meteo_london", "meteo_beijing"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_marine_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["marine_suez", "marine_panama", "marine_malacca"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_air_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["air_beijing", "air_delhi", "air_tokyo"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_wiki_climate_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["wiki_climate_change", "wiki_global_warming", "wiki_co2", "wiki_drought"]:
            assert name in COLLECTORS, f"{name} not registered"
