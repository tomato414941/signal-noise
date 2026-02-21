"""Tests for remote sensing / satellite / earth observation collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.nasa_eonet import (
    EONETWildfireCollector,
    EONETStormCollector,
    EONETVolcanoCollector,
    EONETTotalCollector,
)
from signal_noise.collector.nasa_power import (
    NASA_POWER_POINTS,
    get_power_collectors,
    _make_power_collector,
)
from signal_noise.collector.usgs_water import (
    USGS_WATER_SITES,
    get_water_collectors,
    _make_water_collector,
)
from signal_noise.collector.base import SourceMeta


EONET_RESPONSE = {
    "events": [
        {"id": "1", "title": "Fire A", "categories": [{"id": "wildfires", "title": "Wildfires"}]},
        {"id": "2", "title": "Fire B", "categories": [{"id": "wildfires", "title": "Wildfires"}]},
        {"id": "3", "title": "Storm X", "categories": [{"id": "severeStorms", "title": "Severe Storms"}]},
        {"id": "4", "title": "Volcano Y", "categories": [{"id": "volcanoes", "title": "Volcanoes"}]},
    ]
}


class TestNASAEONET:
    @patch("signal_noise.collector.nasa_eonet.requests.get")
    def test_wildfire_count(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EONET_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = EONETWildfireCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 2.0

    @patch("signal_noise.collector.nasa_eonet.requests.get")
    def test_storm_count(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EONET_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = EONETStormCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 1.0

    @patch("signal_noise.collector.nasa_eonet.requests.get")
    def test_volcano_count(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EONET_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = EONETVolcanoCollector().fetch()
        assert df["value"].iloc[0] == 1.0

    @patch("signal_noise.collector.nasa_eonet.requests.get")
    def test_total_count(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EONET_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = EONETTotalCollector().fetch()
        assert df["value"].iloc[0] == 4.0

    def test_meta(self):
        assert EONETWildfireCollector.meta.data_type == "natural_disaster"
        assert EONETStormCollector.meta.data_type == "natural_disaster"


class TestNASAPower:
    def test_point_count(self):
        assert len(NASA_POWER_POINTS) >= 10

    def test_no_duplicate_names(self):
        names = [t[3] for t in NASA_POWER_POINTS]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_power_collector(35.0, 139.0, "Test", "test_power", "Test", "ALLSKY_SFC_SW_DWN")
        assert cls.meta.name == "test_power"
        assert cls.meta.data_type == "satellite"

    def test_get_collectors_returns_dict(self):
        collectors = get_power_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(NASA_POWER_POINTS)
        assert "power_solar_tokyo" in collectors

    @patch("signal_noise.collector.nasa_power.requests.get")
    def test_fetch_parses_data(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "properties": {
                "parameter": {
                    "ALLSKY_SFC_SW_DWN": {
                        "20250101": 3.5,
                        "20250102": 4.2,
                        "20250103": -999.0,
                    }
                }
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_power_collector(35.0, 139.0, "Test", "test_power", "Test", "ALLSKY_SFC_SW_DWN")
        df = cls().fetch()
        # -999.0 should be skipped
        assert len(df) == 2
        assert df["value"].iloc[0] == 3.5


class TestUSGSWater:
    def test_site_count(self):
        assert len(USGS_WATER_SITES) >= 6

    def test_no_duplicate_names(self):
        names = [t[1] for t in USGS_WATER_SITES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_water_collector("09380000", "test_water", "Test Water", "drought")
        assert cls.meta.name == "test_water"
        assert cls.meta.data_type == "hydrology"

    def test_get_collectors_returns_dict(self):
        collectors = get_water_collectors()
        assert isinstance(collectors, dict)
        assert "usgs_colorado_lees" in collectors

    @patch("signal_noise.collector.usgs_water.requests.get")
    def test_fetch_parses_json(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "value": {
                "timeSeries": [{
                    "values": [{
                        "value": [
                            {"dateTime": "2025-01-01T00:00:00.000-05:00", "value": "5000"},
                            {"dateTime": "2025-01-02T00:00:00.000-05:00", "value": "5200"},
                            {"dateTime": "2025-01-03T00:00:00.000-05:00", "value": "-999999"},
                        ]
                    }]
                }]
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_water_collector("09380000", "test_water", "Test", "drought")
        df = cls().fetch()
        # -999999 should be skipped
        assert len(df) == 2
        assert df["value"].iloc[0] == 5000.0


class TestRemoteSensingRegistration:
    def test_eonet_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["eonet_wildfires", "eonet_storms", "eonet_volcanoes", "eonet_total"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_power_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["power_solar_tokyo", "power_solar_nyc", "power_precip_london"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_water_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["usgs_colorado_lees", "usgs_mississippi_stl"]:
            assert name in COLLECTORS, f"{name} not registered"

    def test_wiki_disaster_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["wiki_wildfire", "wiki_volcanic", "wiki_tsunami", "wiki_hurricane"]:
            assert name in COLLECTORS, f"{name} not registered"
