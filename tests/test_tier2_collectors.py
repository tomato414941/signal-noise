"""Tests for Tier 2 diversity collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES


# ── Sonitus ──

class TestSonitus:
    def test_factory_count(self):
        from signal_noise.collector.sonitus import get_sonitus_collectors
        assert len(get_sonitus_collectors()) == 4

    def test_meta(self):
        from signal_noise.collector.sonitus import get_sonitus_collectors
        for name, cls in get_sonitus_collectors().items():
            assert cls.meta.domain == "environment"
            assert cls.meta.category == "noise"
            assert cls.meta.category in CATEGORIES

    @patch("signal_noise.collector.sonitus.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"datetime": "2025-03-01T10:00:00Z", "laeq": 55.2},
            {"datetime": "2025-03-01T11:00:00Z", "laeq": 58.7},
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        from signal_noise.collector.sonitus import get_sonitus_collectors
        cls = get_sonitus_collectors()["sonitus_ballyfermot"]
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(55.2)

    def test_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "sonitus_ballyfermot" in COLLECTORS


# ── NSIDC Sea Ice ──

class TestNSIDC:
    def test_factory_count(self):
        from signal_noise.collector.nsidc import get_nsidc_collectors
        assert len(get_nsidc_collectors()) == 2

    def test_meta(self):
        from signal_noise.collector.nsidc import get_nsidc_collectors
        for name, cls in get_nsidc_collectors().items():
            assert cls.meta.domain == "environment"
            assert cls.meta.category == "cryosphere"
            assert cls.meta.category in CATEGORIES

    @patch("signal_noise.collector.nsidc.requests.get")
    def test_fetch_parses_csv(self, mock_get):
        csv_text = (
            " Some header info\n"
            " Year, Month, Day,     Extent,    Missing, Source Data\n"
            " 2025,    01,  15,    13.456,    0.123, final\n"
            " 2025,    02,  15,    14.789,    0.100, final\n"
        )
        mock_resp = MagicMock()
        mock_resp.text = csv_text
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        from signal_noise.collector.nsidc import get_nsidc_collectors
        cls = get_nsidc_collectors()["nsidc_ice_north"]
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(13.456)

    def test_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "nsidc_ice_north" in COLLECTORS
        assert "nsidc_ice_south" in COLLECTORS


# ── USGS Groundwater ──

class TestUSGSGroundwater:
    def test_factory_count(self):
        from signal_noise.collector.usgs_groundwater import get_groundwater_collectors
        from signal_noise.collector.usgs_groundwater import _GW_SITES
        assert len(get_groundwater_collectors()) == len(_GW_SITES)

    def test_meta(self):
        from signal_noise.collector.usgs_groundwater import get_groundwater_collectors
        for name, cls in get_groundwater_collectors().items():
            assert cls.meta.domain == "environment"
            assert cls.meta.category == "hydrology"
            assert cls.meta.category in CATEGORIES

    @patch("signal_noise.collector.usgs_groundwater.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "value": {
                "timeSeries": [{
                    "values": [{
                        "value": [
                            {"dateTime": "2025-01-01T00:00:00Z", "value": "25.3"},
                            {"dateTime": "2025-01-02T00:00:00Z", "value": "25.1"},
                        ]
                    }]
                }]
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        from signal_noise.collector.usgs_groundwater import get_groundwater_collectors
        cls = get_groundwater_collectors()["usgs_gw_nj"]
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(25.3)

    def test_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "usgs_gw_ok_ogallala" in COLLECTORS


# ── RIPEstat BGP ──

class TestRIPEstatBGP:
    def test_meta_v4(self):
        from signal_noise.collector.ripestat_bgp import BGPIPv4PrefixCountCollector
        assert BGPIPv4PrefixCountCollector.meta.name == "bgp_ipv4_prefix_count"
        assert BGPIPv4PrefixCountCollector.meta.category == "internet"
        assert BGPIPv4PrefixCountCollector.meta.category in CATEGORIES

    def test_meta_v6(self):
        from signal_noise.collector.ripestat_bgp import BGPIPv6PrefixCountCollector
        assert BGPIPv6PrefixCountCollector.meta.name == "bgp_ipv6_prefix_count"

    @patch("signal_noise.collector.ripestat_bgp.requests.get")
    def test_fetch_v4(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": {
                "visibility": {
                    "v4": {"total_space": 1050000},
                }
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        from signal_noise.collector.ripestat_bgp import BGPIPv4PrefixCountCollector
        df = BGPIPv4PrefixCountCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 1050000.0

    def test_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "bgp_ipv4_prefix_count" in COLLECTORS
        assert "bgp_ipv6_prefix_count" in COLLECTORS


# ── Twitch ──

class TestTwitch:
    def test_factory_count(self):
        from signal_noise.collector.twitch import get_twitch_collectors
        assert len(get_twitch_collectors()) == 7  # 6 games + total

    def test_meta(self):
        from signal_noise.collector.twitch import get_twitch_collectors
        for name, cls in get_twitch_collectors().items():
            assert cls.meta.domain == "sentiment"
            assert cls.meta.category == "gaming"
            assert cls.meta.requires_key is True

    @patch("signal_noise.collector.twitch._get_credentials", return_value=("id", "tok"))
    @patch("signal_noise.collector.twitch.requests.get")
    def test_fetch_total(self, mock_get, _mock_creds):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [
                {"viewer_count": 50000},
                {"viewer_count": 30000},
                {"viewer_count": 20000},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        from signal_noise.collector.twitch import TwitchTotalViewersCollector
        df = TwitchTotalViewersCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 100000.0

    def test_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "twitch_total_viewers" in COLLECTORS
        assert "twitch_league" in COLLECTORS


# ── FRED Manufacturing (added to fred_generic) ──

class TestFREDManufacturing:
    def test_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["fred_mfg_production", "fred_durable_goods_orders",
                     "fred_mfg_new_orders", "fred_mfg_weekly_hours"]:
            assert name in COLLECTORS, f"{name} not registered"
            cls = COLLECTORS[name]
            assert cls.meta.category == "manufacturing"


# ── World Bank Food Security (added to worldbank_generic) ──

class TestWBFoodSecurity:
    def test_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["wb_undernourish_world", "wb_undernourish_in",
                     "wb_undernourish_ng", "wb_undernourish_bd"]:
            assert name in COLLECTORS, f"{name} not registered"
            cls = COLLECTORS[name]
            assert cls.meta.category == "food_security"
