"""Tests for nature / physical phenomena collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.moon import MoonPhaseCollector
from signal_noise.collector.earthquake import EarthquakeCountCollector
from signal_noise.collector.solar import SolarXrayCollector
from signal_noise.collector.sunspot import SunspotCollector
from signal_noise.collector.climate_indices import (
    ArcticOscillationCollector,
    EnsoCollector,
    NaoCollector,
)


# ── Moon Phase ──────────────────────────────────────────────

class TestMoonPhase:
    def test_fetch_returns_dataframe(self):
        c = MoonPhaseCollector()
        df = c.fetch()
        assert "date" in df.columns
        assert "value" in df.columns
        assert len(df) == 365 * 3

    def test_phase_range(self):
        df = MoonPhaseCollector().fetch()
        assert df["value"].min() >= 0.0
        assert df["value"].max() < 1.0

    def test_meta(self):
        assert MoonPhaseCollector.meta.name == "moon_phase"
        assert MoonPhaseCollector.meta.category == "celestial"


# ── Earthquake ──────────────────────────────────────────────

USGS_RESPONSE = {
    "features": [
        {"properties": {"time": 1708000000000, "mag": 5.2}},
        {"properties": {"time": 1708000000000, "mag": 4.8}},
        {"properties": {"time": 1708086400000, "mag": 6.1}},
    ]
}


class TestEarthquake:
    @patch("signal_noise.collector.earthquake.requests.get")
    def test_fetch_parses_usgs(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = USGS_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = EarthquakeCountCollector().fetch()
        assert "date" in df.columns
        assert "value" in df.columns
        # 2 events on day 1, 1 on day 2
        assert len(df) == 2
        assert df["value"].sum() == 3.0

    def test_meta(self):
        assert EarthquakeCountCollector.meta.name == "earthquake_count"


# ── Solar X-ray ─────────────────────────────────────────────

SOLAR_RESPONSE = [
    {"time_tag": "2024-02-15 00:00:00.000", "flux": 1.5e-6},
    {"time_tag": "2024-02-15 00:01:00.000", "flux": 1.6e-6},
    {"time_tag": "2024-02-15 01:00:00.000", "flux": 2.0e-6},
    {"time_tag": "2024-02-15 01:01:00.000", "flux": 2.1e-6},
]


class TestSolarXray:
    @patch("signal_noise.collector.solar.requests.get")
    def test_fetch_resamples_hourly(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = SOLAR_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = SolarXrayCollector().fetch()
        assert "timestamp" in df.columns
        assert "value" in df.columns
        # 4 minute-level entries → 2 hourly buckets
        assert len(df) == 2

    def test_meta(self):
        assert SolarXrayCollector.meta.name == "solar_xray"


# ── Sunspot ─────────────────────────────────────────────────

SILSO_CSV = """\
2024; 1; 1;2024.001;  120;  8.0; 25; 0
2024; 1; 2;2024.004;   95; 10.2; 22; 0
2024; 1; 3;2024.007;   -1; -1.0;  0; 1
"""


class TestSunspot:
    @patch("signal_noise.collector.sunspot.requests.get")
    def test_fetch_parses_csv(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = SILSO_CSV
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = SunspotCollector().fetch()
        assert "date" in df.columns
        assert "value" in df.columns
        # Row 3 has -1, should be filtered out
        assert len(df) == 2
        assert df["value"].iloc[0] == 120.0

    def test_meta(self):
        assert SunspotCollector.meta.name == "sunspot"


# ── ENSO ────────────────────────────────────────────────────

ENSO_TEXT = """\
SEAS  YR   TOTAL  ClimAdjust  ANOM
DJF  2024   26.50     26.25     0.25
JFM  2024   26.80     26.51     0.29
FMA  2024   27.10     26.80     0.30
"""


class TestEnso:
    @patch("signal_noise.collector.climate_indices.requests.get")
    def test_fetch_parses_oni(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = ENSO_TEXT
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = EnsoCollector().fetch()
        assert "date" in df.columns
        assert "value" in df.columns
        assert len(df) == 3
        assert df["value"].iloc[0] == pytest.approx(0.25)

    def test_meta(self):
        assert EnsoCollector.meta.name == "enso"


# ── Arctic Oscillation ──────────────────────────────────────

AO_TEXT = """\
 2024   1  -1.234
 2024   2   0.567
 2024   3   1.890
"""


class TestArcticOscillation:
    @patch("signal_noise.collector.climate_indices.requests.get")
    def test_fetch_parses_monthly(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = AO_TEXT
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = ArcticOscillationCollector().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == pytest.approx(-1.234)

    def test_meta(self):
        assert ArcticOscillationCollector.meta.name == "arctic_oscillation"


# ── NAO ─────────────────────────────────────────────────────

NAO_TEXT = """\
 2024   1   0.45
 2024   2  -0.78
"""


class TestNao:
    @patch("signal_noise.collector.climate_indices.requests.get")
    def test_fetch_parses_monthly(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = NAO_TEXT
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = NaoCollector().fetch()
        assert len(df) == 2
        assert df["value"].iloc[1] == pytest.approx(-0.78)

    def test_meta(self):
        assert NaoCollector.meta.name == "nao"


# ── Registration ────────────────────────────────────────────

class TestRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "moon_phase", "earthquake_count", "solar_xray",
            "sunspot", "enso", "arctic_oscillation", "nao",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
