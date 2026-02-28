"""Tests for OECD STES collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.oecd_stes import (
    OECD_CLI_SERIES,
    _make_oecd_cli_collector,
    get_oecd_stes_collectors,
)

OECD_CSV_RESPONSE = (
    "STRUCTURE,STRUCTURE_ID,STRUCTURE_NAME,ACTION,REF_AREA,FREQ,MEASURE,"
    "UNIT_MEASURE,ACTIVITY,ADJUSTMENT,TRANSFORMATION,UNIT_MULT,BASE_PER,"
    "TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
    "dataflow,DF_CLI,CLI,,USA,M,LI,IX,_Z,AA,IX,_Z,H,2024-01,99.85,\n"
    "dataflow,DF_CLI,CLI,,USA,M,LI,IX,_Z,AA,IX,_Z,H,2024-02,99.72,\n"
    "dataflow,DF_CLI,CLI,,USA,M,LI,IX,_Z,AA,IX,_Z,H,2024-03,99.55,\n"
)


class TestOECDCLIFactory:
    @patch("signal_noise.collector.oecd_stes.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = OECD_CSV_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_oecd_cli_collector("USA", "test_cli_us", "Test CLI US")
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 99.85
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.oecd_stes.requests.get")
    def test_empty_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "col1,col2\n"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_oecd_cli_collector("USA", "test_cli_us", "Test CLI US")
        with pytest.raises(RuntimeError, match="No OECD CLI data"):
            cls().fetch()

    def test_meta(self):
        cls = _make_oecd_cli_collector("USA", "test_cli_us", "Test CLI US")
        assert cls.meta.domain == "macro"
        assert cls.meta.category == "economic"
        assert cls.meta.update_frequency == "monthly"


class TestOECDCLIRegistry:
    def test_series_count(self):
        assert len(OECD_CLI_SERIES) >= 10

    def test_no_duplicates(self):
        names = [t[1] for t in OECD_CLI_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_oecd_stes_collectors()
        assert len(collectors) == len(OECD_CLI_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = ["oecd_cli_us", "oecd_cli_jp", "oecd_cli_de"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
