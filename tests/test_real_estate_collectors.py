"""Tests for real estate / housing collectors (OECD, BIS, tuples)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests

from signal_noise.collector.oecd_house_prices import (
    OECD_HP_SERIES,
    _fetch_oecd_csv,
    get_oecd_hp_collectors,
    _make_oecd_hp_collector,
)
from signal_noise.collector.bis_property import (
    BIS_PROPERTY_SERIES,
    get_bis_pp_collectors,
    _make_bis_pp_collector,
)
from signal_noise.collector.base import CollectorMeta


OECD_CSV_RESPONSE = """STRUCTURE,STRUCTURE_ID,STRUCTURE_NAME,ACTION,REF_AREA,Reference area,FREQ,Frequency of observation,MEASURE,Measure,UNIT_MEASURE,Unit of measure,TIME_PERIOD,Time period,OBS_VALUE,Observation value,OBS_STATUS,Observation status,UNIT_MULT,Unit multiplier,ADJUSTMENT,Adjustment,DECIMALS,Decimals,BASE_PER,Base period
DATAFLOW,TEST,Test,I,USA,United States,Q,Quarterly,HPI,Nominal,IX,Index,2023-Q1,,180.89,,A,Normal,0,Units,S,SA,1,One,2015,
DATAFLOW,TEST,Test,I,USA,United States,Q,Quarterly,HPI,Nominal,IX,Index,2023-Q2,,184.33,,A,Normal,0,Units,S,SA,1,One,2015,
DATAFLOW,TEST,Test,I,USA,United States,Q,Quarterly,HPI,Nominal,IX,Index,2023-Q3,,188.36,,A,Normal,0,Units,S,SA,1,One,2015,
"""

BIS_CSV_RESPONSE = """Time Series Search Export,Search term,Search filters,,Last update,Downloaded at,Source,Source URL,Download URL,About
,,,,2026-02-19,2026-02-21,BIS,url,url,url

DATAFLOW_ID:Dataflow ID,KEY:Timeseries Key,FREQ:Frequency,REF_AREA:Reference area,VALUE:Value,UNIT_MEASURE:Unit of measure,Unit,Unit multiplier,TIME_PERIOD:Period,OBS_CONF:Confidentiality,OBS_PRE_BREAK:Pre-break value,OBS_STATUS:Status,OBS_VALUE:Value
"BIS,WS_SPP,1.0",Q.US.N.628,Q:Quarterly,US:US,N:Nominal,"628:Index",Index,Units,2023-03-31,F:Free,,A:Normal,150.5
"BIS,WS_SPP,1.0",Q.US.N.628,Q:Quarterly,US:US,N:Nominal,"628:Index",Index,Units,2023-06-30,F:Free,,A:Normal,152.3
"BIS,WS_SPP,1.0",Q.US.N.628,Q:Quarterly,US:US,N:Nominal,"628:Index",Index,Units,2023-09-30,F:Free,,A:Normal,155.1
"""


class TestOECDHousePrices:
    def test_series_count(self):
        assert len(OECD_HP_SERIES) >= 30

    def test_no_duplicate_names(self):
        names = [t[2] for t in OECD_HP_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_oecd_hp_collector("USA", "HPI", "test_oecd", "Test OECD", "economy", "real_estate")
        assert cls.meta.name == "test_oecd"
        assert cls.meta.update_frequency == "quarterly"
        assert isinstance(cls.meta, CollectorMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_oecd_hp_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(OECD_HP_SERIES)
        assert "oecd_hpi_us" in collectors
        assert "oecd_rhpi_jp" in collectors
        assert "oecd_pti_us" in collectors

    def test_all_domain_categories(self):
        for country, measure, name, display, dom, cat in OECD_HP_SERIES:
            assert dom == "economy", f"{name} has unexpected domain: {dom}"
            assert cat == "real_estate", f"{name} has unexpected category: {cat}"

    @patch("signal_noise.collector.oecd_house_prices.requests.get")
    def test_fetch_parses_csv(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = OECD_CSV_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_oecd_hp_collector("USA", "HPI", "test_oecd", "Test", "economy", "real_estate")
        df = cls().fetch()
        assert len(df) == 3
        assert "date" in df.columns
        assert "value" in df.columns
        assert df["value"].iloc[0] == 180.89
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.oecd_house_prices.requests.get")
    def test_quarter_to_month_conversion(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = OECD_CSV_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_oecd_hp_collector("USA", "HPI", "test_oecd", "Test", "economy", "real_estate")
        df = cls().fetch()
        # Q1 -> Jan, Q2 -> Apr, Q3 -> Jul
        assert df["date"].iloc[0].month == 1
        assert df["date"].iloc[1].month == 4
        assert df["date"].iloc[2].month == 7

    @patch("signal_noise.collector.oecd_house_prices.requests.get")
    def test_fetch_retries_timeout(self, mock_get):
        timeout_error = requests.exceptions.ReadTimeout("slow")
        success = MagicMock()
        success.text = OECD_CSV_RESPONSE
        success.raise_for_status = MagicMock()
        mock_get.side_effect = [timeout_error, success]

        body = _fetch_oecd_csv("https://example.com", timeout=30)

        assert body == OECD_CSV_RESPONSE
        assert mock_get.call_count == 2
        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == (10, 60)


class TestBISPropertyPrices:
    def test_series_count(self):
        assert len(BIS_PROPERTY_SERIES) >= 25

    def test_no_duplicate_names(self):
        names = [t[3] for t in BIS_PROPERTY_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_bis_pp_collector("US", "N", "628", "test_bis", "Test BIS", "economy", "real_estate")
        assert cls.meta.name == "test_bis"
        assert cls.meta.update_frequency == "quarterly"
        assert isinstance(cls.meta, CollectorMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_bis_pp_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(BIS_PROPERTY_SERIES)
        assert "bis_pp_us" in collectors
        assert "bis_rpp_jp" in collectors
        assert "bis_pp_yoy_us" in collectors

    def test_all_domain_categories(self):
        for country, vt, unit, name, display, dom, cat in BIS_PROPERTY_SERIES:
            assert dom == "economy", f"{name} has unexpected domain: {dom}"
            assert cat == "real_estate", f"{name} has unexpected category: {cat}"

    @patch("signal_noise.collector.bis_property.requests.get")
    def test_fetch_parses_csv(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = BIS_CSV_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_bis_pp_collector("US", "N", "628", "test_bis", "Test", "economy", "real_estate")
        df = cls().fetch()
        assert len(df) == 3
        assert "date" in df.columns
        assert "value" in df.columns
        assert df["value"].iloc[0] == 150.5
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.bis_property.requests.get")
    def test_skips_metadata_rows(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = BIS_CSV_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_bis_pp_collector("US", "N", "628", "test_bis", "Test", "economy", "real_estate")
        df = cls().fetch()
        # Should only have 3 data rows, not include metadata
        assert len(df) == 3


class TestRealEstateRegistration:
    def test_oecd_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = ["oecd_hpi_us", "oecd_hpi_jp", "oecd_rhpi_us", "oecd_pti_us", "oecd_ptr_us"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_bis_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = ["bis_pp_us", "bis_pp_jp", "bis_rpp_us", "bis_pp_yoy_us"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_fred_housing_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "fred_fhfa_hpi", "fred_case_shiller_20", "fred_mortgage_30y",
            "fred_building_permits", "fred_existing_home_sales",
            "fred_bis_hpi_us", "fred_bis_hpi_jp",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_yahoo_real_estate_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = ["vnq", "iyr", "vnqi", "xhb", "itb", "pld", "amt"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_wiki_real_estate_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "wiki_re_bubble", "wiki_jp_bubble", "wiki_subprime",
            "wiki_reit", "wiki_case_shiller",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
