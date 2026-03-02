"""Tests for government data collectors (World Bank, ECB, Treasury, IMF)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.worldbank_generic import (
    WORLDBANK_SERIES,
    get_wb_collectors,
    _make_wb_collector,
)
from signal_noise.collector.ecb_generic import (
    ECB_SERIES,
    get_ecb_collectors,
    _make_ecb_collector,
)
from signal_noise.collector.treasury_generic import (
    TREASURY_YIELD_MATURITIES,
    TREASURY_FISCAL_SERIES,
    TREASURY_TIPS_MATURITIES,
    get_treasury_collectors,
    _make_yield_collector,
    _make_fiscal_collector,
    _make_tips_collector,
)
from signal_noise.collector.imf_generic import (
    IMF_SERIES,
    get_imf_collectors,
    _make_imf_collector,
)


# ── World Bank ──────────────────────────────────────────────

WB_API_RESPONSE = [
    {"page": 1, "pages": 1, "per_page": 50, "total": 3},
    [
        {"date": "2023", "value": 2.5, "indicator": {"id": "X"}, "country": {"id": "US"}},
        {"date": "2022", "value": 1.9, "indicator": {"id": "X"}, "country": {"id": "US"}},
        {"date": "2021", "value": None, "indicator": {"id": "X"}, "country": {"id": "US"}},
        {"date": "2020", "value": -3.4, "indicator": {"id": "X"}, "country": {"id": "US"}},
    ],
]


class TestWorldBankFactory:
    def test_series_count(self):
        assert len(WORLDBANK_SERIES) >= 75

    def test_no_duplicate_names(self):
        names = [t[2] for t in WORLDBANK_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_wb_collector(
            "NY.GDP.MKTP.KD.ZG", "US", "test_wb", "Test WB", "economy", "economic"
        )
        assert cls.meta.name == "test_wb"
        assert cls.meta.domain == "economy"

    def test_get_wb_collectors_returns_dict(self):
        collectors = get_wb_collectors()
        assert len(collectors) == len(WORLDBANK_SERIES)

    def test_all_have_valid_domain_category(self):
        from signal_noise.collector.base import DOMAINS, CATEGORIES
        for entry in WORLDBANK_SERIES:
            dom = entry[4]
            cat = entry[5]
            assert dom in DOMAINS, f"{entry[2]} has invalid domain: {dom}"
            assert cat in CATEGORIES, f"{entry[2]} has invalid category: {cat}"


class TestWorldBankFetch:
    @patch("signal_noise.collector.worldbank_generic.requests.get")
    def test_fetch_parses_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = WB_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_wb_collector("X", "US", "test_wb", "Test", "economy", "economic")
        df = cls().fetch()
        assert len(df) == 3  # one None skipped
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.worldbank_generic.requests.get")
    def test_empty_data_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"page": 1}, []]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_wb_collector("X", "US", "test_wb", "Test", "economy", "economic")
        with pytest.raises(RuntimeError, match="No data"):
            cls().fetch()


# ── ECB ─────────────────────────────────────────────────────

ECB_CSV = """KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-02,1.0956
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-03,1.0919
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-04,1.0953
"""


class TestECBFactory:
    def test_series_count(self):
        assert len(ECB_SERIES) >= 20

    def test_no_duplicate_names(self):
        names = [t[1] for t in ECB_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_ecb_collector(
            "EXR/D.USD.EUR.SP00.A", "test_ecb", "Test ECB", "daily", "markets", "forex"
        )
        assert cls.meta.name == "test_ecb"

    def test_get_ecb_collectors_returns_dict(self):
        collectors = get_ecb_collectors()
        assert len(collectors) == len(ECB_SERIES)

    def test_all_have_valid_domain_category(self):
        from signal_noise.collector.base import DOMAINS, CATEGORIES
        for _, name, _, _, dom, cat in ECB_SERIES:
            assert dom in DOMAINS, f"{name} has invalid domain: {dom}"
            assert cat in CATEGORIES, f"{name} has invalid category: {cat}"


class TestECBFetch:
    @patch("signal_noise.collector.ecb_generic.requests.get")
    def test_fetch_parses_csv(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = ECB_CSV
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_ecb_collector(
            "EXR/D.USD.EUR.SP00.A", "test_ecb", "Test", "daily", "markets", "forex"
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 1.0956
        assert df["date"].is_monotonic_increasing


# ── US Treasury ─────────────────────────────────────────────

TREASURY_CSV = """Date,"1 Mo","3 Mo","6 Mo","1 Yr","2 Yr","5 Yr","10 Yr","20 Yr","30 Yr"
01/02/2024,5.40,5.37,5.24,4.79,4.33,3.93,3.95,4.24,4.09
01/03/2024,5.39,5.36,5.23,4.76,4.32,3.92,3.94,4.23,4.08
"""

TREASURY_FISCAL_RESPONSE = {
    "data": [
        {"record_date": "2024-01-02", "tot_pub_debt_out_amt": "34000000000000.00"},
        {"record_date": "2024-01-03", "tot_pub_debt_out_amt": "34100000000000.00"},
    ],
    "meta": {"count": 2},
}


class TestTreasuryFactory:
    def test_yield_maturity_count(self):
        assert len(TREASURY_YIELD_MATURITIES) >= 12

    def test_fiscal_series_count(self):
        assert len(TREASURY_FISCAL_SERIES) >= 4

    def test_tips_maturity_count(self):
        assert len(TREASURY_TIPS_MATURITIES) >= 5

    def test_get_treasury_collectors_total(self):
        collectors = get_treasury_collectors()
        expected = (
            len(TREASURY_YIELD_MATURITIES)
            + len(TREASURY_FISCAL_SERIES)
            + len(TREASURY_TIPS_MATURITIES)
        )
        assert len(collectors) == expected

    def test_no_duplicate_names(self):
        names = (
            [t[1] for t in TREASURY_YIELD_MATURITIES]
            + [t[2] for t in TREASURY_FISCAL_SERIES]
            + [t[1] for t in TREASURY_TIPS_MATURITIES]
        )
        assert len(names) == len(set(names))


class TestTreasuryYieldFetch:
    @patch("signal_noise.collector.treasury_generic.requests.get")
    def test_fetch_parses_csv(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = TREASURY_CSV
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_yield_collector("10 Yr", "test_10y", "Test 10Y")
        df = cls().fetch()
        assert len(df) >= 2
        assert df["value"].iloc[0] == 3.95


class TestTreasuryFiscalFetch:
    @patch("signal_noise.collector.treasury_generic.requests.get")
    def test_fetch_parses_json(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = TREASURY_FISCAL_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_fiscal_collector(
            "v2/accounting/od/debt_to_penny",
            "tot_pub_debt_out_amt",
            "test_debt", "Test Debt", "economy", "fiscal",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] > 1e12


# ── IMF ─────────────────────────────────────────────────────

IMF_API_RESPONSE = {
    "values": {
        "NGDP_RPCH": {
            "USA": {
                "2020": -2.8,
                "2021": 5.9,
                "2022": 2.1,
                "2023": 2.5,
            }
        }
    }
}


class TestIMFFactory:
    def test_series_count(self):
        assert len(IMF_SERIES) >= 20

    def test_no_duplicate_names(self):
        names = [t[2] for t in IMF_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_imf_collector(
            "NGDP_RPCH", "USA", "test_imf", "Test IMF", "economy", "economic"
        )
        assert cls.meta.name == "test_imf"

    def test_get_imf_collectors_returns_dict(self):
        collectors = get_imf_collectors()
        assert len(collectors) == len(IMF_SERIES)

    def test_all_have_valid_domain_category(self):
        from signal_noise.collector.base import DOMAINS, CATEGORIES
        for _, _, name, _, dom, cat in IMF_SERIES:
            assert dom in DOMAINS, f"{name} has invalid domain: {dom}"
            assert cat in CATEGORIES, f"{name} has invalid category: {cat}"


class TestIMFFetch:
    @patch("signal_noise.collector.imf_generic.requests.get")
    def test_fetch_parses_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = IMF_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_imf_collector("NGDP_RPCH", "USA", "test_imf", "Test", "economy", "economic")
        df = cls().fetch()
        assert len(df) == 4
        assert df["date"].is_monotonic_increasing
        assert df["value"].iloc[0] == -2.8

    @patch("signal_noise.collector.imf_generic.requests.get")
    def test_empty_data_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"values": {"X": {"USA": {}}}}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_imf_collector("X", "USA", "test_imf", "Test", "economy", "economic")
        with pytest.raises(RuntimeError, match="No IMF data"):
            cls().fetch()


# ── Registration ────────────────────────────────────────────

class TestGovernmentRegistration:
    def test_worldbank_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "wb_gdp_growth_us" in COLLECTORS
        assert "wb_inflation_cn" in COLLECTORS
        assert "wb_reserves_jp" in COLLECTORS

    def test_ecb_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "ecb_eur_usd" in COLLECTORS
        assert "ecb_main_refi_rate" in COLLECTORS
        assert "ecb_ester" in COLLECTORS
        assert "ecb_hicp_ea" in COLLECTORS

    def test_treasury_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "tsy_yield_10y" in COLLECTORS
        assert "tsy_total_debt" in COLLECTORS
        assert "tsy_avg_rate_bills" in COLLECTORS

    def test_imf_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "imf_gdp_growth_us" in COLLECTORS
        assert "imf_inflation_tr" in COLLECTORS
        assert "imf_gov_debt_jp" in COLLECTORS

    def test_total_collector_count(self):
        from signal_noise.collector import COLLECTORS
        assert len(COLLECTORS) >= 330
