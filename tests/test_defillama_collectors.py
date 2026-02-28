"""Tests for DeFi Llama collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.defillama import (
    CHAIN_TVL_SERIES,
    STABLECOIN_SERIES,
    PROTOCOL_TVL_SERIES,
    DeFiTotalTVLCollector,
    DeFiDEXVolumeCollector,
    DeFiTotalFeesCollector,
    DeFiOptionsVolumeCollector,
    DeFiAvgYieldCollector,
    get_defillama_collectors,
    _make_chain_tvl_collector,
    _make_stablecoin_collector,
    _make_protocol_tvl_collector,
)


CHAIN_TVL_RESPONSE = [
    {"date": 1609459200, "tvl": 20000000000},
    {"date": 1609545600, "tvl": 21000000000},
    {"date": 1609632000, "tvl": 22000000000},
]

TOTAL_TVL_RESPONSE = [
    {"date": 1609459200, "tvl": 50000000000},
    {"date": 1609545600, "tvl": 52000000000},
]

STABLECOIN_RESPONSE = [
    {
        "date": "1609459200",
        "totalCirculatingUSD": {"peggedUSD": 25000000000},
        "totalCirculating": {"peggedUSD": 25000000000},
    },
    {
        "date": "1609545600",
        "totalCirculatingUSD": {"peggedUSD": 26000000000},
        "totalCirculating": {"peggedUSD": 26000000000},
    },
]

DEX_VOLUME_RESPONSE = {
    "totalDataChart": [
        [1609459200, 1500000000],
        [1609545600, 1800000000],
    ],
    "protocols": [],
}

PROTOCOL_TVL_RESPONSE = {
    "tvl": [
        {"date": 1609459200, "totalLiquidityUSD": 10000000000},
        {"date": 1609545600, "totalLiquidityUSD": 11000000000},
    ],
}

FEES_RESPONSE = {
    "totalDataChart": [
        [1609459200, 50000000],
        [1609545600, 55000000],
    ],
}

BRIDGE_RESPONSE = {
    "totalDataChart": [
        [1609459200, 200000000],
        [1609545600, 250000000],
    ],
}

YIELD_RESPONSE = [
    {"timestamp": "2024-01-01T00:00:00.000Z", "medianAPY": 3.5},
    {"timestamp": "2024-01-02T00:00:00.000Z", "medianAPY": 3.7},
]


class TestChainTVL:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch_parses(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = CHAIN_TVL_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_chain_tvl_collector("Ethereum", "test_eth", "Test ETH")
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 20000000000
        assert df["date"].is_monotonic_increasing

    def test_series_count(self):
        assert len(CHAIN_TVL_SERIES) >= 10

    def test_no_duplicates(self):
        names = [t[1] for t in CHAIN_TVL_SERIES]
        assert len(names) == len(set(names))


class TestTotalTVL:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = TOTAL_TVL_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = DeFiTotalTVLCollector().fetch()
        assert len(df) == 2

    def test_meta(self):
        assert DeFiTotalTVLCollector.meta.domain == "financial"
        assert DeFiTotalTVLCollector.meta.category == "crypto"


class TestStablecoins:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch_parses(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = STABLECOIN_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_stablecoin_collector(1, "test_usdt", "Test USDT")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 25000000000

    def test_series_count(self):
        assert len(STABLECOIN_SERIES) >= 5


class TestDEXVolume:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = DEX_VOLUME_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = DeFiDEXVolumeCollector().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 1500000000


class TestProtocolTVL:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = PROTOCOL_TVL_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_protocol_tvl_collector("lido", "test_lido", "Test Lido")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 10000000000

    def test_series_count(self):
        assert len(PROTOCOL_TVL_SERIES) >= 10


class TestFees:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = FEES_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = DeFiTotalFeesCollector().fetch()
        assert len(df) == 2


class TestOptions:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = DEX_VOLUME_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = DeFiOptionsVolumeCollector().fetch()
        assert len(df) == 2


class TestYield:
    @patch("signal_noise.collector.defillama.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = YIELD_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = DeFiAvgYieldCollector().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 3.5


class TestRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "defi_tvl_total", "defi_tvl_eth", "defi_tvl_sol",
            "defi_sc_usdt", "defi_sc_usdc",
            "defi_dex_volume", "defi_total_fees",
            "defi_proto_lido", "defi_proto_aave",
            "defi_options_volume", "defi_avg_yield",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_total_count(self):
        collectors = get_defillama_collectors()
        assert len(collectors) >= 40
