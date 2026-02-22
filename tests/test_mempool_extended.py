"""Tests for extended Mempool.space collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.mempool_extended import (
    MINING_POOLS,
    MempoolBlocksMinedCollector,
    MempoolNetworkHashrateCollector,
    MempoolTxCountCollector,
    MempoolBlockSizeCollector,
    MempoolBlockWeightCollector,
    _make_pool_hashrate_collector,
    get_mempool_extended_collectors,
)


HASHRATE_RESPONSE = {
    "hashrates": [
        {"timestamp": 1704067200, "avgHashrate": 5e17},
        {"timestamp": 1704153600, "avgHashrate": 5.1e17},
    ],
    "difficulty": [
        {"timestamp": 1704067200, "difficulty": 7.0e13},
        {"timestamp": 1704153600, "difficulty": 7.1e13},
    ],
}

BLOCKS_RESPONSE = [
    {"timestamp": 1704067200, "tx_count": 3500, "size": 1500000, "weight": 3993012},
    {"timestamp": 1704070800, "tx_count": 3200, "size": 1400000, "weight": 3800000},
]


class TestPoolHashrate:
    @patch("signal_noise.collector.mempool_extended.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = HASHRATE_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_pool_hashrate_collector("foundryusa", "mp_foundry", "Test Foundry")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 5e17

    def test_pool_count(self):
        assert len(MINING_POOLS) >= 8


class TestNetworkCollectors:
    @patch("signal_noise.collector.mempool_extended.requests.get")
    def test_network_hashrate(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = HASHRATE_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = MempoolNetworkHashrateCollector().fetch()
        assert len(df) == 2

    @patch("signal_noise.collector.mempool_extended.requests.get")
    def test_blocks_mined(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = HASHRATE_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = MempoolBlocksMinedCollector().fetch()
        assert len(df) == 2

    @patch("signal_noise.collector.mempool_extended.requests.get")
    def test_tx_count(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = BLOCKS_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = MempoolTxCountCollector().fetch()
        assert len(df) == 2

    @patch("signal_noise.collector.mempool_extended.requests.get")
    def test_block_size(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = BLOCKS_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = MempoolBlockSizeCollector().fetch()
        assert len(df) == 2


class TestRegistry:
    def test_total_count(self):
        collectors = get_mempool_extended_collectors()
        assert len(collectors) >= 15

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "mp_foundry", "mp_antpool", "mp_network_hashrate",
            "mp_blocks_mined", "mp_tx_count",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
