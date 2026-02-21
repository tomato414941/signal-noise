"""Tests for on-chain / digital behavior collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.mempool import MempoolSizeCollector, MempoolFeeCollector
from signal_noise.collector.lightning import LightningCapacityCollector
from signal_noise.collector.difficulty import DifficultyCollector
from signal_noise.collector.github_activity import (
    BitcoinCommitsCollector,
    EthereumCommitsCollector,
)
from signal_noise.collector.tor import TorUsersCollector
from signal_noise.collector.steam import SteamPlayersCollector


# ── Mempool Size ────────────────────────────────────────────

MEMPOOL_STATS = [
    {"added": 1700000000, "vbytes_per_second": 1500.0},
    {"added": 1700003600, "vbytes_per_second": 2200.0},
    {"added": 1700007200, "vbytes_per_second": 1800.0},
]


class TestMempoolSize:
    @patch("signal_noise.collector.mempool.requests.get")
    def test_fetch_parses_stats(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = MEMPOOL_STATS
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = MempoolSizeCollector().fetch()
        assert "timestamp" in df.columns
        assert "value" in df.columns
        assert len(df) == 3

    def test_meta(self):
        assert MempoolSizeCollector.meta.name == "mempool_size"
        assert MempoolSizeCollector.meta.data_type == "onchain"


# ── Mempool Fee ─────────────────────────────────────────────

class TestMempoolFee:
    @patch("signal_noise.collector.mempool.requests.get")
    def test_fetch_snapshot(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "fastestFee": 25, "halfHourFee": 20, "hourFee": 15,
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = MempoolFeeCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 20.0

    def test_meta(self):
        assert MempoolFeeCollector.meta.name == "mempool_fee"


# ── Lightning ───────────────────────────────────────────────

LIGHTNING_STATS = [
    {"added": 1700000000, "total_capacity": 500000000000},  # 5000 BTC
    {"added": 1700086400, "total_capacity": 510000000000},
]


class TestLightning:
    @patch("signal_noise.collector.lightning.requests.get")
    def test_fetch_parses_capacity(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = LIGHTNING_STATS
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = LightningCapacityCollector().fetch()
        assert "date" in df.columns
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(5000.0)

    def test_meta(self):
        assert LightningCapacityCollector.meta.name == "lightning_capacity"


# ── Difficulty ──────────────────────────────────────────────

DIFFICULTY_DATA = [
    {"timestamp": 1700000000, "difficulty": 62460000000000.0},
    {"timestamp": 1701200000, "difficulty": 64300000000000.0},
]


class TestDifficulty:
    @patch("signal_noise.collector.difficulty.requests.get")
    def test_fetch_parses_adjustments(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = DIFFICULTY_DATA
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = DifficultyCollector().fetch()
        assert "date" in df.columns
        assert len(df) == 2

    def test_meta(self):
        assert DifficultyCollector.meta.name == "btc_difficulty"


# ── GitHub Activity ─────────────────────────────────────────

GITHUB_PARTICIPATION = {
    "all": [10, 15, 20, 8] + [0] * 48,  # 52 weeks
    "owner": [5, 7, 10, 4] + [0] * 48,
}


class TestGitHub:
    @patch("signal_noise.collector.github_activity.requests.get")
    def test_fetch_bitcoin_commits(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = GITHUB_PARTICIPATION
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = BitcoinCommitsCollector().fetch()
        assert "date" in df.columns
        assert len(df) == 52

    @patch("signal_noise.collector.github_activity.requests.get")
    def test_handles_202_computing(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_get.return_value = mock_resp

        with pytest.raises(RuntimeError, match="computing stats"):
            BitcoinCommitsCollector().fetch()

    def test_meta_bitcoin(self):
        assert BitcoinCommitsCollector.meta.name == "github_bitcoin"

    def test_meta_ethereum(self):
        assert EthereumCommitsCollector.meta.name == "github_ethereum"


# ── Tor Users ───────────────────────────────────────────────

TOR_CSV = """\
date,country,users
2024-01-01,US,500000
2024-01-01,DE,300000
2024-01-02,US,520000
2024-01-02,DE,310000
"""


class TestTor:
    @patch("signal_noise.collector.tor.requests.get")
    def test_fetch_aggregates_daily(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = TOR_CSV
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = TorUsersCollector().fetch()
        assert "date" in df.columns
        assert len(df) == 2
        # Day 1: 500000 + 300000 = 800000
        assert df["value"].iloc[0] == 800000.0

    def test_meta(self):
        assert TorUsersCollector.meta.name == "tor_users"


# ── Steam Players ───────────────────────────────────────────

STEAM_DATA = [
    [
        [1700000000000, 30000000],
        [1700003600000, 31000000],
        [1700007200000, 29500000],
    ]
]


class TestSteam:
    @patch("signal_noise.collector.steam.requests.get")
    def test_fetch_parses_json(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = STEAM_DATA
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = SteamPlayersCollector().fetch()
        assert "timestamp" in df.columns
        assert len(df) == 3
        assert df["value"].iloc[0] == 30000000.0

    def test_meta(self):
        assert SteamPlayersCollector.meta.name == "steam_players"


# ── Registration ────────────────────────────────────────────

class TestRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "mempool_size", "mempool_fee", "lightning_capacity",
            "btc_difficulty", "github_bitcoin", "github_ethereum",
            "tor_users", "steam_players",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
