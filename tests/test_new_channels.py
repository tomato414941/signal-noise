"""Tests for new data channels: HN, StackOverflow, GitHub Events,
CoinGecko, Blockchain.com, NPM, Archive.org Wayback."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ── Hacker News ─────────────────────────────────────────────

from signal_noise.collector.hackernews import HNTopCollector, HNBestCollector, HNNewCollector


class TestHackerNews:
    @patch("signal_noise.collector.hackernews.requests.get")
    def test_hn_top_fetch(self, mock_get):
        # First call returns story IDs, subsequent calls return items
        story_ids_resp = MagicMock()
        story_ids_resp.json.return_value = [1, 2, 3]
        story_ids_resp.raise_for_status = MagicMock()

        item_resp = MagicMock()
        item_resp.json.return_value = {"score": 100, "descendants": 50}
        item_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [story_ids_resp, item_resp, item_resp, item_resp]

        df = HNTopCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 450.0  # (100+50)*3

    def test_meta(self):
        assert HNTopCollector.meta.name == "hn_top"
        assert HNBestCollector.meta.name == "hn_best"
        assert HNNewCollector.meta.name == "hn_new"
        assert HNTopCollector.meta.data_type == "tech_attention"


# ── StackOverflow ───────────────────────────────────────────

from signal_noise.collector.stackoverflow import SO_TAGS, get_so_collectors, _make_so_collector


class TestStackOverflow:
    def test_tag_count(self):
        assert len(SO_TAGS) >= 10

    def test_no_duplicate_names(self):
        names = [t[1] for t in SO_TAGS]
        assert len(names) == len(set(names))

    @patch("signal_noise.collector.stackoverflow.requests.get")
    def test_fetch_parses_total(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"total": 5000}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_so_collector("bitcoin", "test_so", "Test")
        df = cls().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 5000.0

    def test_factory_returns_dict(self):
        collectors = get_so_collectors()
        assert len(collectors) == len(SO_TAGS)


# ── GitHub Events ───────────────────────────────────────────

from signal_noise.collector.github_events import (
    GITHUB_REPOS, get_gh_events_collectors, _make_gh_events_collector,
)


class TestGitHubEvents:
    def test_repo_count(self):
        assert len(GITHUB_REPOS) >= 6

    @patch("signal_noise.collector.github_events.requests.get")
    def test_fetch_counts_events(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"type": "PushEvent"},
            {"type": "IssueCommentEvent"},
            {"type": "PullRequestEvent"},
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_gh_events_collector("test/repo", "test_gh", "Test")
        df = cls().fetch()
        assert df["value"].iloc[0] == 3.0

    def test_factory_returns_dict(self):
        collectors = get_gh_events_collectors()
        assert len(collectors) == len(GITHUB_REPOS)


# ── CoinGecko Global ───────────────────────────────────────

from signal_noise.collector.coingecko_global import (
    CG_TotalMarketCapCollector,
    CG_TotalVolumeCollector,
    CG_BtcDominanceCollector,
)

CG_GLOBAL_RESPONSE = {
    "data": {
        "total_market_cap": {"usd": 2400000000000},
        "total_volume": {"usd": 67000000000},
        "market_cap_percentage": {"btc": 56.5, "eth": 12.3},
        "active_cryptocurrencies": 15000,
        "ongoing_icos": 5,
        "markets": 1000,
        "market_cap_change_percentage_24h_usd": 1.5,
    }
}


class TestCoinGeckoGlobal:
    @patch("signal_noise.collector.coingecko_global.requests.get")
    def test_total_mcap(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = CG_GLOBAL_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = CG_TotalMarketCapCollector().fetch()
        assert df["value"].iloc[0] == 2400000000000

    @patch("signal_noise.collector.coingecko_global.requests.get")
    def test_btc_dominance(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = CG_GLOBAL_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = CG_BtcDominanceCollector().fetch()
        assert df["value"].iloc[0] == 56.5

    def test_meta_types(self):
        assert CG_TotalMarketCapCollector.meta.data_type == "crypto_market"
        assert CG_TotalVolumeCollector.meta.data_type == "crypto_market"


# ── Blockchain.com Charts ──────────────────────────────────

from signal_noise.collector.blockchain_charts import (
    BLOCKCHAIN_CHARTS, get_blockchain_collectors, _make_bc_collector,
)


class TestBlockchainCharts:
    def test_chart_count(self):
        assert len(BLOCKCHAIN_CHARTS) >= 10

    def test_no_duplicate_names(self):
        names = [t[1] for t in BLOCKCHAIN_CHARTS]
        assert len(names) == len(set(names))

    @patch("signal_noise.collector.blockchain_charts.requests.get")
    def test_fetch_parses_values(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "values": [
                {"x": 1704067200, "y": 500000},
                {"x": 1704153600, "y": 520000},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_bc_collector("n-unique-addresses", "test_bc", "Test", "onchain", "financial", "crypto")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 500000.0

    def test_factory_returns_dict(self):
        collectors = get_blockchain_collectors()
        assert len(collectors) == len(BLOCKCHAIN_CHARTS)


# ── NPM Downloads ──────────────────────────────────────────

from signal_noise.collector.npm_downloads import (
    NPM_PACKAGES, get_npm_collectors, _make_npm_collector,
)


class TestNPMDownloads:
    def test_package_count(self):
        assert len(NPM_PACKAGES) >= 10

    @patch("signal_noise.collector.npm_downloads.requests.get")
    def test_fetch_parses_downloads(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "downloads": [
                {"day": "2024-01-01", "downloads": 10000},
                {"day": "2024-01-02", "downloads": 12000},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_npm_collector("web3", "test_npm", "Test")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 10000.0

    def test_factory_returns_dict(self):
        collectors = get_npm_collectors()
        assert len(collectors) == len(NPM_PACKAGES)


# ── Archive.org Wayback ─────────────────────────────────────

from signal_noise.collector.wayback import (
    WAYBACK_SITES, get_wayback_collectors, _make_wayback_collector,
)


class TestWayback:
    def test_site_count(self):
        assert len(WAYBACK_SITES) >= 5

    @patch("signal_noise.collector.wayback.requests.get")
    def test_fetch_counts_per_day(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            ["timestamp"],
            ["20240101120000"],
            ["20240101150000"],
            ["20240102100000"],
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_wayback_collector("test.com", "test_wb", "Test")
        df = cls().fetch()
        assert len(df) == 2  # 2 unique days
        assert df["value"].iloc[0] == 2.0  # Jan 1 has 2 snapshots

    def test_factory_returns_dict(self):
        collectors = get_wayback_collectors()
        assert len(collectors) == len(WAYBACK_SITES)


# ── Registration ────────────────────────────────────────────

class TestNewChannelRegistration:
    def test_hackernews_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "hn_top" in COLLECTORS
        assert "hn_best" in COLLECTORS

    def test_stackoverflow_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "so_bitcoin" in COLLECTORS
        assert "so_ethereum" in COLLECTORS

    def test_github_events_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "gh_events_bitcoin" in COLLECTORS
        assert "gh_events_pytorch" in COLLECTORS

    def test_coingecko_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "cg_total_mcap" in COLLECTORS
        assert "cg_btc_dominance" in COLLECTORS

    def test_blockchain_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "bc_unique_addrs" in COLLECTORS
        assert "bc_tx_count" in COLLECTORS

    def test_npm_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "npm_web3" in COLLECTORS
        assert "npm_react" in COLLECTORS

    def test_wayback_registered(self):
        from signal_noise.collector import COLLECTORS
        assert "wb_bitcoin_org" in COLLECTORS
        assert "wb_sec_gov" in COLLECTORS

    def test_total_400_plus(self):
        from signal_noise.collector import COLLECTORS
        assert len(COLLECTORS) >= 400
