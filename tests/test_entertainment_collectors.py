"""Tests for entertainment, events, and social media collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


from signal_noise.collector.events import (
    MajorSportsEventCollector,
    OlympicsCollector,
    SuperBowlCollector,
)
from signal_noise.collector.reddit import (
    RedditBitcoinCollector,
    RedditCryptoCollector,
    RedditWsbCollector,
)


# ── Events ──────────────────────────────────────────────────

class TestMajorSportsEvent:
    def test_fetch_returns_binary(self):
        df = MajorSportsEventCollector().fetch()
        assert "date" in df.columns
        assert "value" in df.columns
        assert set(df["value"].unique()).issubset({0.0, 1.0})

    def test_has_events(self):
        df = MajorSportsEventCollector().fetch()
        assert df["value"].sum() > 0

    def test_meta(self):
        assert MajorSportsEventCollector.meta.name == "major_sports_event"
        assert MajorSportsEventCollector.meta.category == "attention"


class TestSuperBowl:
    def test_fetch_single_days(self):
        df = SuperBowlCollector().fetch()
        event_days = df[df["value"] == 1.0]
        # Each Super Bowl is 1 day; we have 5 in the list
        assert len(event_days) <= 5

    def test_meta(self):
        assert SuperBowlCollector.meta.name == "super_bowl"


class TestOlympics:
    def test_fetch_multi_day_events(self):
        df = OlympicsCollector().fetch()
        event_days = df[df["value"] == 1.0]
        # Olympics are ~2 weeks each, multiple in list
        assert len(event_days) > 10

    def test_meta(self):
        assert OlympicsCollector.meta.name == "olympics"


# ── Reddit ──────────────────────────────────────────────────

REDDIT_RESPONSE = {
    "data": {
        "children": [
            {"data": {"score": 500, "num_comments": 120}},
            {"data": {"score": 300, "num_comments": 80}},
            {"data": {"score": 100, "num_comments": 30}},
        ]
    }
}


class TestRedditCrypto:
    @patch("signal_noise.collector.reddit._get_oauth_token", return_value="fake-token")
    @patch("signal_noise.collector.reddit.requests.get")
    def test_fetch_activity(self, mock_get, _mock_token):
        mock_resp = MagicMock()
        mock_resp.json.return_value = REDDIT_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = RedditCryptoCollector().fetch()
        assert len(df) == 1
        assert "timestamp" in df.columns
        # 500+120 + 300+80 + 100+30 = 1130
        assert df["value"].iloc[0] == 1130.0
        # Verify OAuth endpoint is used
        call_url = mock_get.call_args[0][0]
        assert "oauth.reddit.com" in call_url

    def test_meta(self):
        assert RedditCryptoCollector.meta.name == "reddit_crypto"
        assert RedditCryptoCollector.meta.category == "attention"
        assert RedditCryptoCollector.meta.requires_key is True


class TestRedditWsb:
    @patch("signal_noise.collector.reddit._get_oauth_token", return_value="fake-token")
    @patch("signal_noise.collector.reddit.requests.get")
    def test_fetch_activity(self, mock_get, _mock_token):
        mock_resp = MagicMock()
        mock_resp.json.return_value = REDDIT_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = RedditWsbCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 1130.0

    def test_meta(self):
        assert RedditWsbCollector.meta.name == "reddit_wsb"
        assert RedditWsbCollector.meta.requires_key is True


class TestRedditBitcoin:
    def test_meta(self):
        assert RedditBitcoinCollector.meta.name == "reddit_bitcoin"
        assert RedditBitcoinCollector.meta.requires_key is True


# ── Wikipedia entertainment pages ───────────────────────────

class TestWikiEntertainment:
    def test_entertainment_pages_registered(self):
        from signal_noise.collector.wikipedia_generic import WIKIPEDIA_PAGES
        names = {t[1] for t in WIKIPEDIA_PAGES}
        assert "wiki_super_bowl" in names
        assert "wiki_world_cup" in names
        assert "wiki_olympics" in names
        assert "wiki_elon_musk" in names
        assert "wiki_chatgpt" in names

    def test_new_pages_count(self):
        from signal_noise.collector.wikipedia_generic import WIKIPEDIA_PAGES
        assert len(WIKIPEDIA_PAGES) >= 35


# ── Registration ────────────────────────────────────────────

class TestRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "major_sports_event", "super_bowl", "olympics",
            "reddit_crypto", "reddit_wsb", "reddit_bitcoin",
            "wiki_super_bowl", "wiki_world_cup", "wiki_olympics",
            "wiki_elon_musk", "wiki_chatgpt",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
