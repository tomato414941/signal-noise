"""Tests for ReliefWeb humanitarian crisis report collector."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.reliefweb import ReliefWebCollector

RELIEFWEB_RESPONSE = {
    "embedded": {
        "facets": {
            "date.created": {
                "data": [
                    {"value": "2025-01-01T00:00:00+00:00", "count": 120},
                    {"value": "2025-01-02T00:00:00+00:00", "count": 95},
                ]
            }
        }
    }
}


class TestReliefWeb:
    @patch.dict("os.environ", {"RELIEFWEB_APPNAME": "signal-noise-test"})
    @patch("signal_noise.collector.reliefweb.requests.post")
    def test_fetch(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = RELIEFWEB_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        df = ReliefWebCollector().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 120.0

    @patch.dict("os.environ", {"RELIEFWEB_APPNAME": "signal-noise-test"})
    @patch("signal_noise.collector.reliefweb.requests.post")
    def test_empty_facets_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"embedded": {"facets": {}}}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        with pytest.raises(RuntimeError, match="no facet data"):
            ReliefWebCollector().fetch()

    def test_meta(self):
        assert ReliefWebCollector.meta.name == "reliefweb_disaster_count"
        assert ReliefWebCollector.meta.domain == "sentiment"
        assert ReliefWebCollector.meta.category == "attention"
        assert ReliefWebCollector.meta.requires_key is True

    def test_registered(self):
        from signal_noise.collector import COLLECTORS

        assert "reliefweb_disaster_count" in COLLECTORS

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_appname_raises(self):
        with patch("signal_noise.collector.reliefweb.Path.home") as mock_home:
            mock_home.return_value = MagicMock()
            mock_home.return_value.__truediv__ = lambda s, k: MagicMock()
            # Make secrets file not exist
            from signal_noise.collector.reliefweb import _get_appname

            with patch("signal_noise.collector.reliefweb.Path") as mock_path:
                mock_file = MagicMock()
                mock_file.exists.return_value = False
                mock_path.home.return_value.__truediv__ = lambda s, k: MagicMock(
                    __truediv__=lambda s, k: mock_file
                )
                with pytest.raises(RuntimeError, match="appname not configured"):
                    _get_appname()
