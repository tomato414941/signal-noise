"""Tests for IMF PortWatch chokepoint collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.imf_portwatch import (
    PORTWATCH_CHOKEPOINTS,
    _make_portwatch_collector,
    get_portwatch_collectors,
)

ARCGIS_RESPONSE = {
    "features": [
        {"attributes": {"date": 1704067200000, "n_total": 42}},
        {"attributes": {"date": 1704153600000, "n_total": 55}},
    ]
}


class TestPortWatchFetch:
    @patch("signal_noise.collector.imf_portwatch.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = ARCGIS_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_portwatch_collector("Suez Canal", "test_suez", "Test Suez")
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 42.0

    @patch("signal_noise.collector.imf_portwatch.requests.get")
    def test_empty_features_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"features": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_portwatch_collector("Suez Canal", "test_suez", "Test Suez")
        with pytest.raises(RuntimeError, match="No PortWatch data"):
            cls().fetch()

    def test_meta(self):
        cls = _make_portwatch_collector("Suez Canal", "test_suez", "Test Suez")
        assert cls.meta.domain == "technology"
        assert cls.meta.category == "logistics"
        assert cls.meta.update_frequency == "weekly"


class TestPortWatchRegistry:
    def test_chokepoints_count(self):
        assert len(PORTWATCH_CHOKEPOINTS) == 6

    def test_no_duplicates(self):
        names = [t[1] for t in PORTWATCH_CHOKEPOINTS]
        assert len(names) == len(set(names))

    def test_factory_returns_all(self):
        collectors = get_portwatch_collectors()
        assert len(collectors) == len(PORTWATCH_CHOKEPOINTS)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        for _, name, _ in PORTWATCH_CHOKEPOINTS:
            assert name in COLLECTORS, f"{name} not registered"
