from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests

from signal_noise.collector.pypi_stats import get_pypi_collectors
from signal_noise.config import CollectorConfig


def _mock_response(*, status_code: int = 200, data: list[dict] | None = None, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = {"data": data or []}
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestPyPICollectors:
    @patch("signal_noise.collector.pypi_stats.requests.get")
    def test_fetch_parses_downloads(self, mock_get):
        mock_get.return_value = _mock_response(
            data=[
                {"date": "2026-03-05", "category": "with_mirrors", "downloads": 100},
                {"date": "2026-03-05", "category": "without_mirrors", "downloads": 50},
                {"date": "2026-03-06", "category": "with_mirrors", "downloads": 120},
            ],
        )

        cls = get_pypi_collectors()["pypi_numpy_downloads"]
        df = cls().fetch()

        assert len(df) == 2
        assert df["value"].tolist() == [100.0, 120.0]

    @patch("signal_noise.collector.base.time.sleep", return_value=None)
    @patch("signal_noise.collector.pypi_stats.time.sleep", return_value=None)
    @patch("signal_noise.collector.pypi_stats.requests.get")
    def test_fetch_with_retry_handles_429(self, mock_get, _pypi_sleep, _base_sleep):
        first = _mock_response(status_code=429, headers={"Retry-After": "0"})
        second = _mock_response(
            data=[{"date": "2026-03-06", "category": "with_mirrors", "downloads": 42}],
        )
        mock_get.side_effect = [first, second]

        cls = get_pypi_collectors()["pypi_langchain_downloads"]
        collector = cls(config=CollectorConfig(max_retries=2))

        with patch("signal_noise.collector.pypi_stats._PYPI_MIN_INTERVAL", 0.0), patch(
            "signal_noise.collector.pypi_stats._PYPI_DEFAULT_RETRY_AFTER", 0.0,
        ), patch("signal_noise.collector.pypi_stats._PYPI_NEXT_REQUEST_AT", 0.0):
            df = collector.fetch_with_retry()

        assert len(df) == 1
        assert df["value"].iloc[0] == 42.0
        assert mock_get.call_count == 2
