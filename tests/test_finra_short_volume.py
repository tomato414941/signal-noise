from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS
from signal_noise.collector.finra_short_volume import (
    _RAW_SPECS,
    _SPREAD_SPECS,
    _fetch_finra_history,
    _finra_cache,
    _make_raw_collector,
    _make_spread_collector,
    _parse_finra_text,
    get_finra_short_volume_collectors,
)


def _response(text: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = RuntimeError(f"http {status_code}")
    return resp


@pytest.fixture(autouse=True)
def _clear_cache():
    _finra_cache.clear()
    yield
    _finra_cache.clear()


FINRA_DAY_1 = """Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260305|SPY|600|0|1000|B,Q,N
20260305|QQQ|330|0|600|Q,N
20260305|IWM|280|0|400|Q,N
20260305|HYG|200|0|500|Q,N
20260305|XLF|150|0|300|Q,N
20260305|SMH|250|0|400|Q,N
20260305|TSLA|700|0|1000|Q,N
20260305|NVDA|510|0|800|Q,N
8
"""

FINRA_DAY_2 = """Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20260304|SPY|550|0|1000|B,Q,N
20260304|QQQ|350|0|700|Q,N
20260304|IWM|300|0|500|Q,N
20260304|HYG|190|0|500|Q,N
20260304|XLF|180|0|320|Q,N
20260304|SMH|270|0|450|Q,N
20260304|TSLA|640|0|1000|Q,N
20260304|NVDA|495|0|900|Q,N
8
"""


class TestParse:
    def test_parse_finra_text(self):
        ratios = _parse_finra_text(FINRA_DAY_1, {"SPY", "QQQ", "NVDA"})
        assert ratios["SPY"] == pytest.approx(0.6)
        assert ratios["QQQ"] == pytest.approx(0.55)
        assert ratios["NVDA"] == pytest.approx(0.6375)

    def test_parse_finra_text_ignores_missing(self):
        ratios = _parse_finra_text(FINRA_DAY_1, {"ABC"})
        assert ratios == {}


class TestHistory:
    @patch("signal_noise.collector.finra_short_volume._iter_candidate_days")
    @patch("signal_noise.collector.finra_short_volume.requests.get")
    def test_fetch_finra_history(self, mock_get, mock_days):
        mock_days.return_value = [
            __import__("datetime").date(2026, 3, 6),
            __import__("datetime").date(2026, 3, 5),
            __import__("datetime").date(2026, 3, 4),
        ]
        mock_get.side_effect = [
            _response("", 403),
            _response(FINRA_DAY_1),
            _response(FINRA_DAY_2),
        ]

        history = _fetch_finra_history()

        assert len(history["SPY"]) == 2
        assert history["SPY"][0]["value"] == pytest.approx(0.6)
        assert history["SPY"][1]["value"] == pytest.approx(0.55)

    @patch("signal_noise.collector.finra_short_volume._iter_candidate_days")
    @patch("signal_noise.collector.finra_short_volume.requests.get")
    def test_fetch_finra_history_uses_cache(self, mock_get, mock_days):
        mock_days.return_value = [__import__("datetime").date(2026, 3, 5)]
        mock_get.return_value = _response(FINRA_DAY_1)

        first = _fetch_finra_history()
        second = _fetch_finra_history()

        assert mock_get.call_count == 1
        assert first == second


class TestCollectors:
    @patch("signal_noise.collector.finra_short_volume._fetch_finra_history")
    def test_raw_collector(self, mock_history):
        mock_history.return_value = {
            "SPY": [
                {"date": __import__("pandas").Timestamp("2026-03-04", tz="UTC"), "value": 0.55},
                {"date": __import__("pandas").Timestamp("2026-03-05", tz="UTC"), "value": 0.60},
            ]
        }

        spec = next(spec for spec in _RAW_SPECS if spec.symbol == "SPY")
        df = _make_raw_collector(spec)().fetch()

        assert len(df) == 2
        assert df["value"].iloc[-1] == pytest.approx(0.60)

    @patch("signal_noise.collector.finra_short_volume._fetch_finra_history")
    def test_spread_collector(self, mock_history):
        mock_history.return_value = {
            "SPY": [
                {"date": __import__("pandas").Timestamp("2026-03-04", tz="UTC"), "value": 0.55},
                {"date": __import__("pandas").Timestamp("2026-03-05", tz="UTC"), "value": 0.60},
            ],
            "QQQ": [
                {"date": __import__("pandas").Timestamp("2026-03-04", tz="UTC"), "value": 0.50},
                {"date": __import__("pandas").Timestamp("2026-03-05", tz="UTC"), "value": 0.55},
            ],
        }

        spec = next(spec for spec in _SPREAD_SPECS if spec.name == "finra_tech_broad_short_spread")
        df = _make_spread_collector(spec)().fetch()

        assert len(df) == 2
        assert list(df["value"]) == pytest.approx([-0.05, -0.05])

    def test_get_collectors_returns_all(self):
        collectors = get_finra_short_volume_collectors()
        assert len(collectors) == len(_RAW_SPECS) + len(_SPREAD_SPECS)
        assert "finra_spy_short_ratio" in collectors
        assert "finra_tech_broad_short_spread" in collectors

    def test_meta_taxonomy_valid(self):
        collectors = get_finra_short_volume_collectors()
        for cls in collectors.values():
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    def test_registration(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "finra_spy_short_ratio",
            "finra_qqq_short_ratio",
            "finra_tsla_short_ratio",
            "finra_small_large_short_spread",
            "finra_tech_broad_short_spread",
        ]
        for name in expected:
            assert name in COLLECTORS
