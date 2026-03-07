from __future__ import annotations

from unittest.mock import patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS
from signal_noise.collector.finnhub_breadth import (
    _breadth_cache,
    get_finnhub_breadth_collectors,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    _breadth_cache.clear()
    yield
    _breadth_cache.clear()


def _rec(strong_buy: int, buy: int, hold: int, sell: int, strong_sell: int, period: str) -> dict:
    return {
        "strongBuy": strong_buy,
        "buy": buy,
        "hold": hold,
        "sell": sell,
        "strongSell": strong_sell,
        "period": period,
    }


def _earn(period: str, surprise: float) -> dict:
    return {"period": period, "surprisePercent": surprise}


def _insider(year: int, month: int, mspr: float) -> dict:
    return {"year": year, "month": month, "mspr": mspr}


class TestFinnhubBreadth:
    @patch("signal_noise.collector.finnhub_breadth._fetch_recommendation")
    def test_recommendation_breadth(self, mock_fetch):
        def _fake(symbol: str) -> list[dict]:
            mapping = {
                "NVDA": [_rec(3, 0, 0, 0, 0, "2026-01-01"), _rec(0, 0, 1, 0, 0, "2026-02-01")],
                "AMD": [_rec(0, 0, 0, 0, 2, "2026-01-01"), _rec(0, 2, 0, 0, 0, "2026-02-01")],
                "AVGO": [_rec(0, 1, 1, 0, 0, "2026-01-01"), _rec(0, 0, 0, 1, 1, "2026-02-01")],
            }
            return mapping[symbol]

        mock_fetch.side_effect = _fake

        spec = next(
            spec for spec in get_finnhub_breadth_collectors().values()
            if spec.meta.name == "finnhub_semis_rec_breadth"
        )
        df = spec().fetch()

        assert list(df["value"]) == pytest.approx([2.0 / 3.0, 1.0 / 3.0])

    @patch("signal_noise.collector.finnhub_breadth._fetch_earnings")
    def test_earnings_spread(self, mock_fetch):
        def _fake(symbol: str) -> list[dict]:
            mapping = {
                "DHI": [_earn("2026-03-31", 5.0)],
                "LEN": [_earn("2026-03-31", -2.0)],
                "CAT": [_earn("2026-03-31", 1.0)],
                "DE": [_earn("2026-03-31", -1.0)],
                "UNP": [_earn("2026-03-31", 3.0)],
                "FDX": [_earn("2026-03-31", 4.0)],
            }
            return mapping[symbol]

        mock_fetch.side_effect = _fake

        collectors = get_finnhub_breadth_collectors()
        df = collectors["finnhub_housing_industrials_earnings_spread"]().fetch()

        assert len(df) == 1
        assert df["value"].iloc[0] == pytest.approx(-0.25)

    @patch("signal_noise.collector.finnhub_breadth._fetch_insider_sentiment")
    def test_insider_breadth(self, mock_fetch):
        def _fake(symbol: str) -> list[dict]:
            mapping = {
                "XOM": [_insider(2026, 1, 15.0)],
                "CVX": [_insider(2026, 1, -3.0)],
            }
            return mapping[symbol]

        mock_fetch.side_effect = _fake

        collectors = get_finnhub_breadth_collectors()
        df = collectors["finnhub_energy_insider_breadth"]().fetch()

        assert len(df) == 1
        assert df["value"].iloc[0] == pytest.approx(0.5)

    def test_meta_taxonomy_valid(self):
        collectors = get_finnhub_breadth_collectors()
        for cls in collectors.values():
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES
            assert cls.meta.requires_key is True

    def test_registration(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "finnhub_semis_rec_breadth",
            "finnhub_energy_insider_breadth",
            "finnhub_housing_industrials_earnings_spread",
        ]
        for name in expected:
            assert name in COLLECTORS
