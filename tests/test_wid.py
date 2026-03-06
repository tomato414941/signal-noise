from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.wid import (
    _extract_wid_rows,
    _get_wid_api_config,
    _wid_cache,
    get_wid_collectors,
)


_SAMPLE_RESPONSE = {
    "sptinc_p99p100_992_j": [
        {
            "US": {
                "values": [
                    {"y": 2018, "v": 0.2045},
                    {"y": 2019, "v": 0.2067},
                    {"y": 2020, "v": 0.1989},
                    {"y": 2021, "v": 0.2134},
                ]
            }
        }
    ]
}


class TestWidFactory:
    def test_creates_collectors(self):
        collectors = get_wid_collectors()
        # 8 top1 income + 8 bottom50 income + 4 top1 wealth = 20
        assert len(collectors) == 20

    def test_meta_fields(self):
        collectors = get_wid_collectors()
        for name, cls in collectors.items():
            assert isinstance(cls.meta, CollectorMeta)
            assert cls.meta.domain == "economy"
            assert cls.meta.category == "inequality"
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    def test_extract_rows(self):
        rows = _extract_wid_rows(_SAMPLE_RESPONSE, "US")
        assert len(rows) == 4
        assert rows[0]["value"] == pytest.approx(0.2045)
        assert rows[-1]["value"] == pytest.approx(0.2134)

    @patch("signal_noise.collector.wid._fetch_wid_indicator")
    def test_fetch_top1_income(self, mock_fetch):
        mock_fetch.return_value = _SAMPLE_RESPONSE
        collectors = get_wid_collectors()
        cls = collectors["wid_top1_income_us"]
        df = cls().fetch()
        assert len(df) == 4
        assert df["value"].iloc[0] == pytest.approx(0.2045)
        assert df["value"].iloc[3] == pytest.approx(0.2134)

    @patch("signal_noise.collector.wid._fetch_wid_indicator")
    def test_fetch_empty_response(self, mock_fetch):
        mock_fetch.return_value = {}
        collectors = get_wid_collectors()
        cls = collectors["wid_top1_income_us"]
        with pytest.raises(RuntimeError, match="No WID data"):
            cls().fetch()

    @patch("signal_noise.collector.wid.requests.get")
    def test_discovers_api_config_from_frontend(self, mock_get):
        _wid_cache.clear()
        page = MagicMock()
        page.raise_for_status = MagicMock()
        page.text = '<script src="/www-site/themes/default/js/app.js?v=9.99"></script>'

        script = MagicMock()
        script.raise_for_status = MagicMock()
        script.text = (
            "this.apiKey = 'public-key';"
            " this.apiURL = 'https://api.example.com/prod/';"
        )

        mock_get.side_effect = [page, script]
        api_url, api_key = _get_wid_api_config(timeout=10)

        assert api_url == "https://api.example.com/prod/"
        assert api_key == "public-key"
        _wid_cache.clear()


class TestWidRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "wid_top1_income_us", "wid_top1_income_cn",
            "wid_bottom50_income_us", "wid_top1_wealth_us",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
