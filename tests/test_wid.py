from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.wid import get_wid_collectors


_SAMPLE_RESPONSE = {
    "US": {
        "sptinc_p99p100_z": {
            "2018": 0.2045,
            "2019": 0.2067,
            "2020": 0.1989,
            "2021": 0.2134,
        }
    }
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

    @patch("signal_noise.collector.wid.requests.get")
    def test_fetch_top1_income(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        collectors = get_wid_collectors()
        cls = collectors["wid_top1_income_us"]
        df = cls().fetch()
        assert len(df) == 4
        assert df["value"].iloc[0] == pytest.approx(0.2045)
        assert df["value"].iloc[3] == pytest.approx(0.2134)

    @patch("signal_noise.collector.wid.requests.get")
    def test_fetch_empty_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        collectors = get_wid_collectors()
        cls = collectors["wid_top1_income_us"]
        with pytest.raises(RuntimeError, match="No WID data"):
            cls().fetch()


class TestWidRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "wid_top1_income_us", "wid_top1_income_cn",
            "wid_bottom50_income_us", "wid_top1_wealth_us",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
