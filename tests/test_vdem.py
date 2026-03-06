from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.vdem import _fetch_owid_csv, get_vdem_collectors


_SAMPLE_CSV = """\
Entity,Code,Year,Electoral democracy index
United States,USA,2020,0.85
United States,USA,2021,0.82
United States,USA,2022,0.80
China,CHN,2020,0.05
China,CHN,2021,0.05
Japan,JPN,2020,0.80
"""


class TestVdemFactory:
    @patch("signal_noise.collector.vdem.requests.get")
    def test_fetch_owid_csv_uses_csv_endpoint(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_CSV
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = _fetch_owid_csv("electoral-democracy-index")

        assert not df.empty
        assert mock_get.call_args.args[0].endswith("/electoral-democracy-index.csv")

    def test_creates_collectors(self):
        collectors = get_vdem_collectors()
        assert len(collectors) >= 14  # 8 democracy + 6 human rights

    def test_meta_fields(self):
        collectors = get_vdem_collectors()
        for name, cls in collectors.items():
            assert isinstance(cls.meta, CollectorMeta)
            assert cls.meta.domain == "society"
            assert cls.meta.category == "governance"
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    @patch("signal_noise.collector.vdem._fetch_owid_csv")
    def test_fetch_filters_country(self, mock_fetch):
        mock_fetch.return_value = pd.read_csv(
            __import__("io").StringIO(_SAMPLE_CSV)
        )
        collectors = get_vdem_collectors()
        cls = collectors["vdem_democracy_us"]
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == pytest.approx(0.85)

    @patch("signal_noise.collector.vdem._fetch_owid_csv")
    def test_fetch_china(self, mock_fetch):
        mock_fetch.return_value = pd.read_csv(
            __import__("io").StringIO(_SAMPLE_CSV)
        )
        collectors = get_vdem_collectors()
        cls = collectors["vdem_democracy_cn"]
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(0.05)


class TestVdemRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = ["vdem_democracy_us", "vdem_democracy_cn", "vdem_democracy_jp"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
