from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.entsoe import ENTSOE_ZONES, get_entsoe_collectors


_SAMPLE_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <TimeSeries>
    <Period>
      <timeInterval>
        <start>2025-03-01T23:00Z</start>
        <end>2025-03-02T23:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><price.amount>45.50</price.amount></Point>
      <Point><position>2</position><price.amount>42.30</price.amount></Point>
      <Point><position>3</position><price.amount>38.10</price.amount></Point>
    </Period>
  </TimeSeries>
</Publication_MarketDocument>
"""


class TestEntsoeFactory:
    def test_creates_all_collectors(self):
        collectors = get_entsoe_collectors()
        assert len(collectors) == len(ENTSOE_ZONES)

    def test_meta_fields(self):
        collectors = get_entsoe_collectors()
        for name, cls in collectors.items():
            assert isinstance(cls.meta, CollectorMeta)
            assert cls.meta.domain == "economy"
            assert cls.meta.category == "energy"
            assert cls.meta.requires_key is True
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    @patch("signal_noise.collector.entsoe._get_token", return_value="test_token")
    @patch("signal_noise.collector.entsoe.requests.get")
    def test_fetch_parses_xml(self, mock_get, _mock_token):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_XML
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        collectors = get_entsoe_collectors()
        cls = collectors["entsoe_dayahead_de"]
        df = cls().fetch()
        # XML starts at 23:00 UTC Mar 1 → pos 1 falls on Mar 1, pos 2-3 on Mar 2
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(45.50)
        assert df["value"].iloc[1] == pytest.approx((42.30 + 38.10) / 2, rel=1e-2)


class TestEntsoeRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        for _, _, name, _ in ENTSOE_ZONES:
            assert name in COLLECTORS, f"{name} not registered"
