"""Tests for OFAC SDN sanctions list collector."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.ofac_sdn import OFACSDNCollector

_NS = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML"

SDN_XML = f"""\
<?xml version="1.0" encoding="utf-8"?>
<sdnList xmlns="{_NS}">
  <publshInformation>
    <Publish_Date>01/15/2025</Publish_Date>
    <Record_Count>12345</Record_Count>
  </publshInformation>
</sdnList>
"""

SDN_XML_MISSING_COUNT = f"""\
<?xml version="1.0" encoding="utf-8"?>
<sdnList xmlns="{_NS}">
  <publshInformation>
    <Publish_Date>01/15/2025</Publish_Date>
  </publshInformation>
</sdnList>
"""


class TestOFACSDN:
    @patch("signal_noise.collector.ofac_sdn.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.content = SDN_XML.encode()
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        df = OFACSDNCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 12345.0

    @patch("signal_noise.collector.ofac_sdn.requests.get")
    def test_missing_record_count_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.content = SDN_XML_MISSING_COUNT.encode()
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with pytest.raises(RuntimeError, match="Record_Count not found"):
            OFACSDNCollector().fetch()

    def test_meta(self):
        assert OFACSDNCollector.meta.name == "ofac_sdn_count"
        assert OFACSDNCollector.meta.domain == "macro"
        assert OFACSDNCollector.meta.category == "trade"
        assert OFACSDNCollector.meta.update_frequency == "weekly"

    def test_registered(self):
        from signal_noise.collector import COLLECTORS

        assert "ofac_sdn_count" in COLLECTORS
