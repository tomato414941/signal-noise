from __future__ import annotations

import xml.etree.ElementTree as ET
from unittest.mock import patch

import pandas as pd
import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS
from signal_noise.collector.sec_form4 import (
    _FilingRef,
    _SIGNAL_SPECS,
    _fetch_ticker_history,
    _make_signal_collector,
    _parse_open_market_transactions,
    _sec_form4_cache,
    get_sec_form4_collectors,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    _sec_form4_cache.clear()
    yield
    _sec_form4_cache.clear()


XML_SAMPLE = """<?xml version="1.0"?>
<ownershipDocument>
  <periodOfReport>2026-03-05</periodOfReport>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-03-05</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-03-05</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>4</value></transactionShares>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-03-05</value></transactionDate>
      <transactionCoding><transactionCode>A</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>99</value></transactionShares>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

XML_SELL_ONLY = """<?xml version="1.0"?>
<ownershipDocument>
  <periodOfReport>2026-03-04</periodOfReport>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5</value></transactionShares>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


class TestParsing:
    def test_parse_open_market_transactions_filters_non_open_market(self):
        root = ET.fromstring(XML_SAMPLE)

        rows = _parse_open_market_transactions(root)

        assert len(rows) == 2
        assert rows[0].code == "P"
        assert rows[0].shares == pytest.approx(10.0)
        assert rows[1].code == "S"
        assert rows[1].shares == pytest.approx(4.0)


class TestHistory:
    @patch("signal_noise.collector.sec_form4._fetch_filing_root")
    @patch("signal_noise.collector.sec_form4._fetch_recent_form4_filings")
    @patch("signal_noise.collector.sec_form4._utc_today")
    def test_fetch_ticker_history_dense_zero_fills(
        self,
        mock_today,
        mock_filings,
        mock_filing_root,
    ):
        mock_today.return_value = __import__("datetime").date(2026, 3, 6)
        mock_filings.return_value = [
            _FilingRef(cik=1318605, accession="0000000000-26-000001"),
            _FilingRef(cik=1318605, accession="0000000000-26-000002"),
        ]
        mock_filing_root.side_effect = [
            ET.fromstring(XML_SAMPLE),
            ET.fromstring(XML_SELL_ONLY),
        ]

        history = _fetch_ticker_history("TSLA", lookback_days=2)

        assert list(history["date"]) == [
            pd.Timestamp("2026-03-04", tz="UTC"),
            pd.Timestamp("2026-03-05", tz="UTC"),
            pd.Timestamp("2026-03-06", tz="UTC"),
        ]
        assert list(history["net_share_ratio"]) == pytest.approx([-1.0, 6.0 / 14.0, 0.0])
        assert list(history["open_market_tx_count"]) == pytest.approx([1.0, 2.0, 0.0])


class TestCollectors:
    @patch("signal_noise.collector.sec_form4._fetch_ticker_history")
    def test_signal_collector(self, mock_history):
        mock_history.return_value = pd.DataFrame(
            {
                "date": [
                    pd.Timestamp("2026-03-04", tz="UTC"),
                    pd.Timestamp("2026-03-05", tz="UTC"),
                ],
                "net_share_ratio": [-1.0, 0.5],
                "open_market_tx_count": [1.0, 2.0],
            }
        )

        spec = next(spec for spec in _SIGNAL_SPECS if spec.name == "form4_tsla_net_share_ratio")
        df = _make_signal_collector(spec)().fetch()

        assert len(df) == 2
        assert list(df["value"]) == pytest.approx([-1.0, 0.5])

    def test_get_collectors_returns_all(self):
        collectors = get_sec_form4_collectors()
        assert len(collectors) == len(_SIGNAL_SPECS)
        assert "form4_tsla_net_share_ratio" in collectors
        assert "form4_cvx_open_market_tx_count" in collectors

    def test_meta_taxonomy_valid(self):
        collectors = get_sec_form4_collectors()
        for cls in collectors.values():
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    def test_registration(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "form4_tsla_net_share_ratio",
            "form4_meta_open_market_tx_count",
            "form4_gs_net_share_ratio",
            "form4_jpm_open_market_tx_count",
            "form4_cvx_net_share_ratio",
        ]
        for name in expected:
            assert name in COLLECTORS
