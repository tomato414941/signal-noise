from __future__ import annotations

from unittest.mock import MagicMock, patch

from signal_noise.collector.gbif import GBIFOccurrenceCollector, GBIF_LOOKBACK_DAYS


class TestGBIFCollector:
    @patch("signal_noise.collector.gbif.requests.get")
    def test_fetch_uses_short_lookback_window(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"count": 123}
        mock_get.return_value = mock_resp

        df = GBIFOccurrenceCollector().fetch()

        assert len(df) == GBIF_LOOKBACK_DAYS
        assert df["value"].tolist() == [123] * GBIF_LOOKBACK_DAYS
        assert mock_get.call_count == GBIF_LOOKBACK_DAYS
