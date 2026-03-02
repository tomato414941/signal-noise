from __future__ import annotations

from unittest.mock import MagicMock, patch


from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.congress import CongressBillCountCollector, get_congress_collectors


class TestCongressBillCount:
    def test_meta(self):
        assert CongressBillCountCollector.meta.name == "congress_bill_count"
        assert CongressBillCountCollector.meta.domain == "society"
        assert CongressBillCountCollector.meta.category == "legislation"
        assert CongressBillCountCollector.meta.requires_key is True
        assert CongressBillCountCollector.meta.domain in DOMAINS
        assert CongressBillCountCollector.meta.category in CATEGORIES

    @patch("signal_noise.collector.congress._get_key", return_value="test_key")
    @patch("signal_noise.collector.congress.requests.get")
    def test_fetch_counts_bills(self, mock_get, _mock_key):
        def side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            # Return different counts per congress
            if "/117" in url:
                resp.json.return_value = {"pagination": {"count": 16601}}
            elif "/118" in url:
                resp.json.return_value = {"pagination": {"count": 15234}}
            else:
                resp.json.return_value = {"pagination": {"count": 12000}}
            return resp

        mock_get.side_effect = side_effect

        df = CongressBillCountCollector().fetch()
        assert len(df) >= 2
        assert "date" in df.columns
        assert "value" in df.columns
        assert all(df["value"] > 0)


class TestCongressFactory:
    def test_creates_all_collectors(self):
        collectors = get_congress_collectors()
        expected = [
            "congress_bill_count", "congress_hr_count",
            "congress_s_count", "congress_hjres_count", "congress_sjres_count",
        ]
        for name in expected:
            assert name in collectors, f"{name} not in factory output"

    def test_meta_fields(self):
        collectors = get_congress_collectors()
        for name, cls in collectors.items():
            assert isinstance(cls.meta, CollectorMeta)
            assert cls.meta.category == "legislation"
            assert cls.meta.category in CATEGORIES


class TestCongressRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = ["congress_bill_count", "congress_hr_count", "congress_s_count"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
