from __future__ import annotations

from unittest.mock import MagicMock, patch


from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.fbi_crime import FBI_OFFENSES, get_fbi_crime_collectors


_SAMPLE_RESPONSE = [
    {
        "year": 2020,
        "violent_crime": 1313105,
        "property_crime": 6452038,
        "homicide": 21570,
        "robbery": 209643,
        "burglary": 898573,
        "arson": 41029,
    },
    {
        "year": 2019,
        "violent_crime": 1203808,
        "property_crime": 6925677,
        "homicide": 16425,
        "robbery": 267988,
        "burglary": 1117696,
        "arson": 43555,
    },
]


class TestFbiFactory:
    def test_creates_all_collectors(self):
        collectors = get_fbi_crime_collectors()
        assert len(collectors) == len(FBI_OFFENSES)

    def test_meta_fields(self):
        collectors = get_fbi_crime_collectors()
        for name, cls in collectors.items():
            assert isinstance(cls.meta, CollectorMeta)
            assert cls.meta.domain == "society"
            assert cls.meta.category == "crime"
            assert cls.meta.requires_key is True
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    @patch("signal_noise.collector.fbi_crime._get_key", return_value="test_key")
    @patch("signal_noise.collector.fbi_crime.requests.get")
    def test_fetch_violent_crime(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        collectors = get_fbi_crime_collectors()
        cls = collectors["fbi_violent_crime"]
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[1] == 1313105.0  # 2020 is later

    @patch("signal_noise.collector.fbi_crime._get_key", return_value="test_key")
    @patch("signal_noise.collector.fbi_crime.requests.get")
    def test_fetch_homicide(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        collectors = get_fbi_crime_collectors()
        cls = collectors["fbi_homicide"]
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[1] == 21570.0


class TestFbiRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        for _, name, _ in FBI_OFFENSES:
            assert name in COLLECTORS, f"{name} not registered"
