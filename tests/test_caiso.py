from __future__ import annotations

import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.caiso import CAISO_HUBS, get_caiso_collectors


class TestCaisoFactory:
    def test_creates_all_collectors(self):
        collectors = get_caiso_collectors()
        assert len(collectors) == len(CAISO_HUBS)

    def test_meta_fields(self):
        collectors = get_caiso_collectors()
        for name, cls in collectors.items():
            assert isinstance(cls.meta, CollectorMeta)
            assert cls.meta.domain == "economy"
            assert cls.meta.category == "energy"
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES

    @patch("signal_noise.collector.caiso.requests.get")
    def test_fetch_parses_zip_csv(self, mock_get):
        csv_content = (
            "INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,OPR_DT,OPR_HR,"
            "OPR_INTERVAL,NODE_ID_XML,NODE_ID,NODE,MARKET_RUN_ID,"
            "LMP_TYPE,XML_DATA_ITEM,PNODE_RESMRID,GRP_TYPE,POS,MW,GROUP\n"
            "2025-03-01T08:00:00-00:00,2025-03-01T09:00:00-00:00,"
            "2025-03-01,1,0,,,,DAM,LMP,,,,,35.50,\n"
            "2025-03-01T09:00:00-00:00,2025-03-01T10:00:00-00:00,"
            "2025-03-01,2,0,,,,DAM,LMP,,,,,40.25,\n"
            "2025-03-01T08:00:00-00:00,2025-03-01T09:00:00-00:00,"
            "2025-03-01,1,0,,,,DAM,MCE,,,,,5.00,\n"
        )
        # Add HUB column
        csv_lines = csv_content.strip().split("\n")
        header = csv_lines[0] + ",HUB"
        rows = [
            csv_lines[1] + ",TH_NP15_GEN-APND",
            csv_lines[2] + ",TH_NP15_GEN-APND",
            csv_lines[3] + ",TH_NP15_GEN-APND",
        ]
        csv_data = "\n".join([header] + rows)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("data.csv", csv_data)
        buf.seek(0)

        mock_resp = MagicMock()
        mock_resp.content = buf.read()
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        collectors = get_caiso_collectors()
        cls = collectors["caiso_lmp_np15"]
        df = cls().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == pytest.approx(37.875)


class TestCaisoRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        for _, name, _ in CAISO_HUBS:
            assert name in COLLECTORS, f"{name} not registered"
