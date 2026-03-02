from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, UTC

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://web-api.tp.entsoe.eu/api"
_ENTSOE_TOKEN: str | None = None

# Area EIC codes for major bidding zones
_AREAS = {
    "DE_LU": "10Y1001A1001A82H",  # Germany-Luxembourg
    "FR": "10YFR-RTE------C",     # France
    "NL": "10YNL----------L",     # Netherlands
    "ES": "10YES-REE------0",     # Spain
}


def _get_token() -> str:
    global _ENTSOE_TOKEN
    if _ENTSOE_TOKEN:
        return _ENTSOE_TOKEN
    key = os.environ.get("ENTSOE_API_KEY")
    if not key:
        secret_path = os.path.expanduser("~/.secrets/entsoe")
        if os.path.exists(secret_path):
            with open(secret_path) as f:
                for line in f:
                    if line.startswith("export ENTSOE_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip("'\"")
                        break
    if not key:
        raise RuntimeError("ENTSOE_API_KEY not set")
    _ENTSOE_TOKEN = key
    return key


def _parse_prices_xml(xml_text: str) -> list[dict]:
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
    root = ET.fromstring(xml_text)
    rows = []
    for ts in root.findall(".//ns:TimeSeries", ns):
        for period in ts.findall("ns:Period", ns):
            start_el = period.find("ns:timeInterval/ns:start", ns)
            if start_el is None:
                continue
            start = pd.Timestamp(start_el.text)
            for point in period.findall("ns:Point", ns):
                pos = int(point.find("ns:position", ns).text)
                price = float(point.find("ns:price.amount", ns).text)
                dt = start + timedelta(hours=pos - 1)
                rows.append({"date": dt, "value": price})
    return rows


def _make_entsoe_collector(
    area_code: str, area_eic: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://transparency.entsoe.eu/",
            requires_key=True,
            domain="economy",
            category="energy",
        )

        def fetch(self) -> pd.DataFrame:
            token = _get_token()
            end = datetime.now(UTC)
            start = end - timedelta(days=30)
            params = {
                "securityToken": token,
                "documentType": "A44",
                "in_Domain": area_eic,
                "out_Domain": area_eic,
                "periodStart": start.strftime("%Y%m%d0000"),
                "periodEnd": end.strftime("%Y%m%d0000"),
            }
            resp = requests.get(
                _API_URL, params=params,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            rows = _parse_prices_xml(resp.text)
            if not rows:
                raise RuntimeError(f"No ENTSO-E data for {area_code}")
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"], utc=True)
            # Aggregate to daily average
            daily = (
                df.groupby(df["date"].dt.date)["value"]
                .mean()
                .reset_index()
            )
            daily["date"] = pd.to_datetime(daily["date"], utc=True)
            return daily.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"ENTSOE_{name}"
    _Collector.__qualname__ = f"ENTSOE_{name}"
    return _Collector


ENTSOE_ZONES = [
    ("DE_LU", _AREAS["DE_LU"], "entsoe_dayahead_de", "ENTSO-E Day-Ahead: Germany"),
    ("FR", _AREAS["FR"], "entsoe_dayahead_fr", "ENTSO-E Day-Ahead: France"),
    ("NL", _AREAS["NL"], "entsoe_dayahead_nl", "ENTSO-E Day-Ahead: Netherlands"),
    ("ES", _AREAS["ES"], "entsoe_dayahead_es", "ENTSO-E Day-Ahead: Spain"),
]


def get_entsoe_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_entsoe_collector(*t) for t in ENTSOE_ZONES}
