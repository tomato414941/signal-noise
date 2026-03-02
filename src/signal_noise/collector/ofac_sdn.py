"""OFAC SDN (Specially Designated Nationals) sanctions list collector.

No API key required. Downloads XML from US Treasury.
Docs: https://ofac.treasury.gov/specially-designated-nationals-and-blocked-persons-list-sdn-human-readable-lists
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.xml"
_NS = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML"


class OFACSDNCollector(BaseCollector):
    meta = CollectorMeta(
        name="ofac_sdn_count",
        display_name="OFAC SDN Entity Count",
        update_frequency="weekly",
        api_docs_url="https://ofac.treasury.gov/specially-designated-nationals-and-blocked-persons-list-sdn-human-readable-lists",
        domain="economy",
        category="trade",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_SDN_URL, timeout=120)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        ns = {"sdn": _NS}

        publish_el = root.find(".//sdn:publshInformation/sdn:Publish_Date", ns)
        record_el = root.find(".//sdn:publshInformation/sdn:Record_Count", ns)

        if publish_el is None or publish_el.text is None:
            raise RuntimeError("OFAC SDN: Publish_Date not found")
        if record_el is None or record_el.text is None:
            raise RuntimeError("OFAC SDN: Record_Count not found")

        date = pd.to_datetime(publish_el.text.strip(), utc=True)
        count = int(record_el.text.strip())

        return pd.DataFrame([{"date": date, "value": float(count)}])
