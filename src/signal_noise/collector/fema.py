from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class FEMADisasterCountCollector(BaseCollector):
    """Monthly count of FEMA disaster declarations (US).

    Acceleration in disaster frequency signals climate-driven insurance
    risk and infrastructure spending pressure.
    """

    meta = CollectorMeta(
        name="fema_disaster_count",
        display_name="FEMA Disaster Declarations (monthly count)",
        update_frequency="monthly",
        api_docs_url="https://www.fema.gov/about/openfema/api",
        domain="infrastructure",
        category="safety",
    )

    def fetch(self) -> pd.DataFrame:
        start = datetime.now(UTC) - timedelta(days=365 * 5)
        url = (
            "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?"
            f"$filter=declarationDate ge '{start.strftime('%Y-%m-%d')}'"
            "&$select=declarationDate,disasterNumber"
            "&$orderby=declarationDate asc"
            "&$top=10000&$format=json"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("DisasterDeclarationsSummaries", [])
        if not data:
            raise RuntimeError("No FEMA data")

        seen: set[int] = set()
        rows: list[dict] = []
        for entry in data:
            dn = entry.get("disasterNumber")
            if dn in seen:
                continue
            seen.add(dn)
            try:
                dt = pd.Timestamp(entry["declarationDate"][:10], tz="UTC")
                rows.append({"date": dt, "disaster_number": dn})
            except (KeyError, ValueError):
                continue

        df = pd.DataFrame(rows)
        monthly = (
            df.set_index("date")
            .resample("MS")
            .size()
            .reset_index(name="value")
        )
        monthly["value"] = monthly["value"].astype(float)
        return monthly.sort_values("date").reset_index(drop=True)


class FEMAMajorDisasterCountCollector(BaseCollector):
    """Monthly count of Major Disaster declarations only (DR type).

    Major disasters (vs emergencies, fire management) indicate
    larger-scale catastrophic events.
    """

    meta = CollectorMeta(
        name="fema_major_disaster_count",
        display_name="FEMA Major Disaster Declarations (monthly)",
        update_frequency="monthly",
        api_docs_url="https://www.fema.gov/about/openfema/api",
        domain="infrastructure",
        category="safety",
    )

    def fetch(self) -> pd.DataFrame:
        start = datetime.now(UTC) - timedelta(days=365 * 5)
        url = (
            "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?"
            f"$filter=declarationDate ge '{start.strftime('%Y-%m-%d')}'"
            " and declarationType eq 'DR'"
            "&$select=declarationDate,disasterNumber"
            "&$orderby=declarationDate asc"
            "&$top=10000&$format=json"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("DisasterDeclarationsSummaries", [])
        if not data:
            raise RuntimeError("No FEMA major disaster data")

        seen: set[int] = set()
        rows: list[dict] = []
        for entry in data:
            dn = entry.get("disasterNumber")
            if dn in seen:
                continue
            seen.add(dn)
            try:
                dt = pd.Timestamp(entry["declarationDate"][:10], tz="UTC")
                rows.append({"date": dt})
            except (KeyError, ValueError):
                continue

        df = pd.DataFrame(rows)
        monthly = (
            df.set_index("date")
            .resample("MS")
            .size()
            .reset_index(name="value")
        )
        monthly["value"] = monthly["value"].astype(float)
        return monthly.sort_values("date").reset_index(drop=True)
