from __future__ import annotations

from datetime import UTC, datetime

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NHTSARecallCountCollector(BaseCollector):
    """Monthly vehicle recall campaign count from NHTSA.

    Spikes in recall campaigns signal automotive quality crises.
    Major recalls (>100k vehicles) can move OEM stock prices.
    """

    meta = CollectorMeta(
        name="nhtsa_recall_count",
        display_name="NHTSA Vehicle Recall Campaigns (monthly)",
        update_frequency="monthly",
        api_docs_url="https://www.nhtsa.gov/nhtsa-datasets-and-apis",
        domain="infrastructure",
        category="safety",
    )

    def fetch(self) -> pd.DataFrame:
        current_year = datetime.now(UTC).year
        all_recalls = []
        for year in range(current_year - 3, current_year + 1):
            url = f"https://api.nhtsa.gov/recalls/recallsByYear?year={year}&type=vehicle"
            try:
                resp = requests.get(url, timeout=self.config.request_timeout)
                resp.raise_for_status()
                results = resp.json().get("results", [])
                all_recalls.extend(results)
            except Exception:
                continue

        if not all_recalls:
            raise RuntimeError("No NHTSA recall data")

        rows = []
        for recall in all_recalls:
            try:
                date_str = recall.get("ReportReceivedDate", "")
                if not date_str:
                    continue
                dt = pd.Timestamp(date_str, tz="UTC")
                rows.append({"date": dt})
            except (ValueError, TypeError):
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


class NHTSARecallVehicleCountCollector(BaseCollector):
    """Monthly total vehicles affected by recalls from NHTSA.

    Measures the SCALE of recalls, not just count.
    Millions of vehicles recalled = systemic quality issue.
    """

    meta = CollectorMeta(
        name="nhtsa_recall_vehicles",
        display_name="NHTSA Vehicles Affected by Recalls (monthly)",
        update_frequency="monthly",
        api_docs_url="https://www.nhtsa.gov/nhtsa-datasets-and-apis",
        domain="infrastructure",
        category="safety",
    )

    def fetch(self) -> pd.DataFrame:
        current_year = datetime.now(UTC).year
        all_recalls = []
        for year in range(current_year - 3, current_year + 1):
            url = f"https://api.nhtsa.gov/recalls/recallsByYear?year={year}&type=vehicle"
            try:
                resp = requests.get(url, timeout=self.config.request_timeout)
                resp.raise_for_status()
                results = resp.json().get("results", [])
                all_recalls.extend(results)
            except Exception:
                continue

        if not all_recalls:
            raise RuntimeError("No NHTSA recall data")

        rows = []
        for recall in all_recalls:
            try:
                date_str = recall.get("ReportReceivedDate", "")
                affected = recall.get("PotentialNumberofUnitsAffected", 0)
                if not date_str:
                    continue
                dt = pd.Timestamp(date_str, tz="UTC")
                rows.append({"date": dt, "value": float(affected or 0)})
            except (ValueError, TypeError):
                continue

        df = pd.DataFrame(rows)
        monthly = (
            df.set_index("date")
            .resample("MS")["value"]
            .sum()
            .reset_index()
        )
        return monthly.sort_values("date").reset_index(drop=True)
