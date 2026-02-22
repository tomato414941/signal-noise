from __future__ import annotations

import os

import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class ElectricityCollector(BaseCollector):
    meta = CollectorMeta(
        name="electricity",
        display_name="US Electricity Price",
        update_frequency="monthly",
        api_docs_url="https://www.eia.gov/opendata/",
        requires_key=True,
        domain="financial",
        category="commodity",
    )

    def fetch(self) -> pd.DataFrame:
        api_key = os.environ.get("EIA_API_KEY")
        if not api_key:
            raise RuntimeError(
                "EIA_API_KEY not set. Get a free key at https://www.eia.gov/opendata/"
            )
        raise NotImplementedError("ElectricityCollector requires EIA API key for data access")
