"""WHO Global Health Observatory (GHO) collectors.

Uses the GHO OData API. No API key required.
https://www.who.int/data/gho/info/gho-odata-api
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://ghoapi.azureedge.net/api"

# (indicator_code, spatial_dim, sex_filter, collector_name, display_name, domain, category)
# sex_filter: "SEX_BTSX" for both sexes, None for no sex filter
WHO_GHO_SERIES: list[tuple[str, str, str | None, str, str, str, str]] = [
    # Life expectancy at birth
    ("WHOSIS_000001", "GLOBAL", "SEX_BTSX", "who_life_expectancy", "WHO Life Expectancy (Global)", "health", "public_health"),
    # Healthy life expectancy (HALE) at birth
    ("WHOSIS_000002", "GLOBAL", "SEX_BTSX", "who_hale", "WHO Healthy Life Expectancy (Global)", "health", "public_health"),
    # Health expenditure % GDP
    ("GHED_CHEGDP_SHA2011", "USA", None, "who_health_exp_us", "Health Expenditure % GDP: US", "health", "public_health"),
    ("GHED_CHEGDP_SHA2011", "JPN", None, "who_health_exp_jp", "Health Expenditure % GDP: Japan", "health", "public_health"),
    ("GHED_CHEGDP_SHA2011", "CHN", None, "who_health_exp_cn", "Health Expenditure % GDP: China", "health", "public_health"),
    ("GHED_CHEGDP_SHA2011", "DEU", None, "who_health_exp_de", "Health Expenditure % GDP: Germany", "health", "public_health"),
    ("GHED_CHEGDP_SHA2011", "GBR", None, "who_health_exp_gb", "Health Expenditure % GDP: UK", "health", "public_health"),
    # Premature NCD mortality (30-69 yrs)
    ("NCDMORT3070", "USA", "SEX_BTSX", "who_ncd_mort_us", "Premature NCD Mortality: US", "health", "public_health"),
    ("NCDMORT3070", "JPN", "SEX_BTSX", "who_ncd_mort_jp", "Premature NCD Mortality: Japan", "health", "public_health"),
    ("NCDMORT3070", "CHN", "SEX_BTSX", "who_ncd_mort_cn", "Premature NCD Mortality: China", "health", "public_health"),
    ("NCDMORT3070", "DEU", "SEX_BTSX", "who_ncd_mort_de", "Premature NCD Mortality: Germany", "health", "public_health"),
    # Under-5 mortality rate (global)
    ("MDG_0000000007", "GLOBAL", "SEX_BTSX", "who_under5_mort", "WHO Under-5 Mortality (Global)", "health", "public_health"),
    # DTP3 immunization coverage (global)
    ("WHS4_100", "GLOBAL", None, "who_dtp3_coverage", "WHO DTP3 Immunization (Global)", "health", "public_health"),
    # TB incidence per 100k (global)
    ("MDG_0000000020", "GLOBAL", None, "who_tb_incidence", "WHO TB Incidence (Global)", "health", "epidemiology"),
    # Physicians per 10k population
    ("HWF_0001", "USA", None, "who_physicians_us", "Physicians per 10k: US", "health", "public_health"),
    ("HWF_0001", "JPN", None, "who_physicians_jp", "Physicians per 10k: Japan", "health", "public_health"),
    ("HWF_0001", "DEU", None, "who_physicians_de", "Physicians per 10k: Germany", "health", "public_health"),
    # Air pollution (PM2.5 annual mean)
    ("SDGPM25", "USA", None, "who_pm25_us", "Air Pollution PM2.5: US", "health", "public_health"),
    ("SDGPM25", "CHN", None, "who_pm25_cn", "Air Pollution PM2.5: China", "health", "public_health"),
    ("SDGPM25", "IND", None, "who_pm25_in", "Air Pollution PM2.5: India", "health", "public_health"),
    # Alcohol consumption per capita (litres)
    ("SA_0000001688", "USA", "SEX_BTSX", "who_alcohol_us", "Alcohol Consumption: US", "health", "public_health"),
    ("SA_0000001688", "JPN", "SEX_BTSX", "who_alcohol_jp", "Alcohol Consumption: Japan", "health", "public_health"),
    ("SA_0000001688", "DEU", "SEX_BTSX", "who_alcohol_de", "Alcohol Consumption: Germany", "health", "public_health"),
    # Obesity prevalence (BMI >= 30, %)
    ("NCD_BMI_30A", "USA", "SEX_BTSX", "who_obesity_us", "Obesity Prevalence: US", "health", "public_health"),
    ("NCD_BMI_30A", "JPN", "SEX_BTSX", "who_obesity_jp", "Obesity Prevalence: Japan", "health", "public_health"),
    ("NCD_BMI_30A", "GBR", "SEX_BTSX", "who_obesity_gb", "Obesity Prevalence: UK", "health", "public_health"),
    ("NCD_BMI_30A", "DEU", "SEX_BTSX", "who_obesity_de", "Obesity Prevalence: Germany", "health", "public_health"),
    # Suicide rate per 100k
    ("SDGSUICIDE", "USA", "SEX_BTSX", "who_suicide_us", "Suicide Rate: US", "health", "public_health"),
    ("SDGSUICIDE", "JPN", "SEX_BTSX", "who_suicide_jp", "Suicide Rate: Japan", "health", "public_health"),
    ("SDGSUICIDE", "KOR", "SEX_BTSX", "who_suicide_kr", "Suicide Rate: S.Korea", "health", "public_health"),
]


def _make_who_gho_collector(
    indicator: str, spatial_dim: str, sex_filter: str | None,
    name: str, display_name: str, domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://www.who.int/data/gho/info/gho-odata-api",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            filters = [f"SpatialDim eq '{spatial_dim}'"]
            if sex_filter:
                filters.append(f"Dim1 eq '{sex_filter}'")
            filter_str = " and ".join(filters)
            url = f"{_BASE_URL}/{indicator}?$filter={filter_str}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json().get("value", [])
            if not data:
                raise RuntimeError(f"No WHO data for {indicator}/{spatial_dim}")

            rows = []
            for entry in data:
                try:
                    year = int(entry["TimeDim"])
                    val = float(entry["NumericValue"])
                    dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                    rows.append({"date": dt, "value": val})
                except (KeyError, ValueError, TypeError):
                    continue
            if not rows:
                raise RuntimeError(f"No parseable WHO data for {indicator}/{spatial_dim}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WHO_{name}"
    _Collector.__qualname__ = f"WHO_{name}"
    return _Collector


def get_who_gho_collectors() -> dict[str, type[BaseCollector]]:
    return {
        t[3]: _make_who_gho_collector(*t) for t in WHO_GHO_SERIES
    }
