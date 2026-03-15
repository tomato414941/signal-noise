"""OSV.dev open source vulnerability stats.

Tracks known vulnerabilities for key packages across ecosystems.
Rising counts indicate supply-chain risk awareness and disclosure.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://api.osv.dev/v1/query"


def _make_osv_collector(
    name: str, display_name: str, pkg: str, ecosystem: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://osv.dev/docs/",
            domain="technology",
            category="internet",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.post(
                _API_URL,
                json={"package": {"name": pkg, "ecosystem": ecosystem}},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            vulns = resp.json().get("vulns", [])
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(len(vulns))}])

    _Collector.__name__ = f"OSV_{name}"
    _Collector.__qualname__ = f"OSV_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str, str]] = [
    ("osv_lodash", "OSV Vulns: lodash (npm)", "lodash", "npm"),
    ("osv_requests", "OSV Vulns: requests (PyPI)", "requests", "PyPI"),
    ("osv_django", "OSV Vulns: Django (PyPI)", "Django", "PyPI"),
    ("osv_express", "OSV Vulns: express (npm)", "express", "npm"),
    ("osv_linux_kernel", "OSV Vulns: Linux Kernel", "Kernel", "Linux"),
    ("osv_tensorflow", "OSV Vulns: tensorflow (PyPI)", "tensorflow", "PyPI"),
    ("osv_flask", "OSV Vulns: Flask (PyPI)", "Flask", "PyPI"),
    ("osv_numpy", "OSV Vulns: numpy (PyPI)", "numpy", "PyPI"),
    ("osv_react", "OSV Vulns: react (npm)", "react", "npm"),
    ("osv_axios", "OSV Vulns: axios (npm)", "axios", "npm"),
    ("osv_spring_framework", "OSV Vulns: Spring Framework (Maven)", "org.springframework:spring-framework-bom", "Maven"),
    ("osv_rails", "OSV Vulns: Rails (RubyGems)", "rails", "RubyGems"),
    ("osv_openssl", "OSV Vulns: OpenSSL", "openssl", "OSS-Fuzz"),
]


def get_osv_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_osv_collector(name, display, pkg, eco)
        for name, display, pkg, eco in _SIGNALS
    }
