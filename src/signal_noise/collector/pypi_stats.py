from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import threading
import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (pypi_package, collector_name, display_name)
PYPI_PACKAGES: list[tuple[str, str, str]] = [
    ("numpy", "pypi_numpy_downloads", "PyPI: numpy"),
    ("pandas", "pypi_pandas_downloads", "PyPI: pandas"),
    ("scipy", "pypi_scipy_downloads", "PyPI: scipy"),
    ("scikit-learn", "pypi_sklearn_downloads", "PyPI: scikit-learn"),
    ("tensorflow", "pypi_tensorflow_downloads", "PyPI: tensorflow"),
    ("torch", "pypi_torch_downloads", "PyPI: torch"),
    ("flask", "pypi_flask_downloads", "PyPI: flask"),
    ("django", "pypi_django_downloads", "PyPI: django"),
    ("fastapi", "pypi_fastapi_downloads", "PyPI: fastapi"),
    ("requests", "pypi_requests_downloads", "PyPI: requests"),
    ("boto3", "pypi_boto3_downloads", "PyPI: boto3"),
    ("transformers", "pypi_transformers_downloads", "PyPI: transformers"),
    ("langchain-core", "pypi_langchain_downloads", "PyPI: langchain-core"),
    ("anthropic", "pypi_anthropic_downloads", "PyPI: anthropic"),
    ("openai", "pypi_openai_downloads", "PyPI: openai"),
]

_PYPI_REQUEST_LOCK = threading.Lock()
_PYPI_NEXT_REQUEST_AT = 0.0
_PYPI_MIN_INTERVAL = 2.0
_PYPI_DEFAULT_RETRY_AFTER = 30.0


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return _PYPI_DEFAULT_RETRY_AFTER
    try:
        return max(float(value), _PYPI_MIN_INTERVAL)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
            return max(seconds, _PYPI_MIN_INTERVAL)
        except (TypeError, ValueError):
            return _PYPI_DEFAULT_RETRY_AFTER


def _rate_limited_get(url: str, *, headers: dict[str, str], timeout: int):
    global _PYPI_NEXT_REQUEST_AT

    with _PYPI_REQUEST_LOCK:
        wait = _PYPI_NEXT_REQUEST_AT - time.monotonic()
        if wait > 0:
            time.sleep(wait)

        resp = requests.get(url, headers=headers, timeout=timeout)
        next_delay = _PYPI_MIN_INTERVAL
        if resp.status_code == 429:
            next_delay = _parse_retry_after(resp.headers.get("Retry-After"))
        _PYPI_NEXT_REQUEST_AT = time.monotonic() + next_delay
        return resp


def _make_pypi_collector(
    package: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://pypistats.org/api/",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://pypistats.org/api/packages/{package}/overall?mirrors=true"
            headers = {"User-Agent": "signal-noise/0.1"}
            resp = _rate_limited_get(
                url,
                headers=headers,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            items = resp.json().get("data", [])
            rows = []
            for item in items:
                if item.get("category") == "without_mirrors":
                    continue
                try:
                    rows.append({
                        "date": pd.Timestamp(item["date"], tz="UTC"),
                        "value": float(item["downloads"]),
                    })
                except (KeyError, ValueError, TypeError):
                    continue
            if not rows:
                raise RuntimeError(f"No PyPI download data for {package}")
            df = pd.DataFrame(rows)
            daily = df.groupby("date")["value"].sum().reset_index()
            return daily.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"PyPI_{name}"
    _Collector.__qualname__ = f"PyPI_{name}"
    return _Collector


def get_pypi_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_pypi_collector(*t) for t in PYPI_PACKAGES}
