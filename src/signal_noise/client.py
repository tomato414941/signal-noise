from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import AsyncIterator

import pandas as pd
import requests
import websockets

log = logging.getLogger(__name__)


class SignalClient:
    """HTTP client for the signal-noise REST API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: int = 30,
        retry_count: int = 2,
        retry_backoff: float = 1.0,
        batch_chunk_size: int = 250,
    ):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._retry_count = retry_count
        self._retry_backoff = retry_backoff
        self._batch_chunk_size = max(int(batch_chunk_size), 1)
        self._session = requests.Session()
        self._cache: dict[str, pd.DataFrame] = {}
        self._last_seen: dict[str, str] = {}

    def _normalize_timestamp_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse mixed ISO8601 timestamps without failing the whole response."""
        if "timestamp" not in df.columns:
            return df

        out = df.copy()
        out["timestamp"] = pd.to_datetime(
            out["timestamp"],
            format="mixed",
            utc=True,
            errors="coerce",
        )
        invalid = int(out["timestamp"].isna().sum())
        if invalid:
            log.warning("Dropped %d rows with invalid timestamps", invalid)
            out = out.dropna(subset=["timestamp"])
        return out

    @property
    def base_url(self) -> str:
        return self._base_url

    def health(self) -> bool:
        try:
            r = self._get("/health")
            return r.get("status") in ("ok", "degraded")
        except Exception:
            return False

    def health_detail(self) -> dict:
        try:
            return self._get("/health")
        except Exception:
            return {
                "status": "unreachable",
                "fresh": -1,
                "stale": -1,
                "failing": -1,
                "never_seen": -1,
                "suppressed": -1,
            }

    def stale_signals(self) -> list[dict]:
        try:
            data = self._get("/health/signals")
            return data.get("stale", [])
        except Exception:
            return []

    def get_latest(self, name: str) -> dict | None:
        try:
            return self._get(f"/signals/{name}/latest")
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise

    def get_data(
        self, name: str, since: str | None = None, resolution: str | None = None,
    ) -> pd.DataFrame:
        params = {}
        if since:
            params["since"] = since
        elif name in self._last_seen:
            params["since"] = self._last_seen[name]
        if resolution:
            params["resolution"] = resolution

        data = self._get(f"/signals/{name}/data", params=params)
        if not data:
            return pd.DataFrame(columns=["timestamp", "value"])

        df = pd.DataFrame(data)
        if "date" in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={"date": "timestamp"})
        df = self._normalize_timestamp_column(df)

        if name in self._cache:
            combined = pd.concat([self._cache[name], df])
            combined = combined.drop_duplicates(subset=["timestamp"], keep="last")
            self._cache[name] = combined.sort_values("timestamp").reset_index(drop=True)
        else:
            self._cache[name] = df.sort_values("timestamp").reset_index(drop=True)

        if not df.empty:
            self._last_seen[name] = str(df["timestamp"].max())

        return self._cache[name]

    def get_batch(
        self,
        names: list[str],
        since: str | None = None,
        columns: list[str] | None = None,
        resolution: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        if len(names) > self._batch_chunk_size:
            result: dict[str, pd.DataFrame] = {}
            for start in range(0, len(names), self._batch_chunk_size):
                chunk = names[start:start + self._batch_chunk_size]
                result.update(
                    self.get_batch(
                        chunk,
                        since=since,
                        columns=columns,
                        resolution=resolution,
                    )
                )
            return result

        body: dict = {"names": names}
        if since:
            body["since"] = since
        if columns:
            body["columns"] = columns
        if resolution:
            body["resolution"] = resolution

        data = self._post("/signals/batch", json=body, timeout=max(self._timeout, 90))
        result: dict[str, pd.DataFrame] = {}
        for name, records in data.items():
            if not records:
                result[name] = pd.DataFrame(columns=["timestamp", "value"])
                continue
            df = pd.DataFrame(records)
            if "date" in df.columns and "timestamp" not in df.columns:
                df = df.rename(columns={"date": "timestamp"})
            df = self._normalize_timestamp_column(df)
            result[name] = df.sort_values("timestamp").reset_index(drop=True)
        return result

    def list_signals(
        self,
        domain: str | None = None,
        category: str | None = None,
        signal_type: str | None = None,
    ) -> list[dict]:
        params: dict[str, str] = {}
        if domain:
            params["domain"] = domain
        if category:
            params["category"] = category
        if signal_type:
            params["signal_type"] = signal_type
        return self._get("/signals", params=params)

    async def subscribe(
        self, pattern: str, *, reconnect: bool = True,
    ) -> AsyncIterator[dict]:
        """Subscribe to real-time signal updates via WebSocket.

        Args:
            pattern: Comma-separated signal name patterns (fnmatch glob).
            reconnect: Auto-reconnect on disconnect (default True).

        Yields:
            dict with keys: name, timestamp, value, event_type, detail.
        """
        ws_url = self._base_url.replace("http://", "ws://").replace("https://", "wss://")
        url = f"{ws_url}/ws/signals?names={pattern}"

        delay = 1.0
        max_delay = 60.0

        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    delay = 1.0
                    log.info("WebSocket connected: %s", url)
                    async for msg in ws:
                        data = json.loads(msg)
                        yield data
                # Connection closed normally
                if not reconnect:
                    return
                log.info("WebSocket closed, reconnecting...")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not reconnect:
                    raise
                log.warning(
                    "WebSocket disconnected: %s. Reconnecting in %.0fs", exc, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        url = f"{self._base_url}{path}"
        last_err: Exception | None = None
        for attempt in range(self._retry_count):
            try:
                r = self._session.get(url, params=params, timeout=self._timeout)
                r.raise_for_status()
                return r.json()
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code < 500:
                    raise
                last_err = e
            except Exception as e:
                last_err = e
            if attempt < self._retry_count - 1:
                wait = self._retry_backoff ** attempt
                log.warning("API request failed (%s), retry in %.1fs: %s", url, wait, last_err)
                time.sleep(wait)
        raise last_err  # type: ignore[misc]

    def _post(self, path: str, **kwargs) -> dict | list:
        url = f"{self._base_url}{path}"
        timeout = kwargs.pop("timeout", self._timeout)
        last_err: Exception | None = None
        for attempt in range(self._retry_count):
            try:
                r = self._session.post(url, timeout=timeout, **kwargs)
                r.raise_for_status()
                return r.json()
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code < 500:
                    raise
                last_err = e
            except Exception as e:
                last_err = e
            if attempt < self._retry_count - 1:
                wait = self._retry_backoff ** attempt
                log.warning("API POST failed (%s), retry in %.1fs: %s", url, wait, last_err)
                time.sleep(wait)
        raise last_err  # type: ignore[misc]
