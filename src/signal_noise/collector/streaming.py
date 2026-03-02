from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from typing import AsyncIterator

import pandas as pd

from signal_noise.collector.base import BaseCollector

log = logging.getLogger(__name__)


class StreamingCollector(BaseCollector):
    """Base class for WebSocket-based real-time collectors.

    Instead of periodic ``fetch()``, implements ``stream()`` which yields
    DataFrames as data arrives. The scheduler runs ``stream()`` as a
    long-lived task.
    """

    reconnect_delay: float = 5.0
    max_reconnect_delay: float = 300.0
    use_realtime_store: bool = False

    def fetch(self) -> pd.DataFrame:
        """Not used for streaming collectors."""
        return pd.DataFrame(columns=["timestamp", "value"])

    @abstractmethod
    async def stream(self) -> AsyncIterator[pd.DataFrame]:
        """Yield DataFrames as data arrives from the stream."""
        ...
        yield  # type: ignore[misc]  # make this a generator

    async def connect_with_retry(self) -> AsyncIterator[pd.DataFrame]:
        """Wrapper around stream() with auto-reconnect and backoff."""
        delay = self.reconnect_delay
        while True:
            try:
                async for df in self.stream():
                    yield df
                    delay = self.reconnect_delay
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "Stream %s disconnected: %s. Reconnecting in %.0fs",
                    self.meta.name, exc, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.max_reconnect_delay)
