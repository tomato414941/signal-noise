"""Shared API response cache for collectors hitting the same endpoints.

Multiple collectors sharing a single API endpoint share a cached response
to avoid duplicate requests and rate limit issues.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


class SharedAPICache:
    """Thread-safe TTL cache for shared API responses.

    Usage::

        cache = SharedAPICache(ttl=60)
        data = cache.get_or_fetch("global", lambda: requests.get(url).json())
    """

    def __init__(self, ttl: int = 60) -> None:
        self._ttl = ttl
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get_or_fetch(self, key: str, fetch_fn: Callable[[], T], ttl: int | None = None) -> T:
        effective_ttl = ttl if ttl is not None else self._ttl
        now = time.monotonic()

        with self._lock:
            if key in self._store:
                ts, data = self._store[key]
                if (now - ts) < effective_ttl:
                    return data

        data = fetch_fn()

        with self._lock:
            self._store[key] = (time.monotonic(), data)

        return data

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    @property
    def size(self) -> int:
        return len(self._store)
