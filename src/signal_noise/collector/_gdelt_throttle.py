"""Shared per-domain throttle for all GDELT API collectors.

GDELT rate limit is ~1 request per 5 seconds.  Multiple collector modules
(gdelt.py, gdelt_food.py, gdelt_tone.py) share this lock so concurrent
scheduler workers respect the limit.
"""
from __future__ import annotations

import threading
import time

_lock = threading.Lock()
_last_request: float = 0.0
_MIN_INTERVAL: float = 5.0


def throttle() -> None:
    """Block until at least _MIN_INTERVAL seconds since the last GDELT request."""
    global _last_request
    with _lock:
        now = time.monotonic()
        wait = _MIN_INTERVAL - (now - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()
