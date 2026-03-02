from __future__ import annotations

import asyncio
import fnmatch
import logging
from dataclasses import dataclass
from typing import AsyncIterator

log = logging.getLogger(__name__)


@dataclass
class SignalEvent:
    name: str
    timestamp: str
    value: float | None
    event_type: str  # "update" | "anomaly" | "circuit_break"
    detail: str = ""


class EventBus:
    """In-process pub/sub for signal updates.

    Each subscriber gets its own asyncio.Queue. Publishing fans out
    to all subscriber queues whose pattern matches the event name.
    """

    def __init__(self, max_queue_size: int = 1000):
        self._subscribers: dict[int, tuple[str, asyncio.Queue[SignalEvent]]] = {}
        self._next_id = 0
        self._max_queue_size = max_queue_size

    async def publish(self, event: SignalEvent) -> int:
        """Publish event to all matching subscribers. Returns delivery count."""
        delivered = 0
        for sub_id, (pattern, queue) in list(self._subscribers.items()):
            if self._matches(pattern, event.name):
                try:
                    queue.put_nowait(event)
                    delivered += 1
                except asyncio.QueueFull:
                    log.warning(
                        "Subscriber %d queue full, dropping event %s",
                        sub_id, event.name,
                    )
        return delivered

    async def subscribe(self, pattern: str) -> AsyncIterator[SignalEvent]:
        """Subscribe to events matching pattern (fnmatch glob).

        Pattern examples: "funding_rate_*", "liq_*", "*" (all).
        Comma-separated: "funding_rate_btc,liq_ratio_btc_1h"
        """
        sub_id = self._next_id
        self._next_id += 1
        queue: asyncio.Queue[SignalEvent] = asyncio.Queue(
            maxsize=self._max_queue_size,
        )
        self._subscribers[sub_id] = (pattern, queue)
        try:
            while True:
                event = await queue.get()
                yield event
        finally:
            self._subscribers.pop(sub_id, None)

    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @staticmethod
    def _matches(pattern: str, name: str) -> bool:
        for p in pattern.split(","):
            p = p.strip()
            if fnmatch.fnmatch(name, p):
                return True
        return False
