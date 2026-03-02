from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field


@dataclass
class CircuitBreakerState:
    consecutive_failures: int = 0
    cooldown: float = 300.0
    in_cooldown_until: float = 0.0

    def record_failure(
        self, max_failures: int, base_cooldown: float, max_cooldown: float,
    ) -> bool:
        """Record a failure. Returns True if circuit just tripped."""
        self.consecutive_failures += 1
        if self.consecutive_failures >= max_failures:
            self.in_cooldown_until = time.monotonic() + self.cooldown
            self.cooldown = min(self.cooldown * 2, max_cooldown)
            return True
        return False

    def record_success(self, base_cooldown: float) -> None:
        self.consecutive_failures = 0
        self.cooldown = base_cooldown
        self.in_cooldown_until = 0.0

    @property
    def is_in_cooldown(self) -> bool:
        return time.monotonic() < self.in_cooldown_until


@dataclass(order=True)
class ScheduleEntry:
    next_run: float
    name: str = field(compare=False)
    interval: int = field(compare=False)
    meta_dict: dict = field(compare=False)


class ScheduleQueue:
    def __init__(self) -> None:
        self._heap: list[ScheduleEntry] = []
        self._circuit_breakers: dict[str, CircuitBreakerState] = {}

    def push(self, entry: ScheduleEntry) -> None:
        heapq.heappush(self._heap, entry)

    def peek_delay(self) -> float:
        if not self._heap:
            return 86400.0
        return max(0.0, self._heap[0].next_run - time.monotonic())

    def pop_due(self) -> ScheduleEntry | None:
        if not self._heap:
            return None
        if self._heap[0].next_run <= time.monotonic():
            return heapq.heappop(self._heap)
        return None

    def reschedule(self, entry: ScheduleEntry) -> None:
        entry.next_run = time.monotonic() + entry.interval
        heapq.heappush(self._heap, entry)

    def reschedule_after(self, entry: ScheduleEntry, delay: float) -> None:
        entry.next_run = time.monotonic() + delay
        heapq.heappush(self._heap, entry)

    def get_breaker(self, name: str) -> CircuitBreakerState:
        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreakerState()
        return self._circuit_breakers[name]

    def __len__(self) -> int:
        return len(self._heap)
