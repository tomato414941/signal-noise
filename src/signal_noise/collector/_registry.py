from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Protocol

from signal_noise.collector.streaming import StreamingCollector

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector, CollectorMeta


def meta_to_dict(meta: "CollectorMeta") -> dict[str, object]:
    return {
        "display_name": meta.display_name,
        "domain": meta.domain,
        "category": meta.category,
        "update_frequency": meta.update_frequency,
        "requires_key": meta.requires_key,
        "signal_type": meta.signal_type,
        "collection_level": meta.collection_level,
        "interval": meta.interval,
    }


class CollectorRegistry(Protocol):
    def __getitem__(self, name: str) -> type["BaseCollector"]:
        ...

    def __iter__(self) -> Iterator[str]:
        ...

    def __len__(self) -> int:
        ...

    def keys(self):  # noqa: ANN201
        ...

    def load(self, name: str) -> type["BaseCollector"] | None:
        ...

    def get_meta(self, name: str) -> dict[str, object] | None:
        ...

    def is_streaming(self, name: str) -> bool:
        ...


class EagerCollectorRegistry(Mapping[str, type["BaseCollector"]]):
    def __init__(self, collectors: Mapping[str, type["BaseCollector"]]) -> None:
        self._collectors = dict(collectors)
        self._meta = {
            name: meta_to_dict(cls.meta)
            for name, cls in self._collectors.items()
        }
        self._streaming = {
            name: issubclass(cls, StreamingCollector)
            for name, cls in self._collectors.items()
        }

    def __getitem__(self, name: str) -> type["BaseCollector"]:
        return self._collectors[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._collectors)

    def __len__(self) -> int:
        return len(self._collectors)

    def keys(self):  # noqa: ANN201
        return self._collectors.keys()

    def load(self, name: str) -> type["BaseCollector"] | None:
        return self._collectors.get(name)

    def get_meta(self, name: str) -> dict[str, object] | None:
        meta = self._meta.get(name)
        return dict(meta) if meta is not None else None

    def is_streaming(self, name: str) -> bool:
        return self._streaming.get(name, False)


def ensure_registry(
    registry: CollectorRegistry | Mapping[str, type["BaseCollector"]],
) -> CollectorRegistry:
    from signal_noise.collector._lazy import LazyCollectorRegistry

    if isinstance(registry, (LazyCollectorRegistry, EagerCollectorRegistry)):
        return registry
    return EagerCollectorRegistry(registry)
