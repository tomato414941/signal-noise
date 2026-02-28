"""Auto-discovery and lazy loading."""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
import re
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector

log = logging.getLogger(__name__)

_SKIP = frozenset({"base", "_loader", "_cache", "_manifest", "_lazy"})
_FACTORY = re.compile(r"^get_\w+_collectors$")


def _discover() -> dict[str, type[BaseCollector]]:
    from signal_noise.collector.base import BaseCollector
    import signal_noise.collector as pkg
    out: dict[str, type[BaseCollector]] = {}
    for info in pkgutil.iter_modules(pkg.__path__):
        if info.name in _SKIP or info.name.startswith("__"):
            continue
        try:
            mod = importlib.import_module(f".{info.name}", pkg.__name__)
        except Exception:
            log.warning("Failed to import %s", info.name, exc_info=True)
            continue
        for obj in vars(mod).values():
            if (
                isinstance(obj, type)
                and issubclass(obj, BaseCollector)
                and obj is not BaseCollector
                and hasattr(obj, "meta")
                and not obj.__name__.startswith("_")
                and not inspect.isabstract(obj)
                and not getattr(obj, "_skip_registration", False)
            ):
                out[obj.meta.name] = obj
        for nm, obj in vars(mod).items():
            if callable(obj) and _FACTORY.match(nm):
                try:
                    out.update(obj())
                except Exception:
                    log.warning("Factory %s.%s failed", info.name, nm, exc_info=True)
    return out


class LazyCollectors:
    """Dict-like object that defers discovery until first access."""

    def __init__(self) -> None:
        self._data: dict[str, type[BaseCollector]] | None = None

    def _load(self) -> dict[str, type[BaseCollector]]:
        if self._data is None:
            self._data = _discover()
        return self._data

    def __getitem__(self, k: str) -> type[BaseCollector]:
        return self._load()[k]

    def __contains__(self, k: object) -> bool:
        return k in self._load()

    def __len__(self) -> int:
        return len(self._load())

    def __iter__(self) -> Iterator[str]:
        return iter(self._load())

    def get(self, k, d=None):  # noqa: ANN001,ANN201
        return self._load().get(k, d)

    def keys(self):  # noqa: ANN201
        return self._load().keys()

    def values(self):  # noqa: ANN201
        return self._load().values()

    def items(self):  # noqa: ANN201
        return self._load().items()

    def __repr__(self) -> str:
        if self._data is None:
            return "LazyCollectors(<not loaded>)"
        return f"LazyCollectors({len(self._data)} collectors)"
