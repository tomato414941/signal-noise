"""Lazy collector registry backed by a manifest file.

keys()/len()/in operate on the manifest (no module imports).
get()/[] import only the needed module on demand.
items()/values() trigger a full load.
"""
from __future__ import annotations

import importlib
import logging
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector

log = logging.getLogger(__name__)


class LazyCollectorRegistry(Mapping[str, type["BaseCollector"]]):

    def __init__(self, manifest_entries: dict[str, dict]) -> None:
        self._manifest = manifest_entries
        self._loaded: dict[str, type[BaseCollector]] = {}
        self._failed: set[str] = set()

    def _load_one(self, name: str) -> type[BaseCollector] | None:
        if name in self._loaded:
            return self._loaded[name]
        if name in self._failed:
            return None

        entry = self._manifest.get(name)
        if not entry:
            return None

        mod_name = entry["module"]
        cls_qualname = entry["class"]

        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            log.warning("Failed to import %s for collector %s", mod_name, name)
            self._failed.add(name)
            return None

        # Try direct attribute lookup (works for static classes)
        cls = getattr(mod, cls_qualname, None)
        if cls is not None:
            self._loaded[name] = cls
            return cls

        # Factory-generated classes: call get_*_collectors() to resolve
        cls = self._load_via_factory(mod, name)
        if cls is not None:
            return cls

        log.warning("Could not resolve collector %s from %s", name, mod_name)
        self._failed.add(name)
        return None

    def _load_via_factory(self, mod: object, name: str) -> type[BaseCollector] | None:
        for attr_name in dir(mod):
            if attr_name.startswith("get_") and attr_name.endswith("_collectors"):
                func = getattr(mod, attr_name)
                if not callable(func):
                    continue
                try:
                    result = func()
                    if isinstance(result, dict):
                        for n, c in result.items():
                            if n not in self._loaded:
                                self._loaded[n] = c
                        if name in self._loaded:
                            return self._loaded[name]
                except Exception:
                    log.warning("Factory %s.%s() failed", getattr(mod, "__name__", "?"), attr_name)
        return None

    def _load_all(self) -> None:
        for name in self._manifest:
            if name not in self._loaded and name not in self._failed:
                self._load_one(name)

    # ── Mapping interface ──

    def __getitem__(self, name: str) -> type[BaseCollector]:
        cls = self._load_one(name)
        if cls is None:
            raise KeyError(name)
        return cls

    def __contains__(self, name: object) -> bool:
        return name in self._manifest

    def __len__(self) -> int:
        return len(self._manifest)

    def __iter__(self) -> Iterator[str]:
        return iter(self._manifest)

    def keys(self):  # noqa: ANN201
        return self._manifest.keys()

    def get(self, name: str, default=None):  # noqa: ANN001,ANN201
        cls = self._load_one(name)
        return cls if cls is not None else default

    def items(self):  # noqa: ANN201
        self._load_all()
        return self._loaded.items()

    def values(self):  # noqa: ANN201
        self._load_all()
        return self._loaded.values()

    def __repr__(self) -> str:
        loaded = len(self._loaded)
        total = len(self._manifest)
        return f"LazyCollectorRegistry({loaded}/{total} loaded)"
