"""Auto-discovery for signal-noise collectors."""
from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector

log = logging.getLogger(__name__)

_SKIP = frozenset({"base", "_loader", "_cache", "_manifest", "_lazy"})
_FACTORY = re.compile(r"^get_\w+_collectors$")


def _discover() -> dict[str, type[BaseCollector]]:
    """Scan all collector modules and return {name: class} dict."""
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
