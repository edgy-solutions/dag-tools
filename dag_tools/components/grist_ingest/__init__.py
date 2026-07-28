from typing import TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from .component import GristIngestComponent

_module_lookup = {
    "GristIngestComponent": ".component",
}

__all__ = list(_module_lookup.keys())


def __getattr__(name: str):
    if name in _module_lookup:
        module = importlib.import_module(_module_lookup[name], __package__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
