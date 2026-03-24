from typing import TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from .arrow_component import S3ToArrowComponent
    from .sensor_component import S3SensorComponent
    from .file_component import S3ToFileComponent

_module_lookup = {
    "S3ToArrowComponent": ".arrow_component",
    "S3SensorComponent": ".sensor_component",
    "S3ToFileComponent": ".file_component",
}

__all__ = list(_module_lookup.keys())

def __getattr__(name: str):
    if name in _module_lookup:
        module = importlib.import_module(_module_lookup[name], __package__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
