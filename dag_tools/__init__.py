from typing import TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from dag_tools.components.dlt_pipeline import DltPipelineComponent
    from dag_tools.components.dbt_project import CustomDbtProjectComponent
    from dag_tools.components.datahub_lineage import DatahubLineageComponent
    from dag_tools.components.otel_api_sync import OtelApiSyncComponent
    from dag_tools.components.restate_api_sync import RestateApiSyncComponent
    from dag_tools.components.restate_dlt_sync import RestateDltSyncComponent
    from dag_tools.components.s3_sensor import S3ToArrowComponent, S3SensorComponent, S3ToFileComponent

_module_lookup = {
    "DltPipelineComponent": "dag_tools.components.dlt_pipeline.component",
    "CustomDbtProjectComponent": "dag_tools.components.dbt_project.component",
    "DatahubLineageComponent": "dag_tools.components.datahub_lineage.component",
    "OtelApiSyncComponent": "dag_tools.components.otel_api_sync.component",
    "RestateApiSyncComponent": "dag_tools.components.restate_api_sync.component",
    "RestateDltSyncComponent": "dag_tools.components.restate_dlt_sync.component",
    "S3ToArrowComponent": "dag_tools.components.s3_sensor.arrow_component",
    "S3SensorComponent": "dag_tools.components.s3_sensor.sensor_component",
    "S3ToFileComponent": "dag_tools.components.s3_sensor.file_component",
}

__all__ = list(_module_lookup.keys())

def __getattr__(name: str):
    if name in _module_lookup:
        module = importlib.import_module(_module_lookup[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
