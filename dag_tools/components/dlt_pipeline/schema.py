from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class DltPipelineScheduleSchema(BaseModel):
    """A pipeline's ``schedule:`` block. Created STOPPED unless ``enabled``."""
    model_config = ConfigDict(extra="forbid")
    cron: str
    enabled: bool = False
    timezone: Optional[str] = None


class DltPipelineGroupSchema(BaseModel):
    """Schema for a specific DLT pipeline configuration (e.g. 'fast_refresh')"""
    name: Optional[str] = Field(default=None, description="Name of the pipeline group")
    io_manager_key: str = Field(default="io_manager")
    dest_schema: Optional[str] = Field(default=None)
    backend: str = Field(default="sqlalchemy")
    backend_kwargs: Dict[str, Any] = Field(default_factory=dict)
    pipeline_kwargs: Dict[str, Any] = Field(default_factory=dict)
    hints: Dict[str, Any] = Field(default_factory=dict)
    select_columns: Dict[str, List[str]] = Field(default_factory=dict)
    limit: int = Field(default=0)
    add_timestamp: bool = Field(default=False)
    schedule: Optional[DltPipelineScheduleSchema] = None
    table_prefix: Optional[str] = Field(
        default=None,
        description="Prefix for raw table names. Read only by a source kind that declares it; a legacy kind refuses it.",
    )
    sources: List[str] = Field(
        default_factory=list, 
        description="A list of table names this pipeline extracts."
    )


class DltPipelineSchema(BaseModel):
    """The root schema for the 'DltPipelineComponent' definition."""
    
    # E.g. mssql_source_config
    source_config: Dict[str, Any] = Field(
        description="The source database/credential configuration."
    )
    
    # E.g. snowflake_dest_config
    dest_config: Dict[str, Any] = Field(
        description="The destination system credential configuration."
    )
    
    staging_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional object detailing the staging bucket/filesystem."
    )

    pipelines: Dict[str, DltPipelineGroupSchema] = Field(
        description="A map of distinct pipeline configurations (e.g. fast_refresh, slow_refresh)."
    )
