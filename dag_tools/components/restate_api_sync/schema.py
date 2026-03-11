from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RestateApiSyncGroupSchema(BaseModel):
    """Schema for a specific DLT pipeline configuration that triggers Restate API ACKs per row."""
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
    sources: List[str] = Field(
        default_factory=list, 
        description="A list of table names this pipeline extracts."
    )
    primary_key: str = Field(
        description="The primary key column sent to Restate for acknowledgment and API mapping."
    )
    api_path: str = Field(
        description="The specific API path suffix for this table's payload (e.g., '/v1/orders')."
    )

class RestateApiSyncSchema(BaseModel):
    """The root schema for the 'RestateApiSyncComponent' definition."""
    
    source_config: Dict[str, Any] = Field(
        description="The source database/credential configuration."
    )
    
    dest_config: Dict[str, Any] = Field(
        description="The destination system credential configuration."
    )
    
    staging_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional object detailing the staging bucket/filesystem."
    )

    restate_endpoint: str = Field(
        description="The HTTP endpoint for the generic Restate service to send rows to."
    )

    pipelines: Dict[str, RestateApiSyncGroupSchema] = Field(
        description="A map of distinct pipeline configurations targeting the Restate handler."
    )
