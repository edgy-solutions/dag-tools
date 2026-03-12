from typing import Dict, Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

class ODataAuthConfig(BaseModel):
    username: str
    password: str

class ODataConfig(BaseModel):
    base_url: str = Field(..., alias="baseUrl")
    auth: ODataAuthConfig

class EntitySetsConfig(BaseModel):
    material_search: str = Field(..., alias="materialSearch")
    quotation_header: str = Field(..., alias="quotationHeader")
    quotation_item: str = Field(..., alias="quotationItem")

class FieldMappingConfig(BaseModel):
    part_number: str = Field(..., alias="partNumber")
    material: str = Field(..., alias="material")
    quotation_key: str = Field(..., alias="quotationKey")
    item_key: str = Field(..., alias="itemKey")
    serial_number: str = Field(..., alias="serialNumber")

class FunctionImportParamsConfig(BaseModel):
    quotation: str
    quotation_item: str = Field(..., alias="quotationItem")
    material_number: str = Field(..., alias="materialNumber")
    serial_number: str = Field(..., alias="serialNumber")

class FunctionImportConfig(BaseModel):
    name: str
    parameters: FunctionImportParamsConfig

class SourceSystemColumnsConfig(BaseModel):
    status: str
    status_timestamp: str = Field(..., alias="statusTimestamp")
    error_message: str = Field(..., alias="errorMessage")
    sap_document: str = Field(..., alias="sapDocument")
    last_attempt: str = Field(..., alias="lastAttempt")
    retry_count: str = Field(..., alias="retryCount")

class SourceSystemConfig(BaseModel):
    table: str
    columns: SourceSystemColumnsConfig

class ScheduleConfig(BaseModel):
    max_error_retries: int = Field(5, alias="maxErrorRetries")
    alert_email: str = Field(..., alias="alertEmail")

class CallbackApiConfig(BaseModel):
    base_url: str = Field(..., alias="baseUrl")
    auth_header: Optional[str] = Field(None, alias="authHeader")

class SapInductionSettings(BaseSettings):
    odata: ODataConfig
    entity_sets: EntitySetsConfig = Field(..., alias="entitySets")
    fields: FieldMappingConfig
    function_import: FunctionImportConfig = Field(..., alias="functionImport")
    source_system: SourceSystemConfig = Field(..., alias="sourceSystem")
    schedule: ScheduleConfig
    callback_api: CallbackApiConfig = Field(..., alias="callbackApi")

    class Config:
        env_prefix = "SAP_INDUCTION_"
        env_nested_delimiter = "__"
        populate_by_name = True
