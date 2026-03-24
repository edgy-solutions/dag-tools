from typing import Dict, Optional, Any
from sqlalchemy.engine.url import URL
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

class DatabaseConfig(BaseModel):
    host: str = Field(..., alias="host")
    port: int = Field(5432, alias="port")
    username: str = Field(..., alias="username")
    password: str = Field(..., alias="password")
    database: str = Field(..., alias="database")
    driver: str = Field("postgresql", alias="driver")
    query: Dict[str, Any] = Field(default_factory=dict, alias="query")

    @property
    def dsn(self) -> str:
        """Constructs a safe DSN string using SQLAlchemy's URL builder."""
        url = URL.create(
            drivername=self.driver,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=self.query,
        )
        return url.render_as_string(hide_password=False)

class ODataAuthConfig(BaseModel):
    username: str
    password: str

class ODataConfig(BaseModel):
    base_url: str = Field(..., alias="baseUrl")
    auth: ODataAuthConfig

class EntitySetsConfig(BaseModel):
    material_search: str = Field("MaterialSearchSet", alias="materialSearch")
    quotation_header: str = Field("QuotationHeaderSet", alias="quotationHeader")
    quotation_item: str = Field("QuotationItemSet", alias="quotationItem")

class FieldMappingConfig(BaseModel):
    part_number: str = Field("part_number", alias="partNumber")
    material: str = Field("Material", alias="material")
    quotation_key: str = Field("Quotation", alias="quotationKey")
    item_key: str = Field("Item", alias="itemKey")
    serial_number: str = Field("serial_number", alias="serialNumber")
    retry_count: str = Field("retry_count", alias="retryCount")

class FunctionImportParamsConfig(BaseModel):
    quotation: str = Field("Quotation")
    quotation_item: str = Field("QuotationItem", alias="quotationItem")
    material_number: str = Field("MaterialNumber", alias="materialNumber")
    serial_number: str = Field("SerialNumber", alias="serialNumber")

class FunctionImportConfig(BaseModel):
    name: str = Field("Z_SAP_INDUCTION_FUNC")
    parameters: FunctionImportParamsConfig = Field(default_factory=FunctionImportParamsConfig)

class SourceSystemColumnsConfig(BaseModel):
    status: str = Field("status")
    status_timestamp: str = Field("status_timestamp", alias="statusTimestamp")
    error_message: str = Field("error_message", alias="errorMessage")
    sap_document: str = Field("sap_document", alias="sapDocument")
    last_attempt: str = Field("last_attempt", alias="lastAttempt")
    retry_count: str = Field("retry_count", alias="retryCount")

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
    database: DatabaseConfig = Field(..., alias="database")

    class Config:
        env_prefix = "SAP_INDUCTION_"
        env_nested_delimiter = "__"
        populate_by_name = True
