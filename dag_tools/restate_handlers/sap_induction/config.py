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

class SapInductionSettings(BaseSettings):
    odata: ODataConfig
    entity_sets: EntitySetsConfig = Field(..., alias="entitySets")
    fields: FieldMappingConfig
    function_import: FunctionImportConfig = Field(..., alias="functionImport")

    class Config:
        env_prefix = "SAP_INDUCTION_"
        env_nested_delimiter = "__"
        populate_by_name = True
