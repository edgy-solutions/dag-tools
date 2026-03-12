-- sap_outbox.sql
-- This model cleans raw data from the dlt extraction and initializes the state machine columns

{{ config(materialized='table', schema='staging') }}

SELECT
    -- Technical IDs and Keys
    CAST("id" AS VARCHAR) as "record_id",
    "PO_NUMBER" as "part_number",                 -- Logical name for SapInductionService
    "SERIAL_NUMBERS" as "serial_number",          -- Multi-value field (handled by service)
    
    -- Initialize State Machine Columns
    'NEW' as "status",                            -- Initial state
    CURRENT_TIMESTAMP as "status_timestamp",
    NULL as "error_message",
    NULL as "sap_document",
    NULL as "last_attempt",
    0 as "retry_count"                            -- Starting from zero

FROM {{ source('raw_sap_data', 'vw_sap_source_data') }}
