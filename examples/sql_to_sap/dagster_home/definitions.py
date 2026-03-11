"""Dagster definitions for the sql_to_sap example.

Instantiates the RestateApiSyncComponent directly from Python attributes,
bypassing the ComponentTree (which conflicts with this non-standard project 
structure where definitions.py lives inside the defs module root).
"""
from dagster.components.core.load_defs import build_defs_for_component
from dag_tools.components.restate_api_sync import RestateApiSyncComponent

component = RestateApiSyncComponent(
    restate_endpoint="http://localhost:8083/GenericApiSyncService/process_record/send",
    source_config={
        "drivername": "mssql+pyodbc",
        "database": "INTERNAL_ERP",
        "schema": "dbo",
        "host": "localhost",
        "port": 1433,
        "username": "sa",
        "password": "Password123!",
    },
    dest_config={
        "drivername": "postgresql",
        "database": "staging_db",
        "schema": "sap_buffer",
        "host": "localhost",
        "port": 5433,
        "username": "admin",
        "password": "password",
    },
    pipelines={
        "sap_api_dispatch": {
            "primary_key": "PO_NUMBER",
            "api_path": "/sap/v1/orders",
            "sources": ["PURCHASE_ORDERS"],
        }
    },
)

defs = build_defs_for_component(component)
