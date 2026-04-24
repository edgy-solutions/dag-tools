import httpx
import polars as pl
from typing import Dict, Any

class CortexDataClient:
    """
    The Universal Data Client (The Data Plane).
    Shared library used by JupyterHub users, Dagster IO Managers, and AI Agents to actually touch the data.
    """
    
    def __init__(self, broker_url: str, jwt_token: str):
        # broker_url is the Central Gateway URL
        self.gateway_url = broker_url.rstrip("/")
        self.jwt_token = jwt_token

    def get_dataframe(self, asset_key_or_urn: str) -> pl.LazyFrame:
        """
        Retrieves a Polars LazyFrame for the requested asset by first obtaining
        a routing ticket from the Central Gateway.
        """
        # 1. HTTP POST to the Central Gateway's /authorize endpoint using the JWT.
        headers = {
            "Authorization": f"Bearer {self.jwt_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{self.gateway_url}/api/v1/assets/{asset_key_or_urn}/authorize"
        
        with httpx.Client() as client:
            response = client.post(url, headers=headers, timeout=10.0)
            response.raise_for_status()
            ticket = response.json()
            
        # 2. Parse the returned ticket.
        source_type = ticket.get("source_type")
        physical_uri = ticket.get("physical_uri")
        credentials = ticket.get("credentials", {})
        
        # 3. Branch based on source_type
        if source_type == "s3_parquet":
            storage_options = {
                "aws_access_key_id": credentials.get("aws_access_key_id", ""),
                "aws_secret_access_key": credentials.get("aws_secret_access_key", ""),
                "aws_session_token": credentials.get("aws_session_token", "")
            }
            return pl.scan_parquet(physical_uri, storage_options=storage_options)
            
        elif source_type == "s3_delta":
            storage_options = {
                "aws_access_key_id": credentials.get("aws_access_key_id", ""),
                "aws_secret_access_key": credentials.get("aws_secret_access_key", ""),
                "aws_session_token": credentials.get("aws_session_token", "")
            }
            return pl.scan_delta(physical_uri, storage_options=storage_options)
            
        elif source_type == "s3_iceberg":
            # Note: The agent may need to use pyiceberg depending on the exact Polars version.
            # Polars native scan_iceberg is evolving.
            return pl.scan_iceberg(physical_uri)
            
        elif source_type == "postgres":
            # Parse physical_uri: postgres://host:port/schema/table
            parts = physical_uri.replace("postgres://", "").split("/")
            host_port = parts[0]
            schema = parts[1] if len(parts) > 1 else "public"
            table = parts[2] if len(parts) > 2 else asset_key_or_urn
            
            # Use adbc_driver_postgresql. Construct the URI using the PG18 OAUTHBEARER pattern
            # passing the JWT as the password.
            username = credentials.get("username", "oauth")
            db_name = credentials.get("database", "postgres")
            
            adbc_uri = f"postgresql://{username}:{self.jwt_token}@{host_port}/{db_name}"
            query = f"SELECT * FROM {schema}.{table}"
            
            # pl.read_database returns a DataFrame; convert to LazyFrame to match signature
            df = pl.read_database(query, connection=adbc_uri, engine="adbc")
            return df.lazy()
            
        elif source_type == "clickhouse":
            # Parse physical_uri: clickhouse://host:port/schema/table
            parts = physical_uri.replace("clickhouse://", "").split("/")
            host_port = parts[0]
            schema = parts[1] if len(parts) > 1 else "default"
            table = parts[2] if len(parts) > 2 else asset_key_or_urn
            
            username = credentials.get("username", "default")
            password = credentials.get("token", self.jwt_token)
            
            clickhouse_uri = f"clickhouse://{username}:{password}@{host_port}/{schema}"
            query = f"SELECT * FROM {schema}.{table}"
            
            df = pl.read_database(query, connection=clickhouse_uri, engine="adbc")
            return df.lazy()
            
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")
