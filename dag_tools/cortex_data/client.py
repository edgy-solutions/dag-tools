import os
import httpx
import polars as pl
from typing import Dict, Any, Optional

class CortexDataClient:
    """
    The Universal Data Client (The Data Plane).
    Shared library used by JupyterHub users, Dagster IO Managers, and AI Agents to actually touch the data.
    """
    
    def __init__(
        self, 
        broker_url: Optional[str] = None, 
        jwt_token: Optional[str] = None, 
        client_id: Optional[str] = None, 
        client_secret: Optional[str] = None, 
        keycloak_url: Optional[str] = None
    ):
        # 1. Resolve Broker URL (Central Gateway)
        resolved_broker = broker_url or os.getenv("CORTEX_BROKER_URL")
        if not resolved_broker:
            raise ValueError("Must provide broker_url or set CORTEX_BROKER_URL environment variable.")
        self.gateway_url = resolved_broker.rstrip("/")

        # 2. Resolve Authentication
        self.jwt_token = jwt_token or os.getenv("MESH_DEV_TOKEN")
        self.client_id = client_id or os.getenv("CORTEX_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("CORTEX_CLIENT_SECRET")
        self.keycloak_url = keycloak_url or os.getenv("KEYCLOAK_TOKEN_URL", "http://keycloak/realms/master/protocol/openid-connect/token")

        if not self.jwt_token and self.client_id and self.client_secret:
            self._fetch_m2m_token()
        elif not self.jwt_token:
            raise ValueError("Must provide either jwt_token (MESH_DEV_TOKEN) or M2M credentials (CORTEX_CLIENT_ID/SECRET).")

    def _fetch_m2m_token(self):
        """Fetches a short-lived Service Account JWT using client_credentials grant."""
        with httpx.Client() as client:
            response = client.post(
                self.keycloak_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                },
                timeout=10.0
            )
            response.raise_for_status()
            self.jwt_token = response.json().get("access_token")

    def get_dataframe(self, urn: str) -> pl.LazyFrame:
        """
        Retrieves a Polars LazyFrame for the requested asset by first obtaining
        a routing ticket from the Central Gateway.
        """
        # 1. HTTP POST to the Central Gateway's /authorize endpoint using the JWT.
        headers = {
            "Authorization": f"Bearer {self.jwt_token}",
            "Content-Type": "application/json"
        }
        
        url = f"{self.gateway_url}/api/v1/assets/{urn}/authorize"
        
        with httpx.Client() as client:
            response = client.post(url, headers=headers, timeout=10.0)
            response.raise_for_status()
            ticket = response.json()
            
        # 2. Parse the returned ticket.
        source_type = ticket.get("source_type")
        physical_uri = ticket.get("physical_uri")
        credentials = ticket.get("credentials", {})
        
        # 3. Branch based on source_type
        lf = None
        apply_security = True
        
        if source_type == "s3_parquet":
            storage_options = {
                "aws_access_key_id": credentials.get("aws_access_key_id", ""),
                "aws_secret_access_key": credentials.get("aws_secret_access_key", ""),
                "aws_session_token": credentials.get("aws_session_token", "")
            }
            lf = pl.scan_parquet(physical_uri, storage_options=storage_options)
            
        elif source_type == "s3_delta":
            storage_options = {
                "aws_access_key_id": credentials.get("aws_access_key_id", ""),
                "aws_secret_access_key": credentials.get("aws_secret_access_key", ""),
                "aws_session_token": credentials.get("aws_session_token", "")
            }
            lf = pl.scan_delta(physical_uri, storage_options=storage_options)
            
        elif source_type == "s3_iceberg":
            # Note: The agent may need to use pyiceberg depending on the exact Polars version.
            # Polars native scan_iceberg is evolving.
            lf = pl.scan_iceberg(physical_uri)
            
        elif source_type == "postgres":
            # Parse physical_uri: postgres://host:port/schema/table
            parts = physical_uri.replace("postgres://", "").split("/")
            host_port = parts[0]
            schema = parts[1] if len(parts) > 1 else "public"
            table = parts[2] if len(parts) > 2 else (urn.split(",")[-2] if "urn:li:dataset" in urn else urn)

            # The intended pattern was PG18 OAUTHBEARER passing the JWT as the
            # bearer token. libpq's OAUTHBEARER has no Python API to inject a
            # pre-existing JWT (only device flow or PQsetAuthDataHook in C),
            # and ADBC postgres doesn't expose the auth hook to Python. So
            # honor an explicit credentials.password if the broker provides
            # one (the sandbox path), and only fall back to the JWT pattern
            # when something downstream actually wires it up.
            username = credentials.get("username", "postgres")
            db_name = credentials.get("database", "postgres")
            password = credentials.get("password") or self.jwt_token

            adbc_uri = f"postgresql://{username}:{password}@{host_port}/{db_name}"
            query = f"SELECT * FROM {schema}.{table}"

            df = pl.read_database(query, connection=adbc_uri, engine="adbc")
            lf = df.lazy()
            apply_security = False  # Handled natively by Postgres RLS/CLS
            
        elif source_type == "clickhouse":
            # Parse physical_uri: clickhouse://host:port/schema/table
            parts = physical_uri.replace("clickhouse://", "").split("/")
            host_port = parts[0]
            schema = parts[1] if len(parts) > 1 else "default"
            table = parts[2] if len(parts) > 2 else (urn.split(",")[-2] if "urn:li:dataset" in urn else urn)
            
            username = credentials.get("username", "default")
            password = credentials.get("token", self.jwt_token)
            
            clickhouse_uri = f"clickhouse://{username}:{password}@{host_port}/{schema}"
            query = f"SELECT * FROM {schema}.{table}"
            
            df = pl.read_database(query, connection=clickhouse_uri, engine="adbc")
            lf = df.lazy()
            
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")
            
        if apply_security:
            allowed_columns = ticket.get("allowed_columns")
            row_filters = ticket.get("row_filters")
            
            if allowed_columns:
                lf = lf.select(allowed_columns)
                
            if row_filters:
                lf = lf.filter(pl.sql_expr(row_filters))
                
        return lf
