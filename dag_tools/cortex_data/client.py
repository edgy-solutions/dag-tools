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
        keycloak_url: Optional[str] = None,
        originator_sub: Optional[str] = None,
        originator_email: Optional[str] = None,
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
        # When the caller fetches data on behalf of an end user (rather than
        # for the service itself), pass the originator's Keycloak sub here.
        # The gateway sees this in X-Originator-Sub and applies user-level
        # authz against it instead of the M2M token's sub.
        self.originator_sub = originator_sub
        # The originator's authz_id / ENTITLEMENT KEY (email in sandbox,
        # employee-id at work-deploy). The gateway's can_read topaz check
        # is email-keyed, so it MUST see the end user's email here (via
        # X-Originator-Email) — the M2M token has no user email. Without
        # this the DA-read gate denied everyone (broken-closed).
        self.originator_email = originator_email

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
        if self.originator_sub:
            headers["X-Originator-Sub"] = self.originator_sub
        if self.originator_email:
            headers["X-Originator-Email"] = self.originator_email

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
        
        # Build common S3 storage options. aws_endpoint_url is what makes this
        # work against MinIO / Ceph / any non-AWS S3-compatible store. Without
        # it, the object_store backend tries to resolve s3.amazonaws.com.
        # aws_allow_http permits http://minio endpoints (no TLS in sandbox).
        def _s3_storage_options() -> Dict[str, Any]:
            opts: Dict[str, Any] = {
                "aws_access_key_id": credentials.get("aws_access_key_id", ""),
                "aws_secret_access_key": credentials.get("aws_secret_access_key", ""),
            }
            session_token = credentials.get("aws_session_token") or ""
            if session_token:
                opts["aws_session_token"] = session_token
            endpoint_url = credentials.get("aws_endpoint_url")
            if endpoint_url:
                opts["aws_endpoint_url"] = endpoint_url
                if endpoint_url.startswith("http://"):
                    opts["aws_allow_http"] = "true"
            region = credentials.get("aws_region")
            if region:
                opts["aws_region"] = region
            return opts

        if source_type == "s3_parquet":
            lf = pl.scan_parquet(physical_uri, storage_options=_s3_storage_options())

        elif source_type == "s3_delta":
            lf = pl.scan_delta(physical_uri, storage_options=_s3_storage_options())

        elif source_type == "s3_iceberg":
            # pl.scan_iceberg(uri) only handles Hadoop-style tables
            # (version-hint.text + manual layout). Real catalogs (SQL,
            # REST, Glue) don't write that file. Load the table via
            # pyiceberg.catalog instead, then pass the Table OBJECT to
            # pl.scan_iceberg — polars 1.x accepts both forms.
            from pyiceberg.catalog import load_catalog

            catalog_uri = credentials.get("catalog_uri")
            warehouse = credentials.get("warehouse_uri") or physical_uri
            table_identifier = credentials.get("table_identifier")
            if not catalog_uri or not table_identifier:
                raise ValueError(
                    "s3_iceberg credentials must include catalog_uri and "
                    "table_identifier (e.g. 'sales.customers')."
                )

            catalog_props: Dict[str, str] = {
                "type": credentials.get("catalog_type", "sql"),
                "uri": catalog_uri,
                "warehouse": warehouse,
                "s3.access-key-id": credentials.get("aws_access_key_id", ""),
                "s3.secret-access-key": credentials.get("aws_secret_access_key", ""),
            }
            if credentials.get("aws_endpoint_url"):
                catalog_props["s3.endpoint"] = credentials["aws_endpoint_url"]
            if credentials.get("aws_region"):
                catalog_props["s3.region"] = credentials["aws_region"]

            catalog = load_catalog("sandbox", **catalog_props)
            table = catalog.load_table(table_identifier)
            lf = pl.scan_iceberg(table)
            
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

            # Polars 1.x removed the `engine=` kwarg on read_database; use
            # read_database_uri for URI-based connections (it dispatches to
            # ADBC/ConnectorX automatically based on the URI scheme).
            df = pl.read_database_uri(query, uri=adbc_uri)
            lf = df.lazy()
            apply_security = False  # Handled natively by Postgres RLS/CLS
            
        elif source_type == "clickhouse":
            # Parse physical_uri: clickhouse://host:port/schema/table
            # The port is the HTTP interface (default 8123), not native 9000.
            parts = physical_uri.replace("clickhouse://", "").split("/")
            host_port = parts[0]
            schema = parts[1] if len(parts) > 1 else "default"
            table = parts[2] if len(parts) > 2 else (urn.split(",")[-2] if "urn:li:dataset" in urn else urn)
            host, _, port = host_port.partition(":")
            port_int = int(port) if port else 8123

            username = credentials.get("username", "default")
            # Prefer credentials.password (sandbox), then token (PG18 pattern), then JWT.
            password = credentials.get("password") or credentials.get("token") or self.jwt_token
            db_name = credentials.get("database", schema)

            # connectorx doesn't speak clickhouse — use clickhouse-connect's
            # Arrow path, which polars consumes via pl.from_arrow.
            import clickhouse_connect
            client = clickhouse_connect.get_client(
                host=host,
                port=port_int,
                username=username,
                password=password,
                database=db_name,
            )
            arrow_table = client.query_arrow(f"SELECT * FROM {table}")
            lf = pl.from_arrow(arrow_table).lazy()
            
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
