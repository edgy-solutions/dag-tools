"""Cortex Polars IO manager — the unified mesh-aware IO manager.

This module ships the Dagster ``IOManager`` that every mesh-participating
asset uses as its read-side facade. On ``load_input`` it delegates to
``CortexDataClient.get_dataframe(urn)`` — which goes through the central
gateway to look up the asset's physical location, then dispatches to
the right backend reader (Parquet / Delta / Iceberg / PostgreSQL /
ClickHouse). On ``handle_output`` it writes a Polars DataFrame to S3 as
parquet at a path determined by the IO manager's config plus the asset
key, and emits ``datahub/urn`` + ``physical_uri`` metadata on the
materialization event for downstream lineage tooling.

The IO manager is the consumer-side of the mesh contract: every
Dagster asset using it gets uniform, gateway-mediated reads regardless
of where each upstream asset's physical data actually lives. The
producer-side is ``physical_coordinates``, which a remote domain
broker calls to learn how to advertise each asset through the central
gateway — closing the loop.
"""

import os
from typing import Any, Dict, Optional, Sequence

import polars as pl
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
)
from pydantic import Field

from dag_tools.cortex_data.client import CortexDataClient


# Discriminator the cortex data client uses to pick the parquet read
# path. Kept as a module constant so any change to the wire string
# only happens in one place across the IO manager + the client.
_SOURCE_TYPE = "s3_parquet"


class CortexPolarsIOManager(ConfigurableIOManager):
    """Dagster IO manager that talks to the rest of the mesh through the central gateway.

    Reads: every upstream asset is fetched through ``CortexDataClient``,
    so the asset's compute function gets a Polars LazyFrame regardless
    of whether the upstream actually lives on S3 parquet, Delta, Iceberg,
    PostgreSQL, or ClickHouse.

    Writes: outputs are persisted as parquet at
    ``s3://{s3_bucket}/{prefix}/{asset_key}.parquet`` and the
    materialization event carries the resolved DataHub URN plus the
    physical S3 path as metadata, so lineage tools and the broker can
    pick them up after the fact.

    Mesh publishing: ``physical_coordinates`` returns the routing
    ticket a remote broker uses to advertise this IO manager's assets
    through the central gateway. The broker walks Definitions, calls
    ``physical_coordinates`` per asset, and pushes the resulting URN
    list to the gateway — so consumers using a different
    ``CortexPolarsIOManager`` instance somewhere else can read this
    one's outputs without coordinating directly.
    """

    s3_bucket: str = Field(description="S3 bucket where this IO manager writes asset data.")
    prefix: str = Field(description="S3 prefix (folder path) under the bucket.")
    broker_url: str = Field(description="Central gateway URL the consumer side talks to.")
    client_id: str = Field(description="Machine-to-machine OAuth2 client ID for the cortex data client.")
    client_secret: str = Field(description="Machine-to-machine OAuth2 client secret.")
    keycloak_url: Optional[str] = Field(
        default=None, description="Keycloak token URL (defaults to the in-cluster service)."
    )

    def load_input(self, context: InputContext) -> Any:
        """Fetch an upstream asset through the cortex data client.

        Resolves the upstream URN from materialization metadata when
        available (the upstream's ``handle_output`` emits
        ``datahub/urn``) and falls back to deterministic URN derivation
        from the asset key path when the upstream didn't write one.
        The URN is then handed to the cortex data client, which goes
        through the gateway to fetch the physical data — irrespective
        of which backend stores it.
        """
        urn = None
        if context.upstream_output and context.upstream_output.metadata:
            urn_meta = context.upstream_output.metadata.get("datahub/urn")
            if urn_meta:
                urn = (
                    str(urn_meta.value) if hasattr(urn_meta, "value") else str(urn_meta)
                )
        if not urn:
            key_str = context.asset_key.to_user_string()
            urn = (
                f"urn:li:dataset:(urn:li:dataPlatform:dagster,"
                f"{key_str.replace('/', '.')},PROD)"
            )

        context.log.info(f"Loading input for URN: {urn}")

        client = CortexDataClient(
            broker_url=self.broker_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
            keycloak_url=self.keycloak_url,
        )
        return client.get_dataframe(urn)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Persist a Polars DataFrame as parquet to the configured S3 location.

        The path is fully determined by the IO manager's config plus
        the asset key — so ``physical_coordinates`` can return the same
        path deterministically without needing the asset to have been
        materialized yet. The materialization event also carries
        ``datahub/urn`` and ``physical_uri`` for after-the-fact lineage
        lookups, and if ``DATAHUB_SERVER`` is set the URN is pushed to
        DataHub directly via the metadata REST API.
        """
        if not isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
            raise ValueError(
                f"Expected a Polars DataFrame or LazyFrame, got {type(obj)}"
            )

        s3_path = self._s3_path_for_asset_key(context.asset_key.path)
        context.log.info(f"Writing output to {s3_path}")

        # Polars uses object_store under the hood; the Dagster pod's
        # AWS env (set by the IO manager's deployment) supplies the
        # credentials transparently.
        if isinstance(obj, pl.LazyFrame):
            obj.sink_parquet(s3_path)
        else:
            obj.write_parquet(s3_path)

        key_str = context.asset_key.to_user_string()
        urn = (
            f"urn:li:dataset:(urn:li:dataPlatform:dagster,"
            f"{key_str.replace('/', '.')},PROD)"
        )
        context.add_output_metadata(
            {
                "physical_uri": MetadataValue.path(s3_path),
                "datahub/urn": MetadataValue.text(urn),
            }
        )

        # Emit the URN directly to DataHub if a server is configured.
        # Done in-line with the write rather than via an
        # ASSET_MATERIALIZATION sensor for two reasons: no sensor-lag
        # window where the URN is materialized but unindexed, and no
        # version-skew between dagster and acryl-datahub-dagster-plugin.
        # Failures here are non-fatal — the write already succeeded
        # and the materialization metadata is the durable record.
        self._emit_to_datahub(context, urn, s3_path)

    @staticmethod
    def _emit_to_datahub(
        context: OutputContext, urn: str, physical_uri: str
    ) -> None:
        """Push the materialized URN to DataHub via DataHubGraph.

        ``DATAHUB_SERVER`` env var holds the GMS base URL (not the
        GraphQL endpoint — that's a different layer). When unset, this
        is a no-op so the IO manager works on clusters without DataHub
        installed. Any DataHub-side failure is logged and swallowed —
        the IO manager's job is to write data, and emitting to the
        catalog is best-effort.
        """
        import os
        server = os.environ.get("DATAHUB_SERVER")
        if not server:
            context.log.info(
                "DATAHUB_SERVER unset; skipping DataHub emit for %s", urn
            )
            return
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.ingestion.graph.client import (
                DataHubGraph,
                DatahubClientConfig,
            )
            from datahub.metadata.schema_classes import DatasetPropertiesClass

            graph = DataHubGraph(DatahubClientConfig(server=server))
            asset_name = context.asset_key.to_user_string().replace("/", ".")
            mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetPropertiesClass(
                    name=asset_name,
                    description=(
                        f"Dagster asset materialized to {physical_uri}"
                    ),
                    customProperties={
                        "physical_uri": physical_uri,
                        "dagster_asset_key": asset_name,
                    },
                ),
            )
            graph.emit_mcp(mcp)
            context.log.info(f"Emitted URN to DataHub at {server}: {urn}")
        except Exception as exc:
            # Non-fatal: a DataHub outage shouldn't fail the
            # materialization. Logged at WARNING so it's surfaced in
            # Dagit but doesn't trip alerting on transient blips.
            context.log.warning(
                "DataHub emit failed for %s: %s", urn, exc
            )

    def physical_coordinates(
        self, asset_key_path: Sequence[str]
    ) -> Optional[Dict[str, Any]]:
        """Mesh-publishing protocol — routing ticket for an asset.

        A remote broker registering this IO manager's assets with the
        central gateway calls this method to learn how each asset can
        be read. The returned dict matches the ticket shape the cortex
        data client consumes from the gateway's authorize response:
        ``source_type`` discriminates the read path (``"s3_parquet"``),
        ``physical_uri`` is the s3 URI the IO manager would write to
        on materialization, and ``credentials`` carries the AWS auth
        needed for the parquet read.

        AWS credentials come from the broker pod's environment
        (``AWS_ACCESS_KEY_ID`` etc.) — the broker is expected to be
        deployed with the same MinIO / S3 access the Dagster pod
        would have. ``AWS_ENDPOINT_URL`` is included only when set,
        so a missing override falls back to real AWS.
        """
        physical_uri = self._s3_path_for_asset_key(asset_key_path)
        credentials = self._aws_credentials_from_env()
        return {
            "source_type": _SOURCE_TYPE,
            "physical_uri": physical_uri,
            "credentials": credentials,
        }

    def _s3_path_for_asset_key(self, asset_key_path: Sequence[str]) -> str:
        """Build the s3 path this IO manager would write to for ``asset_key_path``.

        The last segment of the asset key becomes the parquet file
        name; intermediate segments are dropped because they map to
        Dagster's logical grouping, not to a directory layout on S3.
        Keep this method aligned with ``handle_output`` — they must
        produce identical paths or the broker will advertise URIs the
        write path never produced.
        """
        return f"s3://{self.s3_bucket}/{self.prefix}/{asset_key_path[-1]}.parquet"

    @staticmethod
    def _aws_credentials_from_env() -> Dict[str, str]:
        """Build the credentials dict from the pod's AWS environment.

        Matches the keys the cortex data client expects in its
        ``_s3_storage_options`` builder — lowercase boto/polars-style
        names. ``aws_endpoint_url`` is only included when set so
        absence falls through to real AWS rather than failing with a
        bad endpoint.
        """
        creds: Dict[str, str] = {
            "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        }
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        if endpoint:
            creds["aws_endpoint_url"] = endpoint
        return creds
