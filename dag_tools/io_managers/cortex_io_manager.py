"""Cortex Polars IO manager — the mesh's READ facade.

This module ships the Dagster ``IOManager`` that mesh-consuming assets
use to read data. On ``load_input`` it delegates to
``CortexDataClient.get_dataframe(urn)`` — which goes through the central
gateway to look up the asset's physical location, then dispatches to
the right backend reader (Parquet / Delta / Iceberg / PostgreSQL /
ClickHouse).

**This IO manager is read-only.** ``handle_output`` deliberately raises.

Why: Dagster loads an asset's input using the IO manager of the
*upstream* asset that produced it. So this manager is attached to the
assets you want to READ — including foreign assets declared as
external stubs, whose data lives in another deployment entirely. That
makes it a consumer-side marker, and it must never be mistaken for a
statement of ownership.

Earlier revisions bolted producer-side behavior onto it — writing
parquet, emitting the URN to DataHub, and implementing
``physical_coordinates`` so the domain broker would advertise the
asset through the gateway. Both announcements encoded "this deployment
owns this asset", which is exactly wrong for a read facade: attach it
to a foreign stub and your broker would advertise
``s3://<your-bucket>/<your-prefix>/<their-asset>.parquet`` — a path you
never wrote — then compete with the real owner for the same routing
key. Those behaviors are gone.

To PUBLISH an asset to the mesh, use a producer IO manager that
implements ``physical_coordinates`` honestly:
:class:`~dag_tools.io_managers.arrow.ConfigurableArrowIOManager`
(parquet on S3), :class:`~dag_tools.io_managers.delta.*`, or
:class:`~dag_tools.io_managers.sql.ConfigurableSQLIOManager`.

Catalog registration (DataHub) is handled globally by
:class:`~dag_tools.components.datahub_lineage.DatahubLineageComponent`,
an ``ASSET_MATERIALIZATION`` sensor — not per-IO-manager.
"""

from typing import Any, Optional

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)
from pydantic import Field

# polars and the cortex data client are LAZILY imported inside the
# methods that need them. Importing them at module load forced the
# 60s Dagster gRPC ``launch_run`` budget to overflow when this IO
# manager landed in a slim user-deployment image: ``import
# dag_tools.user_deployment.definitions`` clocked at ~42s, and the
# subprocess-spawn handshake couldn't complete inside the budget,
# producing reproducible ``DagsterUserCodeUnreachableError`` even
# though the gRPC server was reachable.
#
# Lazy-loading these heavy deps keeps definitions-load fast (the
# common case: enumerating assets for the broker's URN-registration
# loop and the daemon's run-launch RPC). The runtime cost only fires
# when an asset materializes (handle_output) or reads an upstream
# (load_input) — exactly when polars / CortexDataClient are actually
# needed.
#
# See ``[[dag-tools-grpc-import-timeout]]`` for the full diagnosis
# and the alternative fix options (raising the gRPC timeout,
# demoting heavy base deps to optional extras); this is Option A.
#
# When future code reaches for ``CortexDataClient`` here, move it inside
# the method body or accept the import-cost regression.


class CortexPolarsIOManager(ConfigurableIOManager):
    """Read-only Dagster IO manager that fetches mesh data through the gateway.

    Reads: the asset is fetched through ``CortexDataClient``, so the
    consuming compute function gets a Polars LazyFrame regardless of
    whether the data actually lives on S3 parquet, Delta, Iceberg,
    PostgreSQL, or ClickHouse — and regardless of which deployment owns
    it.

    Writes: **unsupported.** ``handle_output`` raises. See the module
    docstring for why, and for which IO managers to use instead when you
    want to publish an asset to the mesh.

    Typical use — declare the foreign asset you want to read as an
    external stub and bind it to this IO manager::

        foreign = AssetSpec(
            key="other_domain_sales",
            metadata={"dagster/io_manager_key": "cortex"},
        )

        @asset
        def my_report(other_domain_sales):   # loaded via THIS manager
            ...
    """

    s3_bucket: str = Field(
        default="",
        description=(
            "Deprecated / unused — retained so existing configs keep "
            "validating. This manager no longer writes."
        ),
    )
    prefix: str = Field(
        default="",
        description="Deprecated / unused — this manager no longer writes.",
    )
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

        # Lazy import — see module-level note on import discipline.
        from dag_tools.cortex_data.client import CortexDataClient

        client = CortexDataClient(
            broker_url=self.broker_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
            keycloak_url=self.keycloak_url,
        )
        return client.get_dataframe(urn)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Always raises — this IO manager is a read-only mesh facade.

        Writing here is a category error. Dagster loads an input using the
        IO manager of the asset that PRODUCED it, so this manager is bound
        to assets you want to read — often external stubs for data another
        deployment owns. Letting it write (and, historically, announce that
        write to DataHub and the domain broker) made a consumer-side marker
        masquerade as a claim of ownership, so a deployment would advertise
        routing for assets it does not own.

        Publish with an IO manager that owns its output and can advertise
        it truthfully via ``physical_coordinates``:

          * ``ConfigurableArrowIOManager`` — parquet on S3
          * ``ConfigurableSQLIOManager``   — PostgreSQL / ClickHouse
          * the Delta IO manager           — Delta Lake on S3
        """
        raise NotImplementedError(
            f"CortexPolarsIOManager is READ-ONLY and cannot materialize "
            f"{context.asset_key.to_user_string() if context.has_asset_key else 'this output'!r}. "
            "It exists to READ mesh data through the central gateway; assets bound "
            "to it may be owned by another deployment, so writing (and advertising "
            "that write) here would claim ownership this deployment does not have. "
            "To publish an asset, use ConfigurableArrowIOManager (parquet on S3), "
            "ConfigurableSQLIOManager (postgres/clickhouse), or the Delta IO manager."
        )
