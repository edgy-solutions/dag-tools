"""Publish an IO manager to the mesh without reimplementing the protocol.

THE PROTOCOL IS DUCK-TYPED, AND THAT IS DELIBERATE. The domain broker calls
``physical_coordinates(asset_key_path)`` on whatever object sits in
``Definitions(resources=...)`` and checks ``hasattr``. Nothing requires a
dag-tools class. Any IO manager from any package participates by growing that
one method.

WHY THIS MODULE EXISTS. Until now the protocol was only ever *implemented* —
four times, independently, inside ``arrow``, ``duckdb``, ``sql`` and ``delta``.
A third party wanting to publish had to read one of those and copy the parts
that generalise, which is a contract that exists only as an example. That is
precisely how a newer implementation comes to omit a property nobody wrote
down: the defect ADR-0044 exists to close was one implementation of an
unwritten contract disagreeing with another.

So the contract is stated here, once, and the parts that are genuinely
per-manager are the only parts a subclass writes.

    from dag_tools.io_managers.mesh_publishing import MeshPublishable

    class MyArrowIOManager(MeshPublishable, ConfigurableIOManagerFactory):
        def mesh_uri(self, asset_key_path):
            # The ONE thing only this manager knows: where the bytes went.
            return f"{self.uri_base.rstrip('/')}/{'/'.join(asset_key_path)}/"

        def mesh_endpoint(self):
            return self.fs.common.end_point

That is the whole integration. ``physical_coordinates`` comes from the mixin.

WHAT A SUBCLASS MUST NOT DO — return credentials. It cannot: the mixin does not
offer a hook for them, on purpose. Per ADR-0044 the BROKER mints, per request,
scoped to the asset and expiring with the access window. An IO manager runs in a
pipeline pod and knows neither the caller nor the window, and giving every user
deployment minting authority would spread assume-role privilege across the whole
fleet to replace one credential with dozens. Advertising and authorising are
different jobs.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

# source_type values the cortex data client knows how to dispatch on. A ticket
# naming anything else is unreadable, so it is refused here rather than
# advertised and discovered at read time by a consumer.
KNOWN_SOURCE_TYPES = frozenset(
    {"s3_parquet", "s3_delta", "s3_iceberg", "postgres", "clickhouse"}
)

_OBJECT_STORE_TYPES = frozenset({"s3_parquet", "s3_delta", "s3_iceberg"})


class MeshPublishable:
    """Mixin supplying ``physical_coordinates`` from a few small hooks.

    Override ``mesh_uri``; override the rest only when they apply.
    """

    # ── the one required hook ───────────────────────────────────────────────
    def mesh_uri(self, asset_key_path: Sequence[str]) -> Optional[str]:
        """Where this asset's bytes actually are, or None to not advertise.

        RETURNING None IS A FIRST-CLASS ANSWER, not a failure. An asset on the
        Dagster pod's local disk, or in a format no consumer can read, has no
        location another process can use — and an advertised-but-unreadable
        location is worse than an unadvertised asset, because the gateway will
        route consumers to it with full confidence.

        THE URI MUST BE WHERE THE WRITER WROTE, not where the asset key
        suggests. When something other than this IO manager performs the write
        — a dlt pipeline with its own filesystem destination, say — deriving
        the path from the asset key is a guess that will be wrong in a way
        nothing detects until a read returns nothing.

        For a DIRECTORY of part files the trailing slash is load-bearing:
        ``scan_parquet`` treats a slash-less path as an object key and the HEAD
        404s against real object storage, while working fine on a local
        filesystem — invisible until deployment.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement mesh_uri() to publish to the mesh"
        )

    # ── optional hooks ──────────────────────────────────────────────────────
    def mesh_source_type(self, asset_key_path: Sequence[str]) -> str:
        """Which read path the consumer should take. Default: parquet on S3."""
        return "s3_parquet"

    def mesh_endpoint(self) -> Optional[str]:
        """Object-store endpoint, if not AWS.

        MUST BE RESOLVABLE FROM WHEREVER THE CONSUMER RUNS. A bare Kubernetes
        service name resolves only inside this deployment's namespace, so a
        notebook or agent elsewhere cannot read the asset. Use the
        ``.svc.cluster.local`` form. The broker reports violations in
        ``/health.adr0044.non_fqdn_hosts``.
        """
        return None

    def mesh_region(self) -> Optional[str]:
        return "us-east-1"

    def mesh_scope(self, asset_key_path: Sequence[str]) -> Optional[Dict[str, str]]:
        """Bucket/prefix a minted credential must be confined to.

        Default None — the broker derives it from the URI, which is right for
        the ordinary layout. Override when this manager knows something the URI
        does not, such as a dataset spanning prefixes.
        """
        return None

    def mesh_extra(self, asset_key_path: Sequence[str]) -> Dict[str, Any]:
        """Non-secret coordinates a specific backend needs.

        ``s3_iceberg`` requires ``catalog_uri`` and ``table_identifier``;
        database source types carry ``database``. Never credentials.
        """
        return {}

    # ── the protocol ────────────────────────────────────────────────────────
    def physical_coordinates(
        self, asset_key_path: Sequence[str]
    ) -> Optional[Dict[str, Any]]:
        """The routing ticket the broker advertises. Do not override."""
        path = list(asset_key_path or [])
        if not path:
            # Nothing sensible to advertise; guessing produces a dangling route.
            return None

        uri = self.mesh_uri(path)
        if not uri:
            return None

        source_type = self.mesh_source_type(path)
        if source_type not in KNOWN_SOURCE_TYPES:
            # Refused here rather than advertised: the client dispatches on
            # this value and raises ValueError on an unknown one, so the
            # failure would otherwise land in the consumer's process with no
            # indication of which producer caused it.
            return None

        ticket: Dict[str, Any] = {
            "source_type": source_type,
            "physical_uri": uri,
        }

        if source_type in _OBJECT_STORE_TYPES:
            ticket["mode"] = "mint-sts"
            scope = self.mesh_scope(path) or _scope_from_s3_uri(uri)
            if scope:
                ticket["scope"] = scope
            endpoint = self.mesh_endpoint()
            if endpoint:
                ticket["endpoint_url"] = endpoint
            region = self.mesh_region()
            if region:
                ticket["region"] = region
        else:
            # No minter exists for the database backends yet (ADR-0044's
            # capability matrix). Declared as unprotected rather than dressed
            # up as minted, so the broker can count it as live exposure instead
            # of passing it over silently.
            ticket["mode"] = "producer-credential-unprotected"

        ticket.update(self.mesh_extra(path) or {})
        return ticket


def _scope_from_s3_uri(uri: str) -> Optional[Dict[str, str]]:
    if not uri.startswith("s3://"):
        return None
    bucket, _, prefix = uri[len("s3://"):].partition("/")
    if not bucket:
        return None
    return {"bucket": bucket, "prefix": prefix.strip("/")}
