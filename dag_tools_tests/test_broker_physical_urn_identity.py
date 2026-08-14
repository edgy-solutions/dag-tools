"""One table, one identity — the broker's routing key must equal the catalog's URN.

WHAT WENT WRONG. The broker took its Redis routing key from ``record.urn``, whose
derivation forces ``platform="dagster"``. That argument does not only set the platform
segment: the converter picks the NAME LAYOUT from it, and ``dagster`` is not in
``FILESYSTEM_PLATFORMS``, so it takes the ``".".join(asset_key)`` branch. One asset key
therefore produced two irreconcilable identities:

    registered   ...(dagster, minio-svc.publog-lake.publog.p_cage, PROD)   ← broker/Redis
    catalogued   ...(s3,      minio-svc.publog-lake/publog/p_cage, PROD)   ← DataHub

The dotted form is not merely spelled differently — it destroys the boundary between
platform instance, bucket and key prefix. That boundary is load-bearing: one S3 path on
two servers is two different tables, and only the instance segment distinguishes them.

Observed at work 2026-08-14. Engine D resolved the catalogued URN, the gateway looked it
up, missed, and returned 404 — with a routing table that appeared fully populated and 66
assets registered. Nothing in the system could report the mismatch, because both halves
were internally consistent and each looked correct on its own.

WHY THESE PINS. The invariant is not "the broker emits an s3 URN"; it is "the broker and
the catalog derive the SAME identity from the same asset". Two independent derivations of
one identity drift — that is what happened — so the pin that matters is the agreement
itself (``test_broker_and_catalog_agree_on_identity``), not either side's output.

RELATED, AND DELIBERATELY UNCHANGED: ``test_inventory_urn_derivation.py`` pins
``_derive_urn`` to ``dataPlatform:dagster``. That remains correct — it is the identity for
an asset with NO physical location, which is exactly when the physical derivation here
declines to answer. Note that test's stated rationale ("consumers build URNs with
dataPlatform:dagster") predates the physical-URN convention and no longer describes
consumers of physical assets; the assertion still holds, its justification has aged.
"""
from __future__ import annotations

import types

import pytest

from dag_tools.domain_broker.main import physical_urn_for


PUBLOG_KEY = ["minio-svc", "publog-lake", "publog", "p_cage"]
CATALOGUED = "urn:li:dataset:(urn:li:dataPlatform:s3,minio-svc.publog-lake/publog/p_cage,PROD)"


def _record(asset_key, urn=None, tags=None):
    return types.SimpleNamespace(
        asset_key=list(asset_key), urn=urn, tags=tags or {},
        io_manager_key="io_manager", io_manager_family="s3_parquet",
        io_manager_class="DuckDBIOManager",
    )


def _io_manager(source_type="s3_parquet", uri="s3://publog-lake/publog/p_cage/"):
    """An IO manager implementing the mesh-publishing protocol."""
    return types.SimpleNamespace(
        physical_coordinates=lambda key: (
            None if source_type is None
            else {"source_type": source_type, "physical_uri": uri, "credentials": {}}
        )
    )


# ---------------------------------------------------------------------------
# The invariant
# ---------------------------------------------------------------------------
def test_broker_and_catalog_agree_on_identity():
    """THE PIN THAT MATTERS. Not "the broker emits s3" — that is a symptom of the real
    property, which is that both derivations land on the same string. Anything that makes
    them disagree (a platform argument, a layout list, a mapping table) fails here."""
    datahub = pytest.importorskip(
        "dag_tools.components.datahub_lineage.component",
        reason="datahub plugin absent — the converter under test cannot be imported",
    )
    from dag_tools.components.datahub_lineage.platforms import (
        FILESYSTEM_PLATFORMS,
        resolve_platform,
    )

    # How the CATALOG names it: the sensor reads the platform the asset declared via
    # `destination_name` and runs it through the shared converter.
    catalog_urn = datahub.asset_keys_to_dataset_urn_converter(
        PUBLOG_KEY,
        platform=resolve_platform("s3_parquet"),
        filesystem_platforms=list(FILESYSTEM_PLATFORMS),
    ).urn()

    # How the BROKER names it, from the routing ticket's source_type — deliberately the
    # same string as `destination_name` (see the SOURCE_TYPE comment in every IO manager).
    broker_urn = physical_urn_for(_record(PUBLOG_KEY), _io_manager())

    assert broker_urn == catalog_urn, (
        "the broker and the catalog derived different identities for one asset — this is "
        "the 404-with-a-populated-routing-table failure"
    )
    assert broker_urn == CATALOGUED, (
        "identity drifted from the URN observed in DataHub at work on 2026-08-14"
    )


# ---------------------------------------------------------------------------
# The specific defect
# ---------------------------------------------------------------------------
def test_the_instance_bucket_key_boundary_survives():
    """The regression pin, stated as the property rather than the string.

    `<instance>.<bucket>/<key...>` keeps the server name recoverable. Collapse it to
    all-dots and `minio-svc.publog-lake.publog.p_cage` cannot be parsed back — you cannot
    tell which segment is the server, and one S3 path on two servers is two tables.
    """
    urn = physical_urn_for(_record(PUBLOG_KEY), _io_manager())
    name = urn.split(",", 1)[1].rsplit(",", 1)[0]

    assert name.startswith("minio-svc."), "the platform instance must lead the name"
    assert "/" in name, (
        "the name collapsed to a dotted identifier — the instance/bucket/key boundary is "
        "gone and the server can no longer be identified"
    )
    assert name == "minio-svc.publog-lake/publog/p_cage"
    assert "dataPlatform:s3," in urn


# ---------------------------------------------------------------------------
# When it must DECLINE — the dagster form is right for assets with no table
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "io_manager, why",
    [
        (None, "no IO manager bound — nothing knows a physical location"),
        (types.SimpleNamespace(), "IO manager does not implement the mesh protocol"),
        (_io_manager(source_type=None), "protocol implemented but declines to advertise"),
        (_io_manager(source_type=""), "no platform declared — do not guess one"),
    ],
)
def test_declines_when_there_is_no_physical_location(io_manager, why):
    """Quoting the rule the sensor owns: "Assets with no physical location (a staging
    step, a source stub) keep a dagster-platform entity, because there is no table to
    point at." Returning None is how this defers to that fallback — it must never invent
    a physical identity for something that has none."""
    assert physical_urn_for(_record(PUBLOG_KEY), io_manager) is None, why


def test_a_local_disk_asset_is_not_advertised_as_physical():
    """The duckdb manager returns None from physical_coordinates for a non-s3 uri_base
    ("Local disk exists on one pod only"), so the broker must not mint an s3 identity for
    it. An advertised-but-unreachable location is worse than an unadvertised asset."""
    assert physical_urn_for(_record(PUBLOG_KEY), _io_manager(source_type=None)) is None


def test_derivation_failure_never_blocks_asset_load():
    """Identity derivation is best-effort. A throwing IO manager degrades to the fallback;
    it must not take the broker's whole asset load down with it."""
    def _boom(_key):
        raise RuntimeError("credentials resource not configured")

    exploding = types.SimpleNamespace(physical_coordinates=_boom)
    assert physical_urn_for(_record(PUBLOG_KEY), exploding) is None


# ---------------------------------------------------------------------------
# Precedence — source-level, because the loop needs a live Dagster Definitions
# ---------------------------------------------------------------------------
def test_an_explicit_datahub_urn_tag_still_wins():
    """Someone who STATED the identity outranks any derivation of it. Pinned at source
    because the surrounding loop requires a real Definitions object to exercise; the
    ordering is the property, and it is visible in the text."""
    import inspect

    from dag_tools.domain_broker import main as broker

    src = inspect.getsource(broker.load_dagster_definitions)
    tag_at = src.index('record.tags.get("datahub/urn")')
    physical_at = src.index("physical_urn_for(record")
    record_urn_at = src.index("urn = record.urn")

    assert tag_at < physical_at < record_urn_at, (
        "identity precedence must be: explicit tag > physical (catalog-agreeing) > "
        "record.urn/dagster fallback"
    )


def test_the_io_manager_is_resolved_before_identity():
    """It used to be fetched AFTER the URN was decided — the one object that knows what
    the asset physically is sat in scope, unused, while the key came from a hardcoded
    platform. Ordering is the fix; pin it so it cannot quietly revert."""
    import inspect

    from dag_tools.domain_broker import main as broker

    src = inspect.getsource(broker.load_dagster_definitions)
    assert src.index("io_manager = resources.get") < src.index("physical_urn_for(record"), (
        "the IO manager must be resolved before identity derivation, which depends on it"
    )
