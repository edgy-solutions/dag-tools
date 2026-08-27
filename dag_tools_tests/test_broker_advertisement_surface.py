"""The mesh-publishing protocol must live on the object the broker reads.

``dag_tools.domain_broker`` resolves an asset's IO manager with
``defs.resources.get(io_manager_key)`` and then checks that object for a
``physical_coordinates`` method. What sits in ``Definitions(resources=)``
is the *Configurable* factory, not the inner IO manager — so a manager
that implements the protocol only on its inner class is never asked, and
every one of its assets falls through to the broker's placeholder ticket
(``bucket: my-data-lake`` / ``host: db.local``). Those resolve to nothing.

That is exactly what had happened to the Delta and SQL managers, while
the broker's own docstring claimed they used the real path. Nothing
failed — the assets registered, the gateway routed, and consumers got
tickets pointing at data that was never there.

This file pins the surface for every producing manager at once, so a new
one cannot repeat it.
"""
import pytest

pytest.importorskip("pyarrow")


def _factories():
    """(label, factory) for every mesh-publishing IO manager."""
    out = []

    try:
        from dag_tools.io_managers.arrow import (
            ConfigurableArrowIOManager,
            S3FSCommonConfig as ArrowCommon,
            S3FSConfig as ArrowS3,
        )

        out.append((
            "arrow",
            ConfigurableArrowIOManager(
                uri_base="s3://lake/arrow",
                fs=ArrowS3(
                    common=ArrowCommon(
                        access_key_id="key",
                        secret_access_key="secret",
                        end_point="http://minio:9000",
                        region="us-east-1",
                        allow_http=True,
                    )
                ),
            ),
        ))
    except ImportError:
        pass

    try:
        from dag_tools.io_managers.duckdb import ConfigurableDuckDBIOManager
        from dag_tools.resources.duckdb import DuckDBResource

        out.append((
            "duckdb",
            ConfigurableDuckDBIOManager(
                duckdb=DuckDBResource(
                    aws_access_key_id="key",
                    aws_secret_access_key="secret",
                    endpoint_url="http://minio:9000",
                ),
                uri_base="s3://lake/duck",
            ),
        ))
    except ImportError:
        pass

    try:
        from dag_tools.io_managers.delta import (
            ConfigurableDeltaIOManager,
            S3FSCommonConfig as DeltaCommon,
            S3FSConfig as DeltaS3,
        )

        out.append((
            "delta",
            ConfigurableDeltaIOManager(
                uri_base="s3://lake/delta",
                fs=DeltaS3(
                    common=DeltaCommon(
                        access_key_id="key",
                        secret_access_key="secret",
                        end_point="http://minio:9000",
                        region="us-east-1",
                        allow_http=True,
                    )
                ),
            ),
        ))
    except ImportError:
        pass

    try:
        from dag_tools.io_managers.sql import ConfigurableSQLIOManager, SQLConfig

        out.append((
            "sql",
            ConfigurableSQLIOManager(
                config=SQLConfig(
                    protocol="postgres",
                    host="pg.internal",
                    port=5432,
                    username="u",
                    password="p",
                    database="db",
                )
            ),
        ))
    except ImportError:
        pass

    return out


FACTORIES = _factories()


def test_at_least_one_factory_was_built():
    """Guard against this whole file silently degrading to zero cases."""
    assert FACTORIES, "no IO manager factories could be constructed"


@pytest.mark.parametrize("label,factory", FACTORIES, ids=[f[0] for f in _factories()])
def test_factory_exposes_the_protocol(label, factory):
    """The broker calls hasattr() on THIS object, not the inner manager."""
    assert hasattr(factory, "physical_coordinates"), (
        f"{label}: broker reads the factory from Definitions(resources=), "
        f"so a protocol only on the inner manager is never called"
    )


@pytest.mark.parametrize("label,factory", FACTORIES, ids=[f[0] for f in _factories()])
def test_factory_returns_a_client_readable_ticket(label, factory):
    """Shape the cortex data client can actually dispatch on."""
    ticket = factory.physical_coordinates(["sales", "orders"])
    assert ticket is not None, f"{label}: advertised nothing for an S3/db config"
    assert ticket["source_type"] in {
        "s3_parquet",
        "s3_delta",
        "s3_iceberg",
        "postgres",
        "clickhouse",
    }, f"{label}: source_type {ticket['source_type']!r} has no client read path"
    assert ticket["physical_uri"], f"{label}: empty physical_uri"

    # ADR-0044. This assertion used to read:
    #
    #     assert ticket["credentials"], "no credentials — unreadable by a consumer"
    #
    # which encoded the defect as the requirement. "Unreadable by a consumer"
    # was true only because the consumer was expected to read with the
    # PRODUCER'S WRITING KEY. A ticket is readable when the BROKER mints
    # against its coordinates — so the correct assertion is the opposite one.
    #
    # It is also why the defect survived a green suite: every ticket test
    # asserted on what a producer PRODUCED, never on what a consumer RECEIVED.
    # See test_broker_mints_ticket_credentials.py for the other side of the seam.
    if ticket.get("mode") == "producer-credential-unprotected":
        # Backends with no minter yet (postgres, clickhouse) still echo, and
        # the broker reports them as live exposure. Explicitly declared, so it
        # cannot be mistaken for an oversight.
        assert ticket["credentials"], f"{label}: unprotected backend must still be readable"
    else:
        assert "credentials" not in ticket, (
            f"{label}: advertised a producer credential. The broker mints per "
            f"request (ADR-0044); a producer cannot know the caller or window."
        )
        assert ticket.get("scope"), f"{label}: no scope for the broker to mint against"


@pytest.mark.parametrize("label,factory", FACTORIES, ids=[f[0] for f in _factories()])
def test_empty_asset_key_is_not_advertised(label, factory):
    """Nothing sensible to advertise; guessing produces a dangling route."""
    try:
        ticket = factory.physical_coordinates([])
    except Exception:
        # Raising is acceptable — the broker catches and degrades.
        return
    if ticket is not None:
        assert ticket["physical_uri"].strip("/"), (
            f"{label}: advertised an empty location"
        )


def test_broker_would_pick_up_every_factory():
    """End-to-end against the broker's own resolution logic rather than a
    restatement of it.

    Needs the [broker] extra. The parametrized tests above assert the same
    surface without it, so this skipping does not leave the property
    unchecked -- which matters, because a silently-skipping test is what
    let the Delta write path rot in the first place.
    """
    pytest.importorskip("fastapi", reason="dag-tools[broker] extra not installed")
    from dag_tools.domain_broker.main import _build_asset_info_from_record

    class Record:
        asset_key = ["sales", "orders"]
        io_manager_key = "io_manager"
        io_manager_family = None
        io_manager_class = None
        tags = {}
        urn = None

    for label, factory in FACTORIES:
        info = _build_asset_info_from_record(Record(), io_manager=factory)
        assert "_routing_ticket" in info, (
            f"{label}: broker fell back to the placeholder ticket instead of "
            f"using physical_coordinates"
        )
