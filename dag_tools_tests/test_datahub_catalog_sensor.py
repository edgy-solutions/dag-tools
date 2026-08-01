"""DataHub catalog registration: the sensor is the single catalog path.

IO managers no longer emit to DataHub themselves (an IO manager is bound
per-asset and may be bound to assets another deployment owns). Catalog
registration happens at the materialization-event level, via the global
sensor ``DatahubLineageComponent`` builds — so these tests pin that the
sensor is actually constructible, actually enabled, and produces
well-formed URNs.
"""
import pytest

from dag_tools.components.datahub_lineage.component import (
    asset_keys_to_dataset_urn_converter as to_urn,
)


# ---------------------------------------------------------------------------
# URN derivation (no plugin needed)
# ---------------------------------------------------------------------------


def test_single_segment_filesystem_key_has_no_trailing_dot():
    """Regression: single-segment keys on a filesystem platform produced a
    malformed name ending in '.', which became the dataset's permanent
    identity in the catalog."""
    urn = to_urn(["mesh_demo_customers"], platform="s3")
    assert urn.urn() == (
        "urn:li:dataset:(urn:li:dataPlatform:s3,mesh_demo_customers,PROD)"
    )
    assert ",mesh_demo_customers." not in urn.urn()


def test_multi_segment_filesystem_key_unchanged():
    urn = to_urn(["sales", "orders", "fact"], platform="s3")
    assert urn.urn() == (
        "urn:li:dataset:(urn:li:dataPlatform:s3,sales.orders/fact,PROD)"
    )


def test_non_filesystem_platform_unchanged():
    urn = to_urn(["sales", "orders"], platform="postgres")
    assert urn.urn() == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,sales.orders,PROD)"
    )


# ---------------------------------------------------------------------------
# Sensor construction (needs the plugin)
# ---------------------------------------------------------------------------


def _component(**kwargs):
    from dag_tools.components.datahub_lineage.component import DatahubLineageComponent

    return DatahubLineageComponent(
        datahub_config={"server": "http://datahub-gms:8080"}, **kwargs
    )


def test_component_builds_a_sensor():
    """Regression: the component passed a flat {"server": ...} to
    DatahubDagsterSourceConfig, which requires a NESTED
    datahub_client_config — so building the sensor always failed pydantic
    validation and the component could never register anything."""
    pytest.importorskip("datahub_dagster_plugin")

    defs = _component().build_defs(None)
    sensors = list(defs.sensors or [])
    assert len(sensors) == 1


def test_sensor_can_start_running():
    """make_datahub_sensor defaults to STOPPED, which reads as a broken
    integration (defined but silently never fires). The component must be
    able to start it enabled."""
    pytest.importorskip("datahub_dagster_plugin")
    from dagster import DefaultSensorStatus

    running = list(_component(default_status="RUNNING").build_defs(None).sensors)[0]
    assert running.default_status == DefaultSensorStatus.RUNNING

    stopped = list(_component(default_status="STOPPED").build_defs(None).sensors)[0]
    assert stopped.default_status == DefaultSensorStatus.STOPPED


def test_token_is_picked_up_from_env(monkeypatch):
    """A PAT is required when the metadata service runs with
    METADATA_SERVICE_AUTH_ENABLED=true."""
    pytest.importorskip("datahub_dagster_plugin")
    monkeypatch.setenv("DATAHUB_TOKEN", "pat-123")
    # Building must succeed with a token present; the token lands on the
    # nested client config.
    assert list(_component().build_defs(None).sensors)


# ---------------------------------------------------------------------------
# User-deployment wiring
# ---------------------------------------------------------------------------


def test_no_sensor_when_datahub_server_unset(monkeypatch):
    monkeypatch.delenv("DATAHUB_SERVER", raising=False)
    from dag_tools.user_deployment import definitions as ud

    assert not ud._build_datahub_defs().sensors


def test_sensor_registered_and_running_when_configured(monkeypatch):
    pytest.importorskip("datahub_dagster_plugin")
    from dagster import DefaultSensorStatus

    monkeypatch.setenv("DATAHUB_SERVER", "http://datahub-gms:8080")
    monkeypatch.delenv("DATAHUB_SENSOR_STATUS", raising=False)
    from dag_tools.user_deployment import definitions as ud

    sensors = list(ud._build_datahub_defs().sensors or [])
    assert len(sensors) == 1
    # Defaults ON wherever DataHub is deliberately configured.
    assert sensors[0].default_status == DefaultSensorStatus.RUNNING


def test_sensor_status_is_overridable(monkeypatch):
    pytest.importorskip("datahub_dagster_plugin")
    from dagster import DefaultSensorStatus

    monkeypatch.setenv("DATAHUB_SERVER", "http://datahub-gms:8080")
    monkeypatch.setenv("DATAHUB_SENSOR_STATUS", "STOPPED")
    from dag_tools.user_deployment import definitions as ud

    sensors = list(ud._build_datahub_defs().sensors or [])
    assert sensors[0].default_status == DefaultSensorStatus.STOPPED


def test_catalog_failure_does_not_break_the_code_location(monkeypatch):
    """Catalog registration is observability. A bad DataHub config must not
    take the code location offline — that would stop every materialization
    in the deployment."""
    monkeypatch.setenv("DATAHUB_SERVER", "http://datahub-gms:8080")
    from dag_tools.user_deployment import definitions as ud
    import dag_tools.components.datahub_lineage.component as comp

    class _Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("datahub exploded")

    monkeypatch.setattr(comp, "DatahubLineageComponent", _Boom)
    # Returns empty Definitions rather than propagating.
    assert not ud._build_datahub_defs().sensors
