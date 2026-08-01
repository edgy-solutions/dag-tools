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
# Lineage extraction helpers
# ---------------------------------------------------------------------------

_UP = "urn:li:dataset:(urn:li:dataPlatform:s3,upstream_tbl,PROD)"


class _MetaValue:
    """Stand-in for a Dagster MetadataValue wrapper."""

    def __init__(self, value):
        self.value = value


def test_extract_upstream_urns_handles_every_metadata_shape():
    """get_datahub_metadata writes a LIST of urn strings, but Dagster wraps
    it in a MetadataValue. The old code only handled TextMetadataValue and
    appended the raw .value, nesting a list inside a list."""
    from dag_tools.components.datahub_lineage.component import _extract_upstream_urns

    assert _extract_upstream_urns({"datahub.inputs": _MetaValue([_UP])}) == [_UP]
    assert _extract_upstream_urns({"datahub.inputs": _MetaValue([[_UP]])}) == [_UP]
    assert _extract_upstream_urns({"datahub.inputs": _MetaValue(_UP)}) == [_UP]
    assert _extract_upstream_urns({}) == []
    assert _extract_upstream_urns(None) == []


def test_to_dataset_urns_skips_malformed_without_aborting():
    """One bad URN must not abort the emit for every other asset in the run."""
    from dag_tools.components.datahub_lineage.component import _to_dataset_urns

    urns = _to_dataset_urns([_UP, "not-a-urn", ""])
    assert len(urns) == 1


def test_dataset_lineage_is_constructed_with_the_real_signature():
    """Regression: the extractor called DatasetLineage(upstream_urns=[...]),
    a kwarg that does not exist — raising TypeError on its LAST statement.
    Because the plugin runs the extractor BEFORE generate_dataflow /
    emit_job_run, that killed the entire emit: no DataFlow, no DataJob, no
    DataProcessInstance, and no merged lineage."""
    pytest.importorskip("datahub_dagster_plugin")
    from datahub_dagster_plugin.client.dagster_generator import DatasetLineage

    from dag_tools.components.datahub_lineage.component import _to_dataset_urns

    # The real shape is a NamedTuple of Set[DatasetUrn].
    assert DatasetLineage._fields == ("inputs", "outputs")
    lin = DatasetLineage(inputs=_to_dataset_urns([_UP]), outputs=set())
    assert len(lin.inputs) == 1


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
