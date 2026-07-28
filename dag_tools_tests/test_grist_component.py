"""Tests for GristIngestComponent.build_defs + sensor evaluation."""
import pytest

pytest.importorskip("pandas")
pytest.importorskip("connectorx")

from dagster import build_sensor_context

from dag_tools.components.grist_ingest.component import GristIngestComponent
from dag_tools.resources.grist import GristResource


def _component(name="crm"):
    return GristIngestComponent(
        grist={"host": "grist.example.com", "org": "myorg", "token": "tok"},
        postgres={
            "protocol": "postgresql",
            "host": "pg",
            "port": 5432,
            "database": "analytics",
            "schema": "grist",
            "username": "u",
            "password": "p",
        },
        name=name,
    )


class _FakeClient:
    def __init__(self, docs, tables):
        self._docs, self._tables = docs, tables

    def list_docs(self, since=None):
        return [d for d in self._docs if not since or d.get("updatedAt", "") > since]

    def list_tables(self, doc_id):
        return self._tables.get(doc_id, [])


# ---------------------------------------------------------------------------
# build_defs shape
# ---------------------------------------------------------------------------


def test_build_defs_produces_asset_sensor_job_resources():
    defs = _component().build_defs(None)

    asset_keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "crm_ingest" in asset_keys

    sensor_names = {s.name for s in defs.sensors}
    assert "crm_sensor" in sensor_names

    job_names = {j.name for j in defs.jobs}
    assert "crm_ingest_job" in job_names

    # Both the grist resource and the SQL IO manager are wired.
    assert "crm_grist_resource" in defs.resources
    assert "crm_sql_io_manager" in defs.resources


def test_ingest_asset_is_dynamic_partitioned():
    defs = _component().build_defs(None)
    ad = next(iter(defs.assets))
    pd_def = ad.partitions_def
    assert pd_def is not None
    assert pd_def.name == "crm_tables"


# ---------------------------------------------------------------------------
# sensor evaluation
# ---------------------------------------------------------------------------


def test_sensor_emits_friendly_partitions_and_run_config(monkeypatch):
    client = _FakeClient(
        docs=[{"id": "abc123", "name": "Quarterly Budget",
               "workspace": "Finance", "updatedAt": "2026-02-01T00:00:00Z"}],
        tables={"abc123": [{"id": "Line_Items"}]},
    )
    monkeypatch.setattr(GristResource, "get_client", lambda self: client)

    defs = _component().build_defs(None)
    sensor_def = next(s for s in defs.sensors if s.name == "crm_sensor")
    ctx = build_sensor_context(cursor=None)
    result = sensor_def(ctx)

    run_requests = list(result.run_requests)
    assert len(run_requests) == 1
    rr = run_requests[0]
    # Friendly partition key = workspace__doc__table (normalized).
    assert rr.partition_key == "finance__quarterly_budget__line_items"
    # Opaque Grist ids ride in run config, keyed under the asset op name.
    cfg = rr.run_config["ops"]["crm_ingest"]["config"]
    assert cfg == {"doc_id": "abc123", "table_id": "Line_Items"}
    # New partition registered under the dynamic partitions def.
    add = result.dynamic_partitions_requests[0]
    assert add.partitions_def_name == "crm_tables"
    assert "finance__quarterly_budget__line_items" in add.partition_keys
    # Cursor advanced to the newest doc's updatedAt.
    assert ctx.cursor == "2026-02-01T00:00:00Z"


def test_sensor_skips_when_no_new_docs(monkeypatch):
    monkeypatch.setattr(GristResource, "get_client", lambda self: _FakeClient([], {}))
    defs = _component().build_defs(None)
    sensor_def = next(s for s in defs.sensors if s.name == "crm_sensor")
    result = sensor_def(build_sensor_context())
    # SkipReason (no run requests).
    assert not getattr(result, "run_requests", None)
