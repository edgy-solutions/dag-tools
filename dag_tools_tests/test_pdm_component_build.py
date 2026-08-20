"""What RestateDltSyncComponent actually generates for a PDM pipeline.

The unit tests around it exercise the handlers and the handshake rule in
isolation. This builds real Definitions from a config shaped like the
twelve-table PDM load and asserts the wiring, because the failure mode of
a generator is not an exception -- it is an asset that quietly does not
exist, or a job whose selection is missing the step that closes the loop.
"""
import pytest

pytest.importorskip("dagster_dlt")

from dagster import AssetKey

from dag_tools.components.restate_dlt_sync.component import RestateDltSyncComponent


INGRESS = "http://restate:8080"

TABLES = ["PDM_PART", "PDM_BOM", "PDM_ROUTING"]


def _component(**overrides):
    pipeline = {
        "name": "oracle_pdm_to_postgres",
        "dest_schema": "pdm_raw",
        "sources": list(TABLES),
        "table_config": {
            "PDM_PART": {"primary_key": "PART_ID", "cursor": "LAST_MODIFIED"},
            "PDM_BOM": {"primary_key": "BOM_ID", "cursor": "CHANGED_ON"},
            "PDM_ROUTING": {"primary_key": "ROUTE_ID", "cursor": "CHANGED_ON"},
        },
        "control_table": {
            "name": "PDM_CONTROL",
            "status_column": "LOAD_STATUS",
            "started_value": "STARTED",
            "completed_value": "COMPLETED",
            "consumer_done_value": "CONSUMED",
            "load_type_column": "LOAD_TYPE",
            "timestamp_column": "LOAD_TS",
        },
        "mei_table": {
            "name": "PDM_MEI_REQUEST",
            "mei_column": "MEI_NUMBER",
            "meis": ["MEI-1", "MEI-2"],
        },
        "cycle_sensor": {"enabled": True, "interval_seconds": 60},
        "pipeline_kwargs": {"write_disposition": "merge"},
    }
    pipeline.update(overrides.pop("pipeline", {}))
    kwargs = dict(
        mei_request_endpoint=f"{INGRESS}/GenericOracleControlService/write_mei_request/send",
        load_complete_endpoint=f"{INGRESS}/GenericOracleControlService/signal_load_complete/send",
    )
    kwargs.update(overrides)
    return RestateDltSyncComponent(
        source_config={
            "type": "sql_database",
            "drivername": "oracle+oracledb",
            "credentials": "sqlite://",
            "database": "FREEPDB1",
            "schema": "PDM",
        },
        dest_config={
            "type": "postgres",
            "credentials": "postgresql://u:p@h/db",
            "database": "pdm_local",
            "schema": "pdm_raw",
        },
        restate_endpoint=f"{INGRESS}/GenericOracleAckService/mark_as_processed/send",
        pipelines={"pdm": pipeline},
        **kwargs,
    )


@pytest.fixture(scope="module")
def defs():
    return _component().build_defs(None)


def _keys(defs):
    return {"/".join(k.path) for a in defs.assets for k in getattr(a, "keys", [])}


def test_every_source_table_gets_an_ack_dispatch(defs):
    names = _keys(defs)
    for table in TABLES:
        assert f"pdm_{table}_ack_dispatch" in names, names


def test_the_mei_request_asset_exists(defs):
    """The write that starts a transaction. Without it PDM is never asked
    for anything and the MEI-scoped tables stay empty."""
    assert "pdm_mei_request" in _keys(defs)


def test_the_completion_asset_exists(defs):
    assert "pdm_load_complete" in _keys(defs)


def _deps_of(defs, key_name):
    holder = next(
        a for a in defs.assets if AssetKey([key_name]) in getattr(a, "keys", [])
    )
    spec = next(s for s in holder.specs if s.key == AssetKey([key_name]))
    return {"/".join(d.asset_key.path) for d in spec.deps}


def test_each_ack_dispatch_depends_only_on_its_own_table(defs):
    """create_dlt_assets returns ONE multi_asset covering every table, so
    its .keys is the whole set. Handing that set to each dispatch made
    every dispatch depend on every table -- a complete bipartite graph at
    a dozen tables, and lineage claiming PDM_ROUTING's acknowledgment is
    derived from PDM_BOM's data.

    Each output spec carries exactly one dep (the external stub for the
    table it came from), which is what makes the per-table mapping
    recoverable.
    """
    expected = {
        "PDM_PART": "dlt/db/pdm_raw/pdm_part",
        "PDM_BOM": "dlt/db/pdm_raw/pdm_bom",
        "PDM_ROUTING": "dlt/db/pdm_raw/pdm_routing",
    }
    for table, dlt_key in expected.items():
        deps = _deps_of(defs, f"pdm_{table}_ack_dispatch")
        assert deps == {dlt_key}, f"{table} dispatch depends on {deps}"


def test_completion_still_waits_for_every_table(defs):
    """Narrowing the dispatch deps must not narrow this one: the
    completion row means "all twelve tables landed"."""
    deps = _deps_of(defs, "pdm_load_complete")
    assert deps == {f"pdm_{t}_ack_dispatch" for t in TABLES}, deps


def test_completion_depends_on_every_ack(defs):
    """It must mean "all twelve tables landed", not "the first one did".
    A completion row written early tells PDM we are done with data we
    have not read."""
    complete = next(
        a for a in defs.assets
        if AssetKey(["pdm_load_complete"]) in getattr(a, "keys", [])
    )
    spec = next(s for s in complete.specs if s.key == AssetKey(["pdm_load_complete"]))
    deps = {"/".join(d.asset_key.path) for d in spec.deps}
    for table in TABLES:
        assert f"pdm_{table}_ack_dispatch" in deps, deps


def test_mei_request_is_a_separate_job_from_the_cycle(defs):
    """PDM needs time between being asked and being ready. If the request
    and the extraction shared a job, the extract would run against tables
    PDM has not filled yet."""
    job_names = {j.name for j in defs.jobs}
    assert {"pdm_mei_request_job", "pdm_cycle_job"} <= job_names, job_names


def test_the_cycle_job_ends_with_the_completion_step(defs):
    """define_asset_job leaves an UnresolvedAssetJobDefinition, whose
    selection is an expression rather than a resolved key set -- resolving
    it needs the asset graph, so the expression is what we read."""
    job = next(j for j in defs.jobs if j.name == "pdm_cycle_job")
    selection = str(job.selection)
    assert 'key:"pdm_load_complete"' in selection, selection
    for table in TABLES:
        assert f'key:"pdm_{table}_ack_dispatch"' in selection, selection
    # The extraction itself, not just the bookkeeping around it.
    assert "dlt/db/pdm_raw/pdm_part" in selection, selection


def test_the_mei_request_job_holds_only_the_request(defs):
    """It must not drag the extraction along: PDM has not been given time
    to fill anything at the moment the request is written."""
    job = next(j for j in defs.jobs if j.name == "pdm_mei_request_job")
    selection = str(job.selection)
    assert 'key:"pdm_mei_request"' in selection, selection
    assert "ack_dispatch" not in selection, selection
    assert "load_complete" not in selection, selection


def test_the_cycle_sensor_is_generated(defs):
    assert "pdm_cycle_sensor" in {s.name for s in defs.sensors}


def test_no_overlay_sensor_without_an_overlay_file(defs):
    """An inline MEI list cannot change underneath us, so watching it
    would be a sensor that can never fire."""
    assert "pdm_mei_overlay_sensor" not in {s.name for s in defs.sensors}


def test_overlay_file_adds_a_watcher(tmp_path):
    overlay = tmp_path / "meis.yaml"
    overlay.write_text("- MEI-1\n")
    d = _component(pipeline={"mei_table": {
        "name": "PDM_MEI_REQUEST",
        "mei_column": "MEI_NUMBER",
        "source_file": str(overlay),
    }}).build_defs(None)
    assert "pdm_mei_overlay_sensor" in {s.name for s in d.sensors}


# ---------------------------------------------------------------------------
# Config errors that must surface at load, not at 3am
# ---------------------------------------------------------------------------


def test_mei_table_without_an_endpoint_is_refused():
    with pytest.raises(ValueError, match="mei_request_endpoint"):
        _component(mei_request_endpoint="").build_defs(None)


def test_control_table_without_an_endpoint_is_refused():
    with pytest.raises(ValueError, match="load_complete_endpoint"):
        _component(load_complete_endpoint="").build_defs(None)


def test_table_config_for_an_unlisted_table_is_refused():
    """Silently-unused index and cursor config reads as configured when
    it is not -- and on a twelve-table pipeline nobody would notice."""
    with pytest.raises(ValueError, match="not in sources"):
        _component(pipeline={"table_config": {
            "PDM_PART": {"primary_key": "PART_ID"},
            "PDM_TYPO": {"primary_key": "X"},
        }}).build_defs(None)


def test_a_table_with_no_primary_key_anywhere_is_refused():
    with pytest.raises(ValueError, match="no primary_key given"):
        _component(pipeline={"table_config": {
            "PDM_PART": {"primary_key": "PART_ID"},
        }}).build_defs(None)


def test_a_composite_key_cannot_be_acked():
    """The ack UPDATE filters on one column, so a composite index has no
    valid IN list. Better to say so at load than to build a broken
    UPDATE against a live Oracle."""
    with pytest.raises(ValueError, match="composite primary_key"):
        _component(pipeline={"table_config": {
            "PDM_PART": {"primary_key": ["A", "B"], "cursor": "TS"},
            "PDM_BOM": {"primary_key": "BOM_ID"},
            "PDM_ROUTING": {"primary_key": "ROUTE_ID"},
        }}).build_defs(None)


def test_a_pipeline_level_primary_key_still_works():
    """Back-compat: the pre-existing single-key config must keep loading."""
    d = _component(pipeline={
        "table_config": {},
        "primary_key": "PART_ID",
    }).build_defs(None)
    for table in TABLES:
        assert f"pdm_{table}_ack_dispatch" in _keys(d)
