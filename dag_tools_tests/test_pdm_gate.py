"""The start marker is a gate, and a gate that is never released is worse
than no gate at all.

The source polls the control table and holds off updating the data while
our start marker stands. That makes two things correctness rather than
tidiness:

  * the marker must land before a single row is read, or the read happens
    unprotected;
  * something must release it on EVERY ending, because only the success
    path writes the completion row and nothing on the source side clears
    it.

The second is why the aborted status is required rather than optional:
half a lock is a deadlock.
"""
import pathlib

import pytest

pytest.importorskip("dagster_dlt")

from dagster import AssetKey

from dag_tools.components.restate_dlt_sync.config import ControlTableSpec

import dag_tools_tests.test_pdm_component_build as build


GATED = {
    "name": "PDM_CONTROL",
    "status_column": "LOAD_STATUS",
    "started_value": "STARTED",
    "completed_value": "COMPLETED",
    "consumer_started_value": "RUNNING",
    "consumer_done_value": "CONSUMED",
    "consumer_aborted_value": "ABORTED",
    "load_type_column": "LOAD_TYPE",
    "timestamp_column": "LOAD_TS",
}

STARTED = "pdm_load_started"


def _gated(**overrides):
    control = {**GATED, **overrides.pop("control", {})}
    return build._component(
        pipeline={"control_table": control, **overrides}
    ).build_defs(None)


def _specs(defs):
    return {
        s.key: s
        for a in defs.assets
        for s in (getattr(a, "specs", None) or [])
    }


# ---------------------------------------------------------------------------
# Claiming the gate before reading
# ---------------------------------------------------------------------------


def test_the_start_marker_asset_exists():
    keys = build._keys(_gated())
    assert STARTED in keys, keys


def test_no_start_marker_when_the_gate_is_not_configured():
    """Leaving consumer_started_value unset must write nothing at all --
    an unasked-for marker would gate a source that is not expecting one."""
    plain = {k: v for k, v in GATED.items()
             if k not in ("consumer_started_value", "consumer_aborted_value")}
    keys = build._keys(build._component(
        pipeline={"control_table": plain}
    ).build_defs(None))
    assert STARTED not in keys, keys


def test_every_extraction_asset_waits_for_the_marker():
    """The load-bearing assertion. One extraction asset without this dep
    is one table read before the source was told to hold off."""
    specs = _specs(_gated())
    extraction = [k for k in specs if k.path[0] == "dlt"]
    assert extraction, "no extraction assets found"
    for key in extraction:
        deps = {"/".join(d.asset_key.path) for d in specs[key].deps}
        assert STARTED in deps, f"{'/'.join(key.path)} does not wait: {deps}"


def test_the_marker_itself_has_no_upstream():
    """It has to be able to run first."""
    specs = _specs(_gated())
    assert not specs[AssetKey([STARTED])].deps


def test_the_gate_does_not_break_the_per_table_ack_mapping():
    """The dep is injected into the same specs the per-table mapping reads.
    Computed before injection for exactly this reason -- otherwise every
    dispatch would pick up the gate as a candidate upstream."""
    defs = _gated()
    specs = _specs(defs)
    for table in build.TABLES:
        deps = {
            "/".join(d.asset_key.path)
            for d in specs[AssetKey([f"pdm_{table}_ack_dispatch"])].deps
        }
        assert len(deps) == 1, f"{table} dispatch: {deps}"
        assert STARTED not in deps, deps


def test_the_cycle_job_includes_the_marker():
    """Outside the job it would never run, and the extraction would block
    forever waiting on an asset nothing materializes."""
    job = next(j for j in _gated().jobs if j.name == "pdm_cycle_job")
    assert f'key:"{STARTED}"' in str(job.selection), str(job.selection)


# ---------------------------------------------------------------------------
# Releasing it
# ---------------------------------------------------------------------------


def test_an_abort_sensor_is_generated():
    assert "pdm_abort_sensor" in {s.name for s in _gated().sensors}


def test_no_abort_sensor_without_a_gate():
    plain = {k: v for k, v in GATED.items()
             if k not in ("consumer_started_value", "consumer_aborted_value")}
    names = {s.name for s in build._component(
        pipeline={"control_table": plain}
    ).build_defs(None).sensors}
    assert "pdm_abort_sensor" not in names, names


def test_a_gate_without_a_release_is_refused():
    """Half a lock is a deadlock: the success path writes the completion
    row, and nothing else would ever clear the marker."""
    with pytest.raises(ValueError, match="consumer_aborted_value"):
        ControlTableSpec.model_validate(
            {k: v for k, v in GATED.items() if k != "consumer_aborted_value"}
        )


# ---------------------------------------------------------------------------
# Status values have to stay distinguishable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", [
    "consumer_started_value", "consumer_aborted_value",
])
def test_our_status_cannot_collide_with_the_sources(field):
    """The two sides read the same column. A shared value means one reads
    the other's marker as its own."""
    with pytest.raises(ValueError, match="the SOURCE writes"):
        ControlTableSpec.model_validate({**GATED, field: GATED["completed_value"]})


def test_the_completion_collision_keeps_its_original_message():
    """consumer_done_value colliding with completed_value is caught by the
    older, more specific check -- it describes the exact consequence (the
    sensor never settles), which is more useful than the generic one."""
    with pytest.raises(ValueError, match="sensor re-fires forever"):
        ControlTableSpec.model_validate(
            {**GATED, "consumer_done_value": GATED["completed_value"]}
        )


def test_our_own_statuses_cannot_collide_with_each_other():
    with pytest.raises(ValueError, match="must be distinct"):
        ControlTableSpec.model_validate(
            {**GATED, "consumer_aborted_value": GATED["consumer_done_value"]}
        )


# ---------------------------------------------------------------------------
# Endpoint naming
# ---------------------------------------------------------------------------


def test_one_endpoint_serves_every_control_status():
    """start, done and abort are the same handler with different
    configured values, so there is one endpoint field and every asset that
    writes a control row reads it from the same place.

    It was two fields briefly, and the deprecated one shipped a live bug:
    the completion asset read the OLD attribute directly instead of the
    accessor, so a config setting only the new name left that asset with
    an empty URL. One field cannot drift from itself."""
    defs = build._component(
        pipeline={"control_table": GATED},
        control_status_endpoint="http://restate:8080/Svc/write_status",
    ).build_defs(None)
    assert STARTED in build._keys(defs)


def test_a_control_table_with_no_endpoint_is_refused():
    with pytest.raises(ValueError, match="control_status_endpoint"):
        build._component(
            pipeline={"control_table": GATED}, control_status_endpoint="",
        ).build_defs(None)


# ---------------------------------------------------------------------------
# The gate must ORDER, not merely appear to
# ---------------------------------------------------------------------------
#
# Reported from a live run: the marker asset and the extraction op started
# at the SAME TIME, and the tables showed "unsynced" because the marker had
# a newer materialization -- in the same run that materialized them all.
#
# The cause: the dep was added after the fact with
# AssetsDefinition.map_asset_specs, which rewrites the DECLARED deps -- so
# the asset graph and the UI show the edge, which is why the dependencies
# "looked good" -- while leaving keys_by_input_name EMPTY. With no input
# mapped to the asset key, Dagster has no edge to order on and the
# extraction op starts immediately.
#
# Measured before the fix: the dependent op started 0.41s BEFORE a gate
# that slept 0.4s. The marker is a LOCK; rows read before it lands are read
# unprotected, so this was the gate not gating.


def _extraction_assets(defs):
    return [
        a for a in defs.assets
        if any(k.path[0] == "dlt" for k in getattr(a, "keys", []))
    ]


def test_the_gate_is_wired_as_a_real_op_input():
    """keys_by_input_name is what Dagster orders on. A declared dep with no
    input mapping is lineage without execution, which is exactly the shape
    that shipped."""
    defs = _gated()
    extraction = _extraction_assets(defs)
    assert extraction, "no extraction assets found"

    for assets_def in extraction:
        mapped = {
            "/".join(key.path) for key in assets_def.keys_by_input_name.values()
        }
        assert STARTED in mapped, (
            f"the gate is not among this op's mapped inputs {mapped or '{}'} "
            f"-- it will not order execution"
        )


def test_every_extraction_op_input_resolves_to_an_asset_key():
    """The failing shape had an input NAMED for the gate and mapped to
    nothing. An input without a key is inert."""
    for assets_def in _extraction_assets(_gated()):
        input_names = {i.name for i in assets_def.node_def.input_defs}
        mapped_names = set(assets_def.keys_by_input_name)
        assert input_names <= mapped_names, (
            f"inputs {input_names - mapped_names} are declared but mapped to "
            f"no asset key, so nothing orders on them"
        )


def test_no_gate_means_no_phantom_input():
    """Without the gate configured the extraction must not acquire an
    input for an asset that does not exist."""
    plain = {k: v for k, v in GATED.items()
             if k not in ("consumer_started_value", "consumer_aborted_value")}
    defs = build._component(pipeline={"control_table": plain}).build_defs(None)
    for assets_def in _extraction_assets(defs):
        mapped = {"/".join(k.path) for k in assets_def.keys_by_input_name.values()}
        assert STARTED not in mapped, mapped


# ---------------------------------------------------------------------------
# Every control-row asset reads the SAME configured endpoint
# ---------------------------------------------------------------------------


def test_no_control_asset_holds_an_empty_endpoint():
    """The shipped bug: the completion asset read a deprecated attribute
    directly instead of the accessor, so config setting only the current
    name left that one asset with an empty URL -- and it failed at
    materialization, not at load."""
    import re

    source = pathlib.Path(
        "dag_tools/components/restate_dlt_sync/component.py"
    ).read_text(encoding="utf-8")

    stale = re.findall(r"self\.load_complete_endpoint", source)
    assert not stale, (
        "load_complete_endpoint is referenced again; it was removed so that "
        "one field cannot drift from itself"
    )

    endpoints = set(re.findall(r"endpoint = (self\.[a-z_]+)", source))
    assert endpoints, "no endpoint assignments found -- has the shape changed?"
    # mei_request_endpoint is a genuinely different handler; everything
    # else that writes a control row must read the one control field.
    assert endpoints <= {
        "self.control_status_endpoint", "self.mei_request_endpoint",
    }, endpoints
    assert "self.control_status_endpoint" in endpoints
