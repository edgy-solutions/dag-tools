"""`ReadAuditComponent` -- the YAML surface, loaded the way a deployment loads it.

The component is the primary surface for the read audit: a deployment writes
`component.yaml`, not Python and not a CLI invocation. So these tests go
through `build_component_defs` over the real example rather than constructing
the class, which is the only way to catch the failures that matter here --
a field Dagster rejects, a resource key that is never required, an asset the
loader silently drops.

**The compat floor is probed, not assumed.** 1.10.19 -- the version
production runs -- rejects unknown keys in a component file outright and
fails the whole code-location load (`85b3b49`). This module therefore loads
the example on whatever Dagster is installed and, if the load is refused for
that reason, skips while naming the field, the same shape as the otel
automation tests. Anything else that goes wrong still fails loudly.

Every object name in the fixtures is synthetic.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import pytest
from dagster.components import build_component_defs

from dag_tools.components.read_audit.component import (
    build_read_audit_asset,
    windows_from_run_rows,
)
from dag_tools.components.read_audit.schema import HistoryBlock, ReadAuditSpec

EXAMPLE = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "read_audit"
    / "dagster_home"
    / "components"
)


def _skip_if_the_component_file_is_refused(exc: Exception) -> None:
    """Skip when this Dagster will not accept this component file.

    Matched on the raised error rather than on a version number: the field
    the loader rejects is the fact, and a version is only a proxy for it.
    """
    text = _chain_text(exc)
    if "extra_forbidden" in text or "Extra inputs are not permitted" in text:
        pytest.skip(f"this Dagster rejects a key in the component file: {text}")


def _chain_text(exc: BaseException) -> str:
    """Every message in the exception chain, as one string."""
    parts = []
    seen = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _load(components_dir: Path) -> dg.Definitions:
    try:
        return build_component_defs(components_dir)
    except Exception as exc:  # pragma: no cover - only on an old Dagster
        _skip_if_the_component_file_is_refused(exc)
        raise


@pytest.fixture()
def example(tmp_path, monkeypatch) -> Path:
    """A copy of the shipped example, so a test can edit the YAML.

    `build_component_defs` imports the folder as `<tmp dir>.components`,
    exactly as the example's own definitions.py does from dagster_home, so
    the parent has to be importable.
    """
    monkeypatch.syspath_prepend(str(tmp_path.parent))
    target = tmp_path / "components"
    shutil.copytree(EXAMPLE, target, ignore=shutil.ignore_patterns("__pycache__"))
    return target


def test_the_shipped_example_loads(example):
    """The example is part of the contract.

    A component whose one worked example cannot load has never been run --
    which is exactly how the dbt example shipped pointing at two projects
    that did not exist.
    """
    defs = _load(example)
    keys = {
        key.to_user_string()
        for key in defs.resolve_asset_graph().get_all_asset_keys()
    }
    assert keys == {"read_audit"}


def test_the_example_materializes_and_reports_its_gap(example):
    """End to end over the synthetic export, through Dagster.

    The fixture exercises every attribution method on purpose, so a change
    that collapses one of them into another shows up here as a number.
    """
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    assert result.success

    output = result.output_for_node("read_audit")
    assert output["relation_count"] == 9
    # Four objects explained by one asset; the other five are the gap, and
    # the gap is itemised rather than summarised away.
    assert output["explained_relations"] == [
        "db1.sch1.t_alpha",
        "db1.sch1.t_beta",
        "db1.sch2.t_gamma",
        "db2.sch1.t_epsilon",
    ]
    assert len(output["unexplained_relations"]) == 5
    assert output["ambiguous_relations"]
    assert output["untagged_relations"]
    assert output["non_pipeline_relations"]
    assert output["row_failures"] == []

    materialization = result.asset_materializations_for_node("read_audit")[0]
    metadata = materialization.metadata
    assert metadata["objects_read"].value == 9
    assert metadata["explained"].value == 4
    assert metadata["no_asset_explains"].value == 5
    assert metadata["coverage"].value == pytest.approx(4 / 9, abs=1e-4)


def test_the_metadata_decomposes_the_gap(example):
    """One number cannot be acted on.

    An untagged read is closed by deploying the tagged resource; a
    not-a-pipeline read is a brokering decision. Those go to different
    people, so they are published separately.
    """
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    metadata = result.asset_materializations_for_node("read_audit")[0].metadata

    for key in ("gap_ambiguous", "gap_untagged", "gap_not_a_pipeline"):
        assert key in metadata
    total = sum(
        metadata[key].value
        for key in ("gap_ambiguous", "gap_untagged", "gap_not_a_pipeline")
    )
    # The parts may double-count one object read two ways, so they bound the
    # whole from below rather than summing to it exactly.
    assert total >= metadata["no_asset_explains"].value


def test_a_malformed_history_block_fails_the_load_not_the_run(example):
    """A bad block is an operator typo, and it should surface at load.

    Failing at materialization instead means the first sign of it is a failed
    run at whatever hour the schedule fires.
    """
    yaml_path = example / "read_audit" / "component.yaml"
    yaml_path.write_text(
        yaml_path.read_text(encoding="utf-8").replace(
            "rows_file: history_rows.json", "resource_key: warehouse"
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    _skip_if_the_component_file_is_refused(excinfo.value)
    # The loader wraps the cause in a tree of its own, so the validation
    # error is read off the chain rather than off the outermost message.
    assert "relation" in _chain_text(excinfo.value)


# --- the resource seam ------------------------------------------------------


class _RecordingWarehouse(dg.ConfigurableResource):
    """Stands in for the tagged warehouse resource of step 2.

    One method is the whole seam. The component builds the statement; this
    executes it. That is what keeps a driver out of CI and a connection out
    of this repo's tests.
    """

    rows_json: str = "[]"

    def fetch_rows(self, statement, parameters):
        _CALLS.append((statement, parameters))
        return json.loads(self.rows_json)


_CALLS = []


def _spec_through_a_resource(**history_overrides) -> ReadAuditSpec:
    history = {
        "resource_key": "warehouse",
        "relation": "db1.sch1.history_view",
        "columns": {
            "query_id": "c_qid",
            "started_at": "c_start",
            "principal": "c_user",
            "query_tag": "c_tag",
            "relations": "c_objects",
        },
        "lookback_hours": 24,
    }
    history.update(history_overrides)
    return ReadAuditSpec.from_attributes(
        asset_name="read_audit",
        group_name=None,
        io_manager_key=None,
        owners=[],
        tags={},
        history=history,
        runs={},
    )


def test_the_component_executes_nothing_itself(monkeypatch):
    """It hands the statement to the resource, with the values bound.

    The component has no driver dependency and no credential handling, and
    this is where that is pinned: what it produces is SQL plus parameters.
    """
    _CALLS.clear()
    rows = [
        {
            "query_id": "q-1",
            "started_at": "2026-10-02T01:00:00+00:00",
            "principal": "svc_account",
            "query_tag": None,
            "relations": ["db1.sch1.t_a"],
        }
    ]
    asset_def = build_read_audit_asset(_spec_through_a_resource(), None)

    result = dg.materialize(
        [asset_def],
        resources={"warehouse": _RecordingWarehouse(rows_json=json.dumps(rows))},
    )
    assert result.success

    assert len(_CALLS) == 1
    statement, parameters = _CALLS[0]
    assert "db1.sch1.history_view" in statement
    # A closed window, both ends bound: rows landing mid-read are not half
    # included, so two runs over the same window see the same rows.
    assert set(parameters) == {"since", "until"}
    assert isinstance(parameters["since"], datetime)
    assert parameters["until"] > parameters["since"]

    output = result.output_for_node("read_audit")
    assert output["rows_read"] == 1
    assert output["unexplained_relations"] == ["db1.sch1.t_a"]


def test_the_resource_key_is_required_so_the_load_names_a_missing_resource():
    asset_def = build_read_audit_asset(_spec_through_a_resource(), None)
    assert "warehouse" in asset_def.required_resource_keys


def test_the_run_config_overrides_the_configured_lookback():
    _CALLS.clear()
    asset_def = build_read_audit_asset(_spec_through_a_resource(lookback_hours=24), None)

    result = dg.materialize(
        [asset_def],
        resources={"warehouse": _RecordingWarehouse()},
        run_config={"ops": {"read_audit": {"config": {"lookback_hours": 1}}}},
    )
    assert result.success

    _, parameters = _CALLS[0]
    reach = datetime.now(timezone.utc) - parameters["since"]
    assert 0 < reach.total_seconds() <= 2 * 3600


def test_a_resource_without_fetch_rows_fails_with_a_pointed_error():
    class _NotAWarehouse(dg.ConfigurableResource):
        pass

    asset_def = build_read_audit_asset(_spec_through_a_resource(), None)
    result = dg.materialize(
        [asset_def],
        resources={"warehouse": _NotAWarehouse()},
        raise_on_error=False,
    )
    assert not result.success
    failures = [
        event
        for event in result.all_events
        if event.event_type_value == "STEP_FAILURE"
    ]
    assert "fetch_rows" in str(failures[0].event_specific_data.error)


# --- schema validation ------------------------------------------------------


def test_history_demands_exactly_one_topology():
    with pytest.raises(Exception):
        HistoryBlock.model_validate({})
    with pytest.raises(Exception):
        HistoryBlock.model_validate({"rows_file": "x.json", "resource_key": "w"})


def test_reading_through_a_resource_demands_every_column_name():
    """Five names, none of them defaulted.

    A partial block would read the right relation through the wrong columns
    and report an empty window as a clean audit.
    """
    with pytest.raises(Exception) as excinfo:
        HistoryBlock.model_validate(
            {
                "resource_key": "warehouse",
                "relation": "db1.sch1.history_view",
                "columns": {"query_id": "c_qid"},
            }
        )
    message = f"{excinfo.value}"
    for missing in ("started_at", "principal", "query_tag", "relations"):
        assert missing in message


def test_an_unknown_key_in_a_block_is_refused():
    """A typo in YAML must not be ignored into a default."""
    with pytest.raises(Exception):
        HistoryBlock.model_validate({"rows_file": "x.json", "lookbackhours": 24})


# --- run windows from the instance ------------------------------------------


def test_run_rows_become_aware_utc_windows():
    windows = windows_from_run_rows(
        [{"run_id": "r1", "asset_keys": ["a"], "start_time": 1761700000.0, "end_time": 1761700600.0}]
    )
    assert len(windows) == 1
    assert windows[0].started_at.tzinfo is not None
    assert windows[0].ended_at > windows[0].started_at


def test_a_still_running_run_yields_an_open_window():
    windows = windows_from_run_rows(
        [{"run_id": "r1", "asset_keys": ["a"], "start_time": 1761700000.0, "end_time": None}]
    )
    assert windows[0].ended_at is None


def test_a_run_with_no_start_time_is_skipped_not_guessed():
    windows = windows_from_run_rows(
        [{"run_id": "r1", "asset_keys": ["a"], "start_time": None}]
    )
    assert windows == []


def test_a_run_with_no_asset_selection_keeps_its_window_and_explains_nothing():
    """It still covers the time, and covering the time is what makes a read
    ambiguous rather than unattributed -- which is the honest answer."""
    windows = windows_from_run_rows(
        [{"run_id": "r1", "asset_keys": [], "start_time": 1761700000.0}]
    )
    assert windows[0].asset_keys == []


# --- the compat floor -------------------------------------------------------
#
# Measured on dagster 1.10.19 (the version production runs): the shipped
# example loads and materializes there, with the same 9 / 4 / 5 numbers as on
# 1.13.1. The two tests below pin the reasons it does, so a later edit that
# would move the floor fails here instead of at a deployment's code-location
# load. `_skip_if_the_component_file_is_refused` above is the other half: if
# some future field is refused anyway, that is reported rather than hidden.


def test_the_component_file_uses_no_top_level_key_beyond_type_and_attributes():
    """What 1.10.19 rejects is an unknown key in the component file itself.

    That is the whole of the otel finding: `post_processing` is a sibling of
    `type`, the floor's model forbids extras, and the rejection fails the
    entire code-location load rather than just the unknown block. Staying
    inside `type` / `attributes` is therefore not a style preference -- it is
    what keeps this component deployable on the floor.
    """
    import yaml

    document = yaml.safe_load(
        (EXAMPLE / "read_audit" / "component.yaml").read_text(encoding="utf-8")
    )
    assert set(document) <= {"type", "attributes"}


def test_every_component_field_is_a_shape_the_floor_resolves():
    """Blocks arrive as `Dict[str, Any]` and are validated in `schema.py`.

    A nested `Resolvable` model per block would read better in a schema dump
    and would put this component's deployability at the mercy of whatever the
    floor's resolver accepts. Validating ourselves costs one indirection and
    keeps the YAML surface to types every version resolves identically -- and
    a malformed block still fails the load, with a pointed error, which is
    the property that mattered.
    """
    from dag_tools.components.read_audit.component import ReadAuditComponent

    allowed = {
        "typing.Dict[str, typing.Any]",
        "typing.Dict[str, str]",
        "typing.List[str]",
        "typing.Optional[str]",
        "str",
    }
    for name, field in ReadAuditComponent.model_fields.items():
        annotation = field.annotation
        # `__name__` on a typing generic is the bare "Dict" / "Optional", so
        # only a real class is rendered that way; everything else keeps its
        # full parameterisation, which is the part under test.
        rendered = (
            annotation.__name__ if isinstance(annotation, type) else str(annotation)
        )
        assert rendered in allowed, f"{name}: {rendered}"
