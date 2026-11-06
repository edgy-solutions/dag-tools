"""Q2 round-trip probe: source generation and what the generated module does
when Dagster actually executes it."""
import ast
import importlib.util
import itertools

import pytest

from dagster import AssetKey, DagsterInstance, Definitions

from dag_tools.qual.classes import (
    ClassKeyComponents,
    EquivalenceClass,
    Representative,
    Runnability,
)
from dag_tools.qual.qualify import ProbeSettings
from dag_tools.qual.runs.state import rep_id_for
from dag_tools.qual.synthetic import (
    generate_roundtrip_module,
    roundtrip_not_applicable_reason,
)
from dag_tools_tests.roundtrip_io_fixture import FixtureIOManager
from dag_tools_tests.test_synthetic_generator import _class

FIXTURE_FQN = "dag_tools_tests.roundtrip_io_fixture.FixtureIOManager"
_counter = itertools.count()


def _rt_class(**kw):
    kw.setdefault("runnability", Runnability.RUNNABLE)
    kw.setdefault("io_manager_class", FIXTURE_FQN)
    return _class(**kw)


def _load(tmp_path, source):
    path = tmp_path / f"rt_{next(_counter)}.py"
    path.write_text(source)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run(mod, probe, instance, partition_key=None):
    getter = getattr(
        mod.defs, "resolve_implicit_global_asset_job_def",
        None,
    ) or mod.defs.get_implicit_global_asset_job_def
    job = getter()
    return job.execute_in_process(
        instance=instance,
        asset_selection=[AssetKey(probe.probe_asset_key)],
        partition_key=partition_key,
        raise_on_error=False,
    )


def test_roundtrip_source_parses_and_loads_as_definitions(tmp_path):
    probe, source = generate_roundtrip_module(_rt_class(), qual_id="q1")
    ast.parse(source)
    mod = _load(tmp_path, source)
    assert isinstance(mod.defs, Definitions)
    keys = set(mod.defs.get_repository_def().asset_graph.get_all_asset_keys())
    assert AssetKey(probe.probe_asset_key) in keys
    assert AssetKey(probe.asset_key) in keys


def test_roundtrip_probe_returns_materialize_result_and_writes_nothing(tmp_path):
    probe, source = generate_roundtrip_module(_rt_class(), qual_id="q1")
    mod = _load(tmp_path, source)
    FixtureIOManager.LOADED_PARTITIONS.clear()
    result = _run(mod, probe, DagsterInstance.ephemeral())
    # The fixture's handle_output raises if called, so success proves it was not.
    assert result.success
    meta = result.get_asset_materialization_events()[0].materialization.metadata
    assert meta["dagtools/loaded_type"].value.endswith("StubFrame")
    assert meta["dagtools/loaded_columns"].value == ["a", "b"]
    assert meta["dagtools/loaded_len"].value == 3
    assert "return MaterializeResult" in source


def test_roundtrip_loads_the_partition_it_was_given(tmp_path):
    cls = _rt_class(partitions_def_class="StaticPartitionsDefinition")
    probe, source = generate_roundtrip_module(cls, qual_id="q1")
    assert probe.partitions_def_name
    mod = _load(tmp_path, source)
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(probe.partitions_def_name, ["2026-03-01"])
    FixtureIOManager.LOADED_PARTITIONS.clear()
    result = _run(mod, probe, instance, partition_key="2026-03-01")
    assert result.success
    assert FixtureIOManager.LOADED_PARTITIONS == ["2026-03-01"]


def test_import_failure_surfaces_the_import_error_not_a_missing_key(tmp_path):
    cls = _rt_class(io_manager_class="no_such_pkg_for_roundtrip.Missing")
    probe, source = generate_roundtrip_module(cls, qual_id="q1")
    mod = _load(tmp_path, source)  # the location must still load
    result = _run(mod, probe, DagsterInstance.ephemeral())
    assert not result.success
    text = " ".join(
        str(e.event_specific_data) for e in result.all_events if e.is_failure
    ) + " ".join(str(e.message) for e in result.all_events)
    assert "no_such_pkg_for_roundtrip" in text
    assert "has not been materialized" not in text


def test_roundtrip_source_does_not_import_dag_tools_qual():
    _, source = generate_roundtrip_module(_rt_class(), qual_id="q1")
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("dag_tools.qual")
        elif isinstance(node, ast.Import):
            assert not any(a.name.startswith("dag_tools.qual") for a in node.names)


def test_env_reference_renders_os_environ_not_the_literal_value(monkeypatch):
    sentinel = "SENTINEL-roundtrip-do-not-persist"
    monkeypatch.setenv("QUAL_RT_TOKEN", sentinel)
    settings = ProbeSettings(
        io_manager_config={FIXTURE_FQN: {"columns": {"env": "QUAL_RT_TOKEN"}}}
    )
    probe, source = generate_roundtrip_module(
        _rt_class(), qual_id="q1", probe_settings=settings,
    )
    assert "os.environ" in source and "QUAL_RT_TOKEN" in source
    assert sentinel not in source
    assert sentinel not in probe.model_dump_json()
    assert probe.io_manager_config_keys == ["columns"]
    ast.parse(source)


def _two_rep_class():
    reps = [
        Representative(
            repo="alpha", git_sha="s", asset_key=["obs"],
            runnability=Runnability.OBSERVE_ONLY, runnability_reason="tag",
        ),
        Representative(
            repo="alpha", git_sha="s", asset_key=["first"],
            runnability=Runnability.RUNNABLE, runnability_reason="default",
        ),
        Representative(
            repo="beta", git_sha="s", asset_key=["second"],
            runnability=Runnability.RUNNABLE, runnability_reason="default",
        ),
    ]
    return EquivalenceClass(
        class_hash="feedfacecafe",
        key=ClassKeyComponents(compute_kind="python", io_manager_class=FIXTURE_FQN),
        member_count=3, member_repo_count=2, members=[], representatives=reps,
    )


def test_one_probe_per_class_from_its_first_runnable_rep():
    probe, _ = generate_roundtrip_module(_two_rep_class(), qual_id="q1")
    assert probe.asset_key == ["first"]
    # The runner looks the Q2 rep up by this id, so it must be Q2's own key.
    assert probe.rep_id == rep_id_for("feedfacecafe", ["first"])


def test_dbt_and_io_managerless_classes_get_no_probe_and_are_listed():
    dbt = _class(runnability=Runnability.RUNNABLE)
    dbt.key = dbt.key.model_copy(update={"integration_libs": ["dagster_dbt"]})
    bare = _class(runnability=Runnability.RUNNABLE, io_manager_class=None)
    for cls in (dbt, bare):
        probe, reason = generate_roundtrip_module(cls, qual_id="q1")
        assert probe is None
        assert reason == roundtrip_not_applicable_reason(cls)
    synthetic = _class(runnability=Runnability.SYNTHETIC_REQUIRED)
    assert generate_roundtrip_module(synthetic, qual_id="q1")[0] is None


@pytest.mark.parametrize(
    "partitions, fragment",
    [
        ("DailyPartitionsDefinition", "time-based"),
        ("MultiPartitionsDefinition", "multi-dimensional"),
    ],
)
def test_time_window_and_multi_partition_classes_get_notes(partitions, fragment):
    probe, source = generate_roundtrip_module(
        _rt_class(partitions_def_class=partitions), qual_id="q1",
    )
    assert any(fragment in n for n in probe.notes)
    ast.parse(source)
