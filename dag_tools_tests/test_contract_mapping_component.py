"""`ContractMappingComponent` -- the YAML surface, loaded the way a deployment loads it.

The component is the primary surface: a deployment writes `component.yaml`, not
Python and not a CLI invocation. So these tests go through `build_component_defs`
over the real example rather than constructing the class, which is the only way
to catch what matters here -- a field Dagster rejects, an asset the loader
silently drops, a check that was supposed to run at load and did not.

**The compat floor is probed, not assumed.** 1.10.19 -- the version production
runs -- rejects unknown keys in a component file outright and fails the whole
code-location load. This module therefore loads the example on whatever Dagster
is installed and, if the load is refused for that reason, skips while naming the
field. Anything else that goes wrong still fails loudly.

Two load-time/materialization splits are load-bearing and each has a test:

  * a contract from a FILE is reconciled at load, so a bad mapping fails where
    an operator is looking;
  * a contract from a URL is fetched at materialization, because a code
    location must not fail to load because a service is down.

Every name in these fixtures is from dag-tools' own synthetic sandbox fixture.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import dagster as dg
import pytest
from dagster.components import build_component_defs

from dag_tools.components.contract_mapping import component as component_module
from dag_tools.components.contract_mapping.schema import ContractMappingSpec

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "contract_mapping"
ASSET = "ev_fixture_contract_mapping"


def _chain_text(exc: BaseException) -> str:
    """Every message in the exception chain, as one string."""
    parts = []
    seen = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _skip_if_the_component_file_is_refused(exc: Exception) -> None:
    """Skip when this Dagster will not accept this component file.

    Matched on the raised error rather than on a version number: the field the
    loader rejects is the fact, and a version is only a proxy for it.
    """
    text = _chain_text(exc)
    if "extra_forbidden" in text or "Extra inputs are not permitted" in text:
        pytest.skip(f"this Dagster rejects a key in the component file: {text}")


@pytest.fixture()
def example(tmp_path, monkeypatch) -> Path:
    """A copy of the shipped example, so a test can edit the YAML.

    The whole example tree is copied, not just the components directory: the
    component file reaches the vendored contract by a relative path, and
    copying one half would quietly change what the example resolves to.

    `build_component_defs` imports the directory as `<its parent>.components`,
    so the copy's `dagster_home` is renamed to something unique per test.
    Without that, every test loads the first test's cached module and its YAML
    edits do nothing -- which looks exactly like a component that tolerates a
    mapping it should refuse.

    The depth is preserved rather than the name, because the component file
    reaches the contract by a relative path.
    """
    root = tmp_path / "ex"
    shutil.copytree(EXAMPLE, root, ignore=shutil.ignore_patterns("__pycache__"))
    home = root / tmp_path.name.replace("-", "_")
    (root / "dagster_home").rename(home)
    monkeypatch.syspath_prepend(str(root))
    return home / "components"


def _load(components_dir: Path) -> dg.Definitions:
    try:
        return build_component_defs(components_dir)
    except Exception as exc:  # pragma: no cover - only on an old Dagster
        _skip_if_the_component_file_is_refused(exc)
        raise


def _yaml(components_dir: Path) -> Path:
    return components_dir / "contract_mapping" / "component.yaml"


def _mapping(components_dir: Path) -> Path:
    return components_dir / "contract_mapping" / "mapping.yaml"


def _edit(path: Path, old: str, new: str) -> None:
    """Replace an anchor that occurs exactly once.

    The example's component file documents its alternatives in comments, so
    several anchors appear twice -- and replacing the commented copy edits
    nothing, which reads as a component that tolerated the change.
    """
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    assert count == 1, f"{path.name} contains {old!r} {count} times, not once"
    path.write_text(text.replace(old, new), encoding="utf-8")


# --- the example -----------------------------------------------------------


def test_the_shipped_example_loads(example):
    """The example is part of the contract.

    A component whose one worked example cannot load has never been run --
    which is exactly how the dbt example shipped pointing at two projects that
    did not exist.
    """
    defs = _load(example)
    keys = {
        key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()
    }
    assert keys == {ASSET}


def test_the_example_materializes_and_publishes_the_bytes_it_would_write(example):
    """A dry run has to show the exact bytes, or it proves nothing.

    Same discipline as rendering OTel plans Dagster-side: the review surface
    and the executed surface are the same text.
    """
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    assert result.success

    metadata = result.asset_materializations_for_node(ASSET)[0].metadata
    assert metadata["dry_run"].value is True
    assert metadata["files_written"].value == {"count": 0, "shown": []}

    rendered = metadata["rendered"].value
    assert f"{metadata['model_name'].value}.sql" in rendered
    sql = rendered[f"{metadata['model_name'].value}.sql"]
    assert metadata["plan_hash"].value in sql
    assert sql.lstrip().startswith("-- generated from contract")


def test_the_materialization_records_the_physical_platform_as_the_asset_of_record(
    example,
):
    """The 11-05 ruling: the asset of record is the physical platform's URN.

    Never a `dagster` platform URN -- the Dagster asset attaches as lineage.
    Published on the materialization so the operator can see which object a
    broker would authorize a read of.
    """
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    metadata = result.asset_materializations_for_node(ASSET)[0].metadata

    assert "dataPlatform:dagster" not in metadata["asset_of_record"].value
    assert "dataPlatform:dagster" not in metadata["source_dataset"].value
    assert metadata["asset_of_record"].value != metadata["source_dataset"].value


def test_the_example_reports_the_contracts_forward_edge_rather_than_hiding_it(example):
    """Two declared-not-produced columns, itemised rather than counted.

    A count tells an operator a number; the names tell them what a reader can
    be written against next.
    """
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    metadata = result.asset_materializations_for_node(ASSET)[0].metadata
    assert metadata["declared_not_produced"].value["shown"] == [
        "status_period",
        "eac_reported",
    ]


def test_the_notes_themselves_are_published_not_just_how_many(example):
    """Every note names something the plan did NOT emit.

    "3 notes" is not actionable; a note that says which test was not written
    is. The dry run itself is one of them, so this is never an empty list.
    """
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    notes = result.asset_materializations_for_node(ASSET)[0].metadata["notes"].value
    assert notes["count"] == len(notes["shown"]) == 1
    assert "nothing written" in notes["shown"][0]


# --- what fails at load ----------------------------------------------------


def test_an_unknown_source_column_fails_the_code_location_load(example):
    """The reason the mapping may declare the source's schema at all.

    With the declaration, this is caught where an operator is editing. Without
    it, the same mapping fails inside a run at whatever hour the schedule
    fires -- which is the whole value of the load-time gate.
    """
    _edit(_mapping(example), "from: PROGRAM_ID", "from: Z_ABSENT")
    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    _skip_if_the_component_file_is_refused(excinfo.value)
    assert "Z_ABSENT" in _chain_text(excinfo.value)


def test_a_mapping_that_drops_a_required_column_fails_the_load(example):
    """A model no governed read can be served from should never be rendered."""
    # The whole block, transform line included: leaving a dangling `transform:`
    # behind makes the YAML unparseable, and the load then fails for that
    # instead -- which looks like this test passing.
    _edit(
        _mapping(example),
        "  program_id:\n    from: PROGRAM_ID\n    transform: trim\n",
        "",
    )
    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    _skip_if_the_component_file_is_refused(excinfo.value)
    assert "program_id" in _chain_text(excinfo.value)


def test_a_malformed_emit_block_fails_the_load_not_the_run(example):
    """A bad block is an operator typo, and it should surface at load.

    Failing at materialization instead means the first sign of it is a failed
    run at whatever hour the schedule fires.
    """
    _edit(_yaml(example), "    dry_run: true", "    dryrun: true")
    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    text = _chain_text(excinfo.value)
    assert "dryrun" in text or "dry_run" in text


def test_a_contract_file_that_is_not_there_fails_the_load(example):
    """Not at materialization: the path is the operator's, and so is the fix."""
    _edit(_yaml(example), "../../../contracts/ev_fixture.yaml", "../../../nope.yaml")
    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    _skip_if_the_component_file_is_refused(excinfo.value)
    assert "nope.yaml" in _chain_text(excinfo.value)


def test_the_expected_contract_id_is_enforced(example):
    """`expect_contract_id` is what stops a served contract re-pointing a mapping.

    Worth setting on the file route too, where it catches a path edited to the
    wrong document -- every column name might still line up.
    """
    # Anchored on the line above it: the commented alternative in the example
    # names the same field, and editing that one would change nothing.
    _edit(
        _yaml(example),
        "    path: ../../../contracts/ev_fixture.yaml\n"
        "    expect_contract_id: ev_fixture",
        "    path: ../../../contracts/ev_fixture.yaml\n"
        "    expect_contract_id: c_other",
    )
    with pytest.raises(Exception) as excinfo:
        build_component_defs(example)
    _skip_if_the_component_file_is_refused(excinfo.value)
    assert "c_other" in _chain_text(excinfo.value)


def test_a_component_with_no_target_and_no_dry_run_is_refused():
    """Rendering into nowhere is not a mode; the spec has to say which it is."""
    with pytest.raises(ValueError) as excinfo:
        ContractMappingSpec.from_attributes(
            asset_name="a_one",
            group_name=None,
            io_manager_key=None,
            owners=None,
            tags=None,
            mapping_file="mapping.yaml",
            contract={"path": "c.yaml"},
            source_schema={},
            emit={},
        )
    assert "dry_run" in str(excinfo.value)


def test_a_contract_from_two_places_is_refused():
    """A contract read from two places is two contracts."""
    with pytest.raises(ValueError) as excinfo:
        ContractMappingSpec.from_attributes(
            asset_name="a_one",
            group_name=None,
            io_manager_key=None,
            owners=None,
            tags=None,
            mapping_file="mapping.yaml",
            contract={"path": "c.yaml", "url": "https://example.invalid/c"},
            source_schema={},
            emit={"dry_run": True},
        )
    assert "exactly one" in str(excinfo.value)


def test_disabling_both_schema_sources_is_refused():
    """It leaves nothing to reconcile the mapping against.

    An operator who wanted no reconciliation would get a component that loads,
    renders, and checks nothing -- which looks exactly like one that works.
    """
    with pytest.raises(ValueError) as excinfo:
        ContractMappingSpec.from_attributes(
            asset_name="a_one",
            group_name=None,
            io_manager_key=None,
            owners=None,
            tags=None,
            mapping_file="mapping.yaml",
            contract={"path": "c.yaml"},
            source_schema={"from_mapping": False, "datahub": False},
            emit={"dry_run": True},
        )
    assert "nothing to reconcile" in str(excinfo.value)


# --- the served-contract route --------------------------------------------


def test_a_served_contract_is_not_fetched_at_load(example, monkeypatch):
    """A code location must not fail to load because a service is down.

    So the fetch moves to materialization -- and this test fails if anything
    ever "helpfully" pulls it forward to validate earlier.
    """
    calls = []

    def _refuse(*args, **kwargs):
        calls.append(args)
        raise AssertionError("the contract was fetched during the load")

    monkeypatch.setattr(component_module, "_fetch_contract_document", _refuse)
    _edit(
        _yaml(example),
        "    path: ../../../contracts/ev_fixture.yaml",
        "    url: https://example.invalid/contracts/ev_fixture",
    )
    defs = _load(example)
    assert calls == []
    keys = {
        key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()
    }
    assert keys == {ASSET}


def test_a_served_contract_is_fetched_at_materialization(example, monkeypatch):
    """The other half: the fetch has to actually happen somewhere.

    A deferred check that never runs is worse than a load-time one that fails.
    """
    import yaml as yaml_module

    document = yaml_module.safe_load(
        (EXAMPLE / "contracts" / "ev_fixture.yaml").read_text(encoding="utf-8")
    )
    calls = []

    def _serve(spec):
        calls.append(spec.contract.url)
        return document

    monkeypatch.setattr(component_module, "_fetch_contract_document", _serve)
    _edit(
        _yaml(example),
        "    path: ../../../contracts/ev_fixture.yaml",
        "    url: https://example.invalid/contracts/ev_fixture",
    )
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    assert result.success
    assert calls == ["https://example.invalid/contracts/ev_fixture"]

    notes = result.asset_materializations_for_node(ASSET)[0].metadata["notes"].value
    assert any("materialization" in note for note in notes["shown"]), notes


def test_a_served_contract_with_the_wrong_id_fails_the_run(example, monkeypatch):
    """What `expect_contract_id` exists for.

    What a service serves is not under the operator's control, so a service
    that starts serving a different contract would otherwise silently
    re-point the mapping -- and the mapping might still reconcile.
    """
    monkeypatch.setattr(
        component_module,
        "_fetch_contract_document",
        lambda spec: {"contract_id": "c_other", "columns": []},
    )
    _edit(
        _yaml(example),
        "    path: ../../../contracts/ev_fixture.yaml",
        "    url: https://example.invalid/contracts/ev_fixture",
    )
    defs = _load(example)
    with pytest.raises(Exception) as excinfo:
        dg.materialize(list(defs.assets or []))
    assert "c_other" in _chain_text(excinfo.value)


# --- writing ---------------------------------------------------------------


def test_the_files_land_where_the_emit_block_says(example, tmp_path):
    """The model under the models subdir, the singular tests under the tests one.

    A dbt project's layout is the deployment's, so both are configurable and
    both have to be honoured -- a model written into the tests directory is
    built as a test and silently never materialises.
    """
    project = tmp_path / "proj"
    (project / "models" / "contracts").mkdir(parents=True)
    (project / "tests" / "contracts").mkdir(parents=True)
    _edit(
        _yaml(example),
        "    dry_run: true",
        "    dbt_project_dir: " + str(project).replace("\\", "/") + "\n"
        "    models_subdir: models/contracts\n"
        "    tests_subdir: tests/contracts",
    )
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    assert result.success

    models = sorted(p.name for p in (project / "models" / "contracts").iterdir())
    tests = sorted(p.name for p in (project / "tests" / "contracts").iterdir())
    assert models == ["ev_fixture.sql", "ev_fixture.yml"]
    assert len(tests) == 8
    assert all(name.startswith("ev_fixture__") for name in tests)

    metadata = result.asset_materializations_for_node(ASSET)[0].metadata
    assert metadata["dry_run"].value is False
    assert metadata["files_written"].value["count"] == 10


def test_a_dbt_project_directory_that_is_not_there_is_refused(example, tmp_path):
    """This component writes into a dbt project; it does not create one.

    Creating the directory would turn a typo'd path into a project nothing
    builds, which is harder to notice than an error.
    """
    _edit(
        _yaml(example),
        "    dry_run: true",
        "    dbt_project_dir: " + str(tmp_path / "absent").replace("\\", "/"),
    )
    defs = _load(example)
    with pytest.raises(Exception) as excinfo:
        dg.materialize(list(defs.assets or []))
    assert "not a directory" in _chain_text(excinfo.value)


def test_a_run_config_dry_run_renders_without_writing(example, tmp_path):
    """The usual way a mapping change gets reviewed.

    An operator should not have to edit the component file to see what a
    change would write.
    """
    project = tmp_path / "proj"
    (project / "models").mkdir(parents=True)
    _edit(
        _yaml(example),
        "    dry_run: true",
        "    dbt_project_dir: " + str(project).replace("\\", "/"),
    )
    defs = _load(example)
    result = dg.materialize(
        list(defs.assets or []),
        run_config={"ops": {ASSET: {"config": {"dry_run": True}}}},
    )
    assert result.success
    assert list((project / "models").iterdir()) == []

    metadata = result.asset_materializations_for_node(ASSET)[0].metadata
    assert metadata["dry_run"].value is True
    assert metadata["rendered"].value


def test_prune_is_off_by_default_so_a_hand_written_test_survives(example, tmp_path):
    """This component writes into a directory it does not own.

    Deleting a test an operator wrote by hand is worse than leaving a stale
    one, so the default never deletes anything.
    """
    project = tmp_path / "proj"
    (project / "models").mkdir(parents=True)
    (project / "tests").mkdir(parents=True)
    stale = project / "tests" / "ev_fixture__r_gone__x_one.sql"
    stale.write_text("select 1\n", encoding="utf-8")
    mine = project / "tests" / "someone_elses_check.sql"
    mine.write_text("select 2\n", encoding="utf-8")

    _edit(
        _yaml(example),
        "    dry_run: true",
        "    dbt_project_dir: " + str(project).replace("\\", "/"),
    )
    defs = _load(example)
    assert dg.materialize(list(defs.assets or [])).success
    assert stale.is_file()
    assert mine.is_file()


def test_prune_removes_only_files_this_component_would_have_written(example, tmp_path):
    """The opt-in, and the limit on it.

    A stale generated test fails a `dbt build` against a rule the contract no
    longer states, so pruning is worth having -- but only over names this
    component owns.
    """
    project = tmp_path / "proj"
    (project / "models").mkdir(parents=True)
    (project / "tests").mkdir(parents=True)
    stale = project / "tests" / "ev_fixture__r_gone__x_one.sql"
    stale.write_text("select 1\n", encoding="utf-8")
    mine = project / "tests" / "someone_elses_check.sql"
    mine.write_text("select 2\n", encoding="utf-8")

    _edit(
        _yaml(example),
        "    dry_run: true",
        "    dbt_project_dir: " + str(project).replace("\\", "/") + "\n"
        "    prune: true",
    )
    defs = _load(example)
    result = dg.materialize(list(defs.assets or []))
    assert result.success
    assert not stale.exists()
    assert mine.is_file()

    metadata = result.asset_materializations_for_node(ASSET)[0].metadata
    assert metadata["files_pruned"].value["shown"] == [
        "tests/ev_fixture__r_gone__x_one.sql"
    ]


def test_nothing_is_written_when_the_render_fails(example, tmp_path):
    """Render everything into memory first, write last.

    A half-written project is a project whose next `dbt build` fails on a file
    nobody edited -- and the render is the part most likely to fail.
    """
    project = tmp_path / "proj"
    (project / "models").mkdir(parents=True)
    _edit(
        _yaml(example),
        "    dry_run: true",
        "    dbt_project_dir: " + str(project).replace("\\", "/"),
    )
    defs = _load(example)

    def _boom(plan):
        raise RuntimeError("render refused")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(component_module, "render_dbt_artifacts", _boom)
        with pytest.raises(Exception) as excinfo:
            dg.materialize(list(defs.assets or []))
    assert "render refused" in _chain_text(excinfo.value)
    assert list((project / "models").iterdir()) == []


# --- registration ----------------------------------------------------------


def test_the_component_is_reachable_by_the_name_the_component_file_uses():
    """`type: dag_tools.ContractMappingComponent` has to resolve.

    The lazy lookup in `dag_tools/__init__.py` is what makes that work, and a
    missing entry fails only at load of a deployment that uses it.
    """
    import dag_tools

    assert dag_tools.ContractMappingComponent is component_module.ContractMappingComponent
