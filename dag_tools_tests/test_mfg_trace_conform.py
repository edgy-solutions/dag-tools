"""Stage 1 of the mfg_trace example: the two conform mappings, proven with no warehouse.

`examples/mfg_trace/conform/` holds two `ContractMappingComponent` instances, one
per raw table. The contract they conform onto is the governed bundle in
`examples/mfg_trace/contracts/`, and the mappings declare their own source shape
(`source.columns`), which is the only thing that makes them provable without a
server this repo has never seen.

Nothing here restates a column name. Contract columns are read through the
library's own `load_contract`, source columns out of the mapping files, and the
instances out of the components directory. The one place a name is compared with
the library is `test_the_library_names_nothing_the_mappings_carry`.

The example is loaded IN PLACE, not through a tmp copy: `build_component_defs`
imports the components directory as a module, so two tests loading two copies
under one name both get whichever reached `sys.modules` first. A copy is worth
its cost only when a test must mutate the example, and the one test that does
(the collision partner) builds its own tree instead.
"""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import dagster as dg
import pytest
import yaml
from dagster.components import build_component_defs

from dag_tools.contract_mapping import (
    MappingPlan,
    apply_to_rows,
    build_mapping_plan,
    check_rows,
    load_contract,
    load_mapping,
    render_dbt_artifacts,
)
from dag_tools.contract_mapping.normalize import load_normalizers_text

REPO = Path(__file__).resolve().parents[1]
MFG = REPO / "examples" / "mfg_trace"
CONFORM = MFG / "conform"
COMPONENTS = CONFORM / "components"
BUNDLE = MFG / "contracts" / "mfg_trace.yaml"
NORMALIZERS = CONFORM / "normalizers.yaml"
LOAD_INSTANT = datetime.fromisoformat("2026-01-02T03:04:05+00:00")


# --- reading the example ----------------------------------------------------


def _instances() -> List[Path]:
    return sorted(p.parent for p in COMPONENTS.glob("*/component.yaml"))


def _attributes(instance: Path) -> Dict[str, Any]:
    return yaml.safe_load((instance / "component.yaml").read_text(encoding="utf-8"))[
        "attributes"
    ]


def _mapping_document(instance: Path) -> Dict[str, Any]:
    return yaml.safe_load((instance / "mapping.yaml").read_text(encoding="utf-8"))


def _bundle() -> Dict[str, Dict[str, Any]]:
    with BUNDLE.open(encoding="utf-8") as handle:
        return {d["contract_id"]: d for d in yaml.safe_load_all(handle) if d}


def _plan(instance: Path) -> MappingPlan:
    """The plan exactly as the component builds it at load: file contract,
    mapping-declared source schema, the shared normalizer document."""
    contract = load_contract(_bundle()[_attributes(instance)["contract"]["expect_contract_id"]])
    mapping = load_mapping(_mapping_document(instance))
    normalizers = load_normalizers_text(NORMALIZERS.read_text(encoding="utf-8"))
    return build_mapping_plan(contract, mapping, dict(mapping.source.columns), normalizers)


def _contract(instance: Path):
    return load_contract(_bundle()[_attributes(instance)["contract"]["expect_contract_id"]])


INSTANCES = _instances()
IDS = [p.name for p in INSTANCES]


def _chain_text(exc: BaseException) -> str:
    parts, seen = [], set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def _skip_if_the_component_file_is_refused(exc: Exception) -> None:
    """Skip when this Dagster rejects a key in a component file.

    The conform component files use only `type:` + `attributes:`, so the
    refusal should not occur; the gate is here so a floor that disagrees is
    reported by name rather than as a mystery failure.
    """
    text = _chain_text(exc)
    if "extra_forbidden" in text or "Extra inputs are not permitted" in text:
        pytest.skip(f"this Dagster rejects a key in the component file: {text}")


def _load(components_dir: Path) -> dg.Definitions:
    try:
        return build_component_defs(components_dir)
    except Exception as exc:  # pragma: no cover - only on an old Dagster
        _skip_if_the_component_file_is_refused(exc)
        raise


def test_the_example_is_there():
    """The guard against every parametrized test below running over nothing."""
    assert len(INSTANCES) == 2, INSTANCES
    assert BUNDLE.is_file() and NORMALIZERS.is_file()


# --- the plan ---------------------------------------------------------------


@pytest.mark.parametrize("instance", INSTANCES, ids=IDS)
def test_both_conformed_mappings_load_with_no_problems(instance):
    """`build_mapping_plan` raises with EVERY problem it found, so reaching
    the plan at all is the empty problem list."""
    plan = _plan(instance)
    assert plan.contract_id == _attributes(instance)["contract"]["expect_contract_id"]


@pytest.mark.parametrize("instance", INSTANCES, ids=IDS)
def test_the_plan_has_nothing_left_to_report(instance):
    """Without this a missing `warehouse_types`, an uncast type change or an
    unwritten test passes silently. Same assertion the EV example makes."""
    assert _plan(instance).notes == []


@pytest.mark.parametrize("instance", INSTANCES, ids=IDS)
def test_every_contract_column_is_produced(instance):
    """Equality both ways against the contract's own columns."""
    contract = _contract(instance)
    produced = {c.name for c in _plan(instance).columns}
    assert produced == set(contract.column_names)
    assert {c.name for c in contract.must_produce} <= produced
    assert produced  # not vacuous


# --- the row route ----------------------------------------------------------

_SAMPLE = {
    "string": "ab-1",
    "integer": 3,
    "float": 1.5,
    "boolean": True,
    "timestamp": "2026-01-02T03:04:05+00:00",
}
_PY_TYPE = {
    "string": str,
    "integer": int,
    "float": float,
    "boolean": bool,
    "timestamp": datetime,
    "numeric": (int, float),
}


def _source_row(instance: Path) -> Dict[str, Any]:
    """One source row shaped by the mapping's own `source.columns`."""
    declared = _mapping_document(instance)["source"]["columns"]
    return {name: _SAMPLE[kind] for name, kind in declared.items()}


@pytest.mark.parametrize("instance", INSTANCES, ids=IDS)
def test_the_row_route_produces_the_declared_types(instance):
    """Run the mapping over rows the declaration describes and read the types
    off what comes out, so the timestamp, boolean and run-timestamp paths are
    exercised by the example and not only by their own modules."""
    plan = _plan(instance)
    rows = apply_to_rows(plan, [_source_row(instance)], run_timestamp=LOAD_INSTANT)
    assert len(rows) == 1
    kinds = {c.name: c.contract_type for c in plan.columns}
    assert set(rows[0]) == set(kinds)
    for name, value in rows[0].items():
        if value is None:
            continue
        expected = _PY_TYPE[kinds[name]]
        assert isinstance(value, expected), (name, kinds[name], value)
        if kinds[name] == "integer":
            assert not isinstance(value, bool), name
    assert check_rows(plan, rows) == []
    # The load instant is the caller's, carried through unchanged.
    for column in plan.columns:
        if column.transform == "run_timestamp":
            assert rows[0][column.name] == LOAD_INSTANT


def test_the_membership_transform_decides_terminal_and_nothing_else_does():
    """`in_set` is the only thing that produces a boolean; both routes agree
    on a value in the list, and a value outside it is False on both."""
    seen = 0
    for instance in INSTANCES:
        plan = _plan(instance)
        in_set = [c for c in plan.columns if c.transform == "in_set"]
        booleans = {c.name for c in plan.columns if c.contract_type == "boolean"}
        assert booleans == {c.name for c in in_set}, instance.name
        sql = render_dbt_artifacts(plan)[f"{plan.model_name}.sql"]
        for column in in_set:
            seen += 1
            source = column.sources[0]
            base = _source_row(instance)
            for literal in column.values:
                row = apply_to_rows(plan, [{**base, source: literal}], LOAD_INSTANT)[0]
                assert row[column.name] is True, literal
                assert f"'{literal}'" in sql, literal
            outside = "NOT-" + str(column.values[0])
            assert outside not in column.values
            row = apply_to_rows(plan, [{**base, source: outside}], LOAD_INSTANT)[0]
            assert row[column.name] is False
            assert outside not in sql
            row = apply_to_rows(plan, [{**base, source: None}], LOAD_INSTANT)[0]
            assert row[column.name] is False
    assert seen >= 1, "no instance carries an in_set column, so this proved nothing"


# --- the declared source shape is load-bearing ------------------------------


def _from_entries() -> List[Tuple[str, str, str]]:
    out = []
    for instance in INSTANCES:
        for column, entry in _mapping_document(instance)["columns"].items():
            sources = entry.get("from", [])
            for source in [sources] if isinstance(sources, str) else sources:
                out.append((instance.name, column, source))
    return out


FROM_ENTRIES = _from_entries()


def test_there_are_from_entries_to_check():
    assert len(FROM_ENTRIES) > 10


@pytest.mark.parametrize(
    "instance_name, column, source",
    FROM_ENTRIES,
    ids=[f"{i}:{c}<-{s}" for i, c, s in FROM_ENTRIES],
)
def test_the_mapping_reads_no_column_its_source_block_does_not_declare(
    instance_name, column, source
):
    """This is what makes the declared shape load-bearing rather than
    decorative: a `from` the block does not declare is a column the example
    never said the source has."""
    declared = _mapping_document(COMPONENTS / instance_name)["source"]["columns"]
    assert source in declared, f"{column} reads {source!r}, which source.columns does not declare"


# --- the library names nothing the mappings carry ---------------------------


def _library_files() -> List[Path]:
    out = []
    for path in (REPO / "dag_tools").rglob("*"):
        if path.suffix not in {".py", ".sql", ".yml", ".yaml"} or "__pycache__" in path.parts:
            continue
        rel = path.relative_to(REPO).as_posix()
        # The kind that stamps and flattens these names legitimately says them.
        if "systemlink" in rel:
            continue
        out.append(path)
    return out


def test_the_library_names_nothing_the_mappings_carry():
    """AGENTS.md section 4m, enforced rather than remembered.

    The names come from the mapping files. Scope, stated: a source spelling
    that is also a conformed column is the shared vocabulary the derive package
    is MEANT to read, and a single ordinary word (`status`, `name`) occurs
    everywhere for unrelated reasons, so the check covers the compound source
    spellings -- the ones with an underscore that the conformed vocabulary does
    not use -- outside the SystemLink kind that owns them.
    """
    source_names = set()
    contract_names = set()
    for instance in INSTANCES:
        source_names |= set(_mapping_document(instance)["source"]["columns"])
        contract_names |= set(_contract(instance).column_names)
    names = sorted(n for n in source_names - contract_names if "_" in n)
    assert names, "no compound source spelling to look for, so this proved nothing"

    files = _library_files()
    assert files, "the library file scan found nothing"
    found = {
        name: [p.relative_to(REPO).as_posix() for p in files if name in p.read_text(encoding="utf-8", errors="ignore")]
        for name in names
    }
    assert {n: hits for n, hits in found.items() if hits} == {}


# --- both instances, one code location --------------------------------------


@pytest.fixture()
def conform_on_path(monkeypatch):
    # The parent of the package has to be importable: the loader imports the
    # components directory as a module of it.
    monkeypatch.syspath_prepend(str(MFG))


def test_both_instances_load_into_one_definitions_without_a_key_collision(conform_on_path):
    defs = _load(COMPONENTS)
    keys = [k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()]
    expected = {_attributes(i)["asset_name"] for i in INSTANCES}
    assert len(expected) == len(INSTANCES) == 2
    assert set(keys) == expected
    assert len(keys) == len(set(keys))


def _write_instance_tree(root: Path, package: str, name_the_assets: bool) -> Path:
    """Two instances over the shipped mapping files, in a tree of our own."""
    components = root / package / "components"
    components.mkdir(parents=True)
    (root / package / "__init__.py").write_text("")
    (components / "__init__.py").write_text("")
    for index, instance in enumerate(INSTANCES):
        target = components / f"instance_{index}"
        target.mkdir()
        (target / "__init__.py").write_text("")
        shutil.copy(instance / "mapping.yaml", target / "mapping.yaml")
        attributes = dict(_attributes(instance))
        attributes["contract"] = {**attributes["contract"], "path": BUNDLE.as_posix()}
        attributes["normalizers"] = {"path": NORMALIZERS.as_posix()}
        if not name_the_assets:
            attributes.pop("asset_name")
        (target / "component.yaml").write_text(
            yaml.safe_dump(
                {"type": "dag_tools.ContractMappingComponent", "attributes": attributes}
            ),
            encoding="utf-8",
        )
    return components


def test_two_instances_that_both_take_the_default_asset_name_collide(tmp_path, monkeypatch):
    """Pins WHY the example names its assets. The default is `contract_mapping`,
    so two instances leaving it unset mint one key and the load fails naming
    that default, which points at neither instance. Built in a tmp tree: the
    shipped example is not edited.
    """
    monkeypatch.syspath_prepend(str(tmp_path))
    package = "ctl_" + tmp_path.name.replace("-", "_")

    # The control: the same tree with the names kept loads, so what follows is
    # the missing name and not the tree.
    control = _write_instance_tree(tmp_path / "named", package + "_named", True)
    monkeypatch.syspath_prepend(str(tmp_path / "named"))
    _load(control)

    colliding = _write_instance_tree(tmp_path / "bare", package + "_bare", False)
    monkeypatch.syspath_prepend(str(tmp_path / "bare"))
    with pytest.raises(Exception) as excinfo:
        defs = _load(colliding)
        defs.resolve_asset_graph()
    assert "contract_mapping" in _chain_text(excinfo.value)
