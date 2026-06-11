"""Tests for the survey loader: module-path loading, workspace.yaml parsing,
and the load-or-fail-but-don't-raise contract.
"""
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("dagster")
pytest.importorskip("yaml")

from dag_tools.qual.survey.loader import (
    LoadResult,
    load_locations,
)


@pytest.fixture
def good_module_file(tmp_path: Path) -> Path:
    """A .py file that defines a valid Dagster Definitions as ``defs``."""
    py = tmp_path / "good_defs.py"
    py.write_text(textwrap.dedent("""
        from dagster import Definitions, InMemoryIOManager, asset

        @asset
        def hello():
            return 1

        defs = Definitions(
            assets=[hello],
            resources={"io_manager": InMemoryIOManager()},
        )
    """))
    return py


@pytest.fixture
def broken_module_file(tmp_path: Path) -> Path:
    """A .py file that raises during import — load must NOT propagate."""
    py = tmp_path / "broken_defs.py"
    py.write_text(textwrap.dedent("""
        raise RuntimeError("simulated import-time failure")
    """))
    return py


@pytest.fixture
def warning_module_file(tmp_path: Path) -> Path:
    """A .py file that emits a DeprecationWarning during import. The loader
    runs with ``-W all`` so this must be captured even when the warning
    filter would normally suppress it."""
    py = tmp_path / "warning_defs.py"
    py.write_text(textwrap.dedent("""
        import warnings
        warnings.warn("survey loader warning capture test", DeprecationWarning)

        from dagster import Definitions, InMemoryIOManager, asset

        @asset
        def hello():
            return 1

        defs = Definitions(
            assets=[hello],
            resources={"io_manager": InMemoryIOManager()},
        )
    """))
    return py


def test_load_locations_loads_py_file(good_module_file):
    results = load_locations(str(good_module_file))
    assert len(results) == 1
    r = results[0]
    assert isinstance(r, LoadResult)
    assert r.loaded, f"expected loaded; got error={r.error!r}"
    assert r.defs is not None
    assert r.error is None


def test_load_locations_load_failure_is_recorded_not_raised(broken_module_file):
    """Critical contract: the loader returns the failure; it never raises."""
    results = load_locations(str(broken_module_file))
    assert len(results) == 1
    r = results[0]
    assert not r.loaded
    assert r.error is not None
    assert "RuntimeError" in r.error
    assert "simulated import-time failure" in r.error
    assert r.traceback is not None


def test_load_locations_captures_warnings(warning_module_file):
    results = load_locations(str(warning_module_file))
    r = results[0]
    assert r.loaded
    msgs = [w.message for w in r.warnings_captured]
    assert any("survey loader warning capture test" in m for m in msgs)
    cats = [w.category for w in r.warnings_captured]
    assert any("DeprecationWarning" in c for c in cats)


def test_load_locations_rejects_unrecognized_spec():
    results = load_locations("not-a-thing-at-all")
    assert len(results) == 1
    r = results[0]
    assert not r.loaded
    assert "unrecognized locations spec" in r.error


def test_load_locations_loads_workspace_yaml(tmp_path: Path, good_module_file: Path):
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(textwrap.dedent(f"""
        load_from:
          - python_file:
              relative_path: {good_module_file.name}
    """))
    results = load_locations(str(workspace))
    assert len(results) == 1
    r = results[0]
    assert r.loaded, f"error={r.error!r}"
    assert r.name == "good_defs"  # default location_name = file stem


def test_load_locations_yaml_with_multiple_entries(
    tmp_path: Path, good_module_file: Path, broken_module_file: Path
):
    """Multiple python_file entries: one loads, one fails. Both come back."""
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(textwrap.dedent(f"""
        load_from:
          - python_file:
              relative_path: {good_module_file.name}
              location_name: good
          - python_file:
              relative_path: {broken_module_file.name}
              location_name: broken
    """))
    results = load_locations(str(workspace))
    assert len(results) == 2
    good = next(r for r in results if r.name == "good")
    broken = next(r for r in results if r.name == "broken")
    assert good.loaded
    assert not broken.loaded


def test_load_locations_yaml_with_no_load_from(tmp_path: Path):
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text("# empty workspace\n")
    results = load_locations(str(workspace))
    assert len(results) == 1
    assert not results[0].loaded
    assert "no 'load_from' entries" in results[0].error


def test_load_locations_yaml_with_bad_entry(tmp_path: Path):
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(textwrap.dedent("""
        load_from:
          - python_module:
              # missing module_name
              attribute: defs
    """))
    results = load_locations(str(workspace))
    assert len(results) == 1
    assert not results[0].loaded
    assert "no module_name" in results[0].error
