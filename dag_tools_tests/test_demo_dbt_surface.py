"""The demo dbt surface, and the switch that gates it.

`CustomDbtProjectComponent` -> DataHub had no deployed exercise anywhere:
`user_deployment/definitions.py` built zero dbt assets and the one example
wiring it up pointed at projects that did not exist. That is how two bugs in
it shipped. `demo_dbt_assets` closes the gap, but only under
`DAG_TOOLS_DEMO_MODE` -- production clusters must not grow synthetic dbt
tables or start publishing demo lineage to a real DataHub.

So the two things worth pinning are: the switch actually gates it, and the
surface never takes the code location down when something is missing.
"""

import os
import shutil
import sys
from pathlib import Path

import pytest

pytest.importorskip("dagster_dbt")

from dag_tools.user_deployment import demo_dbt_assets  # noqa: E402


@pytest.fixture
def dbt_on_path(monkeypatch):
    """Ensure the interpreter's own bin/Scripts dir is on PATH.

    `dbt` is invoked by name. Running pytest as `.venv/bin/python -m pytest`
    does NOT put the venv's bin dir on PATH, so dbt is installed but not
    findable -- which would turn this into a silent skip in CI.
    """
    if shutil.which("dbt"):
        return
    bindir = Path(sys.executable).parent
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ.get('PATH', '')}")
    if not shutil.which("dbt"):
        pytest.skip("the dbt CLI is not installed next to this interpreter")


def _asset_keys(defs):
    return sorted(
        spec.key.to_user_string()
        for asset in (defs.assets or [])
        for spec in asset.specs
    )


# ---------------------------------------------------------------------------
# The demo-mode switch
# ---------------------------------------------------------------------------


def test_no_dbt_assets_when_demo_mode_is_off(monkeypatch):
    """Production posture: the flag off means no dbt surface at all."""
    from dag_tools.user_deployment import definitions

    monkeypatch.delenv("DAG_TOOLS_DEMO_MODE", raising=False)
    monkeypatch.delenv("DAG_TOOLS_GRIST_CONFIG", raising=False)
    monkeypatch.delenv("DATAHUB_SERVER", raising=False)

    keys = _asset_keys(definitions._build_combined_defs())
    assert not [k for k in keys if k.startswith("dbt/")], (
        f"dbt assets leaked into a non-demo deployment: {keys}"
    )


def test_demo_mode_registers_the_dbt_surface(monkeypatch, tmp_path, dbt_on_path):
    from dag_tools.user_deployment import definitions

    monkeypatch.setenv("DAG_TOOLS_DEMO_MODE", "true")
    monkeypatch.setenv("DAG_TOOLS_DEMO_DBT_TARGET_DIR", str(tmp_path / "target"))
    monkeypatch.delenv("DAG_TOOLS_GRIST_CONFIG", raising=False)
    monkeypatch.delenv("DATAHUB_SERVER", raising=False)

    keys = _asset_keys(definitions._build_combined_defs())
    dbt_keys = [k for k in keys if k.startswith("dbt/")]
    assert dbt_keys, f"demo mode is on but no dbt assets were built: {keys}"
    # The seed is an asset too -- it is what makes the project build on a
    # clean database, and its lineage edge is what DataHub ingests.
    assert any(k.endswith("demo_orders") for k in dbt_keys), dbt_keys
    assert any(k.endswith("demo_customer_orders") for k in dbt_keys), dbt_keys
    assert any(k.endswith("demo_regional_totals") for k in dbt_keys), dbt_keys


def test_demo_asset_keys_go_through_the_normalization_registry(
    monkeypatch, tmp_path, dbt_on_path
):
    """The @dbt_assets path does not call the component's get_asset_spec, so
    the surface installs a translator that applies the same registry. Without
    it the demo surface and a YAML-configured component would disagree on
    asset keys for the same project.
    """
    monkeypatch.setenv("DAG_TOOLS_DEMO_DBT_TARGET_DIR", str(tmp_path / "target"))
    keys = _asset_keys(demo_dbt_assets.build_demo_dbt_defs())
    assert keys, "the demo surface built nothing"
    assert all(k.startswith("dbt/") for k in keys), (
        f"keys bypassed AssetNormalizationRegistry: {keys}"
    )


# ---------------------------------------------------------------------------
# Failure tolerance
# ---------------------------------------------------------------------------
#
# A code location that fails to load stops EVERY materialization in the
# deployment, not just this surface. This one has already been taken down
# once by a dagster-dbt private hook moving, so every failure mode here must
# degrade to "no demo dbt assets" rather than raise.


def test_a_missing_project_directory_degrades_instead_of_raising(monkeypatch):
    monkeypatch.setattr(
        demo_dbt_assets, "DEMO_DBT_PROJECT_DIR", Path("/nonexistent/demo_dbt")
    )
    defs = demo_dbt_assets.build_demo_dbt_defs()
    assert _asset_keys(defs) == []


def test_an_unparseable_project_degrades_instead_of_raising(
    monkeypatch, tmp_path
):
    broken = tmp_path / "broken_dbt"
    (broken / "models").mkdir(parents=True)
    (broken / "dbt_project.yml").write_text("this: is: not: a: dbt project\n")
    monkeypatch.setattr(demo_dbt_assets, "DEMO_DBT_PROJECT_DIR", broken)
    monkeypatch.setenv("DAG_TOOLS_DEMO_DBT_TARGET_DIR", str(tmp_path / "target"))

    defs = demo_dbt_assets.build_demo_dbt_defs()
    assert _asset_keys(defs) == []


def test_the_shipped_project_is_package_data():
    """The project lives inside the package so it survives the wheel build.

    If it is ever moved outside `dag_tools/`, the deployed image gets a
    module that logs "project is missing" forever.
    """
    root = demo_dbt_assets.DEMO_DBT_PROJECT_DIR
    assert (root / "dbt_project.yml").is_file()
    assert (root / "profiles.yml").is_file()
    assert (root / "seeds" / "demo_orders.csv").is_file()
    assert "dag_tools" in root.parts


def test_the_demo_project_declares_no_sources():
    """Seeds, not sources -- deliberately.

    `execute()` runs `dbt source snapshot-freshness` BEFORE `dbt build`, so a
    source pointing at a raw table would fail the run before the build could
    create that table, and the demo would never work on a fresh cluster.
    """
    import yaml

    for path in (demo_dbt_assets.DEMO_DBT_PROJECT_DIR / "models").glob("*.yml"):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        assert not doc.get("sources"), (
            f"{path.name} declares dbt sources; freshness runs before build, "
            f"so the demo would fail on a database where they do not exist"
        )
