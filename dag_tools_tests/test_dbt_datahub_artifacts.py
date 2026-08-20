"""The dbt -> DataHub handoff is a FILE handoff, and dagster-dbt scatters files.

``DbtCliResource.cli()`` mints a *fresh* target directory per invocation
(``_get_unique_target_path``: ``target/<op>-<run>-<uuid>``) unless the caller
passes ``target_path=`` explicitly. ``CustomDbtProjectComponent.execute``
shells out to dbt three times, then writes a DataHub recipe whose source
paths are all relative to ONE directory and runs ``datahub ingest`` with that
directory as cwd. Every artifact the recipe names therefore has to land in
that one directory, or ingestion dies on a missing file.

These tests pin that invariant. They drive the real ``execute`` with a fake
resource that reproduces dagster-dbt's directory semantics by calling the
library's own ``_get_unique_target_path``, so the fake cannot drift from the
behaviour it stands in for.
"""

import json
from pathlib import Path

import pytest

pytest.importorskip("dagster_dbt")

import dagster as dg  # noqa: E402


# Artifacts each dbt subcommand drops in its target directory. `docs
# generate` rewriting run_results.json is why the component stashes a
# run_results_build.json copy before invoking it.
_ARTIFACTS_BY_COMMAND = {
    "source": ["sources.json", "run_results.json", "manifest.json"],
    "build": ["manifest.json", "run_results.json"],
    "docs": ["catalog.json", "manifest.json", "run_results.json", "index.html"],
}

_MANIFEST = {"metadata": {"adapter_type": "postgres"}, "nodes": {}}


class _FakeInvocation:
    def __init__(self, target_path: Path, args):
        self.target_path = target_path
        self._args = args

    def stream(self):
        # dbt writes its artifacts into the target dir, then we yield nothing:
        # the real stream yields Output/AssetMaterialization events, and this
        # test is about files, not events.
        self.target_path.mkdir(parents=True, exist_ok=True)
        for name in _ARTIFACTS_BY_COMMAND[self._args[0]]:
            payload = _MANIFEST if name == "manifest.json" else {"command": self._args}
            self.target_path.joinpath(name).write_text(json.dumps(payload))
        return iter(())

    @property
    def manifest(self):
        return _MANIFEST


class _FakeDbt:
    """Stands in for DbtCliResource, borrowing its real target-path logic."""

    def __init__(self, project_dir: Path):
        self.project_dir = project_dir
        self.invocations = []

    def cli(self, args, context=None, target_path=None, **kwargs):
        if target_path is None:
            from dagster_dbt import DbtCliResource

            # The behaviour under test comes from the library, not from us.
            target_path = self.project_dir / DbtCliResource._get_unique_target_path(
                self, context=None
            )
        invocation = _FakeInvocation(Path(target_path), list(args))
        self.invocations.append(invocation)
        return invocation


def _component(datahub_server="http://datahub-gms:8080"):
    from dag_tools.components.dbt_project.component import CustomDbtProjectComponent

    # Same construction trick as test_k8s_env_prefix.py: skip dbt project
    # resolution and exercise one method.
    comp = CustomDbtProjectComponent.__new__(CustomDbtProjectComponent)
    comp.datahub_config = {"server": datahub_server}
    comp.k8s_resource_env_prefix = None
    comp.k8s_default_cpu = "500m"
    comp.k8s_default_mem = "1Gi"
    comp.op = None
    comp.select = "fqn:*"
    comp.exclude = ""
    comp.selector = ""
    comp.get_cli_args = lambda context: ["build"]
    return comp


def _run_execute(tmp_path, monkeypatch):
    """Drive execute() to completion, capturing the datahub ingest call."""
    from dag_tools.components.dbt_project import component as component_module

    calls = {}

    class _FakePopen:
        def __init__(self, cmd, cwd=None, **kwargs):
            calls["cmd"] = cmd
            calls["cwd"] = Path(cwd)
            self.returncode = 0

        def communicate(self):
            return (b"", None)

    monkeypatch.setattr(component_module, "Popen", _FakePopen)

    dbt = _FakeDbt(tmp_path)
    comp = _component()
    context = dg.build_asset_context()
    list(comp.execute(context, dbt))
    return calls


def test_every_artifact_the_datahub_recipe_names_is_in_the_ingest_cwd(
    tmp_path, monkeypatch
):
    """The bug: ``dbt source snapshot-freshness`` runs in its own target dir,
    so ``sources.json`` never reaches the directory ``datahub ingest`` reads
    from, and ingestion fails with a missing-file error.
    """
    import yaml

    calls = _run_execute(tmp_path, monkeypatch)
    run_dir = calls["cwd"]

    recipe = yaml.safe_load((run_dir / "recipe.yaml").read_text())
    config = recipe["source"]["config"]

    referenced = [
        config["manifest_path"],
        config["catalog_path"],
        config["sources_path"],
        *config["run_results_paths"],
    ]
    missing = [p for p in referenced if not (run_dir / p).exists()]
    assert not missing, (
        f"datahub ingest runs with cwd={run_dir} and would fail on {missing}; "
        f"the directory only holds {sorted(p.name for p in run_dir.iterdir())}"
    )


def test_dbt_artifacts_are_not_scattered_across_target_directories(
    tmp_path, monkeypatch
):
    """Every dbt invocation in the datahub path must share one target dir."""
    from dag_tools.components.dbt_project import component as component_module

    monkeypatch.setattr(
        component_module,
        "Popen",
        type(
            "_P",
            (),
            {
                "__init__": lambda self, cmd, cwd=None, **kw: setattr(
                    self, "returncode", 0
                ),
                "communicate": lambda self: (b"", None),
            },
        ),
    )

    dbt = _FakeDbt(tmp_path)
    comp = _component()
    list(comp.execute(dg.build_asset_context(), dbt))

    target_paths = {inv.target_path for inv in dbt.invocations}
    assert len(dbt.invocations) == 3, "expected freshness + build + docs generate"
    assert len(target_paths) == 1, (
        "dbt artifacts are split across target directories, so no single cwd "
        f"can satisfy the datahub recipe: {sorted(str(p) for p in target_paths)}"
    )


def test_run_results_from_build_survive_docs_generate(tmp_path, monkeypatch):
    """``docs generate`` rewrites run_results.json; the recipe must still see
    the *build*'s results, which is what carries the materialization lineage.
    """
    calls = _run_execute(tmp_path, monkeypatch)
    run_dir = calls["cwd"]

    saved = json.loads((run_dir / "run_results_build.json").read_text())
    assert saved["command"] == ["build"], (
        "run_results_build.json should hold the build's results, not "
        f"{saved['command']}'s"
    )


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
#
# The sandbox GMS runs with METADATA_SERVICE_AUTH_ENABLED=true, which is the
# correct posture. Against such a server an unauthenticated recipe fails in a
# genuinely confusing way: `infer_dbt_schemas` reads schemaMetadata back out
# of GMS *before* emitting anything, so the 401 surfaces as a source-side
# HTTPError with "produced 0 events" -- it does not look like an auth problem
# with the sink. DatahubLineageComponent already resolves a token the same
# way; these pin the dbt path to that convention.


def _recipe_for(datahub_config, tmp_path, monkeypatch):
    """Run just the publish step and return the recipe it wrote."""
    import yaml

    from dag_tools.components.dbt_project import component as component_module

    class _FakePopen:
        def __init__(self, cmd, cwd=None, **kwargs):
            self.returncode = 0

        def communicate(self):
            return (b"", None)

    monkeypatch.setattr(component_module, "Popen", _FakePopen)

    comp = _component()
    comp.datahub_config = datahub_config
    comp._publish_to_datahub(tmp_path, dg.build_asset_context(), "postgres")
    return yaml.safe_load((tmp_path / "recipe.yaml").read_text())


def test_recipe_carries_the_datahub_token_from_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAHUB_TOKEN", "pat-from-env")
    recipe = _recipe_for({"server": "http://gms:8080"}, tmp_path, monkeypatch)
    assert recipe["sink"]["config"]["token"] == "pat-from-env"


def test_an_explicit_token_beats_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAHUB_TOKEN", "pat-from-env")
    recipe = _recipe_for(
        {"server": "http://gms:8080", "token": "pat-explicit"}, tmp_path, monkeypatch
    )
    assert recipe["sink"]["config"]["token"] == "pat-explicit"


def test_recipe_omits_the_token_when_there_is_none(tmp_path, monkeypatch):
    monkeypatch.delenv("DATAHUB_TOKEN", raising=False)
    recipe = _recipe_for({"server": "http://gms:8080"}, tmp_path, monkeypatch)
    # An absent key, not an empty string: datahub-rest treats "" as a
    # credential and sends an Authorization header that every GMS rejects.
    assert "token" not in recipe["sink"]["config"]
    assert recipe["sink"]["config"]["server"] == "http://gms:8080"
