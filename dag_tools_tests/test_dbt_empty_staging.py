"""Regressions for the dbt ``--empty`` staging path (Phase B2).

``cli_args: ["build", "--empty"]`` in component.yaml is already supported by
dagster-dbt; it is safe ONLY because ``CustomDbtProjectComponent.execute()``
hands ``get_cli_args()`` to the build invocation and nowhere else. ``--empty``
exists on dbt ``build`` / ``compile`` / ``run`` only -- ``docs generate`` has
the lookalike ``--empty-catalog`` and ``source snapshot-freshness`` has
neither. These tests pin that scoping. dagster_dbt is required (CI installs it).
"""

from pathlib import Path

import pytest

pytest.importorskip("dagster_dbt")

import dagster as dg  # noqa: E402

from dag_tools_tests.test_dbt_datahub_artifacts import (  # noqa: E402
    _component,
    _FakeDbt,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


class _RecordingDbt(_FakeDbt):
    """_FakeDbt that also records the kwargs each cli() call received."""

    def __init__(self, project_dir):
        super().__init__(project_dir)
        self.kwargs = []

    def cli(self, args, context=None, target_path=None, **kwargs):
        self.kwargs.append(kwargs)
        return super().cli(args, context=context, target_path=target_path, **kwargs)


def _drive(tmp_path, monkeypatch, build_args):
    from dag_tools.components.dbt_project import component as component_module

    class _FakePopen:
        def __init__(self, cmd, cwd=None, **kwargs):
            self.returncode = 0

        def communicate(self):
            return (b"", None)

    monkeypatch.setattr(component_module, "Popen", _FakePopen)
    dbt = _RecordingDbt(tmp_path)
    comp = _component()
    comp.get_cli_args = lambda context: list(build_args)
    list(comp.execute(dg.build_asset_context(), dbt))
    return dbt


def _args(dbt):
    return [inv._args for inv in dbt.invocations]


def test_resolved_cli_args_reach_only_the_build_invocation(tmp_path, monkeypatch):
    dbt = _drive(tmp_path, monkeypatch, ["build", "--empty"])
    calls = _args(dbt)

    builds = [a for a in calls if a[0] == "build"]
    assert len(builds) == 1
    assert "--empty" in builds[0]

    freshness = [a for a in calls if a[:2] == ["source", "snapshot-freshness"]]
    docs = [a for a in calls if a[:2] == ["docs", "generate"]]
    assert len(freshness) == 1 and len(docs) == 1
    assert "--empty" not in freshness[0]
    assert "--empty" not in docs[0]


def test_the_non_build_invocations_are_hardcoded_and_unparameterized(
    tmp_path, monkeypatch
):
    dbt = _drive(
        tmp_path, monkeypatch, ["build", "--empty", "--select", "nonsense"]
    )
    calls = _args(dbt)
    assert ["source", "snapshot-freshness"] in calls
    assert ["docs", "generate"] in calls
    # Nothing but those two and the build: no fourth call, no sprayed args.
    assert len(calls) == 3


def test_empty_is_not_a_global_config_flag(tmp_path, monkeypatch):
    """`dbt --empty build` is invalid: --empty is a subcommand option."""
    dbt = _drive(tmp_path, monkeypatch, ["build", "--empty"])
    build = next(a for a in _args(dbt) if a[0] == "build")
    # Carried AFTER the subcommand, never in front of it.
    assert build.index("--empty") > build.index("build")
    # The component routes nothing through global flags today; if someone adds
    # any, this fails so they must prove --empty is not among them.
    for kwargs in dbt.kwargs:
        assert "global_config_flags" not in kwargs
    for a in _args(dbt):
        assert a[0] in {"build", "source", "docs"}, "a flag precedes the subcommand"


def test_empty_makes_dbt_tests_vacuous_is_documented():
    """Guard the honesty item in docs/RECIPE.md.

    Under ``--empty`` models build with ``limit 0`` but dbt tests still run and
    pass vacuously on empty relations; Q6 then compares two sets of vacuous
    passes. The hazard is a documentation invariant, and a silent doc deletion
    is how it becomes a trap.
    """
    text = (REPO_ROOT / "docs" / "RECIPE.md").read_text(encoding="utf-8")
    assert "not evidence of data correctness" in text
