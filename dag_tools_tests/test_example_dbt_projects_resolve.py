"""Every dbt project an example component.yaml points at must exist.

`examples/real_declarative_dbt` shipped for months pointing at
`../../dbt_projects/project_one` and `project_two`, neither of which existed
anywhere in the repo. Nothing caught it because examples are not imported by
the test suite and the component only resolves its `project:` path when
Dagster loads the location -- which nobody did for this example.

That is also why the `CustomDbtProjectComponent` -> DataHub path had never
run anywhere: the one example wiring it up could not load, and the deployed
`dag-tools` code location (`dag_tools/user_deployment/definitions.py`) builds
no dbt assets at all.

This is a cheap path check, not a dbt parse: it fails on a dangling
reference, which is the failure that actually happened.
"""

from pathlib import Path

import pytest
import yaml

EXAMPLES = Path(__file__).parent.parent / "examples"


def _component_yamls():
    if not EXAMPLES.is_dir():
        return []
    return [
        path
        for path in EXAMPLES.rglob("component*.yaml")
        # Skip vendored virtualenvs and Dagster's own cached defs state.
        if ".venv" not in path.parts and ".local_defs_state" not in path.parts
    ]


def _dbt_component_projects():
    """(component.yaml, resolved project dir) for every dbt component."""
    found = []
    for path in _component_yamls():
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:  # a malformed example is its own bug
            pytest.fail(f"{path} is not valid YAML: {exc}")
        if "dbt" not in str(doc.get("type", "")).lower():
            continue
        project = (doc.get("attributes") or {}).get("project")
        if isinstance(project, str) and "{{" not in project:
            # Dagster resolves a relative project path against the
            # directory holding component.yaml.
            found.append((path, (path.parent / project).resolve()))
    return found


def test_there_is_at_least_one_dbt_example():
    """Guards the regression from the other side: if the dbt examples are
    deleted rather than fixed, the check below passes vacuously.
    """
    assert _dbt_component_projects(), (
        "no example wires up a dbt component; the DataHub publishing path "
        "then has no worked example and nothing exercises it"
    )


@pytest.mark.parametrize(
    ("component_yaml", "project_dir"),
    _dbt_component_projects(),
    ids=lambda value: Path(value).name if isinstance(value, Path) else str(value),
)
def test_example_dbt_project_exists(component_yaml, project_dir):
    rel = component_yaml.relative_to(EXAMPLES.parent)
    assert project_dir.is_dir(), (
        f"{rel} points at {project_dir}, which does not exist -- the example "
        f"cannot load"
    )
    assert (project_dir / "dbt_project.yml").is_file(), (
        f"{rel} points at {project_dir}, which has no dbt_project.yml -- dbt "
        f"will not recognise it as a project"
    )
