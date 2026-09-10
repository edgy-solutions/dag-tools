"""Reading a defs.yaml into a runnable pipeline, and saying what it needs.

The dev-loop gap this closes: a dlt pipeline is YAML only the factory can
interpret, so "what env vars does this need" meant reading the component
YAML and the factory, and "run just this one pipeline" meant loading the
whole code location.

Both are derivable from the file. These tests pin that derivation --
including the cases where it should REFUSE rather than guess, because a
config that means one thing locally and another when deployed is a worse
dev loop than none.
"""
import pytest

pytest.importorskip("yaml")
# build_runnables_from_defs imports the dlt factory lazily; several tests
# below reach it.
pytest.importorskip("dlt")

from dag_tools.dlt_dev import (
    DefsError,
    load_defs,
    missing_env_vars,
    pipeline_names,
    referenced_env_vars,
)


DEFS = """\
type: dag_tools.DltPipelineComponent
attributes:
  source_config:
    drivername: sqlite
    credentials: "{{ env.SRC_URL }}"
    password:
      env: SRC_PASSWORD
  dest_config:
    credentials: "{{ env.DEST_URL }}"
  pipelines:
    one:
      sources: [widgets]
    two:
      sources: [gadgets]
"""


@pytest.fixture
def defs_file(tmp_path):
    path = tmp_path / "defs.yaml"
    path.write_text(DEFS, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# What does this file need?
# ---------------------------------------------------------------------------


def test_both_declaration_forms_are_found():
    """Config declares credentials two ways -- `{{ env.NAME }}` in the text
    and `{env: NAME}` as a mapping the factory resolves. A report covering
    only one would send someone hunting for the other."""
    names = referenced_env_vars(DEFS)
    assert "SRC_URL" in names
    assert "DEST_URL" in names
    assert "SRC_PASSWORD" in names, "the {env: NAME} mapping form was missed"


def test_the_order_is_the_file_order():
    """Deterministic so a diff of two reports is readable."""
    assert referenced_env_vars(DEFS) == referenced_env_vars(DEFS)


def test_duplicates_are_reported_once():
    raw = 'a: "{{ env.X }}"\nb: "{{ env.X }}"\n'
    assert referenced_env_vars(raw) == ["X"]


def test_missing_is_computed_against_the_environment(monkeypatch):
    monkeypatch.setenv("SRC_URL", "sqlite://")
    monkeypatch.delenv("DEST_URL", raising=False)
    monkeypatch.delenv("SRC_PASSWORD", raising=False)
    missing = missing_env_vars(referenced_env_vars(DEFS))
    assert "SRC_URL" not in missing
    assert "DEST_URL" in missing


def test_an_empty_value_counts_as_missing(monkeypatch):
    """An exported-but-blank variable is the confusing case: set enough to
    look configured, empty enough to fail at connect time."""
    monkeypatch.setenv("DEST_URL", "")
    assert "DEST_URL" in missing_env_vars(["DEST_URL"])


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_env_templates_resolve(defs_file, monkeypatch):
    monkeypatch.setenv("SRC_URL", "sqlite:///x.db")
    monkeypatch.setenv("DEST_URL", "duckdb:///y.duckdb")
    doc, names = load_defs(str(defs_file))
    assert doc["attributes"]["source_config"]["credentials"] == "sqlite:///x.db"
    assert set(names) >= {"SRC_URL", "DEST_URL"}


def test_an_unset_variable_substitutes_empty_rather_than_raising(
    defs_file, monkeypatch,
):
    """So `dagtools dlt env` can report the WHOLE list at once instead of
    failing on the first one."""
    monkeypatch.delenv("SRC_URL", raising=False)
    doc, _ = load_defs(str(defs_file))
    assert doc["attributes"]["source_config"]["credentials"] == ""


def test_templating_beyond_env_is_refused(tmp_path):
    """Rendering it here would give a different answer than Dagster does.
    A config that means one thing locally and another when deployed is
    worse than a refusal, and the refusal names the line."""
    path = tmp_path / "defs.yaml"
    path.write_text('attributes:\n  x: "{{ 1 + 1 }}"\n', encoding="utf-8")
    with pytest.raises(DefsError, match="templating beyond"):
        load_defs(str(path))


def test_pipelines_are_listed(defs_file, monkeypatch):
    monkeypatch.setenv("SRC_URL", "sqlite://")
    monkeypatch.setenv("DEST_URL", "duckdb://")
    doc, _ = load_defs(str(defs_file))
    assert pipeline_names(doc) == ["one", "two"]


# ---------------------------------------------------------------------------
# Choosing a pipeline, and refusing to guess
# ---------------------------------------------------------------------------


def _build(path, **kw):
    from dag_tools.dlt_dev import build_runnables_from_defs

    return build_runnables_from_defs(str(path), **kw)


def test_several_pipelines_without_a_choice_is_refused(defs_file, monkeypatch):
    """Picking one at random would run the wrong extraction against a real
    source."""
    monkeypatch.setenv("SRC_URL", "sqlite://")
    monkeypatch.setenv("DEST_URL", "duckdb://")
    with pytest.raises(DefsError, match="--pipeline"):
        _build(defs_file)


def test_an_unknown_pipeline_lists_the_real_ones(defs_file, monkeypatch):
    monkeypatch.setenv("SRC_URL", "sqlite://")
    monkeypatch.setenv("DEST_URL", "duckdb://")
    with pytest.raises(DefsError, match="one, two"):
        _build(defs_file, pipeline="three")


def test_a_file_with_no_pipelines_says_so(tmp_path):
    path = tmp_path / "defs.yaml"
    path.write_text("attributes:\n  source_config: {}\n", encoding="utf-8")
    with pytest.raises(DefsError, match="no pipelines"):
        _build(path)


# ---------------------------------------------------------------------------
# The destination override
# ---------------------------------------------------------------------------


def test_only_local_destinations_are_accepted(tmp_path, monkeypatch):
    """A local run must not be able to point at something needing
    credentials -- the real destination comes from the defs file, and the
    override exists to get AWAY from it."""
    from dag_tools.dlt_dev.loader import _destination

    with pytest.raises(DefsError, match="unrecognised --destination"):
        _destination("postgresql://user:pw@prod/db")


@pytest.mark.parametrize("spec", ["./dev.duckdb", "duckdb:///tmp/d.duckdb", ":memory:"])
def test_duckdb_forms_are_accepted(spec):
    pytest.importorskip("dlt")
    from dag_tools.dlt_dev.loader import _destination

    assert _destination(spec) is not None


def test_no_override_leaves_the_configured_destination():
    from dag_tools.dlt_dev.loader import _destination

    assert _destination(None) is None
