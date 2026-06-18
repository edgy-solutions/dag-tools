"""Unit tests for the probe source generator."""
import ast
from datetime import datetime, timezone

from dag_tools.qual.classes import (
    ClassKeyComponents,
    ClassMatrix,
    EquivalenceClass,
    Representative,
    Runnability,
)
from dag_tools.qual.synthetic import (
    ProbeStatus,
    generate_probe_module,
    generate_probe_source,
)


def _class(
    *,
    class_hash="abc123def456",
    io_manager_class="dag_tools.io_managers.arrow.ConfigurableArrowIOManager",
    runnability=Runnability.SYNTHETIC_REQUIRED,
    partitions_def_class=None,
):
    rep = Representative(
        repo="alpha", git_sha="sha",
        asset_key=["probe", "target"],
        runnability=runnability,
        runnability_reason="default",
    )
    return EquivalenceClass(
        class_hash=class_hash,
        key=ClassKeyComponents(
            compute_kind="python",
            io_manager_class=io_manager_class,
            partitions_def_class=partitions_def_class,
        ),
        member_count=1, member_repo_count=1,
        members=[], representatives=[rep],
    )


# ---------------------------------------------------------------------------
# Module-level generation
# ---------------------------------------------------------------------------


def test_generate_probe_module_returns_metadata_and_source():
    cls = _class()
    probe, source = generate_probe_module(cls, qual_id="q1")
    assert probe is not None
    assert probe.class_hash == cls.class_hash
    assert probe.module_name == "probe_abc123de"  # first 8 hex chars
    assert probe.io_manager_class == cls.key.io_manager_class
    assert probe.status == ProbeStatus.GENERATED
    assert "class_hash" in source
    assert "Definitions" in source


def test_generate_probe_module_skips_non_synthetic():
    cls = _class(runnability=Runnability.RUNNABLE)
    probe, reason = generate_probe_module(cls, qual_id="q1")
    assert probe is None
    assert "not synthetic_required" in reason


def test_generate_probe_module_skips_class_without_representatives():
    cls = _class()
    cls.representatives = []
    probe, reason = generate_probe_module(cls, qual_id="q1")
    assert probe is None
    assert "no representatives" in reason


def test_partitions_def_observed_is_noted_not_synthesized_in_v1():
    cls = _class(partitions_def_class="dagster.DailyPartitionsDefinition")
    probe, source = generate_probe_module(cls, qual_id="q1")
    assert probe is not None
    assert any("partitions def" in n for n in probe.notes)
    assert "not synthesized" in source or "not synthesized" in " ".join(probe.notes)


# ---------------------------------------------------------------------------
# Source-text properties
# ---------------------------------------------------------------------------


def test_generated_source_parses_as_python():
    """The generator's output MUST be valid Python; otherwise the dag-
    tools-probes code location won't even load."""
    cls = _class()
    _, source = generate_probe_module(cls, qual_id="q1")
    ast.parse(source)  # raises SyntaxError on failure


def test_generated_source_imports_real_io_manager_by_fqn():
    cls = _class(io_manager_class="dag_tools.io_managers.arrow.ConfigurableArrowIOManager")
    _, source = generate_probe_module(cls, qual_id="q1")
    assert "from dag_tools.io_managers.arrow import ConfigurableArrowIOManager" in source


def test_generated_source_has_inmemory_fallback_for_missing_io_manager():
    """When io_manager_class is None, the probe still loads."""
    cls = _class(io_manager_class=None)
    _, source = generate_probe_module(cls, qual_id="q1")
    assert "InMemoryIOManager" in source
    # No try/except is needed when there's no real class to import.
    assert "_IO_MANAGER_ERROR = None" in source


def test_generated_source_handles_malformed_io_manager_fqn():
    """A class FQN with no dots (just a bare class name) gets the
    InMemoryIOManager fallback + a recorded error."""
    cls = _class(io_manager_class="WeirdSingleNameClass")
    _, source = generate_probe_module(cls, qual_id="q1")
    assert "InMemoryIOManager" in source
    assert "malformed io_manager_class FQN" in source


def test_generated_source_round_trips_class_hash_constant():
    cls = _class(class_hash="ff00aa11bb22")
    _, source = generate_probe_module(cls, qual_id="q1")
    assert 'CLASS_HASH = "ff00aa11bb22"' in source


def test_each_probe_uses_class_unique_io_manager_resource_key():
    """The dag-tools-probes location merges every probe's Definitions; a
    shared 'io_manager' key would clobber across classes. The generator
    must emit per-class keys like 'io_manager_<short>'."""
    cls = _class(class_hash="abcd1234ffff")
    _, source = generate_probe_module(cls, qual_id="q1")
    assert 'IO_MANAGER_KEY = "io_manager_abcd1234"' in source
    # And the bare default ' "io_manager"' key MUST NOT appear elsewhere
    # (would silently re-collide on merge).
    assert '"io_manager"' not in source


def test_generated_source_defines_upstream_and_downstream_assets():
    cls = _class(class_hash="hash5678")
    _, source = generate_probe_module(cls, qual_id="q1")
    assert "def probe_hash5678_upstream" in source
    assert "def probe_hash5678_downstream" in source
    # Downstream depends on upstream
    assert "deps=[probe_hash5678_upstream]" in source


def test_generate_probe_source_is_self_contained():
    """The probe module imports only `dagster` + the IO manager FQN. Any
    `dag_tools.qual.*` import would bind the probes location's deploy
    cycle to the dag-tools release cadence — keep them decoupled."""
    cls = _class()
    _, source = generate_probe_module(cls, qual_id="q1")
    assert "dag_tools.qual" not in source


def test_custom_dbt_translator_is_called_out_in_notes():
    cls = _class()
    cls.key.custom_dbt_translator_classes = ["myco.MyTranslator"]
    probe, source = generate_probe_module(cls, qual_id="q1")
    assert probe is not None
    assert any("dbt" in n and "translator" in n for n in probe.notes)


# ---------------------------------------------------------------------------
# Direct generate_probe_source (the lower-level API)
# ---------------------------------------------------------------------------


def test_generate_probe_source_includes_qual_id_in_docstring():
    cls = _class()
    source = generate_probe_source(
        cls=cls, module_name="probe_x", qual_id="q-2026-06-17",
        notes=["a note"],
    )
    assert "q-2026-06-17" in source
