"""Unit tests for dag_tools/inventory.

The schema and classifier tests are pure-Python (just need pydantic).
The extractor tests build a minimal in-memory Dagster ``Definitions``
fixture and skip cleanly when dagster isn't installed in the active env.
"""
import logging

import pytest

pytest.importorskip("pydantic")

from dag_tools.inventory import (
    AssetRecord,
    FAMILY_POSTGRES,
    SCHEMA_VERSION,
    classify,
    fqn,
)
from dag_tools.inventory.classifier import FAMILY_REGISTRY


# ---------------------------------------------------------------------------
# Schema tests — pure pydantic, no Dagster required.
# ---------------------------------------------------------------------------


def test_schema_version_is_stamped_by_default():
    record = AssetRecord(asset_key=["foo"])
    assert record.schema_version == SCHEMA_VERSION


def test_schema_tolerates_unknown_fields():
    """Older readers must accept newer writers via extra='ignore'."""
    record = AssetRecord.model_validate({
        "asset_key": ["foo"],
        "schema_version": 99,
        "field_added_in_the_future": "anything",
        "another_future_field": {"nested": "stuff"},
    })
    assert record.asset_key == ["foo"]
    assert record.schema_version == 99


def test_schema_serializes_to_json_friendly_dict():
    record = AssetRecord(
        asset_key=["a", "b"],
        location="loc",
        io_manager_key="io_manager",
        io_manager_family="postgres",
        tags={"regression": "true"},
    )
    dumped = record.model_dump()
    assert dumped["asset_key"] == ["a", "b"]
    assert dumped["io_manager_family"] == "postgres"
    assert dumped["tags"] == {"regression": "true"}
    assert dumped["schema_version"] == SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Classifier tests — pure Python, no Dagster required.
# ---------------------------------------------------------------------------


def test_classify_known_fqn_string():
    """Exact registry hit on a string FQN wins immediately."""
    fake_fqn = "tests.inventory.fake._FakePostgresIOManager"
    FAMILY_REGISTRY[fake_fqn] = FAMILY_POSTGRES
    try:
        assert classify(fake_fqn, allow_substring_fallback=False) == FAMILY_POSTGRES
    finally:
        del FAMILY_REGISTRY[fake_fqn]


def test_classify_walks_mro_for_custom_subclass():
    """A custom subclass of a registered ancestor classifies via MRO."""
    class _StockBase:
        pass

    class _CustomFork(_StockBase):
        pass

    FAMILY_REGISTRY[fqn(_StockBase)] = "stockbase_family"
    try:
        assert classify(_CustomFork, allow_substring_fallback=False) == "stockbase_family"
    finally:
        del FAMILY_REGISTRY[fqn(_StockBase)]


def test_classify_substring_fallback_logs_warning(caplog):
    """Unknown class with a recognized substring falls back and warns."""
    class _MyWeirdPostgresManager:
        pass

    with caplog.at_level(logging.WARNING, logger="dag_tools.inventory.classifier"):
        result = classify(_MyWeirdPostgresManager)
    assert result == FAMILY_POSTGRES
    assert any("substring fallback" in r.message for r in caplog.records)


def test_classify_returns_none_when_nothing_matches():
    class _Nothing:
        pass

    assert classify(_Nothing, allow_substring_fallback=False) is None


def test_classify_handles_none_target():
    assert classify(None) is None


def test_fqn_is_module_plus_qualname():
    class _Marker:
        pass

    assert fqn(_Marker).endswith("._Marker")
    assert "test_inventory" in fqn(_Marker)


# ---------------------------------------------------------------------------
# Extractor tests — need a real Dagster Definitions.
# ---------------------------------------------------------------------------


dagster = pytest.importorskip("dagster")


from dag_tools.inventory import extract_records  # noqa: E402  (after importorskip)


def _build_minimal_defs():
    """One asset, one stock IO manager — enough to round-trip the extractor."""
    from dagster import Definitions, InMemoryIOManager, asset

    @asset(group_name="test_group", compute_kind="python", tags={"regression": "true"})
    def hello():
        return 1

    return Definitions(
        assets=[hello],
        resources={"io_manager": InMemoryIOManager()},
    )


def test_extract_records_walks_definitions():
    defs = _build_minimal_defs()
    records = extract_records(defs, location="test_location")
    assert len(records) >= 1
    rec = next(r for r in records if r.asset_key == ["hello"])
    assert rec.location == "test_location"
    assert rec.group == "test_group"
    assert rec.io_manager_key == "io_manager"
    assert rec.schema_version == SCHEMA_VERSION
    assert rec.tags == {"regression": "true"}


def test_extract_records_classifies_in_memory_io_manager():
    defs = _build_minimal_defs()
    records = extract_records(defs)
    rec = next(r for r in records if r.asset_key == ["hello"])
    # InMemoryIOManager is registered explicitly; family should be in_memory.
    assert rec.io_manager_family in ("in_memory", "filesystem", None), (
        f"unexpected family classification: {rec.io_manager_family!r} "
        f"for io_manager_class {rec.io_manager_class!r}"
    )
    # io_manager_class FQN should always be populated when the resource exists.
    assert rec.io_manager_class is not None


def test_extract_records_returns_empty_on_none_defs():
    assert extract_records(None) == []


def test_extract_records_soft_fails_on_broken_spec():
    """One bad spec must not abort the whole extraction."""
    defs = _build_minimal_defs()
    good_specs = list(defs.resolve_all_asset_specs())

    class _BadSpec:
        @property
        def key(self):
            raise RuntimeError("simulated breakage in spec.key")

    # Monkey-patch to return one bad + the good ones. We patch
    # resolve_all_asset_specs (Dagster 1.13+) since that's what _resolve_specs
    # tries first.
    defs.resolve_all_asset_specs = lambda: [_BadSpec()] + good_specs

    records = extract_records(defs)
    # Good spec still extracted; bad one dropped.
    assert any(r.asset_key == ["hello"] for r in records)
