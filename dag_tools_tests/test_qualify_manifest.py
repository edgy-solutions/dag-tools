"""Schema-level tests for the qualification manifest. No registry needed."""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pydantic")

from dag_tools.qual.qualify import (
    SCHEMA_VERSION,
    CoUpgradeRisk,
    Deployment,
    InventoryPin,
    QualificationManifest,
    Selection,
    VersionTarget,
)


def _minimal_manifest(**over) -> QualificationManifest:
    base = dict(
        qual_id="2026-06-15-test",
        created_at=datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
    )
    base.update(over)
    return QualificationManifest(**base)


def test_schema_version_is_stamped():
    m = _minimal_manifest()
    assert m.schema_version == SCHEMA_VERSION


def test_co_upgrade_risk_uses_from_to_aliases():
    """Pydantic aliases ``from_version`` -> ``from`` (and ``to_version`` ->
    ``to``) so the on-disk YAML matches the recipe sample shape and Python
    keyword conflicts are avoided."""
    risk = CoUpgradeRisk(lib="dbt-core", **{"from": "1.8.5", "to": "1.9.0"})
    assert risk.from_version == "1.8.5"
    assert risk.to_version == "1.9.0"
    dumped = risk.model_dump(by_alias=True)
    assert dumped["from"] == "1.8.5"
    assert dumped["to"] == "1.9.0"
    assert "from_version" not in dumped


def test_manifest_dumps_with_aliases():
    """Round trip: by_alias=True produces ``from``/``to`` keys, by default
    Pydantic uses the Python field names. The init orchestrator uses
    by_alias=True for both YAML serialization AND the CLI JSON output."""
    m = _minimal_manifest(
        co_upgrade_risks=[
            CoUpgradeRisk(lib="dbt-core", **{"from": "1.8.5", "to": "1.9.0"}),
        ]
    )
    dumped = m.model_dump(by_alias=True)
    risk = dumped["co_upgrade_risks"][0]
    assert risk["from"] == "1.8.5"
    assert risk["to"] == "1.9.0"


def test_manifest_tolerates_unknown_fields():
    """ADR-2: extra='ignore' so older readers can ingest newer writers."""
    m = QualificationManifest.model_validate({
        "qual_id": "future-qual",
        "created_at": "2026-06-15T12:00:00+00:00",
        "baseline": {"dagster": "1.10.6"},
        "candidate": {"dagster": "1.12.1"},
        "field_from_the_future": {"nested": "anything"},
        "schema_version": 99,
    })
    assert m.qual_id == "future-qual"
    assert m.schema_version == 99


def test_selection_has_recipe_defaults():
    """Recipe sample: prefer_tag='regression', reps_per_class=2."""
    s = Selection()
    assert s.prefer_tag == "regression"
    assert s.reps_per_class == 2


def test_inventory_pin_round_trip():
    pin = InventoryPin(
        repo="patriot",
        git_sha="abc123",
        pinned_timestamp=datetime(2026, 6, 15, tzinfo=timezone.utc),
    )
    dumped = pin.model_dump(mode="json")
    fresh = InventoryPin.model_validate(dumped)
    assert fresh.repo == "patriot"
    assert fresh.git_sha == "abc123"
    assert fresh.pinned_timestamp == pin.pinned_timestamp


def test_version_target_pins_default_empty():
    v = VersionTarget(dagster="1.10.6")
    assert v.pins == {}


def test_deployment_default_is_empty():
    d = Deployment()
    assert d.graphql_url is None
    assert d.auth is None
