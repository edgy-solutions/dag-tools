"""The dlt source-kind seam: ``source_config.type`` selects a SourceKind.

Every behaviour has a case and a control. The existing dlt test modules, run
unmodified, are the regression for "every existing YAML loads unchanged".
"""
import inspect
import logging
import sqlite3

import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")

import dlt
from dagster import AssetsDefinition

from dag_tools_tests._dlt_component_helpers import load_component
from dag_tools.asset_wrappers import dlt_assets_parsing as parsing
from dag_tools.asset_wrappers import source_kinds
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from dag_tools.asset_wrappers.source_kinds import (
    LEGACY,
    LEGACY_ALIASES,
    SOURCE_KINDS,
    SourceKind,
    resolve_source_kind,
)


@pytest.fixture(autouse=True)
def _fresh_warning_state(monkeypatch):
    monkeypatch.setattr(source_kinds, "_warned", set())


class RecordingKind(SourceKind):
    """Delegates to the legacy functions and records which hooks ran."""

    def __init__(self, fail_validate=False, extra=None):
        self.fail_validate = fail_validate
        self.extra = extra or {}
        self.calls = []

    def validate(self, source_config):
        self.calls.append("validate")
        if self.fail_validate:
            raise ValueError("fake kind refuses this block")

    def prepare_units(self, *args, **kwargs):
        self.calls.append("prepare_units")
        return LEGACY.prepare_units(*args, **kwargs)

    def build_source(self, *args, **kwargs):
        self.calls.append("build_source")
        return LEGACY.build_source(*args, **kwargs)

    def extra_defs(self, group_name, asset_keys, source_config, group_config):
        self.calls.append("extra_defs")
        return self.extra


@pytest.fixture
def configs(tmp_path):
    path = tmp_path / "source.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO widgets VALUES (1, 'a')")
    conn.commit()
    conn.close()
    source = {
        "type": "sql_database", "drivername": "sqlite",
        "credentials": f"sqlite:///{path}", "database": "main", "schema": "main",
    }
    dest = {
        "type": "duckdb", "drivername": "duckdb",
        "credentials": str(tmp_path / "w.duckdb"), "database": "warehouse", "schema": "raw",
    }
    return source, dest


def _group():
    return DltAssetGroupConfig(name="kinds", dest_schema="raw")


# 1 ---------------------------------------------------------------------------
def test_a_missing_type_resolves_to_legacy():
    assert resolve_source_kind({"drivername": "sqlite"}) is LEGACY


def test_an_exact_registered_kind_is_selected(monkeypatch):
    kind = RecordingKind()
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", kind)
    assert resolve_source_kind({"type": "fake_stream_kind"}) is kind


# 2 ---------------------------------------------------------------------------
@pytest.mark.parametrize("alias", sorted(LEGACY_ALIASES))
def test_a_legacy_alias_is_legacy_and_silent(alias, caplog):
    with caplog.at_level(logging.WARNING, logger=source_kinds.__name__):
        assert resolve_source_kind({"type": alias}) is LEGACY
    assert caplog.records == []


def test_an_unknown_type_is_legacy_with_exactly_one_warning(monkeypatch, caplog):
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", RecordingKind())
    with caplog.at_level(logging.WARNING, logger=source_kinds.__name__):
        assert resolve_source_kind({"type": "whatever_kind"}) is LEGACY
        assert resolve_source_kind({"type": "whatever_kind"}) is LEGACY
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "whatever_kind" in warnings[0].getMessage()
    assert "fake_stream_kind" in warnings[0].getMessage()


# 3 ---------------------------------------------------------------------------
def test_a_near_miss_of_a_registered_kind_fails_naming_it(monkeypatch):
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", RecordingKind())
    with pytest.raises(ValueError, match="fake_stream_kind"):
        resolve_source_kind({"type": "fake_stream_knd"})


def test_a_far_off_unknown_type_does_not_fail(monkeypatch):
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", RecordingKind())
    assert resolve_source_kind({"type": "zzz_unrelated"}) is LEGACY


def test_a_near_miss_of_a_legacy_alias_does_not_fail(monkeypatch):
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", RecordingKind())
    assert resolve_source_kind({"type": "postgress"}) is LEGACY


# 4 ---------------------------------------------------------------------------
def test_validate_raising_fails_create_dlt_assets_and_is_not_swallowed(monkeypatch, configs):
    kind = RecordingKind(fail_validate=True)
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", kind)
    source, dest = configs
    with pytest.raises(ValueError, match="refuses this block"):
        parsing.create_dlt_assets(
            ["widgets"], {**source, "type": "fake_stream_kind"}, dest, _group(),
        )
    assert "prepare_units" not in kind.calls  # validated BEFORE units are prepared


def test_validate_raising_fails_build_dlt_runnables(monkeypatch, configs):
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", RecordingKind(fail_validate=True))
    source, dest = configs
    with pytest.raises(ValueError, match="refuses this block"):
        parsing.build_dlt_runnables(
            ["widgets"], {**source, "type": "fake_stream_kind"}, dest, _group(),
        )


def test_the_kinds_build_source_is_used_on_both_paths(monkeypatch, configs, tmp_path):
    kind = RecordingKind()
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", kind)
    source, dest = configs
    source = {**source, "type": "fake_stream_kind"}

    assets = parsing.create_dlt_assets(["widgets"], source, dest, _group())
    assert any(isinstance(a, AssetsDefinition) for a in assets)  # the instantiate path ran
    assert kind.calls.count("build_source") == 1

    kind.calls.clear()
    parsing.build_dlt_runnables(
        ["widgets"], source, dest, _group(),
        destination_override=dlt.destinations.duckdb(str(tmp_path / "dev.duckdb")),
    )
    assert kind.calls.count("build_source") == 1
    assert kind.calls.count("validate") == 1


def test_control_a_legacy_type_never_touches_a_registered_kind(monkeypatch, configs):
    kind = RecordingKind()
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", kind)
    source, dest = configs
    parsing.create_dlt_assets(["widgets"], source, dest, _group())
    assert kind.calls == []


def test_extra_defs_reach_the_components_definitions(monkeypatch, tmp_path):
    import dagster as dg

    job = dg.define_asset_job("fake_extra_job", selection=dg.AssetSelection.assets(dg.AssetKey("x")))
    sched = dg.ScheduleDefinition(name="fake_extra_schedule", cron_schedule="0 * * * *", job=job)
    kind = RecordingKind(extra={"jobs": [job], "schedules": [sched]})
    monkeypatch.setitem(SOURCE_KINDS, "fake_stream_kind", kind)
    pipelines = {"p": {"name": "kinds", "dest_schema": "raw", "sources": ["widgets"]}}

    defs = load_component(tmp_path / "a", monkeypatch, source_type="fake_stream_kind", pipelines=pipelines)
    assert [j.name for j in defs.jobs] == ["fake_extra_job"]
    assert [s.name for s in defs.schedules] == ["fake_extra_schedule"]

    control = load_component(tmp_path / "b", monkeypatch, pipelines=pipelines)  # legacy
    assert not control.jobs and not control.schedules


# 5 ---------------------------------------------------------------------------
# Captured from master; AGENTS section 6 forbids changing these (pub-tools imports them).
MASTER_SIGNATURES = {
    "build_dlt_source": [
        "base", "database", "schema", "query_callback", "config", "dest_driver",
        "defer_table_reflect",
    ],
    "_prepare_units": [
        "sources", "source_config", "dest_config", "config", "staging",
        "staging_config", "destination_override",
    ],
    "create_dlt_assets": [
        "sources", "source_config", "dest_config", "config", "query_callback",
        "staging", "staging_config", "extra_deps",
    ],
    "build_dlt_runnables": [
        "sources", "source_config", "dest_config", "config", "query_callback",
        "staging", "staging_config", "destination_override", "defer_table_reflect",
    ],
}


@pytest.mark.parametrize("name", sorted(MASTER_SIGNATURES))
def test_the_legacy_entry_points_keep_their_signatures(name):
    assert list(inspect.signature(getattr(parsing, name)).parameters) == MASTER_SIGNATURES[name]


def test_legacy_delegates_to_the_unchanged_functions(monkeypatch):
    seen = []
    monkeypatch.setattr(parsing, "_prepare_units", lambda *a, **k: seen.append("prep") or ([], []))
    monkeypatch.setattr(parsing, "build_dlt_source", lambda *a, **k: seen.append("build") or "src")
    assert LEGACY.prepare_units(1) == ([], [])
    assert LEGACY.build_source(1) == "src"
    assert seen == ["prep", "build"]
