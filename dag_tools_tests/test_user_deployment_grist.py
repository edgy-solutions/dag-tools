"""Tests for the user-deployment's optional Grist ingest surface.

The disabled / missing-file / disabled-flag paths never import the Grist
component (pandas/connectorx-free), so they run anywhere; the actual
build path is guarded with importorskip.
"""
import textwrap

import pytest

from dag_tools.user_deployment import definitions as ud


# ---------------------------------------------------------------------------
# env-template resolution
# ---------------------------------------------------------------------------


def test_resolve_env_templates_whole_string(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "sekret")
    assert ud._resolve_env_templates("{{ env.MY_TOKEN }}") == "sekret"


def test_resolve_env_templates_embedded_and_nested(monkeypatch):
    monkeypatch.setenv("HOST", "pg.internal")
    out = ud._resolve_env_templates(
        {"a": {"url": "db://{{ env.HOST }}/x"}, "b": ["{{env.HOST}}", 5]}
    )
    assert out == {"a": {"url": "db://pg.internal/x"}, "b": ["pg.internal", 5]}


def test_resolve_env_templates_unset_becomes_empty(monkeypatch):
    monkeypatch.delenv("NOPE_VAR", raising=False)
    assert ud._resolve_env_templates("{{ env.NOPE_VAR }}") == ""


# ---------------------------------------------------------------------------
# disabled paths (no component import required)
# ---------------------------------------------------------------------------


def test_grist_disabled_by_default(monkeypatch):
    monkeypatch.delenv("DAG_TOOLS_GRIST_CONFIG", raising=False)
    defs = ud._build_grist_defs()
    assert not defs.sensors


def test_grist_missing_file_is_disabled(monkeypatch, tmp_path):
    monkeypatch.setenv("DAG_TOOLS_GRIST_CONFIG", str(tmp_path / "nope.yaml"))
    defs = ud._build_grist_defs()
    assert not defs.sensors


def test_grist_enabled_false_is_disabled(monkeypatch, tmp_path):
    cfg = tmp_path / "grist.yaml"
    cfg.write_text("enabled: false\nattributes:\n  name: crm\n", encoding="utf-8")
    monkeypatch.setenv("DAG_TOOLS_GRIST_CONFIG", str(cfg))
    defs = ud._build_grist_defs()
    assert not defs.sensors


# ---------------------------------------------------------------------------
# build path (needs pandas + connectorx)
# ---------------------------------------------------------------------------


def _write_config(tmp_path):
    cfg = tmp_path / "grist.yaml"
    cfg.write_text(textwrap.dedent("""\
        enabled: true
        attributes:
          name: crm
          grist:
            host: grist.example.com
            org: myorg
            token: "{{ env.GRIST_TOKEN }}"
          postgres:
            protocol: postgresql
            host: pg
            port: 5432
            database: analytics
            schema: grist
            username: u
            password: "{{ env.PG_PASSWORD }}"
    """), encoding="utf-8")
    return cfg


def test_grist_builds_and_resolves_env(monkeypatch, tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("connectorx")

    cfg = _write_config(tmp_path)
    monkeypatch.setenv("DAG_TOOLS_GRIST_CONFIG", str(cfg))
    monkeypatch.setenv("GRIST_TOKEN", "tok-123")
    monkeypatch.setenv("PG_PASSWORD", "pw-456")

    defs = ud._build_grist_defs()

    # Asset + sensor materialized under the configured name.
    sensor_names = {s.name for s in defs.sensors}
    assert "crm_sensor" in sensor_names
    asset_keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "crm_ingest" in asset_keys

    # {{ env.X }} secrets resolved into the built resource configs.
    grist_res = defs.resources["crm_grist_resource"]
    assert grist_res.config.token == "tok-123"


def test_grist_merged_into_combined_defs(monkeypatch, tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("connectorx")

    cfg = _write_config(tmp_path)
    monkeypatch.setenv("DAG_TOOLS_GRIST_CONFIG", str(cfg))
    monkeypatch.setenv("GRIST_TOKEN", "t")
    monkeypatch.setenv("PG_PASSWORD", "p")
    monkeypatch.delenv("DAG_TOOLS_DEMO_MODE", raising=False)

    defs = ud._build_combined_defs()
    assert "crm_sensor" in {s.name for s in defs.sensors}
