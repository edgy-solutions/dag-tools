"""Smoke tests for the top-level ``dag_tools.probes_location.definitions``
module — the thing the test deployment imports.

These tests use the same generated probes as the loader tests but
exercise the module-level ``_build_definitions`` path so we catch
regressions in the empty / single-probe / merge code paths from the
operator's deploy entry-point.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from dag_tools.probes_location import DAGTOOLS_PROBES_DIR_ENV
from dag_tools.qual.classes import (
    ClassKeyComponents,
    EquivalenceClass,
    Representative,
    Runnability,
)
from dag_tools.qual.synthetic import generate_probe_module


def _drop_probe(tmp_dir: Path, class_hash: str) -> None:
    rep = Representative(
        repo="alpha", git_sha="sha",
        asset_key=["probe", "target"],
        runnability=Runnability.SYNTHETIC_REQUIRED,
        runnability_reason="default",
    )
    cls = EquivalenceClass(
        class_hash=class_hash,
        key=ClassKeyComponents(
            compute_kind="python",
            io_manager_class="dagster.InMemoryIOManager",
        ),
        member_count=1, member_repo_count=1,
        members=[], representatives=[rep],
    )
    _, source = generate_probe_module(cls, qual_id="q1")
    (tmp_dir / f"{class_hash}.py").write_text(source, encoding="utf-8")


def _reload_module():
    """Force a fresh load of definitions.py — its ``defs`` is built at
    import time off the current env, so reload is the only way to
    re-evaluate after monkeypatch."""
    import dag_tools.probes_location.definitions as m
    return importlib.reload(m)


def test_definitions_module_yields_empty_defs_when_env_unset(monkeypatch):
    monkeypatch.delenv(DAGTOOLS_PROBES_DIR_ENV, raising=False)
    m = _reload_module()
    from dagster import Definitions
    assert isinstance(m.defs, Definitions)


def test_definitions_module_yields_merged_defs_for_two_probes(monkeypatch, tmp_path):
    _drop_probe(tmp_path, "aaaa1111bbbb")
    _drop_probe(tmp_path, "bbbb2222cccc")
    monkeypatch.setenv(DAGTOOLS_PROBES_DIR_ENV, str(tmp_path))
    m = _reload_module()
    ak_set = {ak.to_user_string() for ak in m.defs.resolve_asset_graph().get_all_asset_keys()}
    assert "probe_aaaa1111_upstream" in ak_set
    assert "probe_bbbb2222_upstream" in ak_set


def test_definitions_module_survives_one_broken_probe(monkeypatch, tmp_path):
    _drop_probe(tmp_path, "aaaa1111bbbb")
    (tmp_path / "broken99deadbeef.py").write_text(
        "this is not valid python\n", encoding="utf-8",
    )
    monkeypatch.setenv(DAGTOOLS_PROBES_DIR_ENV, str(tmp_path))
    m = _reload_module()
    # Healthy probe still loads.
    ak_set = {ak.to_user_string() for ak in m.defs.resolve_asset_graph().get_all_asset_keys()}
    assert "probe_aaaa1111_upstream" in ak_set
