"""Tests for the ``dag-tools-probes`` dynamic loader.

Generates real probe sources via the Q5 generator, drops them in a
temp dir, then asserts the loader produces a merge-able Definitions
plus a clean per-probe report.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from dag_tools.probes_location import (
    DAGTOOLS_PROBES_DIR_ENV,
    load_probes_from_dir,
    resolve_probes_dir,
)
from dag_tools.qual.classes import (
    ClassKeyComponents,
    EquivalenceClass,
    Representative,
    Runnability,
)
from dag_tools.qual.synthetic import generate_probe_module


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_probe_file(tmp_dir: Path, *, class_hash: str,
                     io_manager_class: str = "dagster.InMemoryIOManager") -> Path:
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
            io_manager_class=io_manager_class,
        ),
        member_count=1, member_repo_count=1,
        members=[], representatives=[rep],
    )
    _, source = generate_probe_module(cls, qual_id="q1")
    path = tmp_dir / f"{class_hash}.py"
    path.write_text(source, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# resolve_probes_dir
# ---------------------------------------------------------------------------


def test_resolve_probes_dir_returns_none_when_env_unset(monkeypatch):
    monkeypatch.delenv(DAGTOOLS_PROBES_DIR_ENV, raising=False)
    assert resolve_probes_dir() is None


def test_resolve_probes_dir_expands_user(monkeypatch):
    monkeypatch.setenv(DAGTOOLS_PROBES_DIR_ENV, "~/somewhere")
    out = resolve_probes_dir()
    assert out is not None
    assert "~" not in str(out)


# ---------------------------------------------------------------------------
# load_probes_from_dir — empty / missing
# ---------------------------------------------------------------------------


def test_loader_returns_empty_report_when_env_unset(monkeypatch):
    """A test deployment that hasn't yet deployed probes still loads
    the location — the operator should be able to deploy the location
    once and add bundles later."""
    monkeypatch.delenv(DAGTOOLS_PROBES_DIR_ENV, raising=False)
    report = load_probes_from_dir()
    assert report.total == 0
    assert report.loaded == []
    assert report.failures == []


def test_loader_returns_empty_report_when_dir_missing(tmp_path):
    """Pointing at a missing directory must NOT raise — the operator
    sees a clean empty location instead of a code-location load failure."""
    report = load_probes_from_dir(tmp_path / "does-not-exist")
    assert report.total == 0


def test_loader_skips_non_python_files(tmp_path):
    (tmp_path / "probe_manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "README.md").write_text("hi", encoding="utf-8")
    report = load_probes_from_dir(tmp_path)
    assert report.total == 0


# ---------------------------------------------------------------------------
# Happy path — generated probes load
# ---------------------------------------------------------------------------


def test_loader_loads_single_probe(tmp_path):
    _make_probe_file(tmp_path, class_hash="aaaa1111bbbb")
    report = load_probes_from_dir(tmp_path)
    assert len(report.loaded) == 1
    assert report.loaded[0].class_hash == "aaaa1111bbbb"
    assert report.loaded[0].defs is not None
    assert report.failures == []


def test_loader_loads_multiple_probes_in_deterministic_order(tmp_path):
    """Sorted by filename — operator running ``dagtools qual synthetic``
    twice gets the same load order."""
    _make_probe_file(tmp_path, class_hash="bbbb2222cccc")
    _make_probe_file(tmp_path, class_hash="aaaa1111bbbb")
    _make_probe_file(tmp_path, class_hash="cccc3333dddd")
    report = load_probes_from_dir(tmp_path)
    assert [o.class_hash for o in report.loaded] == [
        "aaaa1111bbbb", "bbbb2222cccc", "cccc3333dddd",
    ]


def test_loaded_probes_merge_into_one_definitions_without_resource_collision(tmp_path):
    """The crucial cross-cutting property: each generated probe uses a
    class-unique resource key, so Definitions.merge succeeds across N
    probes. If this fails, the dag-tools-probes location won't load."""
    from dagster import Definitions

    _make_probe_file(tmp_path, class_hash="aaaa1111bbbb")
    _make_probe_file(tmp_path, class_hash="bbbb2222cccc")
    report = load_probes_from_dir(tmp_path)
    assert len(report.loaded) == 2
    merged = Definitions.merge(*(o.defs for o in report.loaded))
    # Both classes contributed assets (upstream + downstream each).
    asset_keys = {ak.to_user_string() for ak in merged.resolve_asset_graph().get_all_asset_keys()}
    assert "probe_aaaa1111_upstream" in asset_keys
    assert "probe_aaaa1111_downstream" in asset_keys
    assert "probe_bbbb2222_upstream" in asset_keys
    assert "probe_bbbb2222_downstream" in asset_keys


# ---------------------------------------------------------------------------
# Soft-fail: one bad probe doesn't block the rest
# ---------------------------------------------------------------------------


def test_loader_soft_fails_a_malformed_probe(tmp_path):
    """A probe whose .py has a Python syntax error becomes a load
    failure entry; healthy probes still load."""
    _make_probe_file(tmp_path, class_hash="aaaa1111bbbb")
    (tmp_path / "broken99deadbeef.py").write_text(
        "this is not valid python\n", encoding="utf-8",
    )
    report = load_probes_from_dir(tmp_path)
    assert len(report.loaded) == 1
    assert report.loaded[0].class_hash == "aaaa1111bbbb"
    assert len(report.failures) == 1
    assert report.failures[0].class_hash == "broken99deadbeef"
    assert "SyntaxError" in report.failures[0].error


def test_loader_soft_fails_module_without_defs_attribute(tmp_path):
    """A .py file that loads but exposes no `defs` is rejected — could
    be a stale file, a half-edited probe, or something not generated
    by Q5 at all."""
    (tmp_path / "abc123def456.py").write_text(
        "x = 1\n# no `defs` here\n", encoding="utf-8",
    )
    report = load_probes_from_dir(tmp_path)
    assert report.loaded == []
    assert len(report.failures) == 1
    assert "no top-level `defs`" in report.failures[0].error


def test_loader_env_var_override(monkeypatch, tmp_path):
    """Setting DAGTOOLS_PROBES_DIR picks up that directory when no
    explicit dir is passed."""
    _make_probe_file(tmp_path, class_hash="dddd4444eeee")
    monkeypatch.setenv(DAGTOOLS_PROBES_DIR_ENV, str(tmp_path))
    report = load_probes_from_dir()  # no explicit dir
    assert len(report.loaded) == 1
    assert report.loaded[0].class_hash == "dddd4444eeee"
