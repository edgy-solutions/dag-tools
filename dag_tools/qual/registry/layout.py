"""Object-store layout contract for the qualification registry.

This module is pure constants + path helpers — no boto3 dependency, so it's
the safe place to centralize the layout rules. Both writers (the CI survey)
and readers (the operator desktop CLI) compute keys via these helpers; the
layout never appears as a string literal anywhere else.

Layout (bucket-relative):

    inventory/
      <repo>/
        <git_sha>/
          meta.json
          assets.json
          automation.json
          io_managers.json
          dbt_projects.json
          load_validation.json
          canary.json            (optional)
        latest.json              ← MUTABLE pointer; written LAST
    qualifications/
      <qual_id>/
        manifest.yaml
        classes/equivalence_classes.json
        baseline/
          runs/<class_hash>/<run_id>.json
          orchestration/*.json
          summary.json
        candidate/
          runs/...
          orchestration/*.json
          summary.json
        verdict.json
        UPGRADE_VERDICT.md

Invariants:

  * Every key under ``inventory/<repo>/<git_sha>/`` and
    ``qualifications/<qual_id>/`` is **immutable**. Re-writing requires
    explicit ``allow_overwrite=True`` on the storage layer.
  * ``inventory/<repo>/latest.json`` is the **only mutable key per repo**
    and is written **last** (after all per-SHA artifacts), so partial
    builds never become visible.
"""
from __future__ import annotations

from typing import List
from urllib.parse import urlparse


# --- top-level prefixes -----------------------------------------------------
INVENTORY_PREFIX = "inventory"
QUALIFICATIONS_PREFIX = "qualifications"

# --- per-build inventory artifact filenames --------------------------------
META_FILE = "meta.json"
ASSETS_FILE = "assets.json"
AUTOMATION_FILE = "automation.json"
IO_MANAGERS_FILE = "io_managers.json"
DBT_PROJECTS_FILE = "dbt_projects.json"
LOAD_VALIDATION_FILE = "load_validation.json"
CANARY_FILE = "canary.json"
LATEST_POINTER_FILE = "latest.json"

# The canonical artifact set written by a survey, in publish order. The
# survey writes immutable artifacts first, then ``LATEST_POINTER_FILE``
# last — so a reader observing the pointer is guaranteed the artifacts
# it points at are all present.
SURVEY_ARTIFACTS: List[str] = [
    META_FILE,
    ASSETS_FILE,
    AUTOMATION_FILE,
    IO_MANAGERS_FILE,
    DBT_PROJECTS_FILE,
    LOAD_VALIDATION_FILE,
]


# --- key constructors -------------------------------------------------------

def inventory_repo_prefix(repo: str) -> str:
    """``inventory/<repo>/`` — all per-SHA artifacts plus the mutable pointer
    for a single repo."""
    return f"{INVENTORY_PREFIX}/{repo}/"


def inventory_build_prefix(repo: str, git_sha: str) -> str:
    """``inventory/<repo>/<sha>/`` — the immutable per-build directory."""
    return f"{INVENTORY_PREFIX}/{repo}/{git_sha}/"


def inventory_artifact_key(repo: str, git_sha: str, filename: str) -> str:
    """Full key for a single per-build artifact (e.g. ``meta.json``)."""
    return f"{inventory_build_prefix(repo, git_sha)}{filename}"


def latest_pointer_key(repo: str) -> str:
    """``inventory/<repo>/latest.json`` — the mutable pointer to the most
    recent successful build."""
    return f"{inventory_repo_prefix(repo)}{LATEST_POINTER_FILE}"


def qualification_prefix(qual_id: str) -> str:
    """``qualifications/<qual_id>/`` — root of one qualification run."""
    return f"{QUALIFICATIONS_PREFIX}/{qual_id}/"


def qualification_manifest_key(qual_id: str) -> str:
    return f"{qualification_prefix(qual_id)}manifest.yaml"


def qualification_classes_key(qual_id: str) -> str:
    return f"{qualification_prefix(qual_id)}classes/equivalence_classes.json"


def qualification_classes_md_key(qual_id: str) -> str:
    """Human-readable companion to ``equivalence_classes.json``."""
    return f"{qualification_prefix(qual_id)}classes/equivalence_classes.md"


def qualification_verdict_key(qual_id: str) -> str:
    return f"{qualification_prefix(qual_id)}verdict.json"


def qualification_side_run_key(
    qual_id: str, side: str, class_hash: str, run_id: str
) -> str:
    """``qualifications/<qual_id>/<side>/runs/<class_hash>/<run_id>.json``."""
    if side not in ("baseline", "candidate"):
        raise ValueError(f"side must be 'baseline' or 'candidate', got {side!r}")
    return f"{qualification_prefix(qual_id)}{side}/runs/{class_hash}/{run_id}.json"


# --- registry URI parsing ---------------------------------------------------

def parse_registry_uri(uri: str) -> str:
    """Parse a ``s3://<bucket>`` URI and return the bucket name.

    Accepts plain bucket names too (no scheme) for convenience. Rejects URIs
    with a non-empty path/prefix component — the layout owns prefixing; the
    caller should not bake one in.
    """
    if not uri:
        raise ValueError("registry URI is empty")

    if "://" not in uri:
        return uri.strip("/")

    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"registry URI must use s3:// scheme, got {parsed.scheme!r}")
    if not parsed.netloc:
        raise ValueError(f"registry URI has no bucket: {uri!r}")
    if parsed.path and parsed.path.strip("/"):
        raise ValueError(
            f"registry URI must not include a path component (got {parsed.path!r}); "
            f"the layout owns all keys under the bucket"
        )
    return parsed.netloc
