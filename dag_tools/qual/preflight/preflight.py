"""Q3 preflight orchestrator + report schema.

Recipe (Phase Q3, item 3):

  > ``dagtools qual preflight --side candidate``: verify via GraphQL that
  > the webserver is up, reports candidate version, all code locations
  > load (this is fleet-wide load validation under candidate, on real
  > infrastructure), and historical runs/materializations from the
  > baseline pass still render (event-log back-compat spot check).

So preflight is **three checks** on the candidate side, **two checks** on
the baseline side (no priors exist yet to back-compat-check):

  1. **Version check.** Read the manifest's ``baseline.dagster`` or
     ``candidate.dagster`` and compare to what the deployment reports via
     ``query { version }``. Mismatch fails fast — the operator forgot
     to bump (or bumped the wrong release).
  2. **Code location load check.** Walk every entry under
     ``workspaceOrError.locationEntries``; any non-``LOADED`` status
     fails the gate. The error message comes from
     ``locationOrLoadError`` for operator triage.
  3. **(Candidate only) Run rendering check.** Sample baseline run_ids
     from the previously-published Q2 state and ``pipelineRunOrError``
     each one. Any not-found = event-log back-compat broke; investigate.

The published artifact is :class:`PreflightReport` at
``qualifications/<qual_id>/<side>/preflight.json``. Immutable by default
— ``--allow-overwrite`` for re-runs after fixes.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, List, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field

from ..graphql import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    DagsterGraphQLError,
    resolve_auth_token,
)
from ..qualify import QualificationManifest
from ..registry import InventoryRegistry
from ..runs.state import QualRunState, RepStatus


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report shape
# ---------------------------------------------------------------------------


SCHEMA_VERSION = 1


class CheckResult(BaseModel):
    """One named check's outcome."""
    model_config = ConfigDict(extra="ignore")

    name: str
    passed: bool
    detail: Optional[str] = None
    """Operator-facing message — short on success, actionable on failure."""


class CodeLocationCheck(BaseModel):
    """Per-location detail for the location-load roll-up."""
    model_config = ConfigDict(extra="ignore")

    name: str
    load_status: str
    error: Optional[str] = None


class RunRenderingCheck(BaseModel):
    """Per-run detail for the candidate-side back-compat spot check."""
    model_config = ConfigDict(extra="ignore")

    run_id: str
    rendered: bool
    detail: Optional[str] = None


class PreflightReport(BaseModel):
    """The persisted ``preflight.json`` payload."""
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    side: str
    generated_at: datetime

    deployment_version: Optional[str] = None
    expected_version: Optional[str] = None

    code_locations: List[CodeLocationCheck] = Field(default_factory=list)
    """All checked locations, in deployment-reported order."""

    sampled_runs: List[RunRenderingCheck] = Field(default_factory=list)
    """Empty when ``side == baseline`` — there's nothing to back-compat-check yet."""

    checks: List[CheckResult] = Field(default_factory=list)
    """Roll-up of the three high-level checks. UI / CLI reads this for
    the operator summary; the per-item lists above are for forensics."""

    passed: bool = False


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_preflight(
    qual_id: str,
    side: str,
    *,
    registry: InventoryRegistry,
    client_factory: Optional[Callable[[QualificationManifest], DagsterGraphQLClient]] = None,
    run_sample_size: int = 5,
    now: Optional[datetime] = None,
) -> PreflightReport:
    """Run the Q3 preflight checks and return the report.

    Caller (CLI / Python API) is responsible for publishing the report
    to the registry — :func:`publish_preflight_report` handles that with
    the standard immutability semantics.
    """
    if side not in ("baseline", "candidate"):
        raise ValueError(f"side must be 'baseline' or 'candidate', got {side!r}")

    now = now or datetime.now(tz=timezone.utc)
    manifest = _read_manifest(registry, qual_id)
    expected_version = (
        manifest.baseline.dagster if side == "baseline"
        else manifest.candidate.dagster
    )

    factory = client_factory or _default_client_factory
    client = factory(manifest)
    deployment_version: Optional[str] = None
    code_locations: List[CodeLocationStatus] = []
    sampled_runs: List[RunRenderingCheck] = []
    checks: List[CheckResult] = []

    try:
        # --- 1. Version --------------------------------------------------
        try:
            deployment_version = client.get_dagster_version()
            version_passed = _versions_compatible(deployment_version, expected_version)
            checks.append(CheckResult(
                name="dagster_version",
                passed=version_passed,
                detail=(
                    f"deployment reports {deployment_version!r}; "
                    f"manifest expects {expected_version!r}"
                ),
            ))
        except DagsterGraphQLError as e:
            checks.append(CheckResult(
                name="dagster_version", passed=False,
                detail=f"version lookup failed: {e}",
            ))

        # --- 2. Code locations -------------------------------------------
        try:
            code_locations = client.get_code_locations()
            unloaded = [c for c in code_locations if not c.loaded]
            checks.append(CheckResult(
                name="code_locations_loaded",
                passed=not unloaded,
                detail=(
                    f"{len(code_locations)} location(s) reported; "
                    + ("all loaded" if not unloaded
                       else f"{len(unloaded)} not loaded: "
                       + ", ".join(f"{c.name}({c.load_status})" for c in unloaded))
                ),
            ))
        except DagsterGraphQLError as e:
            checks.append(CheckResult(
                name="code_locations_loaded", passed=False,
                detail=f"workspace lookup failed: {e}",
            ))

        # --- 3. (Candidate only) historical run rendering ----------------
        if side == "candidate":
            sampled_runs = _check_baseline_runs_render(
                client=client, registry=registry,
                qual_id=qual_id, sample_size=run_sample_size,
            )
            checks.append(_runs_render_check(sampled_runs))
    finally:
        try:
            client.close()
        except Exception:
            pass

    report = PreflightReport(
        qual_id=qual_id,
        side=side,
        generated_at=now,
        deployment_version=deployment_version,
        expected_version=expected_version,
        code_locations=[
            CodeLocationCheck(
                name=c.name, load_status=c.load_status, error=c.error,
            )
            for c in code_locations
        ],
        sampled_runs=sampled_runs,
        checks=checks,
        passed=all(c.passed for c in checks),
    )
    return report


def publish_preflight_report(
    report: PreflightReport,
    *,
    registry: InventoryRegistry,
    allow_overwrite: bool = False,
) -> None:
    """Persist the preflight report. Immutable per (qual_id, side) by default."""
    registry.put_side_preflight(
        qual_id=report.qual_id,
        side=report.side,
        body=report.model_dump_json(indent=2).encode("utf-8"),
        allow_overwrite=allow_overwrite,
    )


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _read_manifest(registry: InventoryRegistry, qual_id: str) -> QualificationManifest:
    body = registry.read_qualification_manifest(qual_id)
    if body is None:
        raise FileNotFoundError(
            f"no qualification manifest at qual_id={qual_id!r}; "
            f"run `dagtools qual init --id {qual_id} ...` first"
        )
    return QualificationManifest.model_validate(yaml.safe_load(body))


def _versions_compatible(deployment: str, expected: str) -> bool:
    """Loose match: prefix on the expected wins so a manifest expecting
    ``"1.12.x"`` accepts a deployment reporting ``"1.12.1"``. Exact
    match is the strict fallback."""
    if not deployment or not expected:
        return False
    if deployment == expected:
        return True
    # Trim trailing ".x" from expected and accept anything that starts with the rest.
    if expected.endswith(".x"):
        prefix = expected[:-2]
        return deployment.startswith(prefix + ".") or deployment == prefix
    return False


def _check_baseline_runs_render(
    *,
    client: DagsterGraphQLClient,
    registry: InventoryRegistry,
    qual_id: str,
    sample_size: int,
) -> List[RunRenderingCheck]:
    """Sample up to ``sample_size`` baseline run_ids and query each via GraphQL.

    Returns an entry per sampled run with ``rendered`` true/false. Empty
    list when there's no baseline state to sample from — preflight
    treats that as the "no baseline yet" case (the check passes
    vacuously; the operator hasn't run Q2 yet)."""
    baseline_state_body = registry.read_side_state(qual_id, "baseline")
    if not baseline_state_body:
        return []

    try:
        baseline_state = QualRunState.model_validate_json(baseline_state_body)
    except Exception as e:
        logger.warning("_check_baseline_runs_render: bad baseline state: %s", e)
        return []

    sampled = _sample_baseline_run_ids(baseline_state, sample_size)
    results: List[RunRenderingCheck] = []
    for run_id in sampled:
        try:
            client.get_run_status(run_id)
            results.append(RunRenderingCheck(run_id=run_id, rendered=True))
        except DagsterGraphQLError as e:
            results.append(RunRenderingCheck(
                run_id=run_id, rendered=False, detail=str(e),
            ))
    return results


def _sample_baseline_run_ids(state: QualRunState, sample_size: int) -> List[str]:
    """Pick up to ``sample_size`` distinct run_ids from PASSED reps,
    deterministically sorted so re-runs probe the same runs."""
    candidates = sorted(
        (rep.run_id for rep in state.reps.values()
         if rep.status == RepStatus.PASSED and rep.run_id),
        key=lambda x: x,
    )
    return candidates[:sample_size]


def _runs_render_check(sampled: List[RunRenderingCheck]) -> CheckResult:
    if not sampled:
        return CheckResult(
            name="baseline_runs_render",
            passed=True,
            detail="no baseline runs to sample (Q2 hasn't run yet) — vacuously OK",
        )
    failed = [r for r in sampled if not r.rendered]
    return CheckResult(
        name="baseline_runs_render",
        passed=not failed,
        detail=(
            f"sampled {len(sampled)} baseline run(s); "
            + ("all rendered" if not failed
               else f"{len(failed)} did NOT render: "
               + ", ".join(r.run_id for r in failed))
        ),
    )


def _default_client_factory(manifest: QualificationManifest) -> DagsterGraphQLClient:
    """Mirror the runner's factory: build a client from the manifest's
    deployment config. Injectable in tests."""
    if not manifest.deployment.graphql_url:
        raise RuntimeError(
            "manifest.deployment.graphql_url is not set; pass --graphql-url "
            "to `dagtools qual init` or update the manifest"
        )
    token = resolve_auth_token(manifest.deployment.auth)
    return DagsterGraphQLClient(
        endpoint_url=manifest.deployment.graphql_url,
        auth_token=token,
    )
