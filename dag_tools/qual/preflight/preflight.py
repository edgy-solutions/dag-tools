"""Q3 preflight orchestrator + report schema.

Recipe (Phase Q3, item 3):

  > ``dagtools qual preflight --side candidate``: verify via GraphQL that
  > the webserver is up, reports candidate version, all code locations
  > load (this is fleet-wide load validation under candidate, on real
  > infrastructure), and historical runs/materializations from the
  > baseline pass still render (event-log back-compat spot check).

So preflight is **four checks** on the candidate side, **three checks** on
the baseline side (no priors exist yet to back-compat-check):

  1. **Version check.** Read the manifest's ``baseline.dagster`` or
     ``candidate.dagster`` and compare to what the deployment reports via
     ``query { version }``. Mismatch fails fast — the operator forgot
     to bump (or bumped the wrong release).
  2. **Code location load check.** Walk every entry under
     ``workspaceOrError.locationEntries``; any non-``LOADED`` status
     fails the gate. The error message comes from
     ``locationOrLoadError`` for operator triage.
  3. **Deployed shas match pins** (``deployed_shas_match_pins``, both
     sides). Pin-driven: every ``manifest.inventory_pins`` entry must be
     matched by a location whose ``displayMetadata`` stamps that repo and a
     compatible git sha. Dagster surfaces no git sha itself, so the stamp is
     an operator contract (``workspace.yaml`` ``additional_metadata``).
     Unstamped / unpinned locations are informational.
  4. **(Candidate only) Run rendering check.** Sample baseline run_ids
     from the previously-published Q2 state and ``pipelineRunOrError``
     each one. Any not-found = event-log back-compat broke; investigate.

The published artifact is :class:`PreflightReport` at
``qualifications/<qual_id>/<side>/preflight.json``. Immutable by default
— ``--allow-overwrite`` for re-runs after fixes.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

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


SCHEMA_VERSION = 2


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


class RepoShaCheck(BaseModel):
    """Per-pin detail for the deployed-sha-vs-pin check."""
    model_config = ConfigDict(extra="ignore")

    repo: str
    expected_sha: str
    deployed_sha: Optional[str] = None
    location_name: Optional[str] = None
    matched: bool = False
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

    pinned_repos: List[RepoShaCheck] = Field(default_factory=list)
    """One entry per ``inventory_pins`` entry, satisfied or not."""

    unstamped_locations: List[str] = Field(default_factory=list)
    """Loaded locations carrying no repo stamp. Informational only."""

    checks: List[CheckResult] = Field(default_factory=list)
    """Roll-up of the high-level checks. UI / CLI reads this for
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
    pinned_repos: List[RepoShaCheck] = []
    unstamped_locations: List[str] = []
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

        # --- 2b. Deployed shas match the manifest's inventory pins -------
        # Reuses check 2's ``code_locations`` (no second workspace call).
        # If check 2 failed the list is empty and every pin is unsatisfied.
        try:
            pinned_repos, unstamped_locations, sha_check = _check_deployed_shas(
                manifest, code_locations,
            )
        except Exception as e:  # §4g: never raise
            pinned_repos, unstamped_locations = [], []
            sha_check = CheckResult(
                name=SHA_CHECK_NAME, passed=False,
                detail=f"sha check failed unexpectedly: {e}",
            )
        checks.append(sha_check)

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
        pinned_repos=pinned_repos,
        unstamped_locations=unstamped_locations,
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


SHA_CHECK_NAME = "deployed_shas_match_pins"
_MIN_SHA_PREFIX = 7


def _shas_match(a: str, b: str) -> bool:
    """Case-insensitive; equal, or one a prefix of the other with the
    shorter at least 7 chars. That is the entire grammar."""
    a, b = a.strip().lower(), b.strip().lower()
    if not a or not b:
        return False
    if a == b:
        return True
    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    return len(short) >= _MIN_SHA_PREFIX and long_.startswith(short)


def _check_deployed_shas(
    manifest: QualificationManifest,
    code_locations: List[CodeLocationStatus],
) -> Tuple[List[RepoShaCheck], List[str], CheckResult]:
    """Pin-driven: every ``inventory_pins`` entry must be satisfied by a
    stamped location. Locations the qualification does not pin (e.g.
    ``dag-tools-probes``) are informational and never fail the check."""
    pins = manifest.inventory_pins
    if not pins:
        return [], [], CheckResult(
            name=SHA_CHECK_NAME, passed=True,
            detail="no inventory pins to verify",
        )

    repo_key = manifest.deployment.repo_metadata_key
    sha_key = manifest.deployment.sha_metadata_key

    by_repo: Dict[str, List[Tuple[str, Optional[str]]]] = {}
    unstamped: List[str] = []
    for loc in code_locations:
        meta = loc.display_metadata or {}
        repo = meta.get(repo_key)
        if not repo:
            unstamped.append(loc.name)
            continue
        by_repo.setdefault(repo, []).append((loc.name, meta.get(sha_key)))

    results: List[RepoShaCheck] = []
    for pin in pins:
        stamps = by_repo.get(pin.repo, [])
        if not stamps:
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha,
                detail="pinned code is not deployed: no location stamps "
                       f"{repo_key}={pin.repo!r}",
            ))
            continue
        first = stamps[0][1] or ""
        # An empty `first` is covered: _shas_match("", x) is False for every
        # x, so a repo stamped by several locations where the first carries no
        # sha reads as a disagreement, which is what it is.
        if any(not _shas_match(first, sha or "") for _, sha in stamps[1:]):
            names = ", ".join(f"{n}={sha!r}" for n, sha in stamps)
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha,
                location_name=stamps[0][0],
                detail=f"ambiguous: locations stamping this repo disagree ({names})",
            ))
            continue
        name, sha = stamps[0]
        if not sha:
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha, location_name=name,
                detail=f"location {name!r} stamps the repo but no {sha_key!r}",
            ))
        elif _shas_match(sha, pin.git_sha):
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha, deployed_sha=sha,
                location_name=name, matched=True,
            ))
        else:
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha, deployed_sha=sha,
                location_name=name,
                detail=f"mismatch: location {name!r} deploys {sha!r}, "
                       f"pin expects {pin.git_sha!r}",
            ))

    pinned_names = {p.repo for p in pins}
    extra = sorted(r for r in by_repo if r not in pinned_names)
    bad = [r for r in results if not r.matched]
    if not bad:
        detail = f"{len(results)} pin(s) verified"
        if extra:
            detail += f"; stamped but not pinned (ignored): {', '.join(extra)}"
        return results, unstamped, CheckResult(
            name=SHA_CHECK_NAME, passed=True, detail=detail,
        )
    parts = [
        f"{r.repo}: {r.detail} (expected {r.expected_sha!r}, "
        f"deployed {r.deployed_sha!r}, location {r.location_name!r})"
        for r in bad
    ]
    detail = f"{len(bad)} of {len(results)} pin(s) unsatisfied: " + "; ".join(parts)
    if unstamped:
        detail += (
            f". Unstamped location(s): {', '.join(unstamped)} - the operator "
            f"likely needs to stamp additional_metadata ({repo_key}, {sha_key}) "
            "in workspace.yaml"
        )
    if extra:
        detail += f". Stamped but not pinned (ignored): {', '.join(extra)}"
    return results, unstamped, CheckResult(
        name=SHA_CHECK_NAME, passed=False, detail=detail,
    )


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
