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
  3. **Deployed identity matches pins** (``deployed_shas_match_pins``, both
     sides). Pin-driven: every ``manifest.inventory_pins`` entry must be
     matched by a location. A location is identified by its image digest
     (against the digests CI recorded for the pinned commit), then a
     ``sha-<short>`` tag, then the ``workspace.yaml`` stamp. Outcomes stay
     distinct: matched, mismatch, a pin nothing matches, and unverified (a
     location reporting none of those), which fails unless the manifest
     waives that location with a reason. Unpinned locations are
     informational.
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
from typing import Callable, List, Literal, Optional, Tuple

import yaml
from pydantic import BaseModel, ConfigDict, Field

from ..graphql import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    DagsterGraphQLError,
    resolve_auth_token,
)
from ..qualify import Deployment, InventoryPin, QualificationManifest
from ..registry import InventoryRegistry
from ..runs.state import QualRunState, RepStatus
from .identity import parse_image_reference, same_repository, sha_from_tag


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report shape
# ---------------------------------------------------------------------------


SCHEMA_VERSION = 3


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
    outcome: Optional[Literal["matched", "mismatch", "unmatched"]] = None
    """Three distinct states: ``matched``; ``mismatch`` (a location was
    attributed to this pin and disagrees); ``unmatched`` (nothing attributed)."""
    source: Optional[Literal["digest", "sha_tag", "stamp"]] = None
    """Which identity source attributed the location (first source wins)."""
    deployed_digest: Optional[str] = None


class LocationIdentity(BaseModel):
    """How one workspace location was identified (report schema 3)."""
    model_config = ConfigDict(extra="ignore")

    location_name: str
    outcome: Literal["matched", "mismatch", "unpinned", "unverified", "waived"]
    source: Optional[Literal["digest", "sha_tag", "stamp"]] = None
    pins: List[str] = Field(default_factory=list)
    """Repos of the pins this location was attributed to."""
    image: Optional[str] = None
    deployed_digest: Optional[str] = None
    deployed_sha: Optional[str] = None
    waiver_reason: Optional[str] = None
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
    """Locations whose identity outcome is ``unverified`` or ``waived``: they
    report no digest, no sha tag and no stamp. An unverified one FAILS the
    sha check; a waived one is named here because the waiver is a decision
    an auditor should be able to see."""

    location_identities: List[LocationIdentity] = Field(default_factory=list)
    """One per workspace location: how it was identified, or why not."""

    unused_waivers: List[str] = Field(default_factory=list)
    """Waiver rows naming no workspace location, or a location that was not
    unverified. Informational."""

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
    location_identities: List[LocationIdentity] = []
    unused_waivers: List[str] = []
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
            (pinned_repos, location_identities, unused_waivers,
             unstamped_locations, sha_check) = _check_deployed_shas(
                manifest, code_locations,
            )
        except Exception as e:  # §4g: never raise
            pinned_repos, unstamped_locations = [], []
            location_identities, unused_waivers = [], []
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
        location_identities=location_identities,
        unused_waivers=unused_waivers,
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


class _Attribution:
    """One location's claim on one pin (internal)."""
    __slots__ = ("pin_index", "kind", "source", "location")

    def __init__(self, pin_index: int, kind: str, source: str, location: str):
        self.pin_index = pin_index
        self.kind = kind  # "matched" | "mismatch"
        self.source = source
        self.location = location


def _classify_location(
    loc: CodeLocationStatus,
    pins: List[InventoryPin],
    deployment: Deployment,
) -> Tuple[LocationIdentity, List[_Attribution]]:
    """Attribute one location to pins: digest, then sha tag, then stamp.

    First source that attributes wins, and a mismatch is final (the digest
    is what the kubelet pulled; a tag claiming the pin does not overrule it).
    """
    meta = loc.display_metadata or {}
    image_text = meta.get(deployment.image_metadata_key) or None
    ref = parse_image_reference(image_text) if image_text else None
    digest = ref.digest if ref else None
    repository = ref.repository if ref and not ref.error else None
    tag_sha = (
        sha_from_tag(ref.tag, deployment.sha_tag_prefix) if ref else None
    )
    stamp_repo = meta.get(deployment.repo_metadata_key)
    stamp_sha = meta.get(deployment.sha_metadata_key)
    stamp_applies = bool(stamp_repo) and bool(stamp_sha)

    def recorded_repo(pin: InventoryPin) -> bool:
        return bool(repository) and any(
            same_repository(i.repository, repository)
            for i in (pin.images or [])
        )

    base = dict(
        location_name=loc.name, image=image_text, deployed_digest=digest,
        deployed_sha=tag_sha or (stamp_sha if stamp_applies else None),
    )

    def done(kind: str, source: str, idxs: List[int], detail: str):
        ident = LocationIdentity(
            outcome=kind, source=source, detail=detail,
            pins=[pins[i].repo for i in idxs], **base,
        )
        return ident, [_Attribution(i, kind, source, loc.name) for i in idxs]

    # 1. digest
    if digest:
        hit = [i for i, p in enumerate(pins)
               if digest in {d.digest for d in (p.images or [])}]
        if hit:
            return done("matched", "digest", hit,
                        f"image digest {digest} is recorded for the pin")
        miss = [i for i, p in enumerate(pins) if recorded_repo(p)]
        if miss:
            return done(
                "mismatch", "digest", miss,
                f"image digest {digest} is not the one recorded for the "
                f"pinned commit of repository {repository!r}",
            )
    # 2. sha tag
    if tag_sha:
        hit = [i for i, p in enumerate(pins) if _shas_match(p.git_sha, tag_sha)]
        if hit:
            return done("matched", "sha_tag", hit,
                        f"image tag names sha {tag_sha!r}, which is the pin")
        miss = [i for i, p in enumerate(pins) if recorded_repo(p)]
        if miss:
            return done(
                "mismatch", "sha_tag", miss,
                f"image tag names sha {tag_sha!r}, not the pinned commit of "
                f"repository {repository!r}",
            )
    # 3. workspace.yaml stamp (kept: deployments use it today)
    if stamp_applies:
        idxs = [i for i, p in enumerate(pins) if p.repo == stamp_repo]
        if idxs:
            ok = [i for i in idxs if _shas_match(stamp_sha, pins[i].git_sha)]
            if ok:
                return done("matched", "stamp", ok,
                            f"stamp {deployment.sha_metadata_key}={stamp_sha!r}")
            return done(
                "mismatch", "stamp", idxs,
                f"stamp {deployment.sha_metadata_key}={stamp_sha!r} is not the pin",
            )
    # no attribution
    reported = bool(digest or tag_sha or stamp_applies)
    if reported:
        ident = LocationIdentity(
            outcome="unpinned", pins=[],
            detail="reports an identity no pin claims (another repo's location)",
            **base,
        )
        return ident, []
    why = (
        f"image reference unparseable ({ref.error})" if ref and ref.error
        else "reports no image digest, no sha tag and no stamp"
    )
    return LocationIdentity(outcome="unverified", pins=[], detail=why, **base), []


def _check_deployed_shas(
    manifest: QualificationManifest,
    code_locations: List[CodeLocationStatus],
) -> Tuple[List[RepoShaCheck], List[LocationIdentity], List[str], List[str], CheckResult]:
    """Pin-driven identity check; see the module docstring (check 3).

    Returns ``(pin results, location identities, unused waivers,
    unstamped location names, roll-up check)``. A location is identified by
    image digest, then ``sha-<short>`` tag, then the workspace stamp.
    Outcomes stay distinct: matched, mismatch, unmatched pin, unverified.
    Unverified is never a pass unless a manifest waiver names the location.
    """
    pins = manifest.inventory_pins
    dep = manifest.deployment
    waivers = {w.location: w.reason for w in dep.unverified_waivers}

    identities: List[LocationIdentity] = []
    attrs: List[_Attribution] = []
    for loc in code_locations:
        ident, claimed = _classify_location(loc, pins, dep)
        if ident.outcome == "unverified" and loc.name in waivers:
            ident = ident.model_copy(update={
                "outcome": "waived", "waiver_reason": waivers[loc.name],
            })
        identities.append(ident)
        attrs.extend(claimed)

    waived_names = {i.location_name for i in identities if i.outcome == "waived"}
    unused = sorted(n for n in waivers if n not in waived_names)
    unverified = [i.location_name for i in identities if i.outcome == "unverified"]
    unstamped = [
        i.location_name for i in identities if i.outcome in ("unverified", "waived")
    ]
    unpinned = [i.location_name for i in identities if i.outcome == "unpinned"]
    ident_by_name = {i.location_name: i for i in identities}

    hint = (
        "deploy by digest (set the chart tag to '<tag>@sha256:<digest>') or "
        f"by a '{dep.sha_tag_prefix}<short>' tag, and record digests in CI "
        "with `dagtools registry record-image`"
    )
    unverified_hint = (
        f"Unverified location(s): {', '.join(unverified)} - {hint}; or waive "
        "with `qual init --waive-unverified-location NAME=REASON`; or stamp "
        f"additional_metadata ({dep.repo_metadata_key}, {dep.sha_metadata_key}) "
        "in workspace.yaml"
    )

    results: List[RepoShaCheck] = []
    for idx, pin in enumerate(pins):
        mine = [a for a in attrs if a.pin_index == idx]
        good = [a for a in mine if a.kind == "matched"]
        bad = [a for a in mine if a.kind == "mismatch"]
        if bad:
            first = bad[0]
            ident = ident_by_name[first.location]
            if good:
                names = ", ".join(f"{a.location}={a.kind}" for a in good + bad)
                detail = (
                    f"ambiguous: locations attributed to this repo disagree ({names})"
                )
            else:
                detail = (
                    f"mismatch: location {first.location!r} deploys "
                    f"{ident.deployed_digest or ident.deployed_sha!r}, "
                    f"pin expects {pin.git_sha!r}"
                )
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha,
                deployed_sha=ident.deployed_sha, location_name=first.location,
                outcome="mismatch", source=first.source,
                deployed_digest=ident.deployed_digest, detail=detail,
            ))
        elif good:
            first = good[0]
            ident = ident_by_name[first.location]
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha,
                deployed_sha=ident.deployed_sha, location_name=first.location,
                matched=True, outcome="matched", source=first.source,
                deployed_digest=ident.deployed_digest,
            ))
        else:
            results.append(RepoShaCheck(
                repo=pin.repo, expected_sha=pin.git_sha, outcome="unmatched",
                detail=(
                    "pinned code is not deployed: no location matches repo "
                    f"{pin.repo!r} by digest, sha tag or workspace stamp"
                ),
            ))

    bad_pins = [r for r in results if not r.matched]
    if not pins:
        # Nothing is pinned, so no location stands in for anything and there is
        # nothing to qualify its identity against: report, do not gate.
        detail = "no inventory pins to verify"
        if unverified:
            detail += (
                f"; {len(unverified)} location(s) unverified; "
                "not gated because nothing is pinned"
            )
        return results, identities, unused, unstamped, CheckResult(
            name=SHA_CHECK_NAME, passed=True, detail=detail,
        )
    if not bad_pins and not unverified:
        detail = f"{len(results)} pin(s) verified"
        if unpinned:
            detail += f"; unpinned location(s) (ignored): {', '.join(unpinned)}"
        if unstamped:
            detail += f"; waived location(s): {', '.join(unstamped)}"
        return results, identities, unused, unstamped, CheckResult(
            name=SHA_CHECK_NAME, passed=True, detail=detail,
        )
    parts: List[str] = []
    if bad_pins:
        parts.append(
            f"{len(bad_pins)} of {len(results)} pin(s) unsatisfied: "
            + "; ".join(
                f"{r.repo}: {r.detail} (expected {r.expected_sha!r}, "
                f"deployed {r.deployed_sha!r}, location {r.location_name!r})"
                for r in bad_pins
            )
            + f". To fix: {hint}"
        )
    if unverified:
        parts.append(unverified_hint)
    if unpinned:
        parts.append(f"Unpinned location(s) (ignored): {', '.join(unpinned)}")
    return results, identities, unused, unstamped, CheckResult(
        name=SHA_CHECK_NAME, passed=False, detail=". ".join(parts),
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
