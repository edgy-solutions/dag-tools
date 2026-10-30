"""End-to-end tests for run_preflight + publish_preflight_report."""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")

import boto3
from moto import mock_aws

from dag_tools.qual.graphql import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    DagsterGraphQLError,
    RunStatusInfo,
)
from dag_tools.qual.preflight import (
    PreflightReport,
    publish_preflight_report,
    run_preflight,
)
from dag_tools.qual.qualify import (
    Deployment,
    InventoryPin,
    VersionTarget,
    create_qualification,
)
from dag_tools.qual.registry import (
    ImmutableKeyExists,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
)


BUCKET = "dag-tools-preflight-test"


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _seed_manifest(registry, baseline_v="1.10.6", candidate_v="1.12.1"):
    create_qualification(
        qual_id="q1", registry=registry,
        baseline=VersionTarget(dagster=baseline_v),
        candidate=VersionTarget(dagster=candidate_v),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )


def _client(*, version=None, locations=None, get_status_side_effect=None):
    c = MagicMock(spec=DagsterGraphQLClient)
    if isinstance(version, BaseException):
        c.get_dagster_version.side_effect = version
    else:
        c.get_dagster_version.return_value = version or "1.10.6"
    if isinstance(locations, BaseException):
        c.get_code_locations.side_effect = locations
    else:
        c.get_code_locations.return_value = locations or [
            CodeLocationStatus(name="patriot", load_status="LOADED"),
        ]
    if get_status_side_effect:
        c.get_run_status.side_effect = get_status_side_effect
    else:
        c.get_run_status.return_value = RunStatusInfo(run_id="r1", status="SUCCESS")
    c.close.return_value = None
    return c


def _factory(client):
    return lambda manifest: client


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_run_preflight_baseline_passes_when_version_and_locations_ok(setup):
    registry = setup
    _seed_manifest(registry)
    client = _client(version="1.10.6")

    report = run_preflight(
        "q1", "baseline",
        registry=registry, client_factory=_factory(client),
    )
    assert report.passed
    assert report.deployment_version == "1.10.6"
    assert report.expected_version == "1.10.6"
    # baseline side has three checks; no run rendering.
    assert {c.name for c in report.checks} == {
        "dagster_version", "code_locations_loaded", "deployed_shas_match_pins",
    }


def test_run_preflight_candidate_includes_run_rendering_check(setup):
    """The candidate side runs the third check (event-log back-compat).
    With no baseline state yet, it's vacuously OK."""
    registry = setup
    _seed_manifest(registry)
    client = _client(version="1.12.1")

    report = run_preflight(
        "q1", "candidate",
        registry=registry, client_factory=_factory(client),
    )
    assert report.passed
    check_names = [c.name for c in report.checks]
    assert "baseline_runs_render" in check_names


def test_run_preflight_accepts_wildcard_version_match(setup):
    """Manifest expects 1.12.x; deployment reports 1.12.1 → match."""
    registry = setup
    _seed_manifest(registry, candidate_v="1.12.x")
    client = _client(version="1.12.5")

    report = run_preflight(
        "q1", "candidate",
        registry=registry, client_factory=_factory(client),
    )
    version_check = next(c for c in report.checks if c.name == "dagster_version")
    assert version_check.passed


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


def test_run_preflight_fails_on_version_mismatch(setup):
    registry = setup
    _seed_manifest(registry, candidate_v="1.12.1")
    client = _client(version="1.11.9")

    report = run_preflight(
        "q1", "candidate",
        registry=registry, client_factory=_factory(client),
    )
    assert not report.passed
    vcheck = next(c for c in report.checks if c.name == "dagster_version")
    assert not vcheck.passed
    assert "1.11.9" in (vcheck.detail or "")


def test_run_preflight_fails_when_a_code_location_does_not_load(setup):
    registry = setup
    _seed_manifest(registry)
    client = _client(
        version="1.12.1",
        locations=[
            CodeLocationStatus(name="ok", load_status="LOADED"),
            CodeLocationStatus(
                name="broken", load_status="ERROR",
                error="ImportError",
            ),
        ],
    )
    report = run_preflight(
        "q1", "candidate",
        registry=registry, client_factory=_factory(client),
    )
    assert not report.passed
    loc_check = next(c for c in report.checks if c.name == "code_locations_loaded")
    assert not loc_check.passed
    assert "broken" in (loc_check.detail or "")
    # The per-location detail survives in the report for forensics.
    assert any(c.name == "broken" for c in report.code_locations)


def test_run_preflight_graphql_failure_becomes_failed_check(setup):
    """A transport-level GraphQL error becomes a failed check, not a crash —
    operators get an actionable report."""
    registry = setup
    _seed_manifest(registry)
    client = _client(version=DagsterGraphQLError("unreachable"))

    report = run_preflight(
        "q1", "baseline",
        registry=registry, client_factory=_factory(client),
    )
    assert not report.passed
    vcheck = next(c for c in report.checks if c.name == "dagster_version")
    assert "unreachable" in (vcheck.detail or "")


def test_run_preflight_rejects_invalid_side(setup):
    registry = setup
    _seed_manifest(registry)
    client = _client()
    with pytest.raises(ValueError, match="baseline|candidate"):
        run_preflight(
            "q1", "rogue",
            registry=registry, client_factory=_factory(client),
        )


def test_run_preflight_raises_when_manifest_missing(setup):
    registry = setup
    client = _client()
    with pytest.raises(FileNotFoundError, match="run `dagtools qual init"):
        run_preflight(
            "never-existed", "baseline",
            registry=registry, client_factory=_factory(client),
        )


# ---------------------------------------------------------------------------
# Run rendering check uses baseline state when present
# ---------------------------------------------------------------------------


def test_candidate_preflight_samples_baseline_runs(setup):
    """When baseline state exists with PASSED reps, candidate preflight
    actually queries Dagster for each sampled run_id."""
    registry = setup
    _seed_manifest(registry)

    # Pre-seed a baseline state file with two PASSED reps.
    state = {
        "schema_version": 1,
        "qual_id": "q1", "side": "baseline",
        "started_at": "2026-06-15T00:00:00+00:00",
        "updated_at": "2026-06-15T00:00:00+00:00",
        "reps": {
            "h1:a": {
                "rep_id": "h1:a", "class_hash": "h1", "asset_key": ["a"],
                "repo": "alpha", "git_sha": "s", "runnability": "runnable",
                "status": "passed", "run_id": "run-A",
            },
            "h2:b": {
                "rep_id": "h2:b", "class_hash": "h2", "asset_key": ["b"],
                "repo": "alpha", "git_sha": "s", "runnability": "runnable",
                "status": "passed", "run_id": "run-B",
            },
        },
    }
    registry.put_side_state("q1", "baseline", json.dumps(state).encode())

    # One of the two runs disappears on the candidate (back-compat broke).
    def status_side_effect(run_id):
        if run_id == "run-B":
            raise DagsterGraphQLError("run not found after migration")
        return RunStatusInfo(run_id=run_id, status="SUCCESS")

    client = _client(version="1.12.1", get_status_side_effect=status_side_effect)
    report = run_preflight(
        "q1", "candidate",
        registry=registry, client_factory=_factory(client),
    )

    rendering = next(c for c in report.checks if c.name == "baseline_runs_render")
    assert not rendering.passed
    assert "run-B" in (rendering.detail or "")
    # And the per-run detail is preserved.
    by_id = {r.run_id: r for r in report.sampled_runs}
    assert by_id["run-A"].rendered is True
    assert by_id["run-B"].rendered is False


def test_baseline_preflight_does_not_sample_baseline_runs(setup):
    """Baseline side has no priors; the candidate-only check is skipped."""
    registry = setup
    _seed_manifest(registry)
    client = _client(version="1.10.6")

    report = run_preflight(
        "q1", "baseline",
        registry=registry, client_factory=_factory(client),
    )
    assert "baseline_runs_render" not in {c.name for c in report.checks}
    assert report.sampled_runs == []


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


def test_publish_preflight_report_is_immutable_by_default(setup):
    registry = setup
    _seed_manifest(registry)
    client = _client(version="1.10.6")
    report = run_preflight(
        "q1", "baseline",
        registry=registry, client_factory=_factory(client),
    )
    publish_preflight_report(report, registry=registry)
    with pytest.raises(ImmutableKeyExists):
        publish_preflight_report(report, registry=registry)


def test_publish_preflight_report_allow_overwrite_bypasses(setup):
    registry = setup
    _seed_manifest(registry)
    client = _client(version="1.10.6")
    report = run_preflight(
        "q1", "baseline",
        registry=registry, client_factory=_factory(client),
    )
    publish_preflight_report(report, registry=registry)
    publish_preflight_report(report, registry=registry, allow_overwrite=True)


def test_published_report_round_trips(setup):
    registry = setup
    _seed_manifest(registry)
    client = _client(version="1.10.6")
    report = run_preflight(
        "q1", "baseline",
        registry=registry, client_factory=_factory(client),
    )
    publish_preflight_report(report, registry=registry)

    body = registry.read_side_preflight("q1", "baseline")
    assert body is not None
    fresh = PreflightReport.model_validate_json(body)
    assert fresh.qual_id == "q1"
    assert fresh.passed == report.passed
    assert [c.name for c in fresh.checks] == [c.name for c in report.checks]


# Helper so mocked .side_effect = exception works cleanly.
BaseException = Exception


# ---------------------------------------------------------------------------
# Deployed sha vs inventory pins
# ---------------------------------------------------------------------------

SHA = "9f2c1ab4d5e6f708192a3b4c5d6e7f8091a2b3c4"
CHECK = "deployed_shas_match_pins"


def _pinned(monkeypatch, registry, pins, **deployment):
    """Seed a manifest, then give it explicit inventory pins."""
    from dag_tools.qual.preflight import preflight as pf

    _seed_manifest(registry)
    real = pf._read_manifest

    def patched(reg, qual_id):
        m = real(reg, qual_id)
        m.inventory_pins = [InventoryPin(repo=r, git_sha=s) for r, s in pins]
        for k, v in deployment.items():
            setattr(m.deployment, k, v)
        return m

    monkeypatch.setattr(pf, "_read_manifest", patched)


def _loc(name, repo=None, sha=None, repo_key="repo", sha_key="git_sha"):
    md = {"host": "h", "port": "4000"}
    if repo is not None:
        md[repo_key] = repo
    if sha is not None:
        md[sha_key] = sha
    return CodeLocationStatus(name=name, load_status="LOADED", display_metadata=md)


def _run(registry, locations, side="candidate"):
    client = _client(
        version="1.12.1" if side == "candidate" else "1.10.6",
        locations=locations,
    )
    report = run_preflight(
        "q1", side, registry=registry, client_factory=_factory(client),
    )
    return report, next(c for c in report.checks if c.name == CHECK)


def test_preflight_fails_when_deployed_sha_does_not_match_pin(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    report, chk = _run(setup, [_loc("a", "domain-a", "deadbeef1234")])
    assert not chk.passed and not report.passed
    assert "deadbeef1234" in chk.detail and SHA in chk.detail and "'a'" in chk.detail


def test_preflight_fails_when_a_pinned_repo_is_not_deployed_at_all(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA), ("domain-b", SHA)])
    report, chk = _run(setup, [_loc("a", "domain-a", SHA)])
    assert not chk.passed
    assert "domain-b" in chk.detail and "not deployed" in chk.detail
    assert [p.matched for p in report.pinned_repos] == [True, False]


def test_preflight_fails_when_no_location_carries_a_stamp(setup, monkeypatch):
    """Non-vacuity: pins present, zero stamps, must FAIL."""
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    report, chk = _run(setup, [_loc("a"), _loc("b")])
    assert not chk.passed and not report.passed
    assert "workspace.yaml" in chk.detail
    assert report.unstamped_locations == ["a", "b"]


def test_preflight_passes_when_every_pin_matches_its_stamped_location(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA), ("domain-b", "abcdef0123456")])
    report, chk = _run(setup, [
        _loc("a", "domain-a", SHA), _loc("b", "domain-b", "ABCDEF0123456"),
    ])
    assert chk.passed and report.passed
    assert "2 pin(s) verified" in chk.detail
    assert all(p.matched for p in report.pinned_repos)


def test_preflight_accepts_a_short_sha_prefix_of_at_least_seven_chars(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    _, chk = _run(setup, [_loc("a", "domain-a", SHA[:7].upper())])
    assert chk.passed


def test_preflight_rejects_a_prefix_shorter_than_seven_chars(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    _, chk = _run(setup, [_loc("a", "domain-a", SHA[:6])])
    assert not chk.passed


def test_preflight_fails_when_two_locations_claim_one_repo_with_different_shas(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    _, chk = _run(setup, [_loc("a1", "domain-a", SHA), _loc("a2", "domain-a", "1234567890")])
    assert not chk.passed
    assert "ambiguous" in chk.detail and "a1" in chk.detail and "a2" in chk.detail


def test_two_replicas_with_the_same_sha_pass(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    _, chk = _run(setup, [_loc("a1", "domain-a", SHA), _loc("a2", "domain-a", SHA)])
    assert chk.passed


def test_an_unstamped_location_is_informational_not_a_failure(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    report, chk = _run(setup, [
        _loc("a", "domain-a", SHA), _loc("dag-tools-probes"),
        _loc("other", "unpinned-repo", "1111111"),
    ])
    assert chk.passed and report.passed
    assert report.unstamped_locations == ["dag-tools-probes"]
    assert "unpinned-repo" in chk.detail


def test_sha_check_runs_on_the_baseline_side_too(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    report, chk = _run(setup, [_loc("a", "domain-a", "deadbeef1234")], side="baseline")
    assert not chk.passed and not report.passed


def test_sha_check_uses_the_manifest_configured_metadata_keys(setup, monkeypatch):
    _pinned(
        monkeypatch, setup, [("domain-a", SHA)],
        sha_metadata_key="build", repo_metadata_key="project",
    )
    _, ok = _run(setup, [_loc("a", "domain-a", SHA, repo_key="project", sha_key="build")])
    assert ok.passed
    _, bad = _run(setup, [_loc("a", "domain-a", SHA)])  # default keys: ignored
    assert not bad.passed


def test_empty_inventory_pins_passes_with_an_explicit_detail(setup):
    _seed_manifest(setup)
    _, chk = _run(setup, [_loc("a")])
    assert chk.passed and "no inventory pins to verify" in chk.detail


def test_sha_check_becomes_a_failed_check_not_an_exception_on_graphql_error(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    report, chk = _run(setup, DagsterGraphQLError("workspace down"))
    assert not chk.passed and not report.passed
    assert "domain-a" in chk.detail


def test_sha_check_does_not_make_a_second_workspace_call(setup, monkeypatch):
    _pinned(monkeypatch, setup, [("domain-a", SHA)])
    calls = []

    def fake_post(self, query, variables=None):
        calls.append(query)
        if query == DagsterGraphQLClient.VERSION_QUERY:
            return {"version": "1.10.6"}
        return {"workspaceOrError": {"__typename": "Workspace", "locationEntries": [{
            "name": "a", "loadStatus": "LOADED", "locationOrLoadError": {},
            "displayMetadata": [{"key": "repo", "value": "domain-a"},
                                {"key": "git_sha", "value": SHA}],
        }]}}

    monkeypatch.setattr(DagsterGraphQLClient, "post", fake_post)
    report = run_preflight(
        "q1", "baseline", registry=setup,
        client_factory=lambda m: DagsterGraphQLClient(endpoint_url="http://x/graphql"),
    )
    assert report.passed
    assert calls.count(DagsterGraphQLClient.LOCATIONS_QUERY) == 1
