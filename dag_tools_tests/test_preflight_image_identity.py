"""Preflight identifies a deployed location by digest, sha tag, then stamp."""
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
    RunStatusInfo,
)
from dag_tools.qual.preflight import run_preflight
from dag_tools.qual.preflight import preflight as pf
from dag_tools.qual.preflight.identity import (
    parse_image_reference,
    sha_from_tag,
)
from dag_tools.qual.qualify import (
    Deployment,
    InventoryPin,
    LocationWaiver,
    PinnedImage,
    VersionTarget,
    create_qualification,
)
from dag_tools.qual.registry import InventoryRegistry, S3Storage, StorageSettings

BUCKET = "dag-tools-identity-test"
CHECK = "deployed_shas_match_pins"
SHA = "9f2c1ab4d5e6f708192a3b4c5d6e7f8091a2b3c4"
OTHER_SHA = "1234567890abcdef1234567890abcdef12345678"
IMG = "registry.example.com:5000/team/app"
D1 = "sha256:" + "a" * 64
D2 = "sha256:" + "b" * 64


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _pin(repo="domain-a", sha=SHA, images=None):
    return InventoryPin(
        repo=repo, git_sha=sha,
        images=None if images is None else [
            PinnedImage(repository=r, digest=d) for r, d in images
        ],
    )


def _setup(monkeypatch, registry, pins, waivers=(), **deployment):
    create_qualification(
        qual_id="q1", registry=registry,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://test/graphql"),
    )
    real = pf._read_manifest

    def patched(reg, qual_id):
        m = real(reg, qual_id)
        m.inventory_pins = list(pins)
        m.deployment.unverified_waivers = [
            LocationWaiver(location=n, reason=r) for n, r in waivers
        ]
        for k, v in deployment.items():
            setattr(m.deployment, k, v)
        return m

    monkeypatch.setattr(pf, "_read_manifest", patched)


def _loc(name, image=None, repo=None, sha=None):
    md = {"host": "h"}
    if image is not None:
        md["image"] = image
    if repo is not None:
        md["repo"] = repo
    if sha is not None:
        md["git_sha"] = sha
    return CodeLocationStatus(name=name, load_status="LOADED", display_metadata=md)


def _run(registry, locations):
    c = MagicMock(spec=DagsterGraphQLClient)
    c.get_dagster_version.return_value = "1.12.1"
    c.get_code_locations.return_value = locations
    c.get_run_status.return_value = RunStatusInfo(run_id="r1", status="SUCCESS")
    c.close.return_value = None
    report = run_preflight("q1", "candidate", registry=registry, client_factory=lambda _m: c)
    return report, next(x for x in report.checks if x.name == CHECK)


def _ident(report, name):
    return next(i for i in report.location_identities if i.location_name == name)


# --- parser ----------------------------------------------------------------


def test_parser_registry_with_a_port_has_no_tag():
    r = parse_image_reference("host:5000/team/app")
    assert (r.repository, r.tag, r.digest, r.error) == ("host:5000/team/app", None, None, None)


def test_parser_digest_only():
    r = parse_image_reference(f"{IMG}@{D1}")
    assert (r.repository, r.tag, r.digest) == (IMG, None, D1)


def test_parser_tag_plus_digest():
    r = parse_image_reference(f"{IMG}:v1.2@{D1}")
    assert (r.repository, r.tag, r.digest) == (IMG, "v1.2", D1)


def test_parser_no_tag_no_digest():
    r = parse_image_reference("team/app")
    assert (r.repository, r.tag, r.digest) == ("team/app", None, None)


def test_parser_a_malformed_digest_is_unparseable_not_half_read():
    r = parse_image_reference(f"{IMG}:sha-{SHA[:7]}@sha256:xyz")
    assert r.error and r.digest is None and r.tag is None


def test_parser_uppercase_hex_is_lowercased():
    r = parse_image_reference(f"{IMG}@sha256:{'A' * 64}")
    assert r.digest == D1


def test_sha_from_tag_needs_the_prefix_and_hex():
    assert sha_from_tag(f"sha-{SHA[:7]}", "sha-") == SHA[:7]
    assert sha_from_tag("sha-zzzzzzz", "sha-") is None
    assert sha_from_tag("v1", "sha-") is None
    assert sha_from_tag("sha-abc", "sha-") is None


# --- digest ----------------------------------------------------------------


def test_a_location_deployed_by_digest_matches_its_pin(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    report, chk = _run(setup, [_loc("a", f"{IMG}@{D1}")])
    assert chk.passed
    assert report.pinned_repos[0].outcome == "matched"
    assert report.pinned_repos[0].source == "digest"
    assert _ident(report, "a").outcome == "matched"


def test_control_one_changed_digest_digit_is_a_mismatch(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    changed = "sha256:" + "a" * 63 + "b"
    report, chk = _run(setup, [_loc("a", f"{IMG}@{changed}")])
    assert not chk.passed
    assert report.pinned_repos[0].outcome == "mismatch"
    assert _ident(report, "a").outcome == "mismatch"


def test_a_release_tag_deployed_by_digest_matches_without_a_registry_lookup(
    setup, monkeypatch,
):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    _, chk = _run(setup, [_loc("a", f"{IMG}:v0.16.0@{D1}")])
    assert chk.passed


def test_control_the_same_release_tag_without_a_digest_is_unverified(
    setup, monkeypatch,
):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    report, chk = _run(setup, [_loc("a", f"{IMG}:v0.16.0")])
    assert not chk.passed
    assert _ident(report, "a").outcome == "unverified"


# --- sha tag ---------------------------------------------------------------


def test_a_sha_tag_matches_its_pin(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()])
    report, chk = _run(setup, [_loc("a", f"{IMG}:sha-{SHA[:7]}")])
    assert chk.passed
    assert report.pinned_repos[0].source == "sha_tag"


def test_control_a_sha_tag_of_another_commit_is_a_mismatch_when_the_repository_is_recorded(
    setup, monkeypatch,
):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    report, chk = _run(setup, [_loc("a", f"{IMG}:sha-{OTHER_SHA[:7]}")])
    assert not chk.passed
    assert report.pinned_repos[0].outcome == "mismatch"


def test_control_a_sha_tag_of_another_commit_leaves_the_pin_unmatched_when_it_is_not(
    setup, monkeypatch,
):
    _setup(monkeypatch, setup, [_pin()])
    report, chk = _run(setup, [_loc("a", f"{IMG}:sha-{OTHER_SHA[:7]}")])
    assert not chk.passed
    assert report.pinned_repos[0].outcome == "unmatched"
    assert _ident(report, "a").outcome == "unpinned"


# --- digest outranks tag ---------------------------------------------------


def test_the_digest_decides_over_a_tag_that_claims_the_pin(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    report, chk = _run(setup, [_loc("a", f"{IMG}:sha-{SHA[:7]}@{D2}")])
    assert not chk.passed
    assert report.pinned_repos[0].outcome == "mismatch"
    assert report.pinned_repos[0].source == "digest"


def test_control_the_same_tag_with_the_pins_digest_matches(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    _, chk = _run(setup, [_loc("a", f"{IMG}:sha-{SHA[:7]}@{D1}")])
    assert chk.passed


# --- the check's verdicts --------------------------------------------------


def test_a_mismatch_fails_the_check(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    report, chk = _run(setup, [_loc("a", f"{IMG}@{D2}")])
    assert not chk.passed and not report.passed
    assert "dagtools registry record-image" in chk.detail


def test_a_pin_no_location_matches_fails(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    report, chk = _run(setup, [_loc("a", "other.example.com/x@" + D2)])
    assert not chk.passed
    assert report.pinned_repos[0].outcome == "unmatched"
    assert "not deployed" in chk.detail


def test_control_adding_the_location_passes(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin(images=[(IMG, D1)])])
    _, chk = _run(setup, [
        _loc("a", "other.example.com/x@" + D2), _loc("b", f"{IMG}@{D1}"),
    ])
    assert chk.passed


def test_an_unverified_location_fails_the_check(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()])
    report, chk = _run(setup, [
        _loc("a", f"{IMG}:sha-{SHA[:7]}"), _loc("probes"),
    ])
    assert not chk.passed and not report.passed
    assert "probes" in chk.detail and "--waive-unverified-location" in chk.detail
    assert report.unstamped_locations == ["probes"]


def test_control_a_waiver_row_passes_it_and_records_the_reason(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()], waivers=[("probes", "no image of its own")])
    report, chk = _run(setup, [
        _loc("a", f"{IMG}:sha-{SHA[:7]}"), _loc("probes"),
    ])
    assert chk.passed
    ident = _ident(report, "probes")
    assert ident.outcome == "waived" and ident.waiver_reason == "no image of its own"
    assert report.unstamped_locations == ["probes"]
    assert report.unused_waivers == []


def test_a_waiver_for_another_location_does_not_pass_it(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()], waivers=[("elsewhere", "reason")])
    report, chk = _run(setup, [
        _loc("a", f"{IMG}:sha-{SHA[:7]}"), _loc("probes"),
    ])
    assert not chk.passed
    assert report.unused_waivers == ["elsewhere"]


def test_a_waiver_without_a_reason_is_refused_by_the_manifest():
    with pytest.raises(Exception):
        LocationWaiver(location="probes", reason="")


def test_an_unpinned_location_is_informational(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()])
    report, chk = _run(setup, [
        _loc("a", f"{IMG}:sha-{SHA[:7]}"), _loc("o", "other.example.com/x:v1@" + D2),
    ])
    assert chk.passed
    assert _ident(report, "o").outcome == "unpinned"
    assert "o" in chk.detail


def test_control_the_same_location_without_a_sha_is_unverified(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()])
    report, chk = _run(setup, [
        _loc("a", f"{IMG}:sha-{SHA[:7]}"), _loc("o", "other.example.com/x:v1"),
    ])
    assert not chk.passed
    assert _ident(report, "o").outcome == "unverified"


def test_the_four_outcomes_land_in_four_buckets(setup, monkeypatch):
    _setup(
        monkeypatch, setup,
        [_pin("good", SHA, [(IMG, D1)]), _pin("bad", OTHER_SHA, [("r.example.com/bad", D1)])],
        waivers=[("waived", "reason")],
    )
    report, _ = _run(setup, [
        _loc("m", f"{IMG}@{D1}"),
        _loc("x", f"r.example.com/bad@{D2}"),
        _loc("u", "other.example.com/x@" + D2),
        _loc("v"),
        _loc("waived"),
    ])
    by = {}
    for i in report.location_identities:
        by.setdefault(i.outcome, []).append(i.location_name)
    assert by == {
        "matched": ["m"], "mismatch": ["x"], "unpinned": ["u"],
        "unverified": ["v"], "waived": ["waived"],
    }
    assert sorted(n for names in by.values() for n in names) == sorted(
        i.location_name for i in report.location_identities
    )


# --- zero pins -------------------------------------------------------------


def test_with_no_pins_an_unverified_location_is_reported_not_gated(setup, monkeypatch):
    _setup(monkeypatch, setup, [])
    report, chk = _run(setup, [_loc("a")])
    assert chk.passed
    assert "no inventory pins to verify" in chk.detail
    assert "1 location(s) unverified; not gated because nothing is pinned" in chk.detail
    assert _ident(report, "a").outcome == "unverified"


def test_control_with_one_pin_the_same_location_fails(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()])
    _, chk = _run(setup, [_loc("a")])
    assert not chk.passed


# --- stamp (section 6) -----------------------------------------------------


def test_the_workspace_yaml_stamp_still_matches(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()])
    report, chk = _run(setup, [_loc("a", repo="domain-a", sha=SHA)])
    assert chk.passed
    assert report.pinned_repos[0].source == "stamp"


def test_control_a_stamp_naming_another_sha_is_a_mismatch(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()])
    report, chk = _run(setup, [_loc("a", repo="domain-a", sha="deadbeef1234")])
    assert not chk.passed
    assert report.pinned_repos[0].outcome == "mismatch"
    assert report.pinned_repos[0].source == "stamp"


def test_the_configured_keys_and_prefix_are_used(setup, monkeypatch):
    _setup(monkeypatch, setup, [_pin()], image_metadata_key="img", sha_tag_prefix="c-")
    loc = CodeLocationStatus(
        name="a", load_status="LOADED",
        display_metadata={"img": f"{IMG}:c-{SHA[:7]}"},
    )
    _, chk = _run(setup, [loc])
    assert chk.passed


def test_the_report_schema_is_3(setup, monkeypatch):
    _setup(monkeypatch, setup, [])
    report, _ = _run(setup, [_loc("a")])
    assert report.schema_version == 3
