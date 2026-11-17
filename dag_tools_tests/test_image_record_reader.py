"""The reader: fetch a pinned image record, pin its hash, re-verify at preflight."""
import ast
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("yaml")
pytest.importorskip("httpx")

import boto3
import httpx
import yaml
from moto import mock_aws

from dag_tools.qual.graphql import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    RunStatusInfo,
)
from dag_tools.qual.image_record import (
    ImageRecord,
    RecordedImage,
    render_image_record,
)
from dag_tools.qual.image_record_source import (
    MAX_RECORD_BYTES,
    ImageRecordAbsent,
    ImageRecordSourceError,
    ImageRecordUnreadable,
    fetch_image_record,
    sha256_hex,
)
from dag_tools.qual.preflight import run_preflight
from dag_tools.qual.preflight import preflight as pf
from dag_tools.qual.qualify import (
    Deployment,
    ImageRecordSource,
    InventoryPin,
    PinnedImage,
    QualificationManifest,
    VersionTarget,
    create_qualification,
)
from dag_tools.qual.qualify import manifest as manifest_mod
from dag_tools.qual.registry import (
    BuildMeta,
    InventoryRegistry,
    S3Storage,
    StorageSettings,
)

BUCKET = "dag-tools-reader-test"
CHECK = "deployed_shas_match_pins"
SHA = "9f2c1ab4d5e6f708192a3b4c5d6e7f8091a2b3c4"
OTHER_SHA = "1234567890abcdef1234567890abcdef12345678"
IMG = "registry.example.com:5000/team/app"
D1 = "sha256:" + "a" * 64
D2 = "sha256:" + "b" * 64
URL = "https://records.example.com/image.json"
NOW = datetime(2026, 11, 16, tzinfo=timezone.utc)


def _body(repo="domain-a", sha=SHA, images=((IMG, D1),)) -> bytes:
    return render_image_record(ImageRecord(
        repo=repo, git_sha=sha, recorded_at=NOW,
        images=[RecordedImage(repository=r, digest=d) for r, d in images],
    ))


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def _serving(body, status=200):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(status, content=body)

    return _client(handler), seen


# --- fetch ------------------------------------------------------------------


def test_https_200_returns_the_bytes():
    c, seen = _serving(b"abc")
    assert fetch_image_record(URL, client=c) == b"abc"
    assert len(seen) == 1


def test_a_404_is_absent_and_says_to_retry():
    c, _ = _serving(b"", 404)
    with pytest.raises(ImageRecordAbsent) as e:
        fetch_image_record(URL, client=c)
    assert e.value.kind == "absent" and e.value.source == URL
    assert "retry" in str(e.value)


def test_a_500_is_unreadable_and_carries_no_body():
    c, _ = _serving(b"SECRET-BODY", 500)
    with pytest.raises(ImageRecordUnreadable) as e:
        fetch_image_record(URL, client=c)
    assert e.value.kind == "unreadable" and "500" in str(e.value)
    assert "SECRET-BODY" not in str(e.value)


def test_a_transport_error_is_unreadable():
    def boom(request):
        raise httpx.ConnectError("down")

    with pytest.raises(ImageRecordUnreadable) as e:
        fetch_image_record(URL, client=_client(boom))
    assert "ConnectError" in str(e.value)


@pytest.mark.parametrize("src", ["http://x.example/image.json", "ftp://x/y", "file:///etc/passwd"])
def test_a_scheme_other_than_https_is_refused_before_any_request(src):
    def never(request):
        raise AssertionError("transport must not be called")

    with pytest.raises(ImageRecordUnreadable) as e:
        fetch_image_record(src, client=_client(never))
    assert src.split(":")[0] in str(e.value)


def test_a_redirect_to_http_is_refused():
    def handler(request):
        if request.url.scheme == "https":
            return httpx.Response(302, headers={"location": "http://evil.example/x"})
        return httpx.Response(200, content=b"x")

    with pytest.raises(ImageRecordUnreadable):
        fetch_image_record(URL, client=_client(handler))


def test_control_a_redirect_to_https_is_followed():
    def handler(request):
        if request.url.host == "records.example.com":
            return httpx.Response(302, headers={"location": "https://cdn.example.com/x"})
        return httpx.Response(200, content=b"final")

    assert fetch_image_record(URL, client=_client(handler)) == b"final"


def test_the_cap_is_exact():
    ok, _ = _serving(b"x" * MAX_RECORD_BYTES)
    assert len(fetch_image_record(URL, client=ok)) == MAX_RECORD_BYTES
    big, _ = _serving(b"x" * (MAX_RECORD_BYTES + 1))
    with pytest.raises(ImageRecordUnreadable):
        fetch_image_record(URL, client=big)


def test_a_local_path_is_read_and_a_missing_one_is_absent(tmp_path):
    f = tmp_path / "r.json"
    f.write_bytes(b"hello")
    assert fetch_image_record(str(f)) == b"hello"
    with pytest.raises(ImageRecordAbsent):
        fetch_image_record(str(tmp_path / "nope.json"))


def test_an_oversized_local_file_is_refused(tmp_path):
    f = tmp_path / "r.json"
    f.write_bytes(b"x" * (MAX_RECORD_BYTES + 1))
    with pytest.raises(ImageRecordUnreadable):
        fetch_image_record(str(f))


def test_no_request_carries_an_authorization_header():
    c, seen = _serving(b"abc")
    fetch_image_record(URL, client=c)
    assert all("authorization" not in r.headers for r in seen)


def test_sha256_hex_is_lowercase_hex():
    assert sha256_hex(b"") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


# --- init -------------------------------------------------------------------


@pytest.fixture
def registry(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _publish(reg, repo="domain-a", sha=SHA):
    reg.publish_build(
        repo=repo, git_sha=sha, artifacts={},
        meta=BuildMeta(
            repo=repo, git_sha=sha,
            timestamp=datetime.now(tz=timezone.utc) - timedelta(hours=1),
        ),
    )


def _init(reg, **kw):
    return create_qualification(
        qual_id=kw.pop("qual_id", "q1"), registry=reg,
        baseline=VersionTarget(dagster="1.10.6"),
        candidate=VersionTarget(dagster="1.12.1"),
        deployment=Deployment(graphql_url="http://test/graphql"),
        **kw,
    )


def test_the_template_is_filled_from_the_pin_and_the_hash_is_the_bytes(registry):
    _publish(registry)
    body = _body()
    c, seen = _serving(body)
    m = _init(
        registry, record_client=c,
        image_record_templates={"domain-a": "https://r.example.com/{repo}/{sha}/image.json"},
    )
    pin = m.inventory_pins[0]
    assert str(seen[0].url) == f"https://r.example.com/domain-a/{SHA}/image.json"
    assert pin.image_record == ImageRecordSource(
        source=f"https://r.example.com/domain-a/{SHA}/image.json",
        sha256=sha256_hex(body),
    )
    assert [(i.repository, i.digest) for i in pin.images] == [(IMG, D1)]


def test_the_override_beats_the_template(registry, tmp_path):
    _publish(registry)
    f = tmp_path / "r.json"
    f.write_bytes(_body())

    def never(request):
        raise AssertionError("template must not be fetched")

    m = _init(
        registry, record_client=_client(never),
        image_record_templates={"domain-a": "https://r.example.com/{sha}"},
        image_record_sources={"domain-a": str(f)},
    )
    assert m.inventory_pins[0].image_record.source == str(f)


@pytest.mark.parametrize("template", [
    "https://r.example.com/{repo}/image.json",      # no {sha}
    "https://r.example.com/{sha}/{branch}.json",    # unknown placeholder
    "https://r.example.com/{sha}/{",                # stray brace
])
def test_a_bad_template_is_refused_before_any_fetch(registry, template):
    _publish(registry)

    def never(request):
        raise AssertionError("no fetch")

    with pytest.raises(ValueError, match="template"):
        _init(registry, record_client=_client(never),
              image_record_templates={"domain-a": template})


def test_control_a_good_template_is_accepted(registry):
    _publish(registry)
    c, _ = _serving(_body())
    _init(registry, record_client=c,
          image_record_templates={"domain-a": "https://r.example.com/{sha}"})


def test_a_repo_naming_no_pin_is_refused(registry):
    _publish(registry)
    with pytest.raises(ValueError, match="match no pin"):
        _init(registry, image_record_sources={"domian-a": "x"})
    with pytest.raises(ValueError, match="match no pin"):
        _init(registry, image_record_templates={"domian-a": "https://x/{sha}"})


def test_a_record_for_another_sha_fails_init(registry):
    _publish(registry)
    c, _ = _serving(_body(sha=OTHER_SHA))
    with pytest.raises(ValueError, match="domain-a"):
        _init(registry, record_client=c,
              image_record_sources={"domain-a": URL})


def test_a_record_that_disagrees_with_the_bucket_record_fails_init(registry):
    _publish(registry)
    registry.record_image("domain-a", SHA, [RecordedImage(repository=IMG, digest=D2)])
    c, _ = _serving(_body())
    with pytest.raises(ValueError, match="disagrees"):
        _init(registry, record_client=c, image_record_sources={"domain-a": URL})


def test_control_a_record_that_agrees_with_the_bucket_record_passes(registry):
    _publish(registry)
    registry.record_image("domain-a", SHA, [RecordedImage(repository=IMG, digest=D1)])
    c, _ = _serving(_body())
    m = _init(registry, record_client=c, image_record_sources={"domain-a": URL})
    assert m.inventory_pins[0].image_record is not None


def test_an_absent_source_fails_init_with_the_absent_message(registry):
    _publish(registry)
    c, _ = _serving(b"", 404)
    with pytest.raises(ValueError, match="absent at source"):
        _init(registry, record_client=c, image_record_sources={"domain-a": URL})


def test_an_unparseable_record_fails_init(registry):
    _publish(registry)
    c, _ = _serving(b"not json")
    with pytest.raises(ValueError, match="domain-a"):
        _init(registry, record_client=c, image_record_sources={"domain-a": URL})


def test_problems_in_two_pins_are_reported_together_and_no_manifest_is_written(registry):
    _publish(registry, "domain-a")
    _publish(registry, "domain-b")
    c, _ = _serving(b"", 404)
    with pytest.raises(ValueError) as e:
        _init(registry, record_client=c, image_record_sources={
            "domain-a": URL, "domain-b": URL + "?b"})
    assert "domain-a" in str(e.value) and "domain-b" in str(e.value)
    assert registry.storage.get_optional("qualifications/q1/manifest.yaml") is None


def test_no_source_gives_todays_manifest_apart_from_the_schema_version(registry):
    _publish(registry)
    m = _init(registry)
    assert m.inventory_pins[0].image_record is None
    assert m.schema_version == manifest_mod.SCHEMA_VERSION == 6


def test_a_v5_manifest_still_loads():
    m = QualificationManifest.model_validate({
        "schema_version": 5, "qual_id": "q", "created_at": NOW,
        "baseline": {"dagster": "1"}, "candidate": {"dagster": "2"},
        "inventory_pins": [{"repo": "r", "git_sha": SHA}],
    })
    assert m.inventory_pins[0].image_record is None


def test_the_pinned_hash_must_be_64_lowercase_hex():
    with pytest.raises(ValueError):
        ImageRecordSource(source="x", sha256="A" * 64)
    with pytest.raises(ValueError):
        ImageRecordSource(source="x", sha256="a" * 63)


# --- preflight --------------------------------------------------------------


def _pin(source=URL, body=None, images=((IMG, D1),)):
    body = _body() if body is None else body
    return InventoryPin(
        repo="domain-a", git_sha=SHA,
        images=[PinnedImage(repository=r, digest=d) for r, d in images],
        image_record=None if source is None else ImageRecordSource(
            source=source, sha256=sha256_hex(body)),
    )


def _setup(monkeypatch, registry, pins):
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
        return m

    monkeypatch.setattr(pf, "_read_manifest", patched)


def _loc(image):
    return CodeLocationStatus(
        name="a", load_status="LOADED",
        display_metadata={"host": "h", "image": image},
    )


def _run(registry, image, record_client):
    c = MagicMock(spec=DagsterGraphQLClient)
    c.get_dagster_version.return_value = "1.12.1"
    c.get_code_locations.return_value = [_loc(image)]
    c.get_run_status.return_value = RunStatusInfo(run_id="r1", status="SUCCESS")
    c.close.return_value = None
    report = run_preflight(
        "q1", "candidate", registry=registry,
        client_factory=lambda _m: c, record_client=record_client,
    )
    return report, next(x for x in report.checks if x.name == CHECK)


def test_unchanged_bytes_and_a_digest_matching_location_match(registry, monkeypatch):
    _setup(monkeypatch, registry, [_pin()])
    c, _ = _serving(_body())
    report, chk = _run(registry, f"{IMG}@{D1}", c)
    assert chk.passed
    assert report.pinned_repos[0].outcome == "matched"
    assert report.schema_version == 4


def test_changed_bytes_fail_even_when_the_location_matches_by_digest(registry, monkeypatch):
    _setup(monkeypatch, registry, [_pin()])
    c, _ = _serving(_body(images=((IMG, D2),)))
    report, chk = _run(registry, f"{IMG}@{D1}", c)
    r = report.pinned_repos[0]
    assert not chk.passed and not r.matched
    assert (r.outcome, r.image_record_problem, r.image_record_source) == (
        "record_failed", "changed", URL)
    assert report.location_identities[0].outcome == "matched"  # still published


def test_an_absent_source_fails_even_when_the_location_matches_by_sha_tag(
    registry, monkeypatch,
):
    _setup(monkeypatch, registry, [_pin(images=())])
    c, _ = _serving(b"", 404)
    report, chk = _run(registry, f"{IMG}:sha-{SHA[:7]}", c)
    r = report.pinned_repos[0]
    assert not chk.passed
    assert (r.outcome, r.image_record_problem) == ("record_failed", "absent")
    assert "retry" in r.detail


def test_control_the_same_location_and_pin_without_a_record_match_by_sha_tag(
    registry, monkeypatch,
):
    _setup(monkeypatch, registry, [_pin(source=None, images=())])
    report, chk = _run(registry, f"{IMG}:sha-{SHA[:7]}", None)
    assert chk.passed
    assert report.pinned_repos[0].source == "sha_tag"


def test_unreadable_is_record_failed(registry, monkeypatch):
    _setup(monkeypatch, registry, [_pin()])
    c, _ = _serving(b"", 500)
    report, _chk = _run(registry, f"{IMG}@{D1}", c)
    assert report.pinned_repos[0].image_record_problem == "unreadable"


def test_unchanged_hash_but_unparseable_is_unparseable(registry, monkeypatch):
    junk = b"not json"
    _setup(monkeypatch, registry, [_pin(body=junk)])
    c, _ = _serving(junk)
    report, _chk = _run(registry, f"{IMG}@{D1}", c)
    assert report.pinned_repos[0].image_record_problem == "unparseable"


def test_unchanged_hash_but_another_commit_is_other_commit(registry, monkeypatch):
    other = _body(sha=OTHER_SHA)
    _setup(monkeypatch, registry, [_pin(body=other)])
    c, _ = _serving(other)
    report, _chk = _run(registry, f"{IMG}@{D1}", c)
    assert report.pinned_repos[0].image_record_problem == "other_commit"


def test_a_fetch_error_never_raises_out_of_run_preflight(registry, monkeypatch):
    _setup(monkeypatch, registry, [_pin()])

    def boom(request):
        raise RuntimeError("kaboom")

    report, chk = _run(registry, f"{IMG}@{D1}", _client(boom))
    assert not chk.passed
    assert report.pinned_repos[0].outcome == "record_failed"


def test_the_roll_up_names_the_source_and_does_not_mention_tags(registry, monkeypatch):
    _setup(monkeypatch, registry, [_pin()])
    c, _ = _serving(b"", 404)
    _report, chk = _run(registry, f"{IMG}:sha-{SHA[:7]}", c)
    assert URL in chk.detail
    assert "qual init" in chk.detail
    assert "tag" not in chk.detail.lower()
    assert "stamp" not in chk.detail.lower()


# --- structure --------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent / "dag_tools"


def test_the_pure_module_still_imports_no_httpx():
    code = (
        "import sys, dag_tools.qual.image_record\n"
        "sys.exit(1 if 'httpx' in sys.modules else 0)"
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_the_new_httpx_importers_are_the_reader_modules_only():
    def imports_httpx(path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for n in tree.body:  # module level only; TYPE_CHECKING blocks are nested
            if isinstance(n, ast.Import) and any(a.name.split(".")[0] == "httpx" for a in n.names):
                return True
            if isinstance(n, ast.ImportFrom) and (n.module or "").split(".")[0] == "httpx":
                return True
        return False

    qual = ROOT / "qual"
    found = {p.relative_to(qual).as_posix() for p in qual.rglob("*.py") if imports_httpx(p)}
    # graphql/client.py predates the reader; init and preflight only name the
    # client type under TYPE_CHECKING.
    assert found == {"image_record_source.py", "graphql/client.py"}
