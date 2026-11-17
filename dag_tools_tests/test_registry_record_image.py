"""``dagtools registry record-image`` and ``qual init`` copying the record."""
import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

pytest.importorskip("moto")
pytest.importorskip("boto3")
pytest.importorskip("typer")
pytest.importorskip("yaml")
pytest.importorskip("httpx")

import boto3
from moto import mock_aws
from pydantic import ValidationError
from typer.testing import CliRunner

from dag_tools.qual.cli import app
from dag_tools.qual.graphql import DagsterGraphQLClient
from dag_tools.qual.qualify import Deployment, LocationWaiver
from dag_tools.qual.registry import (
    BuildMeta,
    ImageRecordMismatch,
    InventoryRegistry,
    RecordedImage,
    S3Storage,
    StorageSettings,
    layout,
)

BUCKET = "dag-tools-record-image-test"
SHA = "9f2c1ab4d5e6f708192a3b4c5d6e7f8091a2b3c4"
D1 = "sha256:" + "a" * 64
D2 = "sha256:" + "b" * 64
REPO_IMG = "registry.example.com:5000/team/app"


@pytest.fixture
def reg(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGTOOLS_HOME", str(tmp_path))
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield InventoryRegistry(S3Storage(StorageSettings(bucket=BUCKET)))


def _invoke(*args):
    return CliRunner().invoke(app, ["--registry", f"s3://{BUCKET}", *args])


def _record(*refs, sha=SHA, repo="domain-a"):
    args = ["registry", "record-image", "--repo", repo, "--sha", sha]
    for r in refs:
        args += ["--image", r]
    return _invoke(*args)


def _publish(reg, repo, sha):
    when = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    reg.publish_build(
        repo=repo, git_sha=sha, artifacts={},
        meta=BuildMeta(repo=repo, git_sha=sha, timestamp=when),
    )


def test_record_image_writes_under_the_layout_key_and_leaves_latest_alone(reg):
    _publish(reg, "domain-a", SHA)
    latest = reg.read_latest_pointer("domain-a")
    upper = "sha256:" + "A" * 64
    r = _record(f"{REPO_IMG}:v1@{upper}")
    assert r.exit_code == 0, r.output
    body = reg.storage.get_optional(
        layout.inventory_artifact_key("domain-a", SHA, layout.IMAGE_FILE)
    )
    rec = json.loads(body)
    assert rec["repo"] == "domain-a" and rec["git_sha"] == SHA
    assert rec["images"] == [{"repository": REPO_IMG, "digest": D1}]
    assert reg.read_latest_pointer("domain-a") == latest


def test_record_image_refuses_a_reference_without_a_digest(reg):
    r = _record(f"{REPO_IMG}:v1")
    assert r.exit_code != 0
    assert reg.read_image_record("domain-a", SHA) is None


def test_control_the_same_reference_with_a_digest_is_accepted(reg):
    assert _record(f"{REPO_IMG}:v1@{D1}").exit_code == 0


def test_record_image_is_idempotent_for_the_same_digests(reg):
    first = _record(f"{REPO_IMG}@{D1}")
    second = _record(f"{REPO_IMG}:other@{D1}")
    assert first.exit_code == 0 and second.exit_code == 0
    assert (
        json.loads(first.output)["recorded_at"]
        == json.loads(second.output)["recorded_at"]
    )


def test_record_image_refuses_a_different_digest_for_a_recorded_commit(reg):
    assert _record(f"{REPO_IMG}@{D1}").exit_code == 0
    r = _record(f"{REPO_IMG}@{D2}")
    assert r.exit_code != 0
    key = layout.inventory_artifact_key("domain-a", SHA, layout.IMAGE_FILE)
    assert key in r.output.replace("\n", "")
    assert [i.digest for i in reg.read_image_record("domain-a", SHA).images] == [D1]


def test_record_image_never_touches_latest_json(reg):
    _publish(reg, "domain-a", SHA)
    before = reg.read_latest_pointer("domain-a")
    assert _record(f"{REPO_IMG}@{D1}").exit_code == 0
    assert reg.read_latest_pointer("domain-a") == before


def test_the_registry_api_refuses_a_different_set(reg):
    reg.record_image("r", SHA, [RecordedImage(repository="x", digest=D1)])
    with pytest.raises(ImageRecordMismatch):
        reg.record_image("r", SHA, [RecordedImage(repository="x", digest=D2)])


# --- qual init -------------------------------------------------------------


def _init(*extra):
    return _invoke(
        "qual", "init", "--id", "q1", "--baseline", "1.10.6",
        "--candidate", "1.12.1", *extra,
    )


def test_init_copies_the_recorded_images_into_the_pin(reg):
    _publish(reg, "domain-a", SHA)
    assert _record(f"{REPO_IMG}@{D1}").exit_code == 0
    r = _init()
    assert r.exit_code == 0, r.output
    pin = json.loads(r.output)["inventory_pins"][0]
    assert pin["images"] == [{"repository": REPO_IMG, "digest": D1}]


def test_init_without_a_record_leaves_images_none(reg):
    _publish(reg, "domain-a", SHA)
    r = _init()
    assert r.exit_code == 0, r.output
    assert json.loads(r.output)["inventory_pins"][0]["images"] is None


def test_init_fails_on_a_malformed_record_naming_the_repo_and_key(reg):
    _publish(reg, "domain-a", SHA)
    key = layout.inventory_artifact_key("domain-a", SHA, layout.IMAGE_FILE)
    reg.storage.put_immutable(key, b"{not json")
    r = _init()
    assert r.exit_code != 0
    flat = r.output.replace("\n", "")
    assert "domain-a" in flat and key in flat
    assert reg.read_qualification_manifest("q1") is None


def test_init_fails_on_a_record_naming_another_commit(reg):
    _publish(reg, "domain-a", SHA)
    other = "1" * 40
    assert _record(f"{REPO_IMG}@{D1}", sha=other).exit_code == 0
    src = layout.inventory_artifact_key("domain-a", other, layout.IMAGE_FILE)
    dst = layout.inventory_artifact_key("domain-a", SHA, layout.IMAGE_FILE)
    reg.storage.put_immutable(dst, reg.storage.get_optional(src))
    r = _init()
    assert r.exit_code != 0 and "domain-a" in r.output
    assert reg.read_qualification_manifest("q1") is None


def test_init_parses_waivers_and_refuses_one_without_a_reason(reg):
    _publish(reg, "domain-a", SHA)
    r = _init(
        "--waive-unverified-location", "probes=no image is built for it = ok",
        "--image-metadata-key", "img", "--sha-tag-prefix", "commit-",
    )
    assert r.exit_code == 0, r.output
    dep = json.loads(r.output)["deployment"]
    assert dep["unverified_waivers"] == [
        {"location": "probes", "reason": "no image is built for it = ok"}
    ]
    assert dep["image_metadata_key"] == "img" and dep["sha_tag_prefix"] == "commit-"
    assert _init("--waive-unverified-location", "probes=").exit_code != 0


@pytest.mark.parametrize("row", ["noequals", "=reason", "name=", "name=  "])
def test_a_malformed_waiver_row_is_refused(reg, row):
    _publish(reg, "domain-a", SHA)
    assert _init("--waive-unverified-location", row).exit_code != 0


# --- manifest model --------------------------------------------------------


def test_a_waiver_without_a_reason_is_refused_by_the_manifest():
    with pytest.raises(ValidationError):
        LocationWaiver(location="a", reason="  ")
    with pytest.raises(ValidationError):
        LocationWaiver(location="a")


def test_two_waiver_rows_for_one_location_are_refused():
    rows = [
        LocationWaiver(location="a", reason="x"),
        LocationWaiver(location="a", reason="y"),
    ]
    with pytest.raises(ValidationError):
        Deployment(unverified_waivers=rows)


# --- client defect 5.2 ----------------------------------------------------


class _Unrenderable:
    def __str__(self):
        raise RuntimeError("cannot render")


def test_one_malformed_display_metadata_entry_drops_only_itself_and_warns(
    caplog,
):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"data": {"workspaceOrError": {
        "__typename": "Workspace",
        "locationEntries": [{
            "name": "loc1", "loadStatus": "LOADED",
            "displayMetadata": [
                {"key": "host", "value": "h"},
                {"key": "bad", "value": _Unrenderable()},
                "not-a-dict",
                {"key": "image", "value": f"{REPO_IMG}@{D1}"},
            ],
        }],
    }}}
    http = MagicMock()
    http.post.return_value = resp
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with caplog.at_level(logging.WARNING):
        (loc,) = client.get_code_locations()
    assert loc.display_metadata == {"host": "h", "image": f"{REPO_IMG}@{D1}"}
    assert sum("loc1" in r.getMessage() for r in caplog.records) == 2


# --- the document's validator at every door --------------------------------


class _ExplodingStorage:
    def __getattr__(self, name):
        raise AssertionError(f"storage touched: {name}")


def test_an_invalid_offer_touches_no_storage():
    from dag_tools.qual.image_record import ImageRecordInvalid

    bad = InventoryRegistry(_ExplodingStorage())
    with pytest.raises(ImageRecordInvalid):
        bad.record_image("r", SHA, [RecordedImage(repository="x", digest="nope")])
    with pytest.raises(ImageRecordInvalid):
        bad.record_image("r", SHA, [])


def test_record_image_writes_the_canonical_bytes(reg):
    from dag_tools.qual.image_record import render_image_record

    now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
    rec = reg.record_image(
        "r", SHA,
        [RecordedImage(repository="z/b", digest=D2), RecordedImage(repository="a/c", digest=D1)],
        now=now,
    )
    body = reg.storage.get_optional(layout.inventory_artifact_key("r", SHA, layout.IMAGE_FILE))
    assert body == render_image_record(rec)
    assert [i["repository"] for i in json.loads(body)["images"]] == ["a/c", "z/b"]


def test_read_image_record_refuses_a_hand_edited_uppercase_digest(reg):
    from dag_tools.qual.image_record import ImageRecordInvalid

    key = layout.inventory_artifact_key("r", SHA, layout.IMAGE_FILE)
    doc = {"schema_version": 1, "repo": "r", "git_sha": SHA,
           "images": [{"repository": "x", "digest": "sha256:" + "A" * 64}],
           "recorded_at": "2026-11-16T12:00:00Z"}
    reg.storage.put_immutable(key, json.dumps(doc).encode())
    with pytest.raises(ImageRecordInvalid):
        reg.read_image_record("r", SHA)
    doc["images"][0]["digest"] = D1
    reg.storage.put_mutable(key, json.dumps(doc).encode())
    assert reg.read_image_record("r", SHA).images[0].digest == D1


def _published_file(tmp_path, sha=SHA, repo="domain-a"):
    from dag_tools.qual.image_record import build_image_record, render_image_record

    rec = build_image_record(
        repo, sha, [f"{REPO_IMG}@{D1}"],
        now=datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc),
    )
    p = tmp_path / "image.json"
    p.write_bytes(render_image_record(rec))
    return p


def test_cli_from_file_records_what_the_file_says(reg, tmp_path):
    p = _published_file(tmp_path)
    r = _invoke("registry", "record-image", "--repo", "domain-a", "--sha", SHA,
                "--from-file", str(p))
    assert r.exit_code == 0, r.output
    stored = reg.read_image_record("domain-a", SHA)
    assert [(i.repository, i.digest) for i in stored.images] == [(REPO_IMG, D1)]
    assert stored.recorded_at == datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)


def test_cli_from_file_for_another_sha_is_refused(reg, tmp_path):
    p = _published_file(tmp_path, sha="1" * 40)
    r = _invoke("registry", "record-image", "--repo", "domain-a", "--sha", SHA,
                "--from-file", str(p))
    assert r.exit_code == 2
    assert reg.read_image_record("domain-a", SHA) is None


def test_cli_from_file_and_image_together_is_refused(reg, tmp_path):
    p = _published_file(tmp_path)
    both = _invoke("registry", "record-image", "--repo", "domain-a", "--sha", SHA,
                   "--from-file", str(p), "--image", f"{REPO_IMG}@{D1}")
    neither = _invoke("registry", "record-image", "--repo", "domain-a", "--sha", SHA)
    for r in (both, neither):
        assert r.exit_code == 2
        assert "--image" in r.output and "--from-file" in r.output
    assert reg.read_image_record("domain-a", SHA) is None


def test_cli_reports_every_bad_image_at_once(reg):
    r = _record(f"{REPO_IMG}:v1", "other/x@sha256:zz", f"ok/y@{D1}")
    assert r.exit_code == 2
    assert REPO_IMG in r.output and "other/x" in r.output
    assert reg.read_image_record("domain-a", SHA) is None
