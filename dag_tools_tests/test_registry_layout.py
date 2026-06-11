"""Pure unit tests for the registry layout helpers — no S3, no moto."""
import pytest

from dag_tools.qual.registry import layout


def test_inventory_repo_prefix():
    assert layout.inventory_repo_prefix("patriot") == "inventory/patriot/"


def test_inventory_build_prefix():
    assert (
        layout.inventory_build_prefix("patriot", "abc123")
        == "inventory/patriot/abc123/"
    )


def test_inventory_artifact_key_uses_constants():
    assert (
        layout.inventory_artifact_key("patriot", "abc123", layout.ASSETS_FILE)
        == "inventory/patriot/abc123/assets.json"
    )
    assert (
        layout.inventory_artifact_key("patriot", "abc123", layout.META_FILE)
        == "inventory/patriot/abc123/meta.json"
    )


def test_latest_pointer_key_lives_above_build_prefix():
    """The pointer must NOT be inside the per-SHA prefix — that would make it
    look like just another immutable artifact."""
    pointer = layout.latest_pointer_key("patriot")
    build = layout.inventory_build_prefix("patriot", "abc123")
    assert pointer == "inventory/patriot/latest.json"
    assert not pointer.startswith(build)


def test_survey_artifacts_is_well_defined():
    """Recipe says the survey writes a fixed set of artifacts. The list owns
    that contract and other modules consume it — verify it has the expected
    members and meta is present so publish_build's explicit param matches."""
    assert layout.META_FILE in layout.SURVEY_ARTIFACTS
    assert layout.ASSETS_FILE in layout.SURVEY_ARTIFACTS
    assert layout.IO_MANAGERS_FILE in layout.SURVEY_ARTIFACTS


def test_qualification_prefixes():
    assert layout.qualification_prefix("q-2026") == "qualifications/q-2026/"
    assert (
        layout.qualification_manifest_key("q-2026")
        == "qualifications/q-2026/manifest.yaml"
    )
    assert (
        layout.qualification_verdict_key("q-2026")
        == "qualifications/q-2026/verdict.json"
    )


def test_qualification_side_run_key_validates_side():
    assert (
        layout.qualification_side_run_key("q", "baseline", "abc", "r1")
        == "qualifications/q/baseline/runs/abc/r1.json"
    )
    with pytest.raises(ValueError, match="baseline|candidate"):
        layout.qualification_side_run_key("q", "rogue", "abc", "r1")


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("s3://dag-tools", "dag-tools"),
        ("s3://my-bucket", "my-bucket"),
        ("dag-tools", "dag-tools"),  # bare bucket name allowed
    ],
)
def test_parse_registry_uri_accepts_valid(uri, expected):
    assert layout.parse_registry_uri(uri) == expected


@pytest.mark.parametrize(
    "uri,err_match",
    [
        ("", "empty"),
        ("http://example.com", "s3://"),
        ("s3://", "no bucket"),
        ("s3://my-bucket/some/path", "must not include a path"),
    ],
)
def test_parse_registry_uri_rejects_bad(uri, err_match):
    with pytest.raises(ValueError, match=err_match):
        layout.parse_registry_uri(uri)
