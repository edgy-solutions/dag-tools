"""A release tag renames the commit's build; it never builds again.

`v0.11.0` and `sha-4924235` are the same commit and were measured as
DIFFERENT images -- different config blob and most layer blobs different --
because the tag push ran the build a second time. Nothing in this repo's image
build is reproducible: every run resolves dependencies fresh (the `0.11.0`
image's `s3fs` came out `2026.9.0` against `0.4.2` three commits earlier) and
buildx stamps build time into layer metadata. So a second build of one commit
is a second set of bytes, and a deployment pinning the release pinned bytes
that no commit tag named.

The workflow therefore splits by ref type: a branch push builds and publishes
`:sha-<short>` + `:latest`, and a tag renames that build. The invariants worth
pinning, because each one silently undoes the fix:

  * the build job must not run for a tag ref, and must emit no `type=semver`
    pattern -- either would publish a release built on its own;
  * the retag job must PUT the source manifest's own bytes. `docker buildx
    imagetools create` assembles a fresh index from its sources, and these
    images carry attestation manifests, so its output is not guaranteed to
    carry the source's digest;
  * `:latest` is a MASTER pointer. A tag must not move it -- tag runs used to,
    because docker/metadata-action counts a tag ref as the default branch, and
    that is how a mistaken tag briefly published `:latest` from the wrong tree;
  * the retag must verify against the registry rather than trust its own PUT.

One coupling is invisible here and worth naming: the retag looks for
`sha-${GITHUB_SHA:0:7}`, which is docker/metadata-action's `type=sha` spelling
(7 hex chars, as `sha-4096a11` and `sha-6609fe6` in the deployed pins show). If
that action ever changes the length, the retag looks for a tag nobody pushed --
and it fails loudly, by design, rather than rebuilding.
"""
import pathlib
import re

import pytest

yaml = pytest.importorskip("yaml")

WORKFLOW = (
    pathlib.Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "build-containers.yml"
)


@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def retag_script(workflow):
    """The one `run:` body of the retag job."""
    steps = [step for step in workflow["jobs"]["retag"]["steps"] if "run" in step]
    assert len(steps) == 1, "the retag job should be a single scripted step"
    return steps[0]["run"]


def test_a_tag_ref_does_not_build(workflow):
    assert workflow["jobs"]["build"]["if"] == "github.ref_type != 'tag'"


def test_a_tag_ref_is_retagged(workflow):
    assert workflow["jobs"]["retag"]["if"] == "github.ref_type == 'tag'"


def test_the_build_job_emits_no_semver_tag_pattern(workflow):
    """A `type=semver` pattern would publish the release from a second build,
    which is the exact thing the retag job exists to replace."""
    meta = [
        step
        for step in workflow["jobs"]["build"]["steps"]
        if str(step.get("uses", "")).startswith("docker/metadata-action")
    ]
    assert len(meta) == 1
    assert "semver" not in meta[0]["with"]["tags"]


def test_the_retag_job_builds_nothing(workflow):
    """No build action and no docker build, or the release is its own bytes
    again however the tags come out."""
    job = workflow["jobs"]["retag"]
    uses = [str(step.get("uses", "")) for step in job["steps"]]
    assert not any("build-push-action" in u for u in uses)
    assert not any("setup-qemu" in u for u in uses)
    scripts = " ".join(step.get("run", "") for step in job["steps"])
    assert "docker build" not in scripts
    assert "buildx build" not in scripts


def test_the_release_does_not_move_latest(workflow, retag_script):
    """`:latest` tracks master. Only the branch build publishes it."""
    assert "latest" not in retag_script
    meta = next(
        step
        for step in workflow["jobs"]["build"]["steps"]
        if str(step.get("uses", "")).startswith("docker/metadata-action")
    )
    assert "type=raw,value=latest,enable={{is_default_branch}}" in meta["with"]["tags"]


def test_the_release_name_is_not_minted_by_imagetools_create(retag_script):
    """`imagetools create` assembles a fresh index, so it cannot promise the
    source's digest. The manifest's own bytes are PUT instead."""
    assert "imagetools create" not in retag_script
    assert "--data-binary @manifest.json" in retag_script
    assert "-X PUT" in retag_script


def test_the_retag_reads_the_source_tag_the_branch_build_publishes(retag_script):
    """Coupled to docker/metadata-action's `type=sha` spelling; see module
    docstring. A mismatch fails loudly rather than rebuilding."""
    assert 'SOURCE_TAG="sha-${GITHUB_SHA:0:7}"' in retag_script


def test_the_retag_accepts_every_manifest_media_type(retag_script):
    """Omit one and the registry may answer with a converted manifest, whose
    digest differs from the one the branch build published."""
    for media_type in (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ):
        assert media_type in retag_script


def test_the_retag_verifies_the_release_resolves_to_the_source_digest(retag_script):
    """Verification is a registry read, not a successful PUT."""
    assert "docker-content-digest" in retag_script
    assert 'if [ "${got}" != "${SOURCE_DIGEST}" ]; then' in retag_script
    assert re.search(r'!= "\$\{SOURCE_DIGEST\}".*?\n.*?::error::', retag_script, re.S)


def test_a_missing_commit_build_fails_instead_of_building(retag_script):
    """A tag on a commit master never built has nothing to rename. That must
    fail and say so -- falling back to a build is what we are removing."""
    assert "never appeared" in retag_script
    assert "exit 1" in retag_script


def test_the_minor_alias_is_gated_on_a_plain_three_number_version(retag_script):
    """A prerelease tag must not take `0.11` off the release that holds it."""
    assert '[[ "${VERSION}" =~ ^[0-9]+\\.[0-9]+\\.[0-9]+$ ]]' in retag_script
    assert 'NAMES+=("${VERSION%.*}")' in retag_script


def test_every_built_service_is_retagged(workflow):
    """A service that builds and is not retagged has no release tag at all."""
    built = {
        entry["service"]
        for entry in workflow["jobs"]["build"]["strategy"]["matrix"]["include"]
    }
    retagged = set(workflow["jobs"]["retag"]["strategy"]["matrix"]["service"])
    assert built == retagged


def test_the_workflow_still_triggers_on_tags(workflow):
    """The retag job is unreachable without the tag trigger."""
    assert "v*" in workflow[True]["push"]["tags"]
