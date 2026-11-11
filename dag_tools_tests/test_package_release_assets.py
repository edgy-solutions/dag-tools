"""A release carries the distributions and nothing else.

`uv build` writes three files into `dist/`: the wheel, the sdist and a
`.gitignore` holding one byte, `*`. A bare `dist/*` picks up all three, and the
third one is visible on every release from `v0.10.0` to `v0.13.0` -- as
`default.gitignore`, because a GitHub release asset's name may not begin with a
dot, so the API renames it rather than refusing it. Nothing broke, which is why
it survived four releases: it is a 1-byte file next to the real assets, and the
only reader it misleads is a human deciding what to download.

Naming the two patterns fixes it at the one place it enters, and it buys a
second property that is worth more than the tidiness. `fail_on_unmatched_files`
is already true, and against `dist/*` that assertion is nearly vacuous -- the
`.gitignore` alone satisfies it. Against named patterns it means what it says:
a build that produces no sdist fails the tag instead of publishing a wheel-only
release that `pip install` would accept and a source-building consumer would
not.

So the invariants are: nothing attaches a bare directory glob, both
distribution kinds are named on each path they travel, and the unmatched-files
gate stays on. The PyPI job is pinned to the same artifact name as the upload,
because the two are coupled only by that string -- publishing would otherwise
start uploading whatever a renamed artifact happened to contain.
"""
import pathlib

import pytest

yaml = pytest.importorskip("yaml")

WORKFLOW = (
    pathlib.Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "build-package.yml"
)

#: What `uv build` produces, and the whole of what a release may carry.
DISTRIBUTION_PATTERNS = {"dist/*.whl", "dist/*.tar.gz"}


@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def steps(workflow):
    return [step for job in workflow["jobs"].values() for step in job["steps"]]


def _patterns(value):
    """A `path:`/`files:` value as the list of globs it really is."""
    return [line.strip() for line in str(value).splitlines() if line.strip()]


@pytest.fixture(scope="module")
def upload_step(steps):
    found = [s for s in steps if "upload-artifact" in s.get("uses", "")]
    assert len(found) == 1
    return found[0]


@pytest.fixture(scope="module")
def release_step(steps):
    found = [s for s in steps if "action-gh-release" in s.get("uses", "")]
    assert len(found) == 1
    return found[0]


def test_no_step_collects_a_bare_dist_glob(upload_step, release_step):
    """The defect itself: `dist/*` sweeps up whatever else the build left there."""
    for step in (upload_step, release_step):
        collected = _patterns(step["with"].get("path") or step["with"].get("files"))
        assert "dist/*" not in collected, step["name"]
        assert "dist" not in collected, step["name"]


def test_both_distribution_kinds_are_named_on_every_path_they_travel(
    upload_step, release_step
):
    """One pattern would be a fix that silently stops shipping the sdist."""
    assert set(_patterns(upload_step["with"]["path"])) == DISTRIBUTION_PATTERNS
    assert set(_patterns(release_step["with"]["files"])) == DISTRIBUTION_PATTERNS


def test_a_missing_distribution_fails_the_tag(release_step):
    """Only meaningful now: against `dist/*` the `.gitignore` satisfied this."""
    assert release_step["with"]["fail_on_unmatched_files"] is True


def test_the_upload_errors_rather_than_publishing_an_empty_artifact(upload_step):
    assert upload_step["with"]["if-no-files-found"] == "error"


def test_pypi_publishes_the_artifact_the_build_uploaded(workflow, upload_step):
    """The two jobs are coupled by this one string and nothing else."""
    download = [
        step
        for step in workflow["jobs"]["publish-pypi"]["steps"]
        if "download-artifact" in step.get("uses", "")
    ]
    assert len(download) == 1
    assert download[0]["with"]["name"] == upload_step["with"]["name"]
