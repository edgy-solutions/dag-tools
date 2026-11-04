"""The user-deployment image's ``s3fs`` floor lives in the ``broker`` extra.

``dag_tools.io_managers.delta`` and ``.arrow`` build an
``s3fs.S3FileSystem(endpoint_url=...)`` at IO-manager construction time.
s3fs 0.4.2 (2020) predates aiobotocore: its ``connect()`` hands the kwargs
to ``botocore.session.Session``, which raises

    TypeError: Session.__init__() got an unexpected keyword argument
    'endpoint_url'

so every MinIO-backed deployment fails. ``pyproject.toml`` floored it at
``>=2023.1.0`` -- but on the ``orchestrator`` extra, which the
user-deployment image deliberately does NOT install. The image named a
**bare** ``s3fs`` on its own ``uv pip install`` line instead, so the floor
never bound there and the resolver stayed free to pick 0.4.2.

A Dockerfile-only pin is invisible to this test suite and to ``uv.lock``,
which is why the fault survived. These tests exist so the floor stays on
the extra, where both can see it.
"""
import pathlib
import tomllib

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
DOCKERFILE = ROOT / "dag_tools" / "user_deployment" / "Dockerfile"
WORKFLOW = ROOT / ".github" / "workflows" / "test.yml"

# The release that moved s3fs onto aiobotocore and gave S3FileSystem a real
# endpoint_url. Anything below this cannot talk to MinIO at all.
FLOOR = Version("2023.1.0")
BROKEN = Version("0.4.2")


def _extras():
    with PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)["project"]["optional-dependencies"]


def _requirements(extra):
    return [Requirement(entry) for entry in _extras()[extra]]


def _image_install_args():
    """The requirement arguments on the image's ``uv pip install`` line."""
    lines = DOCKERFILE.read_text(encoding="utf-8").splitlines()
    starts = [i for i, line in enumerate(lines) if "uv pip install --system" in line]
    assert len(starts) == 1, f"expected one install line, found {starts}"
    args = []
    for line in lines[starts[0] + 1:]:
        args.append(line.strip().rstrip("\\").strip().strip('"'))
        if not line.rstrip().endswith("\\"):
            break
    return [arg for arg in args if arg]


def test_the_broker_extra_floors_s3fs():
    s3fs_reqs = [r for r in _requirements("broker") if r.name == "s3fs"]
    assert len(s3fs_reqs) == 1, "the broker extra must declare s3fs exactly once"
    specifier = s3fs_reqs[0].specifier
    assert not specifier.contains(BROKEN), f"{specifier} still admits s3fs {BROKEN}"
    assert specifier.contains(FLOOR), f"{specifier} excludes the known-good {FLOOR}"


def test_the_image_installs_the_extra_that_carries_the_floor():
    """The two halves of the fix only hold together while the image installs
    an extra that floors s3fs. Swapping `.[broker]` for a different extra, or
    for a bare `.`, would silently drop the floor again."""
    args = _image_install_args()
    extras = _extras()
    carriers = [
        name
        for name in extras
        if any(
            r.name == "s3fs" and not r.specifier.contains(BROKEN)
            for r in _requirements(name)
        )
    ]
    installed = [a for a in args if a.startswith(".[")]
    assert installed, f"image installs no extra at all: {args}"
    assert any(
        any(f"[{c}" in a or f",{c}" in a or f"{c}]" in a for c in carriers)
        for a in installed
    ), f"none of {installed} carries an s3fs floor (carriers: {carriers})"


def test_the_image_pins_no_bare_s3fs():
    """A bare `s3fs` on the Dockerfile line is what the floor replaces. It
    shadows nothing, but it invites the next reader to delete the extra's
    declaration as a duplicate -- and then the image is broken again with
    nothing in the test suite to say so."""
    named = [Requirement(a).name for a in _image_install_args() if not a.startswith(".")]
    assert "s3fs" not in named, (
        "s3fs is pinned on the Dockerfile install line; declare it on the "
        "broker extra instead so uv.lock and this suite can see the floor"
    )


def test_the_compat_floor_job_does_not_pin_s3fs_itself():
    """The compat-floor job asserts the resolved s3fs version. That assertion
    only means something while the job gets its floor from the declaration --
    re-adding a CLI pin would make it pass on the argument instead, which is
    the exact shape of the bug this file guards."""
    assert '"s3fs>=' not in WORKFLOW.read_text(encoding="utf-8"), (
        "the workflow pins s3fs on an install line; remove it so the "
        "'Assert pins actually took' step tests the broker extra"
    )


def test_the_installed_s3fs_accepts_an_endpoint_url():
    """Canary for the library behaviour the floor exists for. On 0.4.2 this
    raises TypeError from botocore's Session; measured, not assumed."""
    s3fs = pytest.importorskip("s3fs")
    assert Version(s3fs.__version__) >= FLOOR, f"s3fs {s3fs.__version__} is below the floor"
    s3fs.S3FileSystem(
        endpoint_url="http://minio:9000",
        key="key",
        secret="secret",
        use_ssl=False,
        skip_instance_cache=True,
    )
