"""CI asks whether ``uv.lock`` still describes ``pyproject.toml``.

Nothing asked until now, and the answer was no for three releases: #60 added
dependencies without relocking, and #63 brought the lock back in line as a side
effect of unrelated work. No job in this repository installs from the lock --
the suite resolves fresh from ``pyproject.toml`` -- so a stale lock is invisible
to every one of them while whoever runs ``uv sync`` gets a different dependency
set than the one the tests passed against.

What this module pins is that the question is asked, and that the answer gates.
The command itself is measured rather than described: ``uv lock --check`` exits
0 on a current lock and 1 on a stale one, which is checked here against this
repository's own files when uv is available.

A non-blocking version of this job would be the same failure again in a
different costume -- a gate nobody has to obey is how the last one lasted three
releases -- so ``continue-on-error`` is asserted absent, separately from the
command.
"""
import shutil
import subprocess
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "test.yml"
JOB = "lockfile"


def _workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _job():
    jobs = _workflow()["jobs"]
    assert JOB in jobs, "no job checks the lockfile against the project file"
    return jobs[JOB]


def _run(args, cwd):
    return subprocess.run(
        args, cwd=str(cwd), capture_output=True, text=True, timeout=600
    )


def test_a_job_checks_the_lockfile_against_the_project_file():
    body = "\n".join(
        str(step.get("run", "")) for step in _job()["steps"]
    )
    assert "uv lock --check" in body, (
        "the job must run `uv lock --check`: it answers the question and changes "
        "nothing. `uv lock` would rewrite the file CI is supposed to be checking."
    )


def test_the_check_is_not_offline():
    """An offline resolve on a cold runner fails for want of a cache entry.

    That is a failure about the runner wearing this check's name, which sends an
    operator to the lockfile over something the lockfile is innocent of.
    """
    body = "\n".join(str(step.get("run", "")) for step in _job()["steps"])
    assert "--offline" not in body


def test_the_check_gates_rather_than_reporting():
    job = _job()
    assert "continue-on-error" not in job, (
        "a lockfile that disagrees with the project file is a defect in the "
        "commit, not information about an unsupported version"
    )


def test_it_runs_on_pushes_and_pull_requests():
    on = _workflow()[True]  # YAML reads a bare `on:` key as the boolean True
    assert "push" in on and "pull_request" in on


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is not installed")
def test_this_repositorys_lockfile_matches_its_project_file():
    """The check CI runs, run here against the files in this tree."""
    done = _run(["uv", "lock", "--check"], ROOT)
    assert done.returncode == 0, (
        "uv.lock does not match pyproject.toml -- run `uv lock` and commit the "
        "result:\n" + done.stdout + done.stderr
    )


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is not installed")
def test_control_an_added_dependency_makes_the_check_fail(tmp_path):
    """Without this, the test above passes on a command that cannot fail.

    A copy of the two files, with one requirement added to the project file and
    the lock left as it was: exactly the state #60 left the repository in.
    """
    shutil.copy(ROOT / "uv.lock", tmp_path / "uv.lock")
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    marker = "dependencies = [\n"
    assert marker in text
    stale = text.replace(marker, marker + '    "tabulate>=0.9.0",\n', 1)
    (tmp_path / "pyproject.toml").write_text(stale, encoding="utf-8")

    done = _run(["uv", "lock", "--check"], tmp_path)
    assert done.returncode != 0, (
        "a requirement absent from the lock was accepted, so the check in CI "
        "proves nothing"
    )
    assert "uv.lock" in done.stdout + done.stderr
