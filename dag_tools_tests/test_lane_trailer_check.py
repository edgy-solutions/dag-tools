"""`scripts/check_lane_trailers.py` against a REAL temporary git repo.

Rule: on a PR whose head branch matches a row of `.githooks/lanes`, every commit in base..head
carries a `Lane:` trailer (last paragraph) naming that row's lane id and a branch that row
globs. A branch in no row passes -- UNLESS it is inside the `lane/` namespace, where matching no
row means an unregistered lane and fails, because the plain reading passed vacuously on exactly
the branches it is for.

The branch a trailer names is checked against the LANE, not against the head branch, which is
what makes a stacked branch checkable at all: a PR opened against master carries its ancestors'
commits, and those name the branch they were authored on.
"""
from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_lane_trailers.py"
_spec = importlib.util.spec_from_file_location("check_lane_trailers", _SCRIPT)
check_lane_trailers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_lane_trailers)

LANES = "# comment\nmylane   lane/01 lane/01-*\nother  feature/*  # trailing comment\n"
BRANCH = "lane/01-thing"
GOOD = "Lane: mylane/lane/01-thing"


def git(repo, *args, env_extra=None):
    import os
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com",
           "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull}
    out = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                         encoding="utf-8", env=env)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def commit(repo, name, message):
    (repo / name).write_text(name, encoding="utf-8")
    git(repo, "add", name)
    msg = repo / ".msg"
    msg.write_text(message, encoding="utf-8")
    git(repo, "commit", "-q", "-F", str(msg))
    msg.unlink()
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "r"
    r.mkdir()
    git(r, "init", "-q", "-b", "master")
    (r / ".githooks").mkdir()
    (r / ".githooks" / "lanes").write_text(LANES, encoding="utf-8")
    git(r, "add", ".githooks/lanes")
    git(r, "commit", "-q", "-m", "base")
    git(r, "switch", "-q", "-c", BRANCH)
    return r


def run(repo, branch=BRANCH):
    base = git(repo, "merge-base", "master", "HEAD")
    return check_lane_trailers.check(repo, base, git(repo, "rev-parse", "HEAD"), branch)


def test_every_commit_carrying_the_right_trailer_passes(repo):
    commit(repo, "a", f"feat: a\n\nbody\n\n{GOOD}\nCo-Authored-By: X <x@example.com>\n")
    commit(repo, "b", f"fix: b\n\n{GOOD}\n")
    code, lines = run(repo)
    assert code == 0, lines
    assert "2 commit(s)" in lines[-1] and "0 bad" in lines[-1]


def test_a_missing_trailer_fails_and_names_the_commit(repo):
    commit(repo, "a", f"feat: a\n\n{GOOD}\n")
    bad = commit(repo, "b", "fix: b\n\nno trailer here\n")
    code, lines = run(repo)
    assert code == 1
    errors = [l for l in lines if l.startswith("::error::")]
    assert len(errors) == 1 and bad[:10] in errors[0]


def test_a_wrong_lane_id_fails(repo):
    commit(repo, "a", "feat: a\n\nLane: other/lane/01-thing\n")
    code, lines = run(repo)
    assert code == 1 and "expected 'Lane: mylane/lane/01-thing'" in "\n".join(lines)
    assert "naming a lane other than 'mylane'" in lines[0]  # not the branch refusal


def test_a_trailer_naming_a_branch_outside_the_lane_fails(repo):
    """Renamed from `test_a_wrong_branch_in_the_trailer_fails`, which asserted the head-branch
    reading: `lane/01-elsewhere` is a branch of this lane and now passes (see the stacked tests
    below). What still fails is a branch no glob of the row matches."""
    commit(repo, "a", "feat: a\n\nLane: mylane/nowhere-at-all\n")
    code, lines = run(repo)
    assert code == 1
    assert "no glob of lane 'mylane' matches" in lines[0]


def test_a_sibling_branch_of_the_same_lane_passes(repo):
    """The behaviour the rename above gave up, asserted as a behaviour rather than left
    implicit: a trailer may name another branch this lane commits on."""
    commit(repo, "a", "feat: a\n\nLane: mylane/lane/01-elsewhere\n")
    code, lines = run(repo)
    assert code == 0, lines
    assert "1 of them name another branch of this lane" in lines[-1]


def test_a_trailer_not_in_the_last_paragraph_is_not_a_trailer(repo):
    commit(repo, "a", f"feat: a\n\n{GOOD}\n\nand then some prose after it\n")
    assert run(repo)[0] == 1


def test_a_lane_line_git_does_not_read_says_so_rather_than_no_trailer(repo):
    """The shape measured on real branches here: a blank line between `Lane:` and
    `Co-Authored-By:`. Git reads only the last paragraph, so the line is plainly in the
    message and is not a trailer. Reported as "has no 'Lane:' trailer" it sends the author
    looking for a line that is already there, so the two cases get different messages."""
    commit(repo, "a", f"feat: a\n\n{GOOD}\n\nCo-Authored-By: X <x@example.com>\n")
    code, with_line = run(repo)
    assert code == 1
    assert "does not read as a trailer" in with_line[0] and "LAST paragraph" in with_line[0]
    assert "has no 'Lane:' trailer" not in with_line[0]


def test_a_message_with_no_lane_line_at_all_still_says_no_trailer(repo):
    """The control for the test above: without it, that one would pass on a check that had
    simply reworded every missing-trailer error."""
    commit(repo, "a", "feat: a\n\nno lane line anywhere\n")
    code, lines = run(repo)
    assert code == 1
    assert "has no 'Lane:' trailer" in lines[0]
    assert "does not read as a trailer" not in lines[0]


def _merge_master_into_lane(repo, message):
    commit(repo, "a", f"feat: a\n\n{GOOD}\n")
    git(repo, "switch", "-q", "master")
    commit(repo, "m", "master moves\n")
    git(repo, "switch", "-q", BRANCH)
    msg = repo / ".msg"
    msg.write_text(message, encoding="utf-8")
    git(repo, "merge", "-q", "--no-ff", "-F", str(msg), "master")
    msg.unlink()
    result = check_lane_trailers.check(
        repo, git(repo, "rev-parse", "master"), git(repo, "rev-parse", "HEAD"), BRANCH)
    return result, git(repo, "rev-parse", "HEAD")


def test_a_merge_commit_is_checked_like_any_other(repo):
    (code, lines), merge_sha = _merge_master_into_lane(repo, "Merge master into the lane\n")
    assert code == 1, "a merge commit without the trailer must fail"
    errors = [l for l in lines if l.startswith("::error::")]
    assert len(errors) == 1 and merge_sha[:10] in errors[0]


def test_a_merge_commit_with_the_trailer_passes(repo):
    (code, lines), _ = _merge_master_into_lane(repo, f"Merge master into the lane\n\n{GOOD}\n")
    assert code == 0, lines


def test_a_branch_outside_the_lane_namespace_passes_with_a_notice_even_with_bad_commits(repo):
    """Renamed from `test_a_branch_in_no_row_passes_...`, which described the behaviour that
    was removed for branches inside `lane/`. A seat's branch, or a maintainer's own, is
    still not a lane and is still not checked."""
    commit(repo, "a", "no trailer at all\n")
    code, lines = run(repo, "chris/own-branch")
    assert code == 0
    assert lines[0].startswith("::notice::") and "chris/own-branch" in lines[0]


def test_an_unregistered_lane_branch_fails_naming_the_lanes_file(repo):
    """The defect this closes. Measured on this repo: one row in `.githooks/lanes` while
    three lanes were committing under `lane/`, so two lanes' PRs passed with "not a lane,
    trailers not checked" and the trailers they did carry went unread."""
    commit(repo, "a", "no trailer at all\n")
    code, lines = run(repo, "lane/unregistered-thing")
    assert code == 1
    assert len(lines) == 1 and lines[0].startswith("::error::")
    assert ".githooks/lanes" in lines[0] and "lane/unregistered-thing" in lines[0]


def test_an_unregistered_lane_fails_even_when_every_trailer_is_right(repo):
    """Control for the test above, which on its own would pass on the missing trailer alone.
    The refusal is that the lane has no row, and there is no row to say what the trailer
    should have been, so a well-formed one does not rescue it."""
    commit(repo, "a", "feat: a\n\nLane: mylane/lane/unregistered-thing\n")
    code, lines = run(repo, "lane/unregistered-thing")
    assert code == 1 and "matches no row" in lines[0]


def test_the_namespace_is_not_derived_from_the_rows_globs(repo):
    """The fixture file carries a `feature/*` row. Deriving the namespace from the globs
    would make an unmatched `feature/...` branch fail too -- a maintainer's own branch
    refused by a rule about lanes, the opposite failure and just as quiet."""
    assert check_lane_trailers.LANE_NAMESPACE == "lane/"
    commit(repo, "a", "no trailer at all\n")
    assert run(repo, "feature/x")[0] == 1       # matches the feature/* row: a lane, no trailer
    assert run(repo, "featurebranch")[0] == 0   # matches no row, outside lane/: not a lane


def test_the_row_is_read_from_the_file_and_the_second_row_matches(repo):
    commit(repo, "a", "feat: a\n\nLane: other/feature/x\n")
    assert run(repo, "feature/x")[0] == 0  # 'other' row, comment stripped, glob feature/*
    assert run(repo, "feature/y")[0] == 0  # same row globs it: a sibling branch of that lane
    assert run(repo, "lane/02")[0] == 1    # no row, but inside lane/: an unregistered lane
    rows = check_lane_trailers.read_rows(repo / ".githooks" / "lanes")
    assert check_lane_trailers.lane_for("lane/01", rows) == "mylane"



ARCH_ROW = "arch  lane/arch lane/arch-*\n"


def test_an_ancestor_commit_of_a_stacked_branch_passes_on_its_own_trailer(repo):
    """The shape measured on this repo: PRs #118-#141, all `base=master`, heads
    `lane/arch-components-41` .. `-64` each built on the one below, every commit carrying
    `Lane: arch/lane/arch-components`. Under the head-branch reading each PR was handed the
    whole stack and every commit in it failed, with nothing the author could do -- an
    ancestor's message is not that PR's to rewrite."""
    (repo / ".githooks" / "lanes").write_text(LANES + ARCH_ROW, encoding="utf-8")
    commit(repo, "a", "docs: the commit below it\n\nLane: arch/lane/arch-components\n")
    commit(repo, "b", "docs: this PR's own\n\nLane: arch/lane/arch-components\n")
    code, lines = run(repo, "lane/arch-components-64")
    assert code == 0, lines
    assert "2 of them name another branch of this lane" in lines[-1]


def test_control_the_branch_part_is_still_read(repo):
    """The partner for the test above, without which it would pass on a check that had simply
    stopped reading the branch part. The stacked reading accepts another branch OF THIS LANE,
    not any text at all."""
    (repo / ".githooks" / "lanes").write_text(LANES + ARCH_ROW, encoding="utf-8")
    commit(repo, "a", "docs: a\n\nLane: arch/somewhere-else\n")
    code, lines = run(repo, "lane/arch-components-64")
    assert code == 1 and "no glob of lane 'arch' matches" in lines[0]


def test_an_ancestor_from_ANOTHER_lane_still_fails(repo):
    """The accountability the stacked reading keeps. A PR to master carrying an unmerged other
    lane's commits is not a stack of this lane's work, and the lane id is the part a registered
    row holds the commit to."""
    (repo / ".githooks" / "lanes").write_text(LANES + ARCH_ROW, encoding="utf-8")
    commit(repo, "a", "docs: a\n\nLane: mylane/lane/01-thing\n")
    commit(repo, "b", "docs: b\n\nLane: arch/lane/arch-components\n")
    code, lines = run(repo, "lane/arch-components-64")
    assert code == 1
    assert len([l for l in lines if l.startswith("::error::")]) == 1
    assert "naming a lane other than 'arch'" in lines[0]


def test_a_stack_is_reported_rather_than_passing_silently(repo):
    """A PR carrying commits that are not its own is something an operator should be able to
    see in the log, not something the check quietly absorbs. House style: a thing being relied
    on is reported, not inferred."""
    (repo / ".githooks" / "lanes").write_text(LANES + ARCH_ROW, encoding="utf-8")
    commit(repo, "a", "docs: a\n\nLane: arch/lane/arch-components-63\n")
    commit(repo, "b", "docs: b\n\nLane: arch/lane/arch-components-64\n")
    code, lines = run(repo, "lane/arch-components-64")
    assert code == 0, lines
    assert "1 of them name another branch of this lane" in lines[-1]


def test_nothing_is_reported_when_every_commit_names_the_head_branch(repo):
    """The other half of the test above: the stacked note is absent on an ordinary PR, so its
    presence means something."""
    commit(repo, "a", f"feat: a\n\n{GOOD}\n")
    code, lines = run(repo)
    assert code == 0, lines
    assert "another branch of this lane" not in lines[-1]


def test_the_workflow_checks_a_pr_whose_base_is_another_branch():
    """Properly stacked, a PR names the parent branch as its base and base..head is exactly its
    own commits -- the one shape `branches: [master, main]` left unchecked entirely, which is
    the same silent exemption as an unregistered lane, one level up."""
    import yaml
    text = (Path(__file__).resolve().parents[1] / ".github" / "workflows"
            / "lane-trailers.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(text)
    triggers = wf.get("on", wf.get(True))
    assert "pull_request" in triggers
    assert "branches" not in (triggers["pull_request"] or {}), \
        "a branches: filter here means a stacked PR is not checked at all"

def test_main_reads_its_inputs_from_the_environment_and_exits_nonzero(repo, monkeypatch, capsys):
    commit(repo, "a", "no trailer\n")
    monkeypatch.chdir(repo)
    monkeypatch.setenv("BASE_SHA", git(repo, "rev-parse", "master"))
    monkeypatch.setenv("HEAD_SHA", git(repo, "rev-parse", "HEAD"))
    monkeypatch.setenv("HEAD_REF", BRANCH)
    assert check_lane_trailers.main() == 1
    assert "::error::" in capsys.readouterr().out
    monkeypatch.delenv("HEAD_REF")
    assert check_lane_trailers.main() == 2
