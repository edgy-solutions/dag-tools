#!/usr/bin/env python3
"""CI check: every commit of a lane's PR carries `Lane: <lane id>/<branch>`.

Ruling: no local hook; the trailer is checked in CI on dag-tools PRs.

The rows come from `.githooks/lanes`: a lane id, then the branch globs that lane commits on.
EVERY commit in `base..head`, merge commits included, must have a `Lane:` trailer, in the last
paragraph of its message, equal to `<that row's id>/<head branch>`.

A branch matching no row is not a lane -- a maintainer's own branch, a seat's branch (a seat is
not a lane: `docs/rulings.md`, 2026-11-03), a release branch -- and passes with a notice. WITH
ONE EXCEPTION, because the plain reading of "matches no row" made this check pass vacuously
exactly where it was meant to bite: a branch inside the `lane/` namespace whose lane has no row
is an UNREGISTERED LANE, and it FAILS. Measured on this repo: `.githooks/lanes` carried one row
while three lanes were committing under `lane/`, so two lanes' PRs -- the larger share of the
open stack -- were waved through with `not a lane, trailers not checked`, and the trailers they
did carry went unread. Registering a lane is a one-line row; being silently exempt from the
check is not something a branch should be able to acquire by never being registered.

The namespace is spelled here rather than derived from the rows' own globs. Derivation would
widen the refusal to whatever any row happens to glob -- a row carrying `feature/*` would start
failing a maintainer's `feature/...` branch -- which is the opposite failure and just as quiet.

STACKED BRANCHES, and why the branch a trailer names is checked against the LANE rather than
against the head branch. `base..head` is this PR's own commits only when its base is what the
branch was cut from. A lane that stacks -- branch N built on branch N-1, every PR opened against
master -- hands this check every commit of the whole stack on every PR in it. Measured on this
repo: the 24 PRs #118-#141 had `base=master` with heads `lane/arch-components-41` .. `-64`, each
built on the one below, and every commit in them carries `Lane: arch/lane/arch-components` --
the lane, not any head branch. Demanding `<id>/<head branch>` refused that stack twice over:
once because an ancestor commit names the branch it was authored on, and once because the lane
never wrote a head branch into a trailer at all. Neither was anything a PR author could fix,
since an ancestor's message is not that PR's to rewrite. The whole stack passed only because no
row claimed `lane/arch-*` yet, which is the hole the paragraph above closes -- so closing that
one without this one turns every lane PR red.

The rule is therefore: the trailer's lane id must be this row's, and the branch it names must be
one the row globs. The head branch stays the canonical spelling and is what every error message
offers. Insisting on it bought no accountability to lose: the branch part of a trailer is text
an author typed, never a fact git can confirm, and only the lane id is held to a registered row.
A commit naming ANOTHER lane still fails, which is what stops a PR to master carrying an
unmerged lane's work in under this lane's name.

The workflow has no `branches:` filter, so a PR whose base is another branch is checked too --
the properly-stacked shape, and the one where `base..head` is exactly the PR's own commits.
While the filter was there, stacking the correct way meant not being checked at all: the same
silent exemption as an unregistered lane, one level up.

Inputs come from the environment (set from `env:` in the workflow, never interpolated into a
shell line): BASE_SHA, HEAD_SHA, HEAD_REF. Exit 0 pass, 1 fail, 2 could not run.
"""
from __future__ import annotations

import fnmatch
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

LANES_FILE = ".githooks/lanes"

#: Branch prefix that means "this is a lane's branch", registered or not. See the module
#: docstring: an unmatched branch inside it fails rather than being waved through.
LANE_NAMESPACE = "lane/"

#: A `Lane:` line anywhere in a message. Only used to tell "no such line at all" from "the line
#: is there and git does not read it as a trailer", which are one error with two very different
#: fixes: write the trailer, versus move it into the last paragraph.
_LANE_LINE = re.compile(r"^[ \t]*lane[ \t]*:", re.IGNORECASE | re.MULTILINE)


def _git(repo: Path, *args: str, stdin: Optional[str] = None) -> str:
    out = subprocess.run(["git", "-C", str(repo), *args], input=stdin, capture_output=True,
                         text=True, encoding="utf-8", check=False)
    if out.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {out.stderr.strip()[-300:]}")
    return out.stdout


def read_rows(lanes_file: Path) -> List[Tuple[str, List[str]]]:
    rows = []
    for line in lanes_file.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        rows.append((parts[0], parts[1:]))
    return rows


def row_for(branch: str, rows: List[Tuple[str, List[str]]]) -> Optional[Tuple[str, List[str]]]:
    """The row this branch commits on, `(lane id, globs)`, or None for a branch in no row."""
    for lane_id, globs in rows:
        if any(fnmatch.fnmatchcase(branch, g) for g in globs):
            return lane_id, globs
    return None


def lane_for(branch: str, rows: List[Tuple[str, List[str]]]) -> Optional[str]:
    row = row_for(branch, rows)
    return None if row is None else row[0]


#: What one `Lane:` value is, read against the row the PR's head branch matched.
OK, OTHER_LANE, OUTSIDE_LANE = "ok", "other_lane", "outside_lane"


def read_trailer(value: str, lane_id: str, globs: List[str]) -> str:
    """`OK`, `OTHER_LANE` or `OUTSIDE_LANE`.

    The branch named need not be the head branch -- see the module docstring on stacked
    branches -- but it must be one this lane commits on, which is what keeps the branch part
    read rather than ignored.
    """
    named_lane, sep, named_branch = value.partition("/")
    if not sep or named_lane != lane_id:
        return OTHER_LANE
    if not any(fnmatch.fnmatchcase(named_branch, g) for g in globs):
        return OUTSIDE_LANE
    return OK


def lane_trailers(repo: Path, sha: str) -> Tuple[List[str], bool]:
    """`(trailer values git reads, whether a 'Lane:' line appears in the message at all)`."""
    message = _git(repo, "log", "-1", "--format=%B", sha)
    parsed = _git(repo, "interpret-trailers", "--parse", stdin=message)
    values = []
    for line in parsed.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip().lower() == "lane":
            values.append(value.strip())
    return values, bool(_LANE_LINE.search(message))


def check(repo: Path, base: str, head: str, branch: str) -> Tuple[int, List[str]]:
    """Returns `(exit code, output lines)`."""
    rows = read_rows(repo / LANES_FILE)
    row = row_for(branch, rows)
    if row is None:
        if branch.startswith(LANE_NAMESPACE):
            return 1, [f"::error::branch '{branch}' is in the '{LANE_NAMESPACE}' namespace and "
                       f"matches no row of {LANES_FILE}: an unregistered lane is not exempt from "
                       f"the check. Add a row for it, or move the work to a branch outside "
                       f"'{LANE_NAMESPACE}'."]
        return 0, [f"::notice::branch '{branch}' matches no row of {LANES_FILE}; not a lane, "
                   "trailers not checked"]
    lane_id, globs = row
    expected = f"{lane_id}/{branch}"
    shas = _git(repo, "rev-list", f"{base}..{head}").split()
    lines, bad, inherited = [], 0, 0
    for sha in shas:
        got, has_line = lane_trailers(repo, sha)
        if not got and has_line:
            bad += 1
            lines.append(f"::error::{sha[:10]} has a 'Lane:' line git does not read as a trailer "
                         f"(it must be in the LAST paragraph, with no blank line between it and "
                         f"any other trailer); expected 'Lane: {expected}'")
        elif not got:
            bad += 1
            lines.append(f"::error::{sha[:10]} has no 'Lane:' trailer (expected 'Lane: {expected}')")
        else:
            verdicts = [read_trailer(v, lane_id, globs) for v in got]
            if OTHER_LANE in verdicts:
                bad += 1
                lines.append(f"::error::{sha[:10]} has Lane trailer {got!r}, naming a lane other "
                             f"than '{lane_id}', which owns '{branch}'. Every commit a PR brings "
                             f"belongs to that lane; expected 'Lane: {expected}'")
            elif OUTSIDE_LANE in verdicts:
                bad += 1
                lines.append(f"::error::{sha[:10]} has Lane trailer {got!r}, naming a branch no "
                             f"glob of lane '{lane_id}' matches ({' '.join(globs)}); expected "
                             f"'Lane: {expected}'")
            elif any(v != expected for v in got):
                inherited += 1
    tail = f"checked {len(shas)} commit(s) on '{branch}' as lane '{lane_id}': {bad} bad"
    if inherited:
        tail += (f"; {inherited} of them name another branch of this lane (stacked, accepted: "
                 f"see {Path(__file__).name})")
    lines.append(tail)
    return (1 if bad else 0), lines


def main() -> int:
    try:
        base, head, branch = (os.environ[k] for k in ("BASE_SHA", "HEAD_SHA", "HEAD_REF"))
        code, lines = check(Path.cwd(), base, head, branch)
    except (KeyError, RuntimeError, OSError) as exc:
        print(f"::error::lane trailer check could not run: {type(exc).__name__}: {exc}")
        return 2
    print("\n".join(lines))
    return code


if __name__ == "__main__":
    sys.exit(main())
