#!/usr/bin/env python3
"""CI check: every commit of a lane's PR carries `Lane: <lane id>/<branch>`.

Ruling: no local hook; the trailer is checked in CI on dag-tools PRs.

The rows come from `.githooks/lanes`: a lane id, then the branch globs that lane commits on.
If the PR's head branch matches no row it is not a lane (a maintainer's own branch, a release
branch) and passes with a notice. Otherwise EVERY commit in `base..head`, merge commits
included, must have a `Lane:` trailer, in the last paragraph of its message, equal to
`<that row's id>/<head branch>`.

Inputs come from the environment (set from `env:` in the workflow, never interpolated into a
shell line): BASE_SHA, HEAD_SHA, HEAD_REF. Exit 0 pass, 1 fail, 2 could not run.
"""
from __future__ import annotations

import fnmatch
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

LANES_FILE = ".githooks/lanes"


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


def lane_for(branch: str, rows: List[Tuple[str, List[str]]]) -> Optional[str]:
    for lane_id, globs in rows:
        if any(fnmatch.fnmatchcase(branch, g) for g in globs):
            return lane_id
    return None


def lane_trailers(repo: Path, sha: str) -> List[str]:
    message = _git(repo, "log", "-1", "--format=%B", sha)
    parsed = _git(repo, "interpret-trailers", "--parse", stdin=message)
    values = []
    for line in parsed.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip().lower() == "lane":
            values.append(value.strip())
    return values


def check(repo: Path, base: str, head: str, branch: str) -> Tuple[int, List[str]]:
    """Returns `(exit code, output lines)`."""
    rows = read_rows(repo / LANES_FILE)
    lane_id = lane_for(branch, rows)
    if lane_id is None:
        return 0, [f"::notice::branch '{branch}' matches no row of {LANES_FILE}; not a lane, "
                   "trailers not checked"]
    expected = f"{lane_id}/{branch}"
    shas = _git(repo, "rev-list", f"{base}..{head}").split()
    lines, bad = [], 0
    for sha in shas:
        got = lane_trailers(repo, sha)
        if not got:
            bad += 1
            lines.append(f"::error::{sha[:10]} has no 'Lane:' trailer (expected 'Lane: {expected}')")
        elif any(v != expected for v in got):
            bad += 1
            lines.append(f"::error::{sha[:10]} has Lane trailer {got!r}, expected '{expected}'")
    lines.append(f"checked {len(shas)} commit(s) on '{branch}' as lane '{lane_id}': {bad} bad")
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
