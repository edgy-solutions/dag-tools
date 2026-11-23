"""``docs/warehouse-table-to-published-dataset.md`` names five shipped examples.

A page whose whole premise is "walk this with the examples this repo already
ships" is wrong the moment one of those directories is renamed, and nothing
would say so: a reader follows a dead link and concludes the path does not
exist. This is the same stance as ``test_example_dbt_projects_resolve.py`` --
the example is part of the contract -- applied to the document that sends
people to it.

Every assertion here carries its own anti-vacuity guard. A link regex that
stopped matching, or a doc that lost its example table, would otherwise pass
every check below by finding nothing to check.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC = REPO_ROOT / "docs" / "warehouse-table-to-published-dataset.md"

# The five examples the page's step table sends an operator to, and one file in
# each that has to be there for that step to be the thing the page describes.
STEPS = {
    "read_audit": "dagster_home/components/read_audit/component.yaml",
    "tagged_warehouse": "dagster_home/components/warehouse/component.yaml",
    "warehouse_source": "classification/dimensions.yaml",
    "contract_mapping": "dagster_home/components/contract_mapping/mapping.yaml",
    "mfg_trace": "README.md",
}

_LINK = re.compile(r"\]\((?!https?://|#)([^)]+)\)")


def _links() -> list[str]:
    return [m.group(1).split("#", 1)[0] for m in _LINK.finditer(DOC.read_text(encoding="utf-8"))]


def test_the_page_exists_and_carries_relative_links() -> None:
    """The guard for the two tests below, which read nothing if this is false."""
    assert DOC.is_file(), DOC
    assert len(_links()) >= len(STEPS), _links()


def test_every_relative_link_in_the_page_resolves() -> None:
    missing = [ref for ref in _links() if not (DOC.parent / ref).exists()]
    assert not missing, missing


def test_every_example_the_page_sends_an_operator_to_is_the_one_it_describes() -> None:
    body = DOC.read_text(encoding="utf-8")
    problems = []
    for name, witness in STEPS.items():
        if f"examples/{name}" not in body:
            problems.append(f"the page no longer names examples/{name}")
        if not (REPO_ROOT / "examples" / name / witness).is_file():
            problems.append(f"examples/{name}/{witness} is gone")
    assert not problems, problems


def test_the_readme_links_the_page() -> None:
    """A guide nothing points at is a guide nobody finds."""
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "docs/warehouse-table-to-published-dataset.md" in readme


def test_the_page_does_not_claim_the_labels_are_enforced() -> None:
    """The one claim on this page that would be a lie if it drifted.

    Nothing in this library consults a classification to decide an access
    question (AGENTS.md section 4m, and the wording of the warehouse_source
    example). A page for a non-engineer reader is exactly where that would get
    smoothed into "classified, therefore protected", so the disclaimer is
    asserted rather than trusted to survive an edit.
    """
    body = DOC.read_text(encoding="utf-8")
    assert "intended, not built" in body
    assert "decide nothing today" in body or "decides nothing today" in body
