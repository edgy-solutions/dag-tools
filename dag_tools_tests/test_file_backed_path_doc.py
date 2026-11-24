"""``docs/file-backed-asset-to-published-dataset.md`` is a page about a pin.

It describes the file-backed producer path as it stood at ``4fbd272``
(v0.16.1), for an engineer whose deployment has not moved -- so most of what it
says is deliberately not a statement about this tree and cannot be sealed here.
Four things can, and each one is a claim that would read as correct after it
had stopped being true:

* the shipped files its step table sends a reader to, since that column is the
  only navigation the page has (there is no ``examples/`` directory for this
  path -- which is itself asserted below, in both directions);
* ``/health.adr0044`` **not** serving the per-reason withholding counts,
  because the page tells an operator to read the log instead and
  ``_unadvertisable_reason``'s own docstring says the opposite;
* the hook a producer is told to write;
* ``KNOWN_SOURCE_TYPES == set(_MINTERS)``, which is the bound under the page's
  one piece of reassurance about the unminted-credential exposure.

Every assertion carries its own anti-vacuity partner, because a link regex that
stopped matching -- or a payload whose keys were all renamed -- would otherwise
pass by finding nothing to check.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC = REPO_ROOT / "docs" / "file-backed-asset-to-published-dataset.md"
BROKER = REPO_ROOT / "dag_tools" / "domain_broker" / "main.py"

# The five shipped files the page's step table sends a reader to. Unlike the
# warehouse-table page, none of these is an example: they are the production
# surfaces, because nothing in this repo runs this path without real storage.
STEP_FILES = (
    "dag_tools/user_deployment/mesh_demo_assets.py",
    "dag_tools/io_managers/mesh_publishing.py",
    "dag_tools/domain_broker/main.py",
    "scripts/diagnose_unadvertised_assets.py",
    "docs/cortex-data-client.md",
)

_LINK = re.compile(r"\]\((?!https?://|#)([^)]+)\)")


def _body() -> str:
    return DOC.read_text(encoding="utf-8")


def _flat() -> str:
    """The page is hard-wrapped, so a phrase check has to ignore the wrapping."""
    return " ".join(_body().split())


def _links() -> list[str]:
    return [m.group(1).split("#", 1)[0] for m in _LINK.finditer(_body())]


def test_the_page_exists_and_carries_relative_links() -> None:
    """The guard for the tests below, which read nothing if this is false."""
    assert DOC.is_file(), DOC
    assert len(_links()) >= len(STEP_FILES), _links()


def test_every_relative_link_in_the_page_resolves() -> None:
    missing = [ref for ref in _links() if not (DOC.parent / ref).exists()]
    assert not missing, missing


def test_every_shipped_file_the_step_table_names_is_still_there() -> None:
    problems = []
    body = _body()
    for ref in STEP_FILES:
        if Path(ref).name not in body:
            problems.append(f"the page no longer names {ref}")
        if not (REPO_ROOT / ref).is_file():
            problems.append(f"{ref} is gone")
    assert not problems, problems


def test_the_readme_links_the_page() -> None:
    """A guide nothing points at is a guide nobody finds."""
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "docs/file-backed-asset-to-published-dataset.md" in readme


def test_the_page_sends_nobody_to_an_examples_directory() -> None:
    """Asserted in both directions, because either alone passes vacuously.

    The page states plainly that this path has no runnable example, which is
    the difference between it and the warehouse-table page beside it. If an
    example for this path is ever added, the second half of this test fails and
    the page has to stop saying so -- that is the point of the second half.
    """
    assert "has no such directory at either pin" in _flat()
    assert not [ref for ref in _links() if ref.startswith("../examples/")]
    examples = REPO_ROOT / "examples"
    assert examples.is_dir(), examples  # the anti-vacuity half of the half
    assert not [p.name for p in examples.iterdir() if "mesh" in p.name]


def test_the_health_payload_still_does_not_serve_the_withholding_reasons() -> None:
    """The page's step 4 rests on this, and the docstring beside it disagrees.

    ``_unadvertisable_reason`` documents its return values as "stable keys for
    ``/health.adr0044.unadvertised``" and no such key is emitted -- the reasons
    are logged per asset and summarised at WARNING, and that is all. So the
    page tells an operator to read the log. If the key is ever added, this
    fails and the page is the thing to correct.
    """
    broker = BROKER.read_text(encoding="utf-8")
    # The anti-vacuity partner: the keys the page DOES name are still keys, so
    # a wholesale rename of the payload cannot make the check below pass.
    for served in (
        '"echoed_credentials_dropped":',
        '"unprotected_source_types":',
        '"non_fqdn_hosts":',
    ):
        assert served in broker, served
    assert '"unadvertised":' not in broker
    assert "_unadvertisable_reason" in broker


def test_the_required_hook_is_the_one_the_page_tells_a_producer_to_write() -> None:
    from dag_tools.io_managers import MeshPublishable

    assert hasattr(MeshPublishable, "mesh_uri")
    assert hasattr(MeshPublishable, "physical_coordinates")
    for optional in ("mesh_source_type", "mesh_endpoint", "mesh_region", "mesh_scope", "mesh_extra"):
        assert hasattr(MeshPublishable, optional), optional
    assert not hasattr(MeshPublishable, "mesh_credentials")


def test_every_source_type_the_mixin_advertises_has_a_minter() -> None:
    """The bound under the page's one reassurance about unminted credentials.

    A producer on the mixin cannot fall off the minted path, because the gate
    it is refused by is the same set the broker can mint for. A HAND-ROLLED
    ``physical_coordinates`` can, which is what the page tells people to grep
    for. ``_MINTERS`` is read with ``ast`` rather than imported: importing the
    broker pulls in a web framework and a cloud client, and the identity is a
    property of the literal.
    """
    from dag_tools.io_managers.mesh_publishing import KNOWN_SOURCE_TYPES

    minters: set[str] | None = None
    for node in ast.parse(BROKER.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_MINTERS" for t in node.targets
        ):
            assert isinstance(node.value, ast.Dict)
            minters = {k.value for k in node.value.keys if isinstance(k, ast.Constant)}
    assert minters, "no module-level _MINTERS literal in the broker"
    assert set(KNOWN_SOURCE_TYPES) == minters
