"""The mfg-trace contract template, read through this library's own reader.

The template is an example document, not a governed one, and every name in it
belongs to its own file. What is worth pinning is that it stays *readable*:
it is the shape a deployment copies, so a document in it that does not load,
or a rule id spelled two ways across its documents, is a defect a reader
inherits.

Nothing here asserts a rule id by name. Spelling them is the document's
business and a rename is a governance decision; what this module refuses is a
rename that only reached some of the documents.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from dag_tools.contract_mapping import load_contract

TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "mfg_trace"
    / "contracts"
    / "mfg_trace.yaml"
)


def _documents() -> List[Dict[str, Any]]:
    with TEMPLATE.open(encoding="utf-8") as handle:
        return [doc for doc in yaml.safe_load_all(handle) if doc]


def test_the_template_is_there_and_carries_documents():
    """The guard against every assertion below passing over an empty list."""
    assert TEMPLATE.is_file(), TEMPLATE
    assert len(_documents()) > 1


def test_every_document_loads_through_the_librarys_own_reader():
    """A template that does not parse is a shape nobody can copy."""
    for document in _documents():
        contract = load_contract(document)
        assert contract.contract_id
        assert contract.columns


def test_every_document_declares_the_same_base_rules():
    """A rename that reached only some documents leaves two spellings.

    Asserted as the *size* of what every document shares, so this test says
    nothing about how any rule is spelled -- only that the documents agree
    with each other. A partial rename drops the shared set by one and fails
    here; a document legitimately declaring extra rules of its own does not.
    """
    rule_sets = [set(load_contract(doc).rules) for doc in _documents()]
    assert all(rule_sets), "a document declares no rules at all"
    shared = set.intersection(*rule_sets)
    assert len(shared) == 2, sorted(
        {rule for rules in rule_sets for rule in rules} - shared
    )


@pytest.mark.parametrize("document", _documents(), ids=lambda d: d["contract_id"])
def test_every_documents_scope_is_a_column_it_produces(document):
    """The template's own documents all scope a read, and say which column.

    `scope: none` is a legitimate thing for a *deployment's* document to say
    and reads as no scope at all; none of these say it, and a scope naming a
    column the document does not declare would be refused at the plan.
    """
    contract = load_contract(document)
    assert contract.scope, f"{contract.contract_id} declares no scope"
    assert contract.scope in contract.column_names
