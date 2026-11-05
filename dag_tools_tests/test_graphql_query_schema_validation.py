"""Validate every GraphQL document the qual client sends against the schema.

AGENTS.md 4f: selecting a field that does not exist on the target type 400s
the ENTIRE request. Mocked-transport tests cannot see that; schema validation
can. Documents are collected by reflection, so a query added later is covered
with no edit here. The schema is the *installed* ``dagster_graphql``, so the
compat-floor CI job validates against the floor's schema for free.
"""
from __future__ import annotations

import pytest
from dagster_graphql.schema import create_schema
from graphql import parse, validate

from dag_tools.qual.graphql import DagsterGraphQLClient


def _documents() -> dict:
    return {
        name: value
        for name, value in vars(DagsterGraphQLClient).items()
        if ("_QUERY" in name or "_MUTATION" in name)
        and name.isupper()
        and isinstance(value, str)
    }


@pytest.fixture(scope="module")
def schema():
    return create_schema().graphql_schema


def test_the_client_exposes_documents_to_validate():
    assert len(_documents()) >= 7


@pytest.mark.parametrize("name", sorted(_documents()))
def test_client_document_validates_against_the_installed_schema(schema, name):
    errors = validate(schema, parse(_documents()[name]))
    assert errors == [], f"{name}: {[e.message for e in errors]}"
