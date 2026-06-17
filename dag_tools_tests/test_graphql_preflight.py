"""Tests for the GraphQL methods added for Q3 preflight:
``get_dagster_version`` and ``get_code_locations``."""
from unittest.mock import MagicMock

import pytest

pytest.importorskip("httpx")

import httpx

from dag_tools.qual.graphql import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    DagsterGraphQLError,
)


def _http_response(payload: dict, status_code: int = 200) -> MagicMock:
    mock = MagicMock(spec=httpx.Client)
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    resp.text = str(payload)
    mock.post.return_value = resp
    return mock


# ---------------------------------------------------------------------------
# get_dagster_version
# ---------------------------------------------------------------------------


def test_get_dagster_version_returns_string():
    http = _http_response({"data": {"version": "1.12.1"}})
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    assert client.get_dagster_version() == "1.12.1"


def test_get_dagster_version_raises_on_missing_field():
    """A deployment that returns no ``version`` field is a misbehaving
    Dagster — surface that loudly rather than silently treating as None."""
    http = _http_response({"data": {}})
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError, match="did not report a version"):
        client.get_dagster_version()


# ---------------------------------------------------------------------------
# get_code_locations
# ---------------------------------------------------------------------------


def test_get_code_locations_parses_loaded_entries():
    http = _http_response({
        "data": {
            "workspaceOrError": {
                "__typename": "Workspace",
                "locationEntries": [
                    {
                        "name": "patriot",
                        "loadStatus": "LOADED",
                        "locationOrLoadError": {
                            "__typename": "RepositoryLocation",
                        },
                    },
                    {
                        "name": "broken",
                        "loadStatus": "ERROR",
                        "locationOrLoadError": {
                            "__typename": "PythonError",
                            "message": "ImportError: missing module",
                        },
                    },
                ],
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    locations = client.get_code_locations()

    assert len(locations) == 2
    healthy = locations[0]
    assert isinstance(healthy, CodeLocationStatus)
    assert healthy.name == "patriot"
    assert healthy.load_status == "LOADED"
    assert healthy.error is None
    assert healthy.loaded is True

    broken = locations[1]
    assert broken.name == "broken"
    assert broken.load_status == "ERROR"
    assert broken.error == "ImportError: missing module"
    assert broken.loaded is False


def test_get_code_locations_raises_on_typed_error():
    http = _http_response({
        "data": {
            "workspaceOrError": {
                "__typename": "PythonError",
                "message": "workspace.yaml missing",
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError, match="workspace lookup failed"):
        client.get_code_locations()


def test_get_code_locations_handles_empty_workspace():
    http = _http_response({
        "data": {
            "workspaceOrError": {
                "__typename": "Workspace",
                "locationEntries": [],
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    assert client.get_code_locations() == []
