"""Tests for DagsterGraphQLClient + resolve_auth_token.

We mock httpx.Client directly so tests are fast and deterministic and
don't need a real Dagster endpoint. The point is to verify the client
constructs the right queries and parses the right shapes — the
deployment-side integration is verified by hand.
"""
from unittest.mock import MagicMock

import pytest

pytest.importorskip("httpx")

import httpx

from dag_tools.qual.graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
    EventLogEntry,
    RunStatusInfo,
    resolve_auth_token,
)


def _mock_http_with_response(payload: dict, status_code: int = 200) -> MagicMock:
    """Return a MagicMock that quacks like an httpx.Client whose .post()
    returns one canned response."""
    mock = MagicMock(spec=httpx.Client)
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    resp.text = str(payload)
    mock.post.return_value = resp
    return mock


# ---------------------------------------------------------------------------
# resolve_auth_token
# ---------------------------------------------------------------------------


def test_resolve_auth_token_none_returns_none():
    assert resolve_auth_token(None) is None
    assert resolve_auth_token("") is None


def test_resolve_auth_token_literal_passes_through():
    assert resolve_auth_token("hardcoded-token") == "hardcoded-token"


def test_resolve_auth_token_env_lookup(monkeypatch):
    monkeypatch.setenv("DAGSTER_TOKEN_FOR_TEST", "secret-123")
    assert resolve_auth_token("env:DAGSTER_TOKEN_FOR_TEST") == "secret-123"


def test_resolve_auth_token_env_missing_returns_none(monkeypatch):
    monkeypatch.delenv("MISSING_VAR_XYZ", raising=False)
    assert resolve_auth_token("env:MISSING_VAR_XYZ") is None


# ---------------------------------------------------------------------------
# post
# ---------------------------------------------------------------------------


def test_post_raises_on_http_error():
    http = _mock_http_with_response({}, status_code=503)
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError, match="HTTP 503"):
        client.post("query {}", {})


def test_post_raises_on_graphql_errors_block():
    http = _mock_http_with_response({"errors": [{"message": "bad query"}]})
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError, match="GraphQL errors"):
        client.post("query {}", {})


def _mock_http_redirect(status_code=302, location="https://oauth/login") -> MagicMock:
    """A redirect response — auth proxy bouncing an unauthenticated request.
    The empty body would explode in resp.json() if the client didn't catch
    the 3xx first."""
    mock = MagicMock(spec=httpx.Client)
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {"location": location}
    resp.text = ""
    resp.json.side_effect = ValueError("Expecting value: line 1 column 1")
    mock.post.return_value = resp
    return mock


def test_post_302_redirect_raises_clear_auth_error_not_json_error():
    """Regression: a 302 (auth proxy) used to slide into resp.json() and
    surface as a cryptic 'non-JSON response'. It must now be a clear
    redirect/auth error that tells the operator to supply a token."""
    http = _mock_http_redirect()
    client = DagsterGraphQLClient("http://x/graphql", http=http)  # no token
    with pytest.raises(DagsterGraphQLError) as exc:
        client.post("query {}", {})
    msg = str(exc.value)
    assert "redirect" in msg.lower()
    assert "not" in msg.lower() and "authenticated" in msg.lower()
    assert "--graphql-auth-env" in msg  # actionable
    assert "non-JSON" not in msg


def test_post_401_raises_auth_error():
    http = _mock_http_with_response({}, status_code=401)
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError) as exc:
        client.post("query {}", {})
    assert "--graphql-auth-env" in str(exc.value)


def test_post_redirect_hint_differs_when_token_already_sent():
    http = _mock_http_redirect()
    client = DagsterGraphQLClient("http://x/graphql", http=http, auth_token="stale")
    with pytest.raises(DagsterGraphQLError) as exc:
        client.post("query {}", {})
    # Token WAS sent -> the hint should say it's likely expired/invalid.
    assert "expired" in str(exc.value).lower()


def test_post_sends_auth_header_when_token_set():
    http = _mock_http_with_response({"data": {}})
    client = DagsterGraphQLClient("http://x/graphql", http=http, auth_token="t1")
    client.post("query {}", {})
    sent_headers = http.post.call_args.kwargs["headers"]
    assert sent_headers.get("Authorization") == "Bearer t1"


def test_post_omits_auth_header_when_no_token():
    http = _mock_http_with_response({"data": {}})
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    client.post("query {}", {})
    sent_headers = http.post.call_args.kwargs["headers"]
    assert "Authorization" not in sent_headers


# ---------------------------------------------------------------------------
# launch_asset_run
# ---------------------------------------------------------------------------


def test_launch_asset_run_returns_run_id_on_success():
    http = _mock_http_with_response({
        "data": {
            "launchPipelineExecution": {
                "__typename": "LaunchRunSuccess",
                "run": {"runId": "run-xyz", "status": "STARTING"},
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    run_id = client.launch_asset_run(
        location_name="loc",
        repository_name="__repository__",
        job_name="__ASSET_JOB",
        asset_selection=[["hello"]],
        tags={"dagtools/qual": "q1"},
    )
    assert run_id == "run-xyz"


def test_launch_run_config_invalid_missing_ops_gives_actionable_hint():
    """A config-requiring asset (e.g. a sensor-driven ingest) launched with
    empty config returns RunConfigValidationInvalid 'Missing ... ops'. The
    error must point the operator at tagging it out of qualification."""
    http = _mock_http_with_response({
        "data": {
            "launchPipelineExecution": {
                "__typename": "RunConfigValidationInvalid",
                "errors": [{
                    "message": "Missing required config entry \"ops\" at the root.",
                    "reason": "MISSING_REQUIRED_FIELD",
                }],
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError) as exc:
        client.launch_asset_run(
            location_name="loc", repository_name="__repository__",
            job_name="__ASSET_JOB", asset_selection=[["grist_ingest"]],
        )
    msg = str(exc.value)
    assert "observe_only" in msg
    assert "empty config" in msg


def test_launch_asset_run_raises_on_typed_failure_shape():
    http = _mock_http_with_response({
        "data": {
            "launchPipelineExecution": {
                "__typename": "PipelineNotFoundError",
                "message": "no such pipeline",
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError, match="PipelineNotFoundError"):
        client.launch_asset_run(
            location_name="loc",
            repository_name="__repository__",
            job_name="__ASSET_JOB",
            asset_selection=[["hello"]],
        )


def test_launch_mutation_does_not_select_message_on_fieldless_error_types():
    """Regression: `InvalidStepError` and `InvalidOutputError` have NO
    `message` field in the Dagster GraphQL schema (only invalidStepKey /
    stepKey+invalidOutputName). Selecting `message` on them 400s the
    ENTIRE mutation, which silently broke every launch against a real
    deployment (found via live testing against dagster dev 1.13.1).

    We assert the mutation never does this. Guarding at the string level
    is the right altitude — the failure mode is a server-side query
    rejection, not a parse-time error on our side."""
    import re
    mutation = DagsterGraphQLClient.LAUNCH_MUTATION
    # No `... on InvalidStepError { ... message ... }` or InvalidOutputError.
    for bad_type in ("InvalidStepError", "InvalidOutputError"):
        # Grab the inline-fragment body for this type, if present at all.
        m = re.search(rf"on {bad_type}\s*{{([^}}]*)}}", mutation)
        if m:
            assert "message" not in m.group(1), (
                f"{bad_type} has no `message` field; selecting it 400s the "
                f"whole launch mutation"
            )


def test_launch_sends_tags_in_execution_metadata():
    http = _mock_http_with_response({
        "data": {
            "launchPipelineExecution": {
                "__typename": "LaunchRunSuccess",
                "run": {"runId": "x"},
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    client.launch_asset_run(
        location_name="loc",
        repository_name="r",
        job_name="j",
        asset_selection=[["a"]],
        tags={"dagtools/qual": "q1", "dagtools/side": "baseline"},
    )
    body = http.post.call_args.kwargs["json"]
    tags = body["variables"]["params"]["executionMetadata"]["tags"]
    tag_pairs = {t["key"]: t["value"] for t in tags}
    assert tag_pairs["dagtools/qual"] == "q1"
    assert tag_pairs["dagtools/side"] == "baseline"


# ---------------------------------------------------------------------------
# get_run_status / poll
# ---------------------------------------------------------------------------


def test_get_run_status_parses_typed_response():
    http = _mock_http_with_response({
        "data": {
            "pipelineRunOrError": {
                "__typename": "Run",
                "runId": "r1",
                "status": "SUCCESS",
                "startTime": 1718712000.0,
                "endTime": 1718712100.0,
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    info = client.get_run_status("r1")
    assert isinstance(info, RunStatusInfo)
    assert info.status == "SUCCESS"
    assert info.is_terminal
    assert info.succeeded


def test_get_run_status_raises_on_not_found():
    http = _mock_http_with_response({
        "data": {
            "pipelineRunOrError": {
                "__typename": "PipelineRunNotFoundError",
                "message": "lost it",
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError):
        client.get_run_status("r1")


def test_poll_to_completion_stops_on_terminal(monkeypatch):
    """First poll returns STARTED, second returns SUCCESS — poll terminates."""
    responses = [
        {"data": {"pipelineRunOrError": {"__typename": "Run", "runId": "r", "status": "STARTED"}}},
        {"data": {"pipelineRunOrError": {"__typename": "Run", "runId": "r", "status": "SUCCESS"}}},
    ]
    http = MagicMock(spec=httpx.Client)
    def post(url, json=None, headers=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = responses.pop(0)
        return resp
    http.post.side_effect = post

    client = DagsterGraphQLClient("http://x/graphql", http=http)
    info = client.poll_to_completion("r", interval_seconds=0.0, sleep=lambda s: None)
    assert info.status == "SUCCESS"


def test_poll_to_completion_times_out(monkeypatch):
    """Never reaches terminal -> times out with a clear error."""
    http = _mock_http_with_response({
        "data": {"pipelineRunOrError": {"__typename": "Run", "runId": "r", "status": "STARTED"}}
    })

    # Force time.monotonic to advance past timeout immediately.
    import dag_tools.qual.graphql.client as gc
    times = [0.0, 999999.0]
    monkeypatch.setattr(gc.time, "monotonic", lambda: times.pop(0) if times else 999999.0)

    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError, match="did not reach terminal"):
        client.poll_to_completion(
            "r", interval_seconds=0.0, timeout_seconds=1.0, sleep=lambda s: None,
        )


# ---------------------------------------------------------------------------
# get_event_log
# ---------------------------------------------------------------------------


def test_get_event_log_parses_materialization_with_metadata():
    http = _mock_http_with_response({
        "data": {
            "logsForRun": {
                "__typename": "EventConnection",
                "events": [
                    {
                        "__typename": "MaterializationEvent",
                        "message": "ok",
                        "timestamp": 1.0,
                        "stepKey": "step1",
                        "assetKey": {"path": ["hello"]},
                        "metadataEntries": [
                            {"label": "row_count"},
                            {"label": "schema_version"},
                        ],
                    }
                ],
            }
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    events = client.get_event_log("r1")
    assert len(events) == 1
    ev = events[0]
    assert isinstance(ev, EventLogEntry)
    assert ev.event_type == "MaterializationEvent"
    assert ev.asset_key == ["hello"]
    assert sorted(ev.metadata_keys) == ["row_count", "schema_version"]


def test_get_event_log_raises_on_not_found_shape():
    http = _mock_http_with_response({
        "data": {
            "logsForRun": {"__typename": "PipelineRunNotFoundError", "message": "gone"}
        }
    })
    client = DagsterGraphQLClient("http://x/graphql", http=http)
    with pytest.raises(DagsterGraphQLError):
        client.get_event_log("r1")
