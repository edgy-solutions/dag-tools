"""Version-tolerant Dagster GraphQL client for Q2/Q3/Q4 launches.

This client is deliberately minimal: it does the three operations
Q2 needs and does them carefully.

  1. ``launch_asset_run`` — submit a ``launchPipelineExecution`` mutation
     targeting an asset's implicit job, returning the new ``run_id``.
  2. ``get_run_status`` — poll a run's terminal-or-not state.
  3. ``get_event_log`` — pull the run's event-log entries once it ends,
     for materialization / asset-check / metadata extraction.

Version tolerance strategy (recipe ADR-ish):

  * Public GraphQL fields only. Internal ``dagster._core.*`` types drift;
    public ``pipelineRunOrError`` / ``logsForRun`` / ``launchPipelineExecution``
    have been stable since Dagster 1.0 in their query *shape*, even when
    individual fields move.
  * Each query has an explicit list of fields we ask for. If a field is
    missing on a given deployment, the response just omits it — we treat
    missing as None. The launcher and parser MUST be tolerant of partial
    responses.
  * No GraphQL library dependency. ``httpx`` posts the JSON body directly
    so the client stays testable with stock HTTP mocks (no schema
    registry to spin up in tests).

Auth: the manifest's ``deployment.auth`` field encodes how to read the
token. ``env:VAR_NAME`` reads from an env var; anything else is treated
as a literal bearer token (legacy / hardcoded fallback). When auth is
None the client sends no Authorization header (test deployments often
run unauthenticated on a private network).
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import httpx


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public data shapes the client returns.
# ---------------------------------------------------------------------------


@dataclass
class RunStatusInfo:
    """The bits of a run's status we use to decide poll-vs-continue."""
    run_id: str
    status: str
    """Dagster pipeline run status: STARTED / SUCCESS / FAILURE / CANCELED /
    QUEUED / NOT_STARTED / STARTING. We treat SUCCESS / FAILURE / CANCELED
    as terminal; the rest mean keep polling."""
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    TERMINAL: tuple = ("SUCCESS", "FAILURE", "CANCELED")

    @property
    def is_terminal(self) -> bool:
        return self.status in self.TERMINAL

    @property
    def succeeded(self) -> bool:
        return self.status == "SUCCESS"


@dataclass
class EventLogEntry:
    """One entry from the run's event log."""
    event_type: Optional[str] = None
    message: Optional[str] = None
    timestamp: Optional[float] = None
    step_key: Optional[str] = None
    asset_key: Optional[List[str]] = None
    metadata_keys: List[str] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CodeLocationStatus:
    """One code location's load status as the test deployment reports it."""
    name: str
    load_status: str
    """``"LOADED"`` is the only happy state. ``"LOADING"`` means the
    operator must wait; ``"ERROR"`` means the user-code image is broken."""
    error: Optional[str] = None
    """When ``load_status`` is ``"ERROR"``, the operator-actionable message."""

    @property
    def loaded(self) -> bool:
        return self.load_status == "LOADED"


# ---------------------------------------------------------------------------
# Auth resolution
# ---------------------------------------------------------------------------


def resolve_auth_token(auth_spec: Optional[str]) -> Optional[str]:
    """Translate the manifest's ``deployment.auth`` field into a bearer token.

    Forms supported:
      * ``"env:VAR_NAME"`` — read from the environment.
      * any other string — used as the literal bearer token.
      * ``None`` / empty — no auth header.
    """
    if not auth_spec:
        return None
    if auth_spec.startswith("env:"):
        var = auth_spec[4:].strip()
        token = os.environ.get(var)
        if not token:
            logger.warning("resolve_auth_token: env var %s is empty or unset", var)
            return None
        return token
    return auth_spec


# ---------------------------------------------------------------------------
# The client itself.
# ---------------------------------------------------------------------------


class DagsterGraphQLError(RuntimeError):
    """Raised when the GraphQL endpoint reports errors that aren't a
    typed Dagster union (e.g. transport errors, GraphQL ``errors[]`` block,
    malformed responses)."""


class DagsterGraphQLClient:
    """Tiny version-tolerant GraphQL client for the Dagster test deployment."""

    DEFAULT_TIMEOUT = 30.0

    def __init__(
        self,
        endpoint_url: str,
        *,
        auth_token: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        http: Optional[httpx.Client] = None,
    ):
        self.endpoint_url = endpoint_url
        self.auth_token = auth_token
        self.timeout = timeout
        # Dependency injection makes the client trivially mockable in tests.
        self._http = http or httpx.Client(timeout=timeout)

    def close(self) -> None:
        try:
            self._http.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # -- core post -------------------------------------------------------

    def post(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run a GraphQL request. Returns the ``data`` block.

        Raises :class:`DagsterGraphQLError` on transport / GraphQL errors
        (those bubble up so callers can fail fast and the operator sees
        what went wrong)."""
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        body = {"query": query, "variables": variables or {}}
        try:
            resp = self._http.post(self.endpoint_url, json=body, headers=headers)
        except httpx.HTTPError as e:
            raise DagsterGraphQLError(f"transport error: {e}") from e
        if resp.status_code >= 400:
            raise DagsterGraphQLError(
                f"HTTP {resp.status_code} from {self.endpoint_url}: {resp.text[:500]}"
            )
        try:
            payload = resp.json()
        except Exception as e:
            raise DagsterGraphQLError(f"non-JSON response: {e}") from e
        if payload.get("errors"):
            raise DagsterGraphQLError(
                f"GraphQL errors: {json.dumps(payload['errors'])[:1000]}"
            )
        return payload.get("data") or {}

    # -- launch ---------------------------------------------------------

    LAUNCH_MUTATION = """
mutation DagtoolsLaunchAssetRun($params: ExecutionParams!) {
  launchPipelineExecution(executionParams: $params) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PipelineNotFoundError { message }
    ... on PythonError { message stack }
    ... on InvalidSubsetError { message }
    ... on RunConfigValidationInvalid { errors { message reason } }
  }
}
""".strip()
    # NOTE: we deliberately select `message` ONLY on union members that
    # actually expose it. `InvalidStepError` (invalidStepKey) and
    # `InvalidOutputError` (stepKey/invalidOutputName) have NO `message`
    # field — selecting it 400s the ENTIRE mutation, which silently broke
    # every launch against a real deployment. Unknown/other failure shapes
    # fall through to `__typename` reporting in `launch_asset_run`, so we
    # don't need a fragment per member to stay informative.

    def launch_asset_run(
        self,
        *,
        location_name: str,
        repository_name: str,
        job_name: str,
        asset_selection: List[List[str]],
        run_config: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        mode: str = "default",
    ) -> str:
        """Launch one asset's materialization on the deployment.

        Args:
          location_name: code-location name as Dagster surfaces it.
          repository_name: repository name (usually ``__repository__`` in
            single-repo locations).
          job_name: which job to launch; for asset-only repos this is
            typically ``__ASSET_JOB``.
          asset_selection: list of AssetKey path lists to materialize
            within the job.
          run_config: run config dict (will be JSON-encoded). When None,
            sent as empty.
          tags: tag map. ``dagtools/qual: <qual_id>`` is the convention
            for distinguishing qualification runs from normal traffic.
          mode: Dagster execution mode; default is fine for asset jobs.

        Returns the ``run_id`` of the launched run.

        Raises :class:`DagsterGraphQLError` on transport errors OR when
        the typed union returns a failure shape — partial info is logged
        and the error carries the original message.
        """
        params = {
            "selector": {
                "repositoryLocationName": location_name,
                "repositoryName": repository_name,
                "pipelineName": job_name,
                "assetSelection": [{"path": key} for key in asset_selection],
            },
            "runConfigData": json.dumps(run_config or {}),
            "mode": mode,
            "executionMetadata": {
                "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()],
            },
        }
        data = self.post(self.LAUNCH_MUTATION, {"params": params})
        result = data.get("launchPipelineExecution") or {}
        typename = result.get("__typename")

        if typename in ("LaunchRunSuccess", "LaunchPipelineRunSuccess"):
            run = result.get("run") or {}
            run_id = run.get("runId")
            if not run_id:
                raise DagsterGraphQLError(f"launch succeeded but no runId: {result}")
            return run_id

        # Typed failure shapes — give the operator the most actionable bit.
        msg = result.get("message")
        if not msg and result.get("errors"):
            msg = "; ".join(e.get("message", "?") for e in result["errors"])
        raise DagsterGraphQLError(
            f"launch failed ({typename or 'unknown'}): {msg or 'no detail'}"
        )

    # -- poll -----------------------------------------------------------

    STATUS_QUERY = """
query DagtoolsRunStatus($runId: ID!) {
  pipelineRunOrError(runId: $runId) {
    __typename
    ... on Run { runId status startTime endTime }
    ... on PipelineRunNotFoundError { message }
    ... on PythonError { message }
  }
}
""".strip()

    def get_run_status(self, run_id: str) -> RunStatusInfo:
        data = self.post(self.STATUS_QUERY, {"runId": run_id})
        result = data.get("pipelineRunOrError") or {}
        typename = result.get("__typename")
        if typename != "Run":
            msg = result.get("message") or "no detail"
            raise DagsterGraphQLError(f"run status lookup failed ({typename}): {msg}")
        return RunStatusInfo(
            run_id=result.get("runId") or run_id,
            status=str(result.get("status") or "NOT_STARTED"),
            start_time=_float_or_none(result.get("startTime")),
            end_time=_float_or_none(result.get("endTime")),
        )

    def poll_to_completion(
        self,
        run_id: str,
        *,
        interval_seconds: float = 5.0,
        timeout_seconds: float = 1800.0,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> RunStatusInfo:
        """Poll until the run reaches a terminal status or ``timeout_seconds``
        elapses. Tests inject a ``sleep`` shim to make this instantaneous."""
        sleep = sleep or time.sleep
        start = time.monotonic()
        while True:
            info = self.get_run_status(run_id)
            if info.is_terminal:
                return info
            if time.monotonic() - start > timeout_seconds:
                raise DagsterGraphQLError(
                    f"poll_to_completion: run {run_id} did not reach terminal "
                    f"status within {timeout_seconds}s (last={info.status})"
                )
            sleep(interval_seconds)

    # -- event log ------------------------------------------------------

    LOGS_QUERY = """
query DagtoolsEventLog($runId: ID!) {
  logsForRun(runId: $runId) {
    __typename
    ... on EventConnection {
      events {
        __typename
        ... on MessageEvent { message timestamp stepKey }
        ... on MaterializationEvent {
          message timestamp stepKey
          assetKey { path }
          metadataEntries { label }
        }
        ... on AssetMaterializationPlannedEvent {
          message timestamp stepKey
          assetKey { path }
        }
        ... on ExecutionStepFailureEvent {
          message timestamp stepKey
          errorSource
        }
      }
    }
    ... on PipelineRunNotFoundError { message }
    ... on PythonError { message }
  }
}
""".strip()

    # -- preflight: deployment health -----------------------------------

    VERSION_QUERY = """
query DagtoolsVersion {
  version
}
""".strip()

    def get_dagster_version(self) -> str:
        """Return the Dagster version string the webserver reports.

        Used by Q3 preflight to confirm the deployment matches the
        manifest's expected side. The ``version`` field has been a stable
        top-level GraphQL query since Dagster 1.0."""
        data = self.post(self.VERSION_QUERY)
        v = data.get("version")
        if not v:
            raise DagsterGraphQLError(
                f"deployment did not report a version (data={data})"
            )
        return str(v)

    LOCATIONS_QUERY = """
query DagtoolsWorkspace {
  workspaceOrError {
    __typename
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        locationOrLoadError {
          __typename
          ... on PythonError { message }
        }
      }
    }
    ... on PythonError { message }
  }
}
""".strip()

    def get_code_locations(self) -> List[CodeLocationStatus]:
        """Return every code location's load status.

        Q3 preflight fails when any entry has ``load_status != "LOADED"``
        — that's the fleet-wide load validation on real infrastructure."""
        data = self.post(self.LOCATIONS_QUERY)
        result = data.get("workspaceOrError") or {}
        typename = result.get("__typename")
        if typename != "Workspace":
            raise DagsterGraphQLError(
                f"workspace lookup failed ({typename}): "
                f"{result.get('message') or 'no detail'}"
            )
        out: List[CodeLocationStatus] = []
        for entry in result.get("locationEntries") or []:
            name = entry.get("name") or "?"
            load_status = str(entry.get("loadStatus") or "UNKNOWN")
            error: Optional[str] = None
            link = entry.get("locationOrLoadError") or {}
            if link.get("__typename") == "PythonError":
                error = link.get("message")
            out.append(CodeLocationStatus(
                name=name, load_status=load_status, error=error,
            ))
        return out

    LOCATION_ASSETS_QUERY = """
query DagtoolsLocationAssets {
  workspaceOrError {
    __typename
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        locationOrLoadError {
          __typename
          ... on RepositoryLocation {
            repositories {
              name
              assetNodes {
                assetKey { path }
              }
            }
          }
          ... on PythonError { message }
        }
      }
    }
    ... on PythonError { message }
  }
}
""".strip()

    def get_location_asset_keys(self, location_name: str) -> List[List[str]]:
        """Return every asset key in the named code location.

        Returns ``[]`` when the location is absent or in an error /
        loading state — operators reading the report can tell from the
        outer status (via :meth:`get_code_locations`) why; this method
        deliberately doesn't raise on a not-yet-loaded location.

        Used by ``dagtools qual probes status`` to cross-reference the
        probe manifest against what the test deployment actually shows.
        """
        data = self.post(self.LOCATION_ASSETS_QUERY)
        result = data.get("workspaceOrError") or {}
        if result.get("__typename") != "Workspace":
            raise DagsterGraphQLError(
                f"workspace lookup failed ({result.get('__typename')}): "
                f"{result.get('message') or 'no detail'}"
            )
        out: List[List[str]] = []
        for entry in result.get("locationEntries") or []:
            if entry.get("name") != location_name:
                continue
            link = entry.get("locationOrLoadError") or {}
            if link.get("__typename") != "RepositoryLocation":
                # Loading / error states: return empty so the caller
                # can distinguish "absent" from "no assets" via the
                # outer location-status check.
                return []
            for repo in link.get("repositories") or []:
                for node in repo.get("assetNodes") or []:
                    ak = (node.get("assetKey") or {}).get("path")
                    if isinstance(ak, list) and ak:
                        out.append([str(p) for p in ak])
        return out

    def get_event_log(self, run_id: str) -> List[EventLogEntry]:
        """Pull all event-log entries for a run. Soft-failing per entry —
        if Dagster adds a new event subtype, unknown shapes still come
        through as raw dicts with as much info as we can decode."""
        data = self.post(self.LOGS_QUERY, {"runId": run_id})
        result = data.get("logsForRun") or {}
        if result.get("__typename") != "EventConnection":
            raise DagsterGraphQLError(
                f"event log lookup failed: {result.get('message') or result.get('__typename')}"
            )
        events = result.get("events") or []
        out: List[EventLogEntry] = []
        for ev in events:
            try:
                out.append(_parse_event(ev))
            except Exception as e:
                logger.warning("get_event_log: skipping malformed event: %s", e)
        return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_event(raw: Dict[str, Any]) -> EventLogEntry:
    asset_key = None
    ak = raw.get("assetKey")
    if isinstance(ak, dict) and isinstance(ak.get("path"), list):
        asset_key = list(ak["path"])
    metadata_entries = raw.get("metadataEntries") or []
    metadata_keys = [m.get("label") for m in metadata_entries if m.get("label")]
    return EventLogEntry(
        event_type=raw.get("__typename"),
        message=raw.get("message"),
        timestamp=_float_or_none(raw.get("timestamp")),
        step_key=raw.get("stepKey"),
        asset_key=asset_key,
        metadata_keys=metadata_keys,
        raw=raw,
    )


def _float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None
