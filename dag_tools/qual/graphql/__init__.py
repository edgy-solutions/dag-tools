"""Dagster GraphQL client used by Q2/Q3/Q4 to launch and observe runs on
the k8s test deployment.

Public surface:
  * :class:`DagsterGraphQLClient` — launch + poll + event-log.
  * :class:`RunStatusInfo`, :class:`EventLogEntry` — return shapes.
  * :func:`resolve_auth_token` — translates ``deployment.auth`` (e.g.
    ``"env:DAGSTER_TEST_TOKEN"``) into a bearer token.
  * :class:`DagsterGraphQLError` — what callers catch.
  * :class:`DagsterSubsetNotSupportedError` — the one launch failure
    the launcher RETRIES (a non-subsettable multi_asset needs all of
    its keys); see ``runs.launcher.expand_to_op_siblings``.
  * :class:`DagsterRunNotFoundError` — the one PERMANENT poll failure;
    any other poll failure leaves a run LAUNCHED for reconciliation.
"""
from .client import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    DagsterGraphQLError,
    DagsterRunNotFoundError,
    DagsterSubsetNotSupportedError,
    EventLogEntry,
    RunStatusInfo,
    resolve_auth_token,
)

__all__ = [
    "CodeLocationStatus",
    "DagsterGraphQLClient",
    "DagsterGraphQLError",
    "DagsterRunNotFoundError",
    "DagsterSubsetNotSupportedError",
    "EventLogEntry",
    "RunStatusInfo",
    "resolve_auth_token",
]
