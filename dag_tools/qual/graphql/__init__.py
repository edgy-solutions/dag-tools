"""Dagster GraphQL client used by Q2/Q3/Q4 to launch and observe runs on
the k8s test deployment.

Public surface:
  * :class:`DagsterGraphQLClient` — launch + poll + event-log.
  * :class:`RunStatusInfo`, :class:`EventLogEntry` — return shapes.
  * :func:`resolve_auth_token` — translates ``deployment.auth`` (e.g.
    ``"env:DAGSTER_TEST_TOKEN"``) into a bearer token.
  * :class:`DagsterGraphQLError` — what callers catch.
"""
from .client import (
    CodeLocationStatus,
    DagsterGraphQLClient,
    DagsterGraphQLError,
    EventLogEntry,
    RunStatusInfo,
    resolve_auth_token,
)

__all__ = [
    "CodeLocationStatus",
    "DagsterGraphQLClient",
    "DagsterGraphQLError",
    "EventLogEntry",
    "RunStatusInfo",
    "resolve_auth_token",
]
