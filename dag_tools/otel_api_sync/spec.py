"""The YAML contract for OpenTelemetry -> API synchronisation.

One spec describes one pipeline: how ClickHouse rows are grouped into
execution groups, what collections are derived per group, and the
ordered list of API calls to make. Nothing in here names a domain
concept — four endpoints or forty, the executor is the same code.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from dag_tools.otel_api_sync.functions import DEFAULT_ATTRIBUTE_COLUMNS

# Statuses worth retrying rather than failing: transient server and
# throttling responses. Everything else in 4xx is a client-side problem
# that retrying cannot fix.
DEFAULT_RETRY_STATUSES = [408, 425, 429, 500, 502, 503, 504]


class ApiSpec(BaseModel):
    """Where calls go and how they authenticate.

    Secrets never travel in a rendered plan. ``headers`` carries static,
    non-secret values; ``header_env`` carries templates expanded from the
    *worker's* environment at execution time, so the plan that crosses
    the Restate ingress contains variable names, not credentials::

        header_env:
          Authorization: "Bearer ${API_TOKEN}"
    """

    base_url: Optional[str] = Field(
        default=None, description="Target API base URL. Overridden by base_url_env when set."
    )
    base_url_env: Optional[str] = Field(
        default=None,
        description="Env var (read by the Restate worker) holding the base URL.",
    )
    headers: Dict[str, str] = Field(
        default_factory=dict, description="Static non-secret headers sent with every call."
    )
    header_env: Dict[str, str] = Field(
        default_factory=dict,
        description="Headers whose ${VAR} references are expanded from the worker's environment.",
    )
    idempotency_header: Optional[str] = Field(
        default=None,
        description=(
            "Header name (e.g. 'Idempotency-Key') for a per-call key derived "
            "from the plan hash and the call's content identity. Set this "
            "whenever the target API supports it. Restate makes the *journal* "
            "exactly-once, but a side effect inside ctx.run is at-least-once "
            "at the crash boundary: if the worker dies after the request "
            "lands but before its result is journaled, replay re-sends it. "
            "For append-style endpoints that means a duplicate record unless "
            "the server can recognise the key. Verified live — a worker "
            "killed mid-plan produced one extra call out of 802."
        ),
    )
    timeout_seconds: float = Field(default=30.0, description="Per-call HTTP timeout.")
    retry_statuses: List[int] = Field(
        default_factory=lambda: list(DEFAULT_RETRY_STATUSES),
        description="Statuses the handler raises on, so Restate retries with backoff.",
    )

    @model_validator(mode="after")
    def _require_a_base_url(self) -> "ApiSpec":
        if not self.base_url and not self.base_url_env:
            raise ValueError("api requires either 'base_url' or 'base_url_env'")
        return self


class FallbackSpec(BaseModel):
    """What to do when a call comes back with a specific status.

    Two modes, and the difference is a data-safety question rather than a
    stylistic one:

    ``item`` (default)
        Retry the single failed item against a different endpoint. The
        payload sees the same scope the failed call saw, so it can
        reference ``item`` and nothing else.

    ``aggregate``
        Collect the failures across a fan-out and issue **one** call
        afterwards carrying only the items that failed. ``fragment`` is
        rendered per item up front (the handler has no template engine)
        and the handler splices the fragments of the failed items into
        ``payload`` at ``collect_into``.

    The aggregate mode exists because bulk-create endpoints are often
    replace-semantics: sending every item in the group — including the
    ones whose call just succeeded — would overwrite server-side state
    that was never in the telemetry. A fallback must therefore be scoped
    to what actually failed, in both modes.
    """

    mode: str = Field(default="item", description="'item' or 'aggregate'.")
    method: str = Field(default="POST")
    path: str = Field(description="Path template for the fallback call.")
    payload: Any = Field(default=None, description="Body template for the fallback call.")
    headers: Dict[str, str] = Field(default_factory=dict)
    collect_into: Optional[str] = Field(
        default=None,
        description="Dotted path in 'payload' where aggregate mode splices failed fragments.",
    )
    fragment: Any = Field(
        default=None,
        description="Per-item body fragment, rendered up front, collected in aggregate mode.",
    )

    @model_validator(mode="after")
    def _validate_mode(self) -> "FallbackSpec":
        if self.mode not in ("item", "aggregate"):
            raise ValueError(f"fallback mode must be 'item' or 'aggregate', got {self.mode!r}")
        if self.mode == "aggregate":
            if not self.collect_into:
                raise ValueError("aggregate fallback requires 'collect_into'")
            if self.fragment is None:
                raise ValueError("aggregate fallback requires 'fragment'")
        return self


class StepSpec(BaseModel):
    """One ordered stage of the plan.

    Without ``for_each`` the step runs once per execution group. With it,
    the step fans out over the rendered collection and each element is
    bound to ``item``.
    """

    id: str = Field(description="Stable identifier; also names the durable step in Restate.")
    method: str = Field(default="POST")
    path: str = Field(description="Path template, appended to the API base URL.")
    for_each: Optional[str] = Field(
        default=None, description="Expression yielding the collection to fan out over."
    )
    item_key: Optional[str] = Field(
        default=None,
        description=(
            "Expression producing a stable, unique label per fanned-out item. "
            "Defaults to the item itself. Used to name the durable Restate step, "
            "so it must be deterministic across retries."
        ),
    )
    payload: Any = Field(default=None, description="Body template; omit for bodyless calls.")
    headers: Dict[str, str] = Field(default_factory=dict)
    query: Dict[str, Any] = Field(default_factory=dict, description="Query-string template.")
    skip_when: Optional[str] = Field(
        default=None, description="Expression; when truthy the call is not emitted."
    )
    on_status: Dict[int, FallbackSpec] = Field(
        default_factory=dict, description="Status -> fallback behaviour."
    )
    continue_on_error: bool = Field(
        default=False,
        description="Treat an unrecoverable failure as non-fatal and continue the plan.",
    )
    dedupe: bool = Field(
        default=True,
        description=(
            "Skip a call this execution group has already delivered verbatim. "
            "Identity is (step, item, method, path, body), so a changed payload "
            "still re-sends. Keep enabled for append-style endpoints: when a "
            "group re-dispatches with late-arriving rows, a per-row step would "
            "otherwise re-send every row it already covered. Disable only for "
            "endpoints that are meant to receive the same call repeatedly."
        ),
    )

    @model_validator(mode="after")
    def _validate_fallbacks(self) -> "StepSpec":
        aggregate = [status for status, fb in self.on_status.items() if fb.mode == "aggregate"]
        if aggregate and not self.for_each:
            raise ValueError(
                f"step '{self.id}': aggregate fallback needs 'for_each' — there is "
                "nothing to aggregate across on a once-per-group step"
            )
        return self


class ReadinessSpec(BaseModel):
    """When an execution group is considered complete enough to dispatch.

    Telemetry streams in: spans belonging to one execution group can land
    across several extraction runs. Dispatching a half-filled group
    produces incomplete collections (missing artifacts, missing items),
    and those go out as authoritative upserts. The gate below is the
    difference between "eventually correct" and "confidently wrong".
    """

    quiet_period_seconds: int = Field(
        default=0,
        description=(
            "Dispatch a group only once its newest row is this old. 0 disables "
            "the gate — correct only when a group always arrives atomically."
        ),
    )
    timestamp_column: str = Field(
        default="Timestamp", description="Row column carrying the event time."
    )
    complete_when: Optional[str] = Field(
        default=None,
        description=(
            "Expression evaluated per group; when truthy the group dispatches "
            "immediately regardless of the quiet period. Use for terminal "
            "marker rows."
        ),
    )
    max_age_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Force-dispatch a group this old even if complete_when never fired, "
            "so a missing terminal marker cannot strand data forever."
        ),
    )


class OtelApiSyncSpec(BaseModel):
    """The full mapping document for one pipeline."""

    api: ApiSpec
    group_by: str = Field(description="Expression evaluated per row, producing the group key.")
    steps: List[StepSpec] = Field(description="Ordered API calls. Order is execution order.")
    derive: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Named collections evaluated once per group, in declaration order, "
            "each visible to those declared after it and to every step."
        ),
    )
    readiness: ReadinessSpec = Field(default_factory=ReadinessSpec)
    attribute_columns: List[str] = Field(
        default_factory=lambda: list(DEFAULT_ATTRIBUTE_COLUMNS),
        description="Map columns searched by attr() when a key is not a real column.",
    )
    strict_undefined: bool = Field(
        default=False,
        description="Raise at render time on undefined template names instead of emitting null.",
    )

    @model_validator(mode="after")
    def _unique_step_ids(self) -> "OtelApiSyncSpec":
        seen = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError(f"duplicate step id '{step.id}' — step ids name durable steps")
            seen.add(step.id)
        if not self.steps:
            raise ValueError("spec declares no steps")
        return self


def load_spec(document: Dict[str, Any]) -> OtelApiSyncSpec:
    """Validate a raw mapping document (already parsed from YAML)."""
    return OtelApiSyncSpec.model_validate(document)


def load_spec_file(path: str) -> OtelApiSyncSpec:
    """Load and validate a mapping YAML file from disk."""
    import yaml

    with open(path, "r", encoding="utf-8") as handle:
        return load_spec(yaml.safe_load(handle))
