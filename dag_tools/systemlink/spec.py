"""The ``source_config`` of the ``systemlink_test_monitor`` kind, as pydantic models.

Pure: no Dagster, no dlt. ``extra="forbid"`` everywhere, so a misspelt knob fails the load.
"""
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

KIND_NAME = "systemlink_test_monitor"

#: Conservative defaults, pending confirmation by the spike. Every value it would confirm is a setting.
#: ALL of them live in this one block.
DEFAULTS: Dict[str, Any] = {
    "limits.page_size": 100,
    "incremental.use_server_time_filter": False,
    "steps_key": ["resultId", "stepId"],  # the other allowed value is ["stepId"]
    "incremental.order_by": "UPDATED_AT",  # the other spelling is "updatedAt"
    "limits.step_filter": "or_chain",  # the other form is "in"
    "limits.step_batch_default": 15,
    "limits.step_batch_max": 40,
}

#: Not a spike-confirmed value: a hard cap on in-flight requests per run.
MAX_CONCURRENCY_CAP = 4

ORDER_BY_SPELLINGS = ("UPDATED_AT", "updatedAt")
STEP_FILTER_FORMS = ("or_chain", "in")
STEPS_KEYS = (["resultId", "stepId"], ["stepId"])


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ConnectionSpec(_Strict):
    base_url: str = Field(min_length=1)
    api_key_env: str = Field(min_length=1)  # the NAME of the env var, never the key
    api_version_path: str = "/nitestmonitor/v2"
    workspace_id: Optional[str] = None
    verify_tls: bool = True


class SeedSpec(_Strict):
    kind: Literal["file", "inline", "clickhouse_query"]
    path: Optional[str] = None
    column: Optional[str] = None
    values: Optional[List[str]] = None
    query: Optional[str] = None
    connection: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _one_kind_fully_specified(self):
        needs = {
            "file": ("path", "column"),
            "inline": ("values",),
            "clickhouse_query": ("query", "connection"),
        }[self.kind]
        for name in needs:
            if not getattr(self, name):
                raise ValueError(f"seed.kind {self.kind!r} requires {name!r}")
        others = {"path", "column", "values", "query", "connection"} - set(needs)
        stray = sorted(o for o in others if getattr(self, o) not in (None, [], {}))
        if stray:
            raise ValueError(f"seed.kind {self.kind!r} does not take {stray}")
        return self


class FiltersSpec(_Strict):
    terminal_statuses: List[str] = ["PASSED", "FAILED", "ERRORED", "TERMINATED", "TIMED_OUT", "DONE"]
    step_detail_for: Literal["all_terminal", "failed_only"] = "all_terminal"
    failed_statuses: List[str] = ["FAILED", "ERRORED", "TERMINATED", "TIMED_OUT"]
    program_include: List[str] = []
    program_exclude: List[str] = []


class LimitsSpec(_Strict):
    page_size: int = Field(default=DEFAULTS["limits.page_size"], ge=1, le=1000)
    step_batch_default: int = Field(default=DEFAULTS["limits.step_batch_default"], ge=1)
    step_batch_max: int = Field(default=DEFAULTS["limits.step_batch_max"], ge=1)
    step_filter: Literal["or_chain", "in"] = DEFAULTS["limits.step_filter"]
    max_concurrency: int = Field(default=1, ge=1)
    request_timeout_s: float = Field(default=30, gt=0)
    max_retries: int = Field(default=5, ge=0)
    not_visible_warn_threshold: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _ordered(self):
        if self.step_batch_default > self.step_batch_max:
            raise ValueError(
                f"limits.step_batch_default ({self.step_batch_default}) exceeds "
                f"limits.step_batch_max ({self.step_batch_max})"
            )
        if self.max_concurrency > MAX_CONCURRENCY_CAP:
            raise ValueError(
                f"limits.max_concurrency {self.max_concurrency} exceeds the cap of {MAX_CONCURRENCY_CAP}"
            )
        return self


class IncrementalSpec(_Strict):
    # `cursor` is deliberately not accepted: only the known-ids strategy is built.
    strategy: Literal["known_ids"] = "known_ids"
    order_by: Literal["UPDATED_AT", "updatedAt"] = DEFAULTS["incremental.order_by"]
    lookback_pages: int = Field(default=3, ge=1)
    use_server_time_filter: bool = DEFAULTS["incremental.use_server_time_filter"]


class OperatorSpec(_Strict):
    store: Literal["pseudonym", "name", "drop"] = "pseudonym"
    pseudonym_key_env: Optional[str] = None

    @model_validator(mode="after")
    def _key_for_pseudonym(self):
        if self.store == "pseudonym" and not self.pseudonym_key_env:
            raise ValueError("operator.store 'pseudonym' requires operator.pseudonym_key_env")
        return self


class ProductsSpec(_Strict):
    enabled: bool = False


class SystemLinkConfig(_Strict):
    type: Literal["systemlink_test_monitor"]
    source_id: str = Field(min_length=1)
    connection: ConnectionSpec
    seed: SeedSpec
    filters: FiltersSpec = FiltersSpec()
    limits: LimitsSpec = LimitsSpec()
    incremental: IncrementalSpec = IncrementalSpec()
    steps_key: List[str] = Field(default_factory=lambda: list(DEFAULTS["steps_key"]))
    keep_json_columns: List[str] = ["properties", "keywords", "inputs", "outputs"]
    operator: OperatorSpec  # required: where a person's name goes is always a stated decision
    products: ProductsSpec = ProductsSpec()

    @model_validator(mode="after")
    def _steps_key_allowed(self):
        if self.steps_key not in [list(k) for k in STEPS_KEYS]:
            raise ValueError(f"steps_key must be one of {[list(k) for k in STEPS_KEYS]}")
        return self


def parse_source_config(source_config: Dict[str, Any]) -> SystemLinkConfig:
    """Validate one ``source_config`` block; raises ``pydantic.ValidationError`` (a ValueError)."""
    return SystemLinkConfig.model_validate(source_config)
