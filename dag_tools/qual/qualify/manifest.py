"""Pydantic schemas for the qualification manifest.

The manifest is the **single source of truth** for one qualification run.
It captures:

  * The version pair under test (baseline vs. candidate).
  * Frozen inventory pins — exactly which (repo, sha) pairs every later
    phase will operate on, so the qualification is immune to builds that
    land mid-qualification.
  * Co-upgrade risks — non-Dagster libraries whose pins changed between
    baseline and candidate (notably dbt-core, dbt adapters, warehouse
    clients). These must be separately validated or pinned back; otherwise
    a hidden dbt-core regression masquerades as a Dagster failure.
  * Test deployment GraphQL endpoint + staging resource override config.
  * Selection knobs for representative picking in Q1.

Evolution rules mirror the rest of the qualification system (recipe ADR-2):

  * Add only ``Optional[...]`` fields with defaults; never rename or remove.
  * Bump ``SCHEMA_VERSION`` in the same commit.
  * Readers use ``extra="ignore"`` so unknown fields are silently tolerated.

The manifest is persisted as YAML (human-readable; mirrors the recipe's
sample) but the on-disk shape is just ``QualificationManifest.model_dump()``.
"""
from __future__ import annotations

from datetime import datetime
import keyword
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


SCHEMA_VERSION = 3
"""Bump on every additive change to the manifest schema."""


class VersionTarget(BaseModel):
    """One side of the qualification version pair.

    ``dagster`` is the Dagster version being tested (baseline or candidate);
    ``pins`` records every other library version that matters for parity
    — notably the dagster-* family (dagster-dbt, dagster-k8s, dagster-aws),
    dbt-core, dbt adapters, and warehouse clients. These get diffed
    across sides to surface ``co_upgrade_risks``.
    """
    model_config = ConfigDict(extra="ignore")

    dagster: str
    pins: Dict[str, str] = Field(default_factory=dict)


class InventoryPin(BaseModel):
    """One frozen (repo, sha) pair from the registry at qualification-init time.

    Captures the timestamp from ``latest.json`` for operator audit trail —
    "this qualification pinned repo R at the build that was current at
    14:32 UTC on 2026-06-15".
    """
    model_config = ConfigDict(extra="ignore")

    repo: str
    git_sha: str
    pinned_timestamp: Optional[datetime] = None


class CoUpgradeRisk(BaseModel):
    """A non-Dagster library whose version changes between baseline and candidate.

    The qualification verdict (Q6) refuses to GO unless every risk here is
    either separately validated or explicitly pinned back to the baseline
    version. Otherwise a hidden dbt-core/warehouse regression would
    propagate as a false-positive Dagster failure.
    """
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    lib: str
    from_version: str = Field(alias="from")
    to_version: str = Field(alias="to")
    severity: str = Field(default="warning")
    """One of ``"warning"`` (minor/patch) or ``"major"`` (major bump)."""


class Deployment(BaseModel):
    """k8s test deployment connection details for Q2/Q3/Q4 GraphQL launches."""
    model_config = ConfigDict(extra="ignore")

    graphql_url: Optional[str] = None
    auth: Optional[str] = None
    """Auth scheme. ``"env:VAR_NAME"`` reads a bearer token from env.
    Future: ``"oauth:..."``, ``"none"``. Phase 2 implementation will
    grow this; the manifest just records the operator intent."""

    location_name: Optional[str] = None
    """Code-location name to target when Q2/Q4 launch RUNNABLE
    representatives, exactly as the test deployment's workspace surfaces
    it (e.g. the user-deployment / gRPC server name, or ``demo_defs.py``
    for a local ``dagster dev -f demo_defs.py``). When unset the launcher
    falls back to ``"default"``, which only works if the deployment
    happens to name its location that — real deployments never do, so set
    this via ``dagtools qual init --location-name``."""

    job_name: Optional[str] = None
    """Job to launch representatives through. Defaults to Dagster's
    implicit ``"__ASSET_JOB"`` when unset, which is correct for
    asset-only locations."""

    sha_metadata_key: str = "git_sha"
    """Key the operator stamps into ``workspace.yaml``'s
    ``grpc_server.additional_metadata`` holding the git sha the code
    location was built from. Preflight uses it to match a live code
    location to an ``inventory_pins`` entry. Configurable because nothing
    in this library hardcodes names a downstream deployment owns."""

    repo_metadata_key: str = "repo"
    """Key the operator stamps into ``additional_metadata`` naming the repo
    the code location was built from (matched against
    ``inventory_pins[].repo``). Configurable for the same reason as
    ``sha_metadata_key``."""


class Selection(BaseModel):
    """Representative-picking knobs consumed by Q1."""
    model_config = ConfigDict(extra="ignore")

    prefer_tag: str = Field(default="regression")
    """When tied for "best" within a class, prefer assets tagged with this."""

    reps_per_class: int = Field(default=2)
    """Number of representatives to pick per equivalence class. The recipe
    says ideally spanning >=2 repos per class — Q1 will best-effort that."""


def is_env_ref(value: Any) -> bool:
    """True for the exact ``{env: VAR_NAME}`` shape (a single-key dict)."""
    return isinstance(value, dict) and set(value.keys()) == {"env"}


def _check_literal(value: Any, where: str) -> None:
    """Recursively require a JSON-able literal (nested env refs unsupported)."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _check_literal(item, where)
        return
    if isinstance(value, dict):
        if "env" in value:
            raise ValueError(
                f"{where}: an 'env' key inside a nested literal is ambiguous; "
                "env references are supported only as a whole top-level value "
                "({env: VAR_NAME})"
            )
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError(f"{where}: nested dict keys must be strings, got {k!r}")
            _check_literal(v, where)
        return
    raise ValueError(
        f"{where}: value of type {type(value).__name__} cannot be rendered "
        "into probe source (allowed: str/int/float/bool/None/list/dict or {env: VAR})"
    )


def _validate_config_map(mapping: Dict[str, Dict[str, Any]], label: str) -> None:
    for owner, kwargs in mapping.items():
        for key, value in kwargs.items():
            where = f"{label}[{owner!r}].{key}"
            if not isinstance(key, str) or not key.isidentifier() or keyword.iskeyword(key):
                raise ValueError(
                    f"{where}: kwarg name {key!r} is not a valid Python identifier"
                )
            if isinstance(value, dict) and "env" in value:
                if not is_env_ref(value):
                    raise ValueError(
                        f"{where}: a dict with an 'env' key must be exactly "
                        f"{{env: VAR_NAME}}; got extra keys {sorted(set(value) - {'env'})}"
                    )
                name = value["env"]
                if not isinstance(name, str) or not name:
                    raise ValueError(f"{where}: env reference needs a non-empty variable name")
                continue
            _check_literal(value, where)


class ProbeSettings(BaseModel):
    """Constructor config for the IO managers the synthetic probes bind.

    A probe whose IO manager needs constructor config (a bucket, a base
    path) cannot be built with no arguments, so it could never PASS and Q6
    would need ``--accept-synthetic-coverage-missing`` forever. The config
    enters here, at ``qual init`` time, because the manifest is immutable.

    Values are JSON-able literals or an env reference ``{env: VAR_NAME}``.
    An env reference is rendered into the generated source as
    ``os.environ["VAR_NAME"]`` and is never resolved at generation time:
    probe source is published to the registry and written to disk, so a
    literal credential would land in both.
    """
    model_config = ConfigDict(extra="ignore")

    io_manager_config: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    """Constructor kwargs per IO manager, keyed by the IO manager's FQN
    exactly as the inventory records it (``ClassKeyComponents.io_manager_class``).
    Keyed by FQN, not class_hash, because class hashes are derived from the
    inventory and change on every re-survey; making the operator re-key
    their config on each ``qual classes`` run would make this unusable.
    FQNs are stable and what the operator recognises."""

    io_manager_config_by_class: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    """Per-equivalence-class override, keyed by class_hash. Takes precedence
    over the FQN entry when both match. Deliberately narrow escape hatch
    for one class needing different config from other users of the same
    IO manager."""

    @field_validator("io_manager_config")
    @classmethod
    def _v_fqn(cls, v):
        _validate_config_map(v, "io_manager_config")
        return v

    @field_validator("io_manager_config_by_class")
    @classmethod
    def _v_class(cls, v):
        _validate_config_map(v, "io_manager_config_by_class")
        return v

    def config_for(self, class_hash: str, io_manager_fqn: Optional[str]) -> Dict[str, Any]:
        """Resolve the kwargs for one class: class_hash entry beats FQN entry."""
        if class_hash in self.io_manager_config_by_class:
            return dict(self.io_manager_config_by_class[class_hash])
        if io_manager_fqn and io_manager_fqn in self.io_manager_config:
            return dict(self.io_manager_config[io_manager_fqn])
        return {}

    def has_config_for(self, class_hash: str, io_manager_fqn: Optional[str]) -> bool:
        return class_hash in self.io_manager_config_by_class or bool(
            io_manager_fqn and io_manager_fqn in self.io_manager_config
        )


class QualificationManifest(BaseModel):
    """The full qualification manifest.

    Written once at ``dagtools qual init`` time; read by every subsequent
    phase. Treated as **immutable** by the registry (Q1..Q6 add their own
    artifacts under ``qualifications/<qual_id>/`` but never rewrite the
    manifest — a re-run with the same qual_id is a refused operation
    unless ``--allow-overwrite`` is passed).
    """
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(default=SCHEMA_VERSION)
    qual_id: str
    created_at: datetime

    baseline: VersionTarget
    candidate: VersionTarget

    co_upgrade_risks: List[CoUpgradeRisk] = Field(default_factory=list)
    inventory_pins: List[InventoryPin] = Field(default_factory=list)

    deployment: Deployment = Field(default_factory=Deployment)
    staging_overrides: Optional[str] = None
    """Pointer (typically ``s3://...``) to the staging resource override
    config. Phase 2 GraphQL launches pull this as run_config."""

    selection: Selection = Field(default_factory=Selection)

    probes: ProbeSettings = Field(default_factory=ProbeSettings)
    """Constructor config for synthetic probe IO managers (schema v3)."""
