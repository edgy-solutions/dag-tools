"""Dagster-facing configuration schema for the contract mapping.

Per `AGENTS.md` §5 the component exposes a Dagster-centric schema and
translates it into what the engine wants. The component's own fields are
`Dict[str, Any]` (the house shape -- nested `Resolvable` models behave
differently across the versions this library supports, down to the 1.10.19
compat floor), and these models are what those dicts are validated against at
component load, so a malformed block fails the code location load with a
pointed error rather than at 3am inside a materialization.

**Nothing here has a column, table, type or level name as a default.** The
contract is the deployment's file (or iagent's served JSON); the mapping is
the deployment's file; the warehouse's type names and its non-finite
predicate come from the mapping. This schema only says where to find them
and what to do with what it finds.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from dagster import Config
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ContractBlock(BaseModel):
    """Where the contract comes from.

    Exactly one of two topologies:

      * `path` -- a file beside the component, or any readable path. Resolved
        and validated at the code location load, so a contract that does not
        parse fails loudly where an operator is looking.
      * `url` -- iagent serves the contract as JSON. Fetched at
        materialization, because a code location must not fail to load
        because a service is down. The cost is that the mapping is only
        reconciled against it at materialization; the component says so in
        its notes.
    """

    model_config = ConfigDict(extra="forbid")

    path: Optional[str] = Field(
        default=None,
        description=(
            "Path to the contract document (YAML or JSON). Relative paths "
            "resolve against the component directory."
        ),
    )
    url: Optional[str] = Field(
        default=None,
        description=(
            "URL serving the contract as JSON. Fetched at materialization, "
            "never at load."
        ),
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description="Sent with the `url` fetch. Use `{{ env('X') }}` for a token.",
    )
    timeout_seconds: float = Field(default=30.0, gt=0)

    expect_contract_id: Optional[str] = Field(
        default=None,
        description=(
            "Refuse a contract whose `contract_id` is not this. Worth setting "
            "with `url`, where what arrives is not under the operator's "
            "control: a service that starts serving a different contract "
            "would otherwise silently re-point the mapping."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_source(self) -> "ContractBlock":
        if bool(self.path) == bool(self.url):
            raise ValueError(
                "set exactly one of `contract.path` or `contract.url`; a "
                "contract read from two places is two contracts"
            )
        return self


class SourceSchemaBlock(BaseModel):
    """Where the source dataset's schema comes from.

    The mapping may carry the schema itself, which is what makes an unknown
    source column fail the code location load. This block is for taking it
    from the catalog instead -- the real shape, as the platform reports it,
    at the cost of the check moving to materialization.
    """

    model_config = ConfigDict(extra="forbid")

    from_mapping: bool = Field(
        default=True,
        description=(
            "Use the schema the mapping declares under `source.columns`. When "
            "both this and `datahub` are set, the catalog's schema wins and "
            "any disagreement with the mapping is reported."
        ),
    )

    datahub: bool = Field(
        default=False,
        description=(
            "Read the source dataset's schema from DataHub at "
            "materialization, by the mapping's `source.dataset_urn`."
        ),
    )
    datahub_config: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "`server` and optionally `token` for the DataHub GMS. A PAT is "
            "required whenever the GMS runs with "
            "`METADATA_SERVICE_AUTH_ENABLED=true`; resolution follows "
            "`DatahubLineageComponent` -- explicit `token`, else the standard "
            "`DATAHUB_TOKEN` env var. Emit no token key when there is none; "
            "an empty string is sent as a credential and rejected."
        ),
    )

    @model_validator(mode="after")
    def _some_source(self) -> "SourceSchemaBlock":
        if not self.from_mapping and not self.datahub:
            raise ValueError(
                "`source_schema` disables both sources, which leaves nothing "
                "to reconcile the mapping against; set `from_mapping: true` "
                "or `datahub: true`"
            )
        return self


class EmitBlock(BaseModel):
    """What the component writes, and where."""

    model_config = ConfigDict(extra="forbid")

    dbt_project_dir: Optional[str] = Field(
        default=None,
        description=(
            "The dbt project the model and its tests are written into. "
            "Relative paths resolve against the component directory. "
            "Required unless `dry_run` is set."
        ),
    )
    models_subdir: str = Field(
        default="models",
        description="Where under the project the model file goes.",
    )
    tests_subdir: str = Field(
        default="tests",
        description="Where under the project the singular tests go.",
    )

    dry_run: bool = Field(
        default=False,
        description=(
            "Render everything and write nothing. The rendered bytes go on "
            "the materialization, so a mapping change can be reviewed before "
            "it reaches a dbt project."
        ),
    )
    prune: bool = Field(
        default=False,
        description=(
            "Delete previously generated test files for this model that this "
            "plan no longer emits. Off by default: this component writes into "
            "a directory it does not own, and deleting a file an operator "
            "wrote by hand is worse than leaving a stale one. Only files "
            "whose name begins with the model name are ever considered."
        ),
    )


class ContractMappingRunConfig(Config):
    """Per-run overrides.

    `dry_run` is here as well as in YAML so an operator can render a mapping
    without editing the component file, which is the usual way to review one.
    """

    dry_run: Optional[bool] = None
    max_listed: int = Field(
        default=20,
        description="How many notes and file names to list in metadata.",
    )


class ContractMappingSpec(BaseModel):
    """The whole validated component configuration."""

    model_config = ConfigDict(extra="forbid")

    asset_name: str
    group_name: Optional[str] = None
    io_manager_key: Optional[str] = None
    owners: List[str] = Field(default_factory=list)
    tags: Dict[str, str] = Field(default_factory=dict)

    mapping_file: str
    contract: ContractBlock
    source_schema: SourceSchemaBlock
    emit: EmitBlock

    @model_validator(mode="after")
    def _target_or_dry_run(self) -> "ContractMappingSpec":
        if not self.emit.dry_run and not self.emit.dbt_project_dir:
            raise ValueError(
                "set `emit.dbt_project_dir` (where the model and tests are "
                "written) or `emit.dry_run: true` (render and write nothing)"
            )
        return self

    @classmethod
    def from_attributes(
        cls,
        *,
        asset_name: str,
        group_name: Optional[str],
        io_manager_key: Optional[str],
        owners: Optional[List[str]],
        tags: Optional[Dict[str, str]],
        mapping_file: str,
        contract: Dict[str, Any],
        source_schema: Optional[Dict[str, Any]],
        emit: Optional[Dict[str, Any]],
    ) -> "ContractMappingSpec":
        return cls(
            asset_name=asset_name,
            group_name=group_name,
            io_manager_key=io_manager_key,
            owners=list(owners or []),
            tags=dict(tags or {}),
            mapping_file=mapping_file,
            contract=ContractBlock.model_validate(contract or {}),
            source_schema=SourceSchemaBlock.model_validate(source_schema or {}),
            emit=EmitBlock.model_validate(emit or {}),
        )
