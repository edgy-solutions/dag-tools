"""Dagster-facing configuration schema for the tagged warehouse resource.

House shape, same as `read_audit.schema` and for the same reason: the
component's own fields are `Dict[str, Any]` and these models are what those
dicts are validated against at load. The 1.10.19 compat floor this library
supports rejects shapes the resolver handles on current Dagster, and its
refusal fails the whole code-location load rather than the one block -- so
every attribute stays a plain scalar or mapping the component validates
itself.

Validating at load is the point of the file. A tag statement that does not
bind `:tag`, or a connection naming nothing, would otherwise surface as a
window of history in which every query ran and none was attributed: the
report would call that a gap and the operator would go looking for an
untagged pipeline. So the blocks are checked when the code location loads,
through the same validators the resource uses at query time -- not a second
copy of them.

Nothing here names a database, schema, table, column or level.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dag_tools.audit.warehouse import PLACEMENTS, TagTransport


class TagBlock(BaseModel):
    """Where the query tag goes, and the statement that puts it there."""

    model_config = ConfigDict(extra="forbid")

    placement: str = Field(
        description=(
            "How the tag reaches the warehouse: "
            + " | ".join(PLACEMENTS)
            + ". 'session' runs your statement on the same connection first; "
            "'comment' prefixes each statement with the tag in a comment."
        ),
    )
    statement: Optional[str] = Field(
        default=None,
        description=(
            "Session placement only: the statement your warehouse uses to set "
            "a query tag, naming the tag as :tag. The tag is always bound, "
            "never interpolated."
        ),
    )

    def transport(self) -> TagTransport:
        """The validated transport. One validator, used at load and at query."""
        return TagTransport(placement=self.placement, statement=self.statement)

    @model_validator(mode="after")
    def _transport_is_valid(self) -> "TagBlock":
        self.transport()
        return self


class PrincipalBlock(BaseModel):
    """Who the warehouse is told the read is for, by default."""

    model_config = ConfigDict(extra="forbid")

    pipeline_name: Optional[str] = Field(
        default=None,
        description=(
            "This service identity's name, tagged as pipeline:<name>. Leave "
            "unset only when every read is bound to a verified caller with "
            "for_user() -- a resource with neither refuses to send rather "
            "than sending a read nothing explains."
        ),
    )


class ConnectionBlock(BaseModel):
    """The warehouse connection, in the shape every other component takes.

    A DSN, or the parts. There is no environment fallback: see
    `dag_tools.resources.warehouse.warehouse_url`. Secrets arrive as
    `{{ env.VAR }}`, resolved by Dagster before this is validated.
    """

    model_config = ConfigDict(extra="forbid")

    dsn: Optional[str] = None
    drivername: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None

    @model_validator(mode="after")
    def _resolves(self) -> "ConnectionBlock":
        """Prove it resolves now, rather than at 3am inside a materialization.

        Resolving opens nothing -- that split is why it is safe to do here.
        """
        from dag_tools.resources.warehouse import warehouse_url

        warehouse_url(**self.model_dump())
        return self


class TaggedWarehouseSpec(BaseModel):
    """The validated whole, as `build_defs` resolves it."""

    model_config = ConfigDict(extra="forbid")

    resource_key: str = "warehouse"
    tag: TagBlock
    principal: PrincipalBlock = Field(default_factory=PrincipalBlock)
    connection: ConnectionBlock

    @classmethod
    def from_attributes(
        cls,
        *,
        resource_key: str,
        tag: Dict[str, Any],
        principal: Optional[Dict[str, Any]],
        connection: Dict[str, Any],
    ) -> "TaggedWarehouseSpec":
        return cls(
            resource_key=resource_key,
            tag=TagBlock.model_validate(tag or {}),
            principal=PrincipalBlock.model_validate(principal or {}),
            connection=ConnectionBlock.model_validate(connection or {}),
        )
