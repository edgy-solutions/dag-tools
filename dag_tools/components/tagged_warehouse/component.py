"""Declarative component: a warehouse resource that tags every query it sends.

Step 2 of the classification build (ADR-0064). One component instance builds
one resource, under a key the deployment chooses, and that resource is what
step 1's `ReadAuditComponent` reads history through -- unchanged, because
`fetch_rows(statement, parameters)` is the whole seam between them.

Shape, per `AGENTS.md` §2: every knob is a YAML field inside `type:` +
`attributes:`, the tag engine stays pure and Dagster-free
(`dag_tools.audit.warehouse`), and this module is the only thing that knows
about Dagster. Per §5 the YAML is a Dagster-centric schema (`schema.py`)
rather than a pass-through of a driver's connection kwargs.

**Why the resource is a component at all, rather than a line of Python in
each deployment's `definitions.py`:** the tag dialect, the identity the reads
are attributed to and the connection are deployment facts, and the standing
rule for this build is that they arrive and leave as data. A deployment that
had to edit Python to change its tag statement would be a deployment whose
query attribution is a code change away from being wrong.

**Nothing here names a database, schema, table, column or level.**
"""
from __future__ import annotations

from typing import Any, Dict

from dagster import Definitions
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.resources.warehouse import TaggedWarehouseResource

from .schema import TaggedWarehouseSpec


def build_tagged_warehouse(spec: TaggedWarehouseSpec) -> TaggedWarehouseResource:
    """Translate the validated YAML into the resource. Pure, and testable alone."""
    connection = spec.connection
    return TaggedWarehouseResource(
        tag_placement=spec.tag.placement,
        tag_statement=spec.tag.statement,
        pipeline_name=spec.principal.pipeline_name,
        dsn=connection.dsn,
        drivername=connection.drivername,
        host=connection.host,
        port=connection.port,
        username=connection.username,
        password=connection.password,
        database=connection.database,
    )


class TaggedWarehouseComponent(Component, Resolvable, Model):
    """A warehouse resource whose every query says which asset it was for.

    Builds no assets. It contributes one resource, which other components --
    step 1's read audit, and anything else that reads the warehouse -- use by
    name.
    """

    tag: Dict[str, Any]
    """How the tag travels: `placement` (`session` or `comment`) and, for
    session placement, your own `statement` binding `:tag`."""

    connection: Dict[str, Any]
    """The warehouse connection: `dsn`, or `drivername` with
    `host`/`port`/`username`/`password`/`database`. No default, no environment
    fallback."""

    principal: Dict[str, Any] = {}
    """Who reads are attributed to by default: `pipeline_name`."""

    resource_key: str = "warehouse"
    """The key other components name this resource by."""

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        spec = TaggedWarehouseSpec.from_attributes(
            resource_key=self.resource_key,
            tag=self.tag,
            principal=self.principal,
            connection=self.connection,
        )
        return Definitions(
            resources={spec.resource_key: build_tagged_warehouse(spec)}
        )
