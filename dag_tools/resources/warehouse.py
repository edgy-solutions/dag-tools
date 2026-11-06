"""The warehouse resource that says who read what, on every query it sends.

Step 2 of the classification build (ADR-0064), and the implementation of the
one seam step 1 pinned: `fetch_rows(statement, parameters)`. Dropping this
resource under `ReadAuditComponent` needs no change to that component.

**Every query carries its attribution or it is not sent.** `fetch_rows` plans
the read through `dag_tools.audit.warehouse`, which refuses a plan with no
tag, and `_run_plan` checks the plan again at the point of execution. The
refusal is the point: an untagged read comes back out of query history
indistinguishable from a pipeline nobody has attributed, so a resource that
quietly sent one would inflate the very gap the audit measures. See the seals
in `test_tagged_warehouse_resource.py`.

**The asset key has to be bound; the run id does not.** Measured on dagster
1.13.1: the `InitResourceContext` a resource is initialised with exposes the
run (`run` / `dagster_run`, `run_id`) and nothing about the step -- no asset
key, no op def, no step context. So the resource self-attributes to
`{principal, run_id}`, which the report resolves to an asset whenever the run
selected exactly one (`BY_RUN_ID`), and an asset that wants the sharper
`BY_ASSET_KEY` form binds its own key with one call:

    rows = warehouse.for_asset(context).fetch_rows(statement, params)

That difference is worth the line: a run selecting several assets leaves a
run-scoped tag ambiguous, and ambiguous is counted as unexplained.

**Resolution is separated from connection.** `warehouse_url` returns a URL
string and opens nothing, so it is assertable with `make_url` on a runner that
installs no driver -- the lesson `otel_api_sync._staging_url` already paid
for. The connection block takes the familiar host/port/username/password/
database shape (or a DSN) rather than a shape of its own, but it is resolved
by SQLAlchemy here rather than by `config_to_credentials`: that helper lives
next to the dlt factory and importing it imports `dlt`, which ships in the
`orchestrator` extra. A base install would then be unable to resolve a
connection at all -- measured, as `ModuleNotFoundError: No module named
'dlt'`, on the 1.10.19 floor venv. This resource writes to no destination and
needs no pipeline; a URL is all it wants.

**No ambient environment fallback.** A connection is named explicitly or the
resource refuses: inventing an env var name here would put a magic name in
the repo and let a variable left over from another pipeline silently
retarget a read. Secrets travel as `{{ env.X }}` in YAML or `EnvVar` in
Python, resolved per field.

**Nothing here names a database, schema, table, column or level.** The
statement, its parameters, the tag dialect and the connection all arrive as
configuration.
"""
# NOTE: no `from __future__ import annotations`. ConfigurableResource fields
# are pydantic fields resolved from real annotations at class creation, and
# the same string-annotation trap that bites a component's `config:` parameter
# is cheaper to avoid than to debug. Same reason as `read_audit.component`.
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr

from dag_tools.audit.tag import (
    QueryAttribution,
    encode,
    on_behalf_of_principal,
    pipeline_principal,
)
from dag_tools.audit.warehouse import (
    COMMENT,
    COMMENT_CLOSE,
    COMMENT_OPEN,
    QueryPlan,
    TagTransport,
    UntaggedQuery,
    plan_tagged_query,
)


def warehouse_url(
    *,
    dsn: Optional[str] = None,
    drivername: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
) -> str:
    """Resolve the connection to a SQLAlchemy URL, opening nothing.

    Split from any engine for the reason `otel_api_sync._staging_url` records:
    `create_engine` eagerly imports the DBAPI, CI installs no warehouse
    driver, and resolution is the part this module decides and the part worth
    pinning. Tests parse the result with `make_url`; they never build an
    engine here.

    SQLAlchemy does the resolving, deliberately, and not
    `config_to_credentials`: that helper sits next to the dlt factory, so
    calling it imports `dlt` -- an `orchestrator` extra, absent from a base
    install. A resource that reads one statement should not need a pipeline
    library to work out where it is reading from.
    """
    from sqlalchemy.engine import URL, make_url

    if not dsn and not host and not drivername:
        raise ValueError(
            "the warehouse resource has no connection: set connection.dsn, or "
            "connection.drivername with host/username/password/database. "
            "There is no default and no environment fallback -- a magic "
            "variable name would belong to whichever pipeline set it last"
        )

    if dsn:
        # Parsed and re-rendered rather than passed through, so a malformed
        # DSN is refused here -- at load -- instead of at the first read.
        return make_url(dsn).render_as_string(hide_password=False)

    if not drivername:
        raise ValueError(
            "the warehouse resource has connection parts but no "
            "connection.drivername, and this library will not guess which "
            "dialect your warehouse speaks"
        )

    # `URL.create` percent-encodes the user info, so a password containing
    # @ : or / survives the round trip instead of splitting the URL.
    return URL.create(
        drivername=drivername,
        username=username or None,
        password=password or None,
        host=host or None,
        port=port or None,
        database=database or None,
    ).render_as_string(hide_password=False)


def rows_from_result(result: Any) -> List[Dict[str, Any]]:
    """Normalise a DBAPI/SQLAlchemy result to a list of plain mappings.

    `fetch_rows` is a contract with `ReadAuditComponent`, which indexes rows
    by the aliases the statement projected. Returning driver row objects would
    make that contract depend on which driver the deployment installed.
    """
    rows: List[Dict[str, Any]] = []
    for row in result:
        mapping = getattr(row, "_mapping", None)
        if mapping is not None:
            rows.append(dict(mapping))
        elif isinstance(row, dict):
            rows.append(dict(row))
        else:
            keys = getattr(result, "keys", None)
            names = list(keys()) if callable(keys) else []
            rows.append(dict(zip(names, row)) if names else {"value": row})
    return rows


def escape_tag_comment(sql: str, placement: str) -> str:
    """Escape the colons inside a leading tag comment, and nothing else.

    Found by running the example, not by review. The tag is JSON, so it
    contains `"schema_version":1` -- and `text()` reads `:1` as a bind
    parameter, so every comment-placed read failed at submission with
    `A value is required for bind parameter '1'`. The tag is attacker-adjacent
    (it carries an asset key) and JSON will keep putting colons in it, so this
    is not a one-off escape of one field.

    Only the comment this library generated is touched, located by the
    close marker a tag may not contain (`comment_tagged` refuses one that
    does). The operator's own statement is left exactly as written: its
    `:name` parameters are the documented style `build_history_statement`
    already emits, and a literal colon in it is escaped the way `text()`
    documents. Escaping the whole statement would silently unbind those.

    The backslashes never reach the warehouse -- `text()` strips them when it
    compiles -- which `test_the_escape_does_not_reach_the_warehouse` pins.
    """
    if placement != COMMENT or not sql.startswith(COMMENT_OPEN):
        return sql
    end = sql.find(COMMENT_CLOSE)
    if end < 0:
        return sql
    cut = end + len(COMMENT_CLOSE)
    return sql[:cut].replace(":", "\\:") + sql[cut:]


def execute_plan(connection: Any, plan: QueryPlan) -> List[Dict[str, Any]]:
    """Run a plan's steps in order on one connection and return the rows.

    One connection is not an optimisation: with `session` placement the tag is
    connection state, so a read that ran on a different connection from its
    tag would be recorded as untagged. The ordering is the other half -- a tag
    set after the read describes the next query.

    Takes a connection rather than making one, which is what lets the ordering
    be tested without a driver.
    """
    import sqlalchemy as sa

    if not plan.carries_tag:
        raise UntaggedQuery(
            "refusing to execute a plan that does not carry its tag; the read "
            "would be recorded as explained by nothing"
        )

    rows: List[Dict[str, Any]] = []
    for step in plan.steps:
        clause = sa.text(escape_tag_comment(step.sql, plan.placement))
        result = connection.execute(clause, step.parameters)
        if step is plan.query:
            rows = rows_from_result(result)
    return rows


@dataclass(frozen=True)
class BoundWarehouse:
    """A warehouse read already attributed to an asset, a run and a principal.

    A wrapper rather than a copy of the resource: a `ConfigurableResource` is
    initialised in place for a step, and cloning one to carry two extra
    strings would trade a clear object for a pydantic subtlety.
    """

    resource: "TaggedWarehouseResource"
    attribution: QueryAttribution

    def tag(self) -> str:
        return encode(self.attribution)

    def plan(
        self, statement: str, parameters: Optional[Dict[str, Any]] = None
    ) -> QueryPlan:
        return plan_tagged_query(
            self.resource.transport(), self.tag(), statement, parameters
        )

    def fetch_rows(
        self, statement: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """The same seam as the resource's, with the sharper attribution."""
        return self.resource.run_plan(self.plan(statement, parameters))


class TaggedWarehouseResource(ConfigurableResource):
    """Reads the warehouse, and tells the warehouse who the read was for.

    Configure it from YAML through `TaggedWarehouseComponent`; the fields are
    the same either way.
    """

    tag_placement: str
    """`session` (the operator's statement sets a connection tag) or `comment`
    (the tag rides in a leading comment). Required: a wrong placement sends
    every query and attributes none, so it is not a thing to default."""

    tag_statement: Optional[str] = None
    """Session placement only: the operator's own statement, with `:tag`."""

    pipeline_name: Optional[str] = None
    """The service identity's name, rendered as `pipeline:<name>`. Optional,
    because a `passthrough` read is attributed to the caller instead -- and
    because a resource with neither must refuse rather than send."""

    dsn: Optional[str] = None
    drivername: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None

    _run_id: Optional[str] = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Capture the run. The step is not on offer here -- see the module docstring."""
        self._run_id = _run_id_from(context)

    def transport(self) -> TagTransport:
        return TagTransport(
            placement=self.tag_placement, statement=self.tag_statement
        )

    def url(self) -> str:
        return warehouse_url(
            dsn=self.dsn,
            drivername=self.drivername,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

    # -- attribution ---------------------------------------------------------

    def attribution(
        self,
        *,
        asset_key: Optional[str] = None,
        run_id: Optional[str] = None,
        principal: Optional[str] = None,
    ) -> QueryAttribution:
        """Who this read is for. Refuses when nothing can say.

        The principal is the one field with no fallback. A read with an asset
        key but no principal would decode as a tag written by something
        outside this system and be attributed to nobody (`parse_principal`
        denies by default), so it is refused here instead -- at the resource,
        where the operator can see which deployment is misconfigured.
        """
        who = principal or (
            pipeline_principal(self.pipeline_name) if self.pipeline_name else None
        )
        if not who:
            raise UntaggedQuery(
                "this warehouse resource has no principal: set pipeline_name "
                "for a service identity reading as itself, or bind a verified "
                "caller with for_user(). A query with no principal would be "
                "recorded as a read no asset explains"
            )
        return QueryAttribution(
            principal=who,
            asset_key=asset_key,
            run_id=run_id if run_id is not None else self._run_id,
        )

    def for_asset(self, context: Any) -> BoundWarehouse:
        """Bind this step's asset key, so history names the asset directly."""
        return BoundWarehouse(
            resource=self,
            attribution=self.attribution(
                asset_key=asset_key_of(context),
                run_id=getattr(context, "run_id", None) or self._run_id,
            ),
        )

    def for_user(
        self,
        authz_id: str,
        *,
        asset_key: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> BoundWarehouse:
        """Bind a verified on-behalf-of caller -- the `passthrough` attribution.

        The caller is responsible for having verified the id; see
        `dag_tools.audit.tag.on_behalf_of_principal`. This binds who the read
        was *for*; exchanging the caller's token for their own session is
        step 5's work and is not done here.
        """
        return BoundWarehouse(
            resource=self,
            attribution=self.attribution(
                asset_key=asset_key,
                run_id=run_id,
                principal=on_behalf_of_principal(authz_id),
            ),
        )

    # -- reading -------------------------------------------------------------

    def plan(
        self,
        statement: str,
        parameters: Optional[Dict[str, Any]] = None,
        *,
        asset_key: Optional[str] = None,
    ) -> QueryPlan:
        attribution = self.attribution(asset_key=asset_key)
        return plan_tagged_query(
            self.transport(), encode(attribution), statement, parameters
        )

    def fetch_rows(
        self, statement: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """The seam `ReadAuditComponent` calls. Run-scoped attribution.

        An asset that can name itself should prefer
        `for_asset(context).fetch_rows(...)`; this form leaves the asset to be
        resolved from the run, which is exact only when the run selected one.
        """
        return self.run_plan(self.plan(statement, parameters))

    def run_plan(self, plan: QueryPlan) -> List[Dict[str, Any]]:
        """The one chokepoint every statement leaves through."""
        engine = self._engine()
        with engine.begin() as connection:
            return execute_plan(connection, plan)

    def _engine(self) -> Any:
        import sqlalchemy as sa

        return sa.create_engine(self.url())


def asset_key_of(context: Any) -> Optional[str]:
    """This step's asset key as the report spells it, or None when ambiguous.

    A multi-asset step covers several keys, and naming one of them would
    attribute reads to a neighbour -- the mistake step 1's report refuses to
    make on the way back out. So several keys yield None and the read stays
    run-scoped, which the report counts honestly as ambiguous.
    """
    try:
        key = getattr(context, "asset_key", None)
        if key is not None:
            return key.to_user_string()
    except Exception:  # noqa: BLE001 -- a multi-asset step raises here
        pass
    try:
        selected: Sequence[Any] = sorted(
            getattr(context, "selected_asset_keys", None) or [],
            key=lambda item: item.to_user_string(),
        )
    except Exception:  # noqa: BLE001
        return None
    if len(selected) == 1:
        return selected[0].to_user_string()
    return None


def _run_id_from(context: Any) -> Optional[str]:
    """The run id, however this Dagster version offers it.

    `InitResourceContext.run_id` and `.dagster_run` are deprecated in favour
    of `.run` on current Dagster, and `.run` does not exist on the 1.10.19
    compat floor. Try them in that order and never fail resource setup over
    it: a missing run id costs attribution sharpness, not correctness.
    """
    for attribute in ("run", "dagster_run"):
        try:
            run = getattr(context, attribute, None)
        except Exception:  # noqa: BLE001
            continue
        run_id = getattr(run, "run_id", None)
        if run_id:
            return str(run_id)
    try:
        run_id = getattr(context, "run_id", None)
    except Exception:  # noqa: BLE001
        return None
    return str(run_id) if run_id else None
