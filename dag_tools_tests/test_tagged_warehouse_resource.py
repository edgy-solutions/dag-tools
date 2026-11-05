"""`TaggedWarehouseResource` -- the thing that refuses to send an unattributed read.

The seal of step 2 lives here: **a query issued through the resource without a
tag fails.** It is asserted twice over, because "it raised" is not the claim
worth making -- the claim is that nothing was sent. Every seal test therefore
fails if the resource so much as asks for a connection.

Connection *resolution* is asserted as a URL string parsed with `make_url`,
never by building an engine: `create_engine` eagerly imports the DBAPI and CI
installs no warehouse driver, which is the lesson `_staging_url` already paid
for. The one place a connection is real is sqlite, in-process, where it pins
a bug that only running the thing could find.

Every object name in here is synthetic.
"""



import sys
from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

from dag_tools.audit.tag import decode
from dag_tools.audit.warehouse import COMMENT, SESSION, TAG_PARAMETER, UntaggedQuery
from dag_tools.resources.warehouse import (
    BoundWarehouse,
    TaggedWarehouseResource,
    asset_key_of,
    escape_tag_comment,
    execute_plan,
    rows_from_result,
    warehouse_url,
)

SET_TAG = "ALTER SESSION SET QUERY_TAG = :tag"


class _Refuses:
    """A connection that fails the test if anything is executed on it."""

    def execute(self, *args, **kwargs):  # pragma: no cover - the point is not reaching it
        raise AssertionError("a query was sent")


class _Recording:
    """Records what was executed, in order, on one connection."""

    def __init__(self, rows=()):
        self.calls = []
        self._rows = list(rows)

    def execute(self, clause, parameters=None):
        self.calls.append((str(clause), dict(parameters or {})))
        return iter(self._rows)


def _resource(**overrides) -> TaggedWarehouseResource:
    config = dict(
        tag_placement=SESSION,
        tag_statement=SET_TAG,
        pipeline_name="svc_reader",
        drivername="sqlite",
        database=":memory:",
    )
    config.update(overrides)
    return TaggedWarehouseResource(**config)


# --- the seal ---------------------------------------------------------------


def test_a_query_with_no_principal_is_refused_and_nothing_is_sent(monkeypatch):
    """The seal. Both halves: it raises, and it never asked for a connection.

    `pipeline_name` is optional on purpose -- a resource reading on behalf of
    verified callers binds its principal per request -- so this state is
    reachable in a real deployment, which is what keeps the seal from being
    a test of an impossible configuration.
    """
    resource = _resource(pipeline_name=None)
    monkeypatch.setattr(
        TaggedWarehouseResource,
        "_engine",
        lambda self: (_ for _ in ()).throw(AssertionError("a connection was opened")),
    )

    with pytest.raises(UntaggedQuery, match="no principal"):
        resource.fetch_rows("SELECT 1 FROM t")


def test_an_untagged_plan_is_refused_at_the_point_of_execution():
    """The second check, on a plan that did not come from `plan_tagged_query`.

    This is the one that holds if a later edit grows another way to build a
    plan. The connection refuses everything, so a plan that slipped through
    would fail loudly rather than being sent bare.
    """
    from dag_tools.audit.warehouse import PlanStep, QueryPlan

    bare = QueryPlan(
        tag="irrelevant",
        placement=COMMENT,
        steps=[PlanStep(kind="query", sql="SELECT 1")],
    )
    with pytest.raises(UntaggedQuery, match="does not carry its tag"):
        execute_plan(_Refuses(), bare)


def test_the_resource_refuses_before_resolving_a_connection():
    """Order matters: a misconfigured connection must not mask a missing tag.

    If resolution ran first, the operator of a half-configured deployment
    would be told about the connection and fix that, and the untagged reads
    would start flowing.
    """
    resource = TaggedWarehouseResource(tag_placement=COMMENT)
    with pytest.raises(UntaggedQuery):
        resource.plan("SELECT 1")


# --- what the tag says ------------------------------------------------------


def test_the_tag_carries_the_principal_the_asset_and_the_run():
    """The step-2 contract, read back through the decoder step 1 uses.

    Asserted by decoding rather than by string equality: the two halves of
    the contract agreeing is the property, and a decoder test is how a
    serialisation change shows up as a behaviour change.
    """
    resource = _resource()
    resource.setup_for_execution(_init_context("run-123"))

    plan = resource.plan("SELECT 1", asset_key="orders_summary")
    tag = decode(plan.prelude[0].parameters[TAG_PARAMETER])

    assert tag.principal == "pipeline:svc_reader"
    assert tag.asset_key == "orders_summary"
    assert tag.run_id == "run-123"


def test_for_asset_sharpens_a_run_scoped_tag_to_the_asset_key():
    """Why `for_asset` exists at all.

    Measured on dagster 1.13.1: the `InitResourceContext` a resource is built
    with exposes the run and nothing about the step -- no asset key, no op
    def, no step context. So an unbound read is attributed to the run, which
    the report can resolve to an asset only when the run selected exactly
    one; binding the key is what makes it exact.
    """
    resource = _resource()
    resource.setup_for_execution(_init_context("run-123"))

    unbound = decode(resource.plan("SELECT 1").prelude[0].parameters[TAG_PARAMETER])
    assert unbound.asset_key is None
    assert unbound.run_id == "run-123"

    bound = resource.for_asset(_asset_context("orders_summary", "run-123"))
    assert decode(bound.tag()).asset_key == "orders_summary"


def test_a_multi_asset_step_stays_run_scoped_rather_than_naming_a_neighbour():
    """Several keys in one step cannot be narrowed to one honestly.

    Naming one of them would attribute a read to a neighbouring asset, which
    is the mistake the report refuses to make on the way back out. Run-scoped
    is counted as ambiguous, which is true.
    """
    assert asset_key_of(_asset_context(["a", "b"], "run-1")) is None
    assert asset_key_of(_asset_context("a", "run-1")) == "a"
    assert asset_key_of(object()) is None


def test_for_user_attributes_the_read_to_the_verified_caller():
    """`passthrough`: the read is for a person, not for the service identity."""
    bound = _resource().for_user("authz-9", asset_key="orders_summary")

    assert isinstance(bound, BoundWarehouse)
    tag = decode(bound.tag())
    assert tag.principal == "user:authz-9"
    assert tag.asset_key == "orders_summary"


def test_the_run_id_is_read_from_the_run_in_preference_to_the_deprecated_field():
    """`run_id` and `dagster_run` are deprecated in favour of `run`.

    And `run` does not exist on the 1.10.19 compat floor, so all three are
    tried in order. A missing run id costs attribution sharpness, never
    correctness, so setup must not fail over it.
    """
    resource = _resource()
    resource.setup_for_execution(_init_context("from-run", legacy="from-legacy"))
    assert decode(resource.plan("SELECT 1").prelude[0].parameters[TAG_PARAMETER]).run_id == (
        "from-run"
    )

    floor = _resource()
    floor.setup_for_execution(_LegacyContext("from-legacy"))
    assert decode(floor.plan("SELECT 1").prelude[0].parameters[TAG_PARAMETER]).run_id == (
        "from-legacy"
    )

    blank = _resource()
    blank.setup_for_execution(object())
    assert decode(blank.plan("SELECT 1").prelude[0].parameters[TAG_PARAMETER]).run_id is None


# --- execution --------------------------------------------------------------


def test_the_tag_and_the_query_run_in_order_on_one_connection():
    """With session placement the tag is connection state.

    A read that ran on a different connection from its tag would be recorded
    as untagged, and a tag that ran after its read would describe the next
    query. Both are invisible in the result and visible here.
    """
    connection = _Recording()
    plan = _resource().plan("SELECT 1 FROM t", {"x": 2})

    execute_plan(connection, plan)

    assert [sql for sql, _ in connection.calls] == [SET_TAG, "SELECT 1 FROM t"]
    assert connection.calls[0][1] == {TAG_PARAMETER: plan.tag}
    assert connection.calls[1][1] == {"x": 2}


def test_a_comment_placed_tag_survives_sqlalchemy_text():
    """The bug the example found, and the reason `escape_tag_comment` exists.

    The tag is JSON, so it contains `"schema_version":1`, and `text()` reads
    `:1` as a bind parameter: every comment-placed read failed at submission
    with `A value is required for bind parameter '1'`. Unit tests that stub
    the transport could not see it -- only running the thing could.
    """
    import sqlalchemy as sa

    resource = _resource(tag_placement=COMMENT, tag_statement=None)
    plan = resource.plan("SELECT :x AS x", {"x": 7}, asset_key="orders_summary")

    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        rows = execute_plan(connection, plan)

    assert rows == [{"x": 7}]


def test_the_escape_does_not_reach_the_warehouse():
    """The backslashes are `text()`'s own escape and must not be sent.

    If they were, the tag in query history would not be the tag the report
    decodes -- the drift this whole contract is built to prevent.
    """
    import sqlalchemy as sa

    tag = '{"principal":"pipeline:p","schema_version":1}'
    escaped = escape_tag_comment(f"/* {tag} */ SELECT 1", COMMENT)
    assert "\\:" in escaped

    compiled = str(sa.text(escaped).compile(dialect=sa.dialects.sqlite.dialect()))
    assert "\\" not in compiled
    assert tag in compiled


def test_the_escape_leaves_the_operators_own_statement_alone():
    """Escaping the whole statement would unbind every `:name` in it.

    The read the audit builds is named-parameter style, so a blanket escape
    would turn `:since` into a literal and the window would stop filtering.
    """
    sql = "/* {\"a\":1} */ SELECT * FROM h WHERE started_at >= :since"
    escaped = escape_tag_comment(sql, COMMENT)

    assert ":since" in escaped
    assert '\\:1' in escaped


def test_the_escape_is_not_applied_to_session_placement():
    """There is no comment to escape, and the statement is the operator's."""
    assert escape_tag_comment(SET_TAG, SESSION) == SET_TAG


def test_rows_come_back_as_plain_mappings():
    """`fetch_rows` is a contract with the read audit, which indexes by alias.

    Returning driver row objects would make that contract depend on which
    driver the deployment installed.
    """

    class _Row:
        _mapping = {"query_id": "q-1", "query_tag": None}

    assert rows_from_result([_Row()]) == [{"query_id": "q-1", "query_tag": None}]
    assert rows_from_result([{"a": 1}]) == [{"a": 1}]


def test_fetch_rows_is_the_seam_the_read_audit_calls():
    """Signature parity with what `ReadAuditComponent` expects.

    The component looks the method up by name and calls it with the statement
    and the bound parameters; a rename here breaks step 1 silently, since the
    component raises only when the attribute is missing entirely.
    """
    import inspect

    signature = inspect.signature(TaggedWarehouseResource.fetch_rows)
    assert list(signature.parameters) == ["self", "statement", "parameters"]
    assert list(inspect.signature(BoundWarehouse.fetch_rows).parameters) == (
        ["self", "statement", "parameters"]
    )


# --- connection resolution --------------------------------------------------


def test_the_parts_resolve_the_way_every_other_component_takes_them():
    """Same shape as `dest_config` elsewhere, through the same resolver."""
    url = make_url(
        warehouse_url(
            drivername="postgresql",
            host="h.example",
            port=5433,
            username="u",
            password="p@ss/word",
            database="d",
        )
    )

    assert url.drivername == "postgresql"
    assert (url.host, url.port, url.database) == ("h.example", 5433, "d")
    # Percent-encoded on the way in, decoded by make_url: a password with @
    # or / survives the round trip rather than splitting the URL.
    assert url.password == "p@ss/word"


def test_resolving_a_connection_does_not_import_dlt():
    """A base install has no `dlt`, and this resource must still work.

    `dlt` ships in the `orchestrator` extra, so routing resolution through
    `config_to_credentials` -- which sits beside the dlt asset factory --
    made every connection unresolvable wherever that extra is absent. It
    surfaced as `ModuleNotFoundError: No module named 'dlt'` on the 1.10.19
    floor venv, nine failures deep into a suite that was green on the
    development machine, where dlt happens to be installed.

    Asserted in a subprocess because this interpreter has almost certainly
    imported dlt already, for some other component.
    """
    import subprocess

    program = (
        "import sys;"
        "from dag_tools.resources.warehouse import warehouse_url;"
        "warehouse_url(drivername='postgresql', host='h', database='d');"
        "assert 'dlt' not in sys.modules, sorted(m for m in sys.modules if 'dlt' in m);"
        "print('clean')"
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1]),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "clean" in result.stdout


def test_a_dsn_is_taken_as_given():
    url = make_url(warehouse_url(dsn="postgresql://u:p@h:5432/d"))
    assert (url.host, url.database) == ("h", "d")


def test_a_connection_naming_nothing_is_refused_with_no_env_fallback(monkeypatch):
    """No magic variable name, by design.

    An ambient DSN belongs to whichever pipeline set it last, and a read
    silently retargeted by a leftover variable is an audit that describes the
    wrong deployment -- the same finding as `_staging_url`'s precedence bug.
    """
    for name in (
        "DESTINATION__CREDENTIALS",
        "DESTINATION__POSTGRES__CREDENTIALS",
        "WAREHOUSE_DSN",
        "DATABASE_URL",
    ):
        monkeypatch.setenv(name, "postgresql://leftover:x@elsewhere:5432/other")

    with pytest.raises(ValueError, match="no connection"):
        warehouse_url()


# --- stand-ins --------------------------------------------------------------


class _Run:
    def __init__(self, run_id):
        self.run_id = run_id


class _InitContext:
    def __init__(self, run_id, legacy=None):
        self.run = _Run(run_id)
        self.dagster_run = _Run(legacy) if legacy else None
        self.run_id = legacy


class _LegacyContext:
    """A floor-era context: `dagster_run`, no `run`."""

    def __init__(self, run_id):
        self.dagster_run = _Run(run_id)


def _init_context(run_id, legacy=None):
    return _InitContext(run_id, legacy)


class _AssetKey:
    def __init__(self, value):
        self._value = value

    def to_user_string(self):
        return self._value


class _AssetContext:
    """Stands in for an execution context, in the two shapes that matter."""

    def __init__(self, keys, run_id):
        self.run_id = run_id
        if isinstance(keys, list):
            self.selected_asset_keys = [_AssetKey(key) for key in keys]
        else:
            self.asset_key = _AssetKey(keys)
            self.selected_asset_keys = [_AssetKey(keys)]


def _asset_context(keys, run_id):
    return _AssetContext(keys, run_id)


def test_the_fixtures_match_what_dagster_offers():
    """A stand-in that drifts from the real thing proves nothing.

    Pinned against the real classes: the execution context must expose
    `asset_key` and `selected_asset_keys`, and the init context must not
    expose an asset key at all -- which is the measurement `for_asset` exists
    because of.
    """
    import dagster as dg

    assert hasattr(dg.AssetExecutionContext, "asset_key")
    assert hasattr(dg.AssetExecutionContext, "selected_asset_keys")

    assert not hasattr(dg.InitResourceContext, "asset_key")
    assert not hasattr(dg.InitResourceContext, "op_def")
    assert hasattr(dg.InitResourceContext, "dagster_run")
    assert hasattr(dg.InitResourceContext, "run_id")
