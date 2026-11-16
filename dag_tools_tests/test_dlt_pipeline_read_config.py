"""``table_config`` and ``row_filters`` on ``DltPipelineComponent`` (PR B3a).

Everything runs against sqlite (source) and duckdb (destination): CI installs
no warehouse driver. All table and column names here are invented.
"""
import inspect
import sqlite3
import uuid
from pathlib import Path

import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")
pytest.importorskip("duckdb")

import dagster as dg
import dlt
import duckdb

import dag_tools.components.dlt_pipeline.component as comp_module
from dag_tools.asset_wrappers import row_filters as rf
from dag_tools.asset_wrappers.dlt_assets_factory import (
    DltAssetGroupConfig,
    TableSpec,
    build_table_hints,
)
from dag_tools.asset_wrappers.dlt_assets_parsing import build_dlt_runnables
from dag_tools.components.dlt_pipeline import row_count_check
from dag_tools_tests._dlt_component_helpers import load_component

BOTH = ("widgets", "gadgets")


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _pipe(table="widgets", **kw):
    return {"name": "alpha", "dest_schema": "raw", "sources": [table], **kw}


class _Spy:
    """Wraps ``create_dlt_assets`` so a test can see what the component passed."""

    def __init__(self, monkeypatch):
        self.calls = []
        real = comp_module.create_dlt_assets

        def spy(**kw):
            self.calls.append(kw)
            return real(**kw)

        monkeypatch.setattr(comp_module, "create_dlt_assets", spy)

    @property
    def last(self):
        return self.calls[-1]


def _load(tmp_path, monkeypatch, pipelines, tables=BOTH):
    spy = _Spy(monkeypatch)
    defs = load_component(tmp_path, monkeypatch, pipelines=pipelines, tables=tables)
    return defs, spy


def _refusal(fn) -> str:
    """The text of a load refusal. Dagster wraps what build_defs raises, so
    the message is read down the whole cause chain."""
    with pytest.raises(Exception) as err:
        fn()
    chain, exc = [], err.value
    while exc is not None and exc not in chain:
        chain.append(exc)
        exc = exc.__cause__ or exc.__context__
    return " | ".join(str(e) for e in chain)


def _seed(path: Path, ddl: str, rows, table="items"):
    conn = sqlite3.connect(path)
    conn.execute(ddl)
    if rows:
        n = len(rows[0])
        conn.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * n)})", rows)
    conn.commit()
    conn.close()


def _execute(tmp_path, call, table, db):
    """Run what the component handed to create_dlt_assets, for real."""
    source_config = {
        "type": "sql_database", "drivername": "sqlite",
        "credentials": f"sqlite:///{db.as_posix()}", "database": "main", "schema": "main",
    }
    dest_config = {
        "type": "duckdb", "drivername": "duckdb", "credentials": str(tmp_path / "x.duckdb"),
        "database": "warehouse", "schema": "raw",
    }
    wh = tmp_path / f"out_{uuid.uuid4().hex[:6]}.duckdb"
    runnable = build_dlt_runnables(
        [table], source_config, dest_config, call["config"],
        query_callback=call["query_callback"],
        destination_override=dlt.destinations.duckdb(str(wh)),
    )[0]
    runnable.run()
    con = duckdb.connect(str(wh), read_only=True)
    try:
        return sorted(r[0] for r in con.execute(f"SELECT id FROM raw.{table}").fetchall())
    finally:
        con.close()


def _run_filtered(tmp_path, monkeypatch, rows, filters, table="items",
                  ddl="CREATE TABLE items (id INTEGER PRIMARY KEY, tag TEXT, qty INTEGER)"):
    """Component -> captured config and callback -> real run -> loaded ids."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    spy = _Spy(monkeypatch)
    load_component(
        tmp_path, monkeypatch,
        pipelines={"p": _pipe(table, row_filters={table: filters})}, tables=("unused",),
    )
    db = tmp_path / "source.db"
    _seed(db, ddl, rows, table)
    return _execute(tmp_path, spy.last, table, db)


# --------------------------------------------------------------------------
# table_config
# --------------------------------------------------------------------------
def test_table_config_reaches_the_hints_the_source_is_built_with(tmp_path, monkeypatch):
    _, spy = _load(tmp_path, monkeypatch, {"p": _pipe(
        table_config={"widgets": {"primary_key": "id", "cursor": "id", "initial_value": 0}},
    )})
    hint = spy.last["config"].hints["widgets"]
    assert hint["primary_key"] == "id"
    assert hint["incremental"] == {"cursor_path": "id", "initial_value": 0}


def test_an_unpopped_table_config_would_be_silently_ignored():
    assert "table_config" not in DltAssetGroupConfig.model_fields


def test_an_explicit_hint_still_wins_over_table_config(tmp_path, monkeypatch):
    _, spy = _load(tmp_path, monkeypatch, {"p": _pipe(
        table_config={"widgets": {"primary_key": "id", "cursor": "id"}},
        hints={"widgets": {"primary_key": "name"}},
    )})
    hint = spy.last["config"].hints["widgets"]
    assert hint["primary_key"] == "name"
    assert hint["incremental"]["cursor_path"] == "id"


def test_a_table_config_naming_a_table_outside_sources_fails_the_load(tmp_path, monkeypatch):
    text = _refusal(lambda: _load(tmp_path, monkeypatch, {"p": _pipe(
        table_config={"gadgets": {"primary_key": "id"}},
    )}))
    assert "table_config names tables that are not in sources: ['gadgets']" in text


def test_a_table_with_no_entry_loads_exactly_as_before(tmp_path, monkeypatch):
    """The section 6 canary: a pipeline with neither field gets no hints, no
    callback and no checks."""
    defs, spy = _load(tmp_path, monkeypatch, {"p": _pipe()})
    assert spy.last["config"].hints == {}
    assert spy.last["query_callback"] is None
    assert not list(defs.asset_checks or [])


def test_a_table_with_no_entry_beside_one_with_an_entry_is_untouched(tmp_path, monkeypatch):
    _, spy = _load(tmp_path, monkeypatch, {"p": {
        "name": "alpha", "dest_schema": "raw", "sources": ["widgets", "gadgets"],
        "table_config": {"widgets": {"primary_key": "id"}},
    }})
    assert set(spy.last["config"].hints) == {"widgets"}


def test_both_names_are_still_importable_from_the_restate_component():
    from dag_tools.components.restate_dlt_sync import component as rc
    from dag_tools.components.restate_dlt_sync import config as rcfg
    assert rc.build_table_hints is build_table_hints
    assert rc.TableSpec is TableSpec
    assert rcfg.TableSpec is TableSpec


def test_the_dev_loader_still_imports_them(tmp_path):
    from dag_tools.dlt_dev.loader import build_runnables_from_defs

    db = tmp_path / "s.db"
    _seed(db, "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)", [(1, "a")], "widgets")
    defs = tmp_path / "defs.yaml"
    defs.write_text(
        "type: x\nattributes:\n  source_config:\n    type: sql_database\n"
        "    drivername: sqlite\n"
        f"    credentials: sqlite:///{db.as_posix()}\n"
        "    database: main\n    schema: main\n"
        "  dest_config:\n    type: duckdb\n    drivername: duckdb\n"
        f"    credentials: {(tmp_path / 'w.duckdb').as_posix()}\n"
        "    database: warehouse\n    schema: raw\n"
        "  pipelines:\n    p:\n      dest_schema: raw\n      sources: [widgets]\n"
        "      table_config:\n        widgets: {primary_key: id}\n",
        encoding="utf-8",
    )
    runnable = build_runnables_from_defs(
        str(defs), destination=f"duckdb:///{(tmp_path / 'd.duckdb').as_posix()}",
    )[0]
    assert "widgets" in runnable.source.resources


# --------------------------------------------------------------------------
# row_filters: the callback
# --------------------------------------------------------------------------
def _cb(filters, pipeline="p"):
    parsed = rf.parse_row_filters(pipeline, filters, list(filters))
    return rf.build_query_callback(pipeline, parsed)


def _items_table():
    import sqlalchemy as sa
    meta = sa.MetaData()
    return sa.Table("items", meta, sa.Column("id", sa.Integer), sa.Column("tag", sa.String))


def test_the_callback_takes_four_parameters():
    import sqlalchemy as sa
    cb = _cb({"items": [{"column": "id", "op": "not_null"}]})
    params = list(inspect.signature(cb).parameters.values())
    assert [p.name for p in params] == ["query", "table", "incremental", "engine"]
    assert all(p.kind is p.POSITIONAL_OR_KEYWORD and p.default is p.empty for p in params)

    # dlt's own four-argument call, not a signature read: a *args stub would
    # pass the inspection above.
    table = _items_table()
    out = cb(sa.select(table), table, None, None)
    assert "IS NOT NULL" in str(out)


def test_dlts_four_argument_call_is_accepted_without_a_typeerror():
    """The same call dlt makes first; the two-argument fallback is only for a
    TypeError, so a callback that raised one would be retried and hidden."""
    import sqlalchemy as sa
    cb = _cb({"items": [{"column": "id", "op": "not_null"}]})
    table = _items_table()
    cb(sa.select(table), table, None, None)  # must not raise
    with pytest.raises(TypeError):
        cb(sa.select(table), table)  # two arguments is the wrong shape


def test_a_filter_restricts_the_rows_the_pipeline_loads(tmp_path, monkeypatch):
    ids = _run_filtered(
        tmp_path, monkeypatch,
        [(1, "a", 1), (2, "b", 1), (3, "a", 2)],
        [{"column": "tag", "op": "=", "value": "a"}],
    )
    assert ids == [1, 3]


ROWS = [(1, "a", 10), (2, "b", None), (3, None, 30), (4, "c", 10)]


@pytest.mark.parametrize("flt, expected", [
    ({"column": "tag", "op": "=", "value": "a"}, [1]),
    ({"column": "tag", "op": "!=", "value": "a"}, [2, 4]),
    ({"column": "tag", "op": "in", "values": ["a", "c"]}, [1, 4]),
    ({"column": "tag", "op": "not_in", "values": ["a", "c"]}, [2]),
    ({"column": "tag", "op": "is_null"}, [3]),
    ({"column": "tag", "op": "not_null"}, [1, 2, 4]),
])
def test_every_operator_renders(tmp_path, monkeypatch, flt, expected):
    assert _run_filtered(tmp_path, monkeypatch, ROWS, [flt]) == expected


def test_not_equal_and_not_in_exclude_nulls(tmp_path, monkeypatch):
    # id 3 has a NULL tag. Bare SQL != drops it anyway and Python's would keep
    # it; asserted by rows so the guard cannot be removed unseen.
    ne = _run_filtered(tmp_path / "a", monkeypatch, ROWS,
                       [{"column": "tag", "op": "!=", "value": "zz"}])
    nin = _run_filtered(tmp_path / "b", monkeypatch, ROWS,
                        [{"column": "tag", "op": "not_in", "values": ["zz"]}])
    assert ne == nin == [1, 2, 4]


def test_the_null_guard_is_in_the_rendered_predicate():
    import sqlalchemy as sa
    t = _items_table()
    for flt in ({"column": "tag", "op": "!=", "value": "a"},
                {"column": "tag", "op": "not_in", "values": ["a"]}):
        sql = str(rf.predicate(t.c.tag, rf.RowFilter.model_validate(flt)))
        assert "IS NOT NULL" in sql
    assert sa is not None


def test_values_are_bound_and_no_literal_is_formatted_into_sql():
    import sqlalchemy as sa
    from sqlalchemy.dialects import sqlite
    table = _items_table()
    marker = "needle-9137"
    cb = _cb({"items": [
        {"column": "tag", "op": "=", "value": marker},
        {"column": "tag", "op": "in", "values": [marker + "x", marker + "y"]},
    ]})
    stmt = cb(sa.select(table), table, None, None).compile(dialect=sqlite.dialect())
    assert marker not in str(stmt)
    bound = {v for v in stmt.params.values() if isinstance(v, str)}
    assert marker in bound
    assert marker + "x" in {x for v in stmt.params.values() if isinstance(v, (list, tuple)) for x in v} | bound


def test_a_literal_carrying_a_quote_round_trips(tmp_path, monkeypatch):
    rows = [(1, "o'brien", 1), (2, 'say "hi"', 1),
            (3, "x'; DROP TABLE items; --", 1), (4, "plain", 1)]
    assert _run_filtered(tmp_path / "a", monkeypatch, rows,
                         [{"column": "tag", "op": "=", "value": "o'brien"}]) == [1]
    assert _run_filtered(tmp_path / "b", monkeypatch, rows,
                         [{"column": "tag", "op": "=", "value": "x'; DROP TABLE items; --"}]) == [3]


def test_an_unknown_column_fails_the_run_naming_the_table_and_the_column(tmp_path, monkeypatch):
    with pytest.raises(Exception) as err:
        _run_filtered(tmp_path, monkeypatch, [(1, "a", 1)],
                      [{"column": "nope", "op": "not_null"}])
    chain, exc = [], err.value
    while exc is not None and exc not in chain:
        chain.append(exc)
        exc = exc.__cause__ or exc.__context__
    text = " | ".join(str(e) for e in chain)
    assert "pipeline 'p'" in text and "'items'" in text and "'nope'" in text
    for existing in ("id", "tag", "qty"):
        assert existing in text


def test_an_unknown_column_does_not_fail_the_load(tmp_path, monkeypatch):
    _load(tmp_path, monkeypatch, {"p": _pipe(
        row_filters={"widgets": [{"column": "nope", "op": "not_null"}]})})


def test_a_filter_for_one_table_does_not_reach_another(tmp_path, monkeypatch):
    tmp_path.mkdir(parents=True, exist_ok=True)
    spy = _Spy(monkeypatch)
    load_component(tmp_path, monkeypatch, tables=("unused",), pipelines={"p": {
        "name": "alpha", "dest_schema": "raw", "sources": ["red", "blue"],
        "row_filters": {"red": [{"column": "tag", "op": "=", "value": "keep"}]},
    }})
    db = tmp_path / "source.db"
    for t in ("red", "blue"):
        _seed(db, f"CREATE TABLE {t} (id INTEGER PRIMARY KEY, tag TEXT)",
              [(1, "keep"), (2, "drop")], t)
    assert _execute(tmp_path, spy.last, "red", db) == [1]
    assert _execute(tmp_path, spy.last, "blue", db) == [1, 2]


def test_row_filters_naming_a_table_outside_sources_fails_the_load(tmp_path, monkeypatch):
    text = _refusal(lambda: _load(tmp_path, monkeypatch, {"p": _pipe(
        row_filters={"gadgets": [{"column": "id", "op": "not_null"}]})}))
    assert "row_filters names tables that are not in sources: ['gadgets']" in text


def test_a_malformed_filter_fails_the_load(tmp_path, monkeypatch):
    text = _refusal(lambda: _load(tmp_path, monkeypatch, {"p": _pipe(
        row_filters={"widgets": [{"column": "id", "op": "like", "value": "a"}]})}))
    assert "like" in text


def test_an_unpopped_row_filters_would_be_silently_ignored():
    assert "row_filters" not in DltAssetGroupConfig.model_fields


def test_a_pipeline_with_no_row_filters_passes_no_callback(tmp_path, monkeypatch):
    _, spy = _load(tmp_path, monkeypatch, {"p": _pipe()})
    assert spy.last["query_callback"] is None
    assert rf.build_query_callback("p", {}) is None


def test_row_filters_on_a_kind_that_builds_no_query_fail_the_load(tmp_path, monkeypatch):
    text = _refusal(lambda: load_component(
        tmp_path, monkeypatch, source_type="systemlink_test_monitor", tables=("widgets",),
        pipelines={"p": _pipe(row_filters={"widgets": [{"column": "id", "op": "not_null"}]})},
    ))
    assert "row_filters is set" in text


def test_row_filters_on_a_filesystem_source_fail_the_load(tmp_path, monkeypatch):
    # The legacy kind's filesystem branch builds no query either; refusing
    # only the non-legacy kinds would leave it ignoring the filters silently.
    text = _refusal(lambda: load_component(
        tmp_path, monkeypatch, tables=("widgets",),
        extra_attrs={"source_config": {
            "type": "sql_database", "drivername": "filesystem",
            "database": "main", "schema": "main",
            "destination": {"bucket_url": (tmp_path / "bkt").as_posix()},
        }},
        pipelines={"p": _pipe(row_filters={"widgets": [{"column": "id", "op": "not_null"}]})},
    ))
    assert "row_filters is set" in text


def test_the_filesystem_branch_refuses_a_callback_rather_than_dropping_it():
    from dag_tools.asset_wrappers.dlt_assets_parsing import build_dlt_source

    class _Creds:
        drivername = "filesystem"

    with pytest.raises(ValueError, match="filesystem"):
        build_dlt_source(
            {"creds": _Creds(), "tables": ["t"], "source_config": {}},
            "db", "sch", lambda *a: a[0], DltAssetGroupConfig(name="x"), "duckdb",
        )


def test_the_dev_loader_applies_row_filters_like_the_deployed_asset(tmp_path):
    """The dev loop and the deployed asset run the same function (AGENTS 4p)."""
    from dag_tools.dlt_dev.loader import build_runnables_from_defs

    db = tmp_path / "s.db"
    _seed(db, "CREATE TABLE widgets (id INTEGER PRIMARY KEY, tag TEXT)",
          [(1, "a"), (2, "b"), (3, "a")], "widgets")
    wh = tmp_path / "d.duckdb"
    defs = tmp_path / "defs.yaml"
    defs.write_text(
        "\n".join([
            "type: x",
            "attributes:",
            "  source_config:",
            "    type: sql_database",
            "    drivername: sqlite",
            f"    credentials: sqlite:///{db.as_posix()}",
            "    database: main",
            "    schema: main",
            "  dest_config:",
            "    type: duckdb",
            "    drivername: duckdb",
            f"    credentials: {(tmp_path / 'w.duckdb').as_posix()}",
            "    database: warehouse",
            "    schema: raw",
            "  pipelines:",
            "    p:",
            "      dest_schema: raw",
            "      sources: [widgets]",
            "      row_filters:",
            "        widgets:",
            "          - {column: tag, op: '=', value: a}",
            "",
        ]),
        encoding="utf-8",
    )
    runnable = build_runnables_from_defs(str(defs), destination=f"duckdb:///{wh.as_posix()}")[0]
    runnable.run()
    con = duckdb.connect(str(wh), read_only=True)
    try:
        ids = sorted(r[0] for r in con.execute("SELECT id FROM raw.widgets").fetchall())
    finally:
        con.close()
    assert ids == [1, 3]


# --------------------------------------------------------------------------
# the row-count check
# --------------------------------------------------------------------------
def _checked(tmp_path, monkeypatch):
    defs, _ = _load(tmp_path, monkeypatch, {"p": {
        "name": "alpha", "dest_schema": "raw", "sources": ["widgets", "gadgets"],
        "table_config": {
            "widgets": {"primary_key": "id"},
            "gadgets": {"primary_key": "id", "cursor": "id"},
        },
    }})
    return defs


def _check_tails(defs):
    return sorted(s.asset_key.path[-1] for c in defs.asset_checks for s in c.check_specs)


def test_a_cursorless_table_gets_a_row_count_check(tmp_path, monkeypatch):
    assert "widgets" in _check_tails(_checked(tmp_path, monkeypatch))


def test_a_table_with_a_cursor_gets_none(tmp_path, monkeypatch):
    assert "gadgets" not in _check_tails(_checked(tmp_path, monkeypatch))


def test_a_table_with_no_entry_at_all_gets_no_check_either(tmp_path, monkeypatch):
    """A source table nobody wrote a ``table_config`` entry for is cursorless too,
    and is deliberately left uncovered -- so this pins the scoping rather than
    leaving a later reader to "fix" it.

    Covering it would attach new asset checks to every pipeline that upgrades
    without having asked for any, which changes the shape of a deployment's asset
    graph as a side effect of a library bump (AGENTS.md section 6). The check
    follows the operator's own declaration instead: a table someone configured is
    a table someone decided about. Widening this is deployment-visible and belongs
    in a release that says so.
    """
    defs, _ = _load(tmp_path, monkeypatch, {"p": {
        "name": "alpha", "dest_schema": "raw",
        "sources": ["widgets", "gadgets", "sprockets"],
        "table_config": {
            "widgets": {"primary_key": "id"},
            "gadgets": {"primary_key": "id", "cursor": "id"},
        },
    }})
    tails = sorted(s.asset_key.path[-1] for c in defs.asset_checks for s in c.check_specs)
    assert "sprockets" not in tails
    # Anti-vacuity: the configured cursorless table IS covered in this same load,
    # so the assertion above cannot pass merely because no check was built at all.
    assert "widgets" in tails


def test_a_configured_primary_key_does_not_reach_the_hints_the_force_rule_reads():
    """Canary on dlt. ``table_config``'s correctness depends on a dlt internal.

    ``dlt_assets_parsing`` forces a cursorless, keyless ``append`` resource to
    ``replace`` so it does not re-append its whole contents every run, and it
    decides "keyless" from ``resource.validator.resource_hints``. A key supplied
    through ``table_config`` is applied with ``apply_hints``, which does NOT write
    there -- so such a table keeps being forced to ``replace``, which is the
    behaviour measured against duckdb (two runs, two rows, not four).

    Nothing states that dependency, and the failure if dlt ever changes it is
    silent: the table would quietly return to ``append`` and start re-appending in
    full on every run, with no error and no failing test. It reads as a working
    pipeline until somebody counts the rows. So the day dlt surfaces an applied
    key on the validator, this says so.
    """
    import dlt as _dlt

    @_dlt.resource(name="widgets", write_disposition="append")
    def _rows():
        yield {"id": 1}

    _rows.apply_hints(primary_key="id")

    # The exact expression the force rule evaluates.
    resource_hints = _rows.validator.resource_hints if _rows.validator else {}
    assert not bool(resource_hints.get("primary_key")), (
        "dlt now exposes an apply_hints primary key on the validator. The "
        "force-to-replace rule in dlt_assets_parsing reads that field, so "
        "cursorless table_config tables have just silently become append and "
        "will re-append in full on every run."
    )


def test_the_check_is_not_blocking(tmp_path, monkeypatch):
    defs = _checked(tmp_path, monkeypatch)
    assert defs.asset_checks
    assert all(not s.blocking for c in defs.asset_checks for s in c.check_specs)


class _Event:
    def __init__(self, metadata):
        self.asset_materialization = dg.AssetMaterialization(asset_key="k", metadata=metadata)


def test_an_absent_rows_loaded_fails_the_check():
    assert not row_count_check.evaluate(_Event({"other": 1})).passed
    assert not row_count_check.evaluate(None).passed


def test_a_present_rows_loaded_passes_the_check():
    assert row_count_check.evaluate(_Event({"rows_loaded": dg.MetadataValue.int(3)})).passed


def test_the_failure_message_states_no_count_rather_than_zero():
    text = row_count_check.evaluate(_Event({})).description
    assert "no row count was recorded" in text
    assert "0" not in text and "zero" not in text.lower()


def _materialize(tmp_path, monkeypatch, empty):
    tmp_path.mkdir(parents=True, exist_ok=True)
    # get_destination knows only the warehouse drivers; a local duckdb file
    # stands in for the destination, as the systemlink suite does.
    from dag_tools.asset_wrappers import dlt_assets_parsing as parsing
    wh = str(tmp_path / "mat.duckdb")
    monkeypatch.setattr(parsing, "get_destination", lambda *a, **k: dlt.destinations.duckdb(wh))
    defs = load_component(tmp_path, monkeypatch, tables=("widgets",), pipelines={"p": {
        "name": "alpha", "dest_schema": "raw", "sources": ["widgets"],
        "table_config": {"widgets": {"primary_key": "id"}},
    }})
    if empty:
        conn = sqlite3.connect(tmp_path / "source.db")
        conn.execute("DELETE FROM widgets")
        conn.commit()
        conn.close()
    assets = [a for a in defs.assets if isinstance(a, dg.AssetsDefinition)]
    result = dg.materialize(assets, resources=defs.resources)
    assert result.success
    events = result.get_asset_materialization_events()
    assert events
    return [ev.materialization.metadata for ev in events]


def test_control_a_zero_row_load_omits_rows_loaded_entirely(tmp_path, monkeypatch):
    """Canary on dagster-dlt: a version that starts emitting rows_loaded: 0
    changes what absence means, which is a finding, not a silent change."""
    for metadata in _materialize(tmp_path, monkeypatch, empty=True):
        assert "rows_loaded" not in metadata


def test_control_a_nonempty_load_carries_rows_loaded(tmp_path, monkeypatch):
    assert any("rows_loaded" in m for m in _materialize(tmp_path, monkeypatch, empty=False))


def test_the_check_opens_no_connection(tmp_path, monkeypatch):
    """The check body touches only context.instance's materialization lookup."""
    defs = _checked(tmp_path, monkeypatch)
    check = next(c for c in defs.asset_checks
                 if any(s.asset_key.path[-1] == "widgets" for s in c.check_specs))

    class _Instance:
        def get_latest_materialization_event(self, key):
            return _Event({"rows_loaded": dg.MetadataValue.int(2)})

    class _Context:
        instance = _Instance()

        def __getattr__(self, name):
            raise AssertionError(f"the check touched context.{name}")

    result = check.node_def.compute_fn.decorated_fn(_Context())
    assert result.passed
    source = inspect.getsource(row_count_check)
    for forbidden in ("create_engine", "sqlalchemy", "get_destination", "sql_client"):
        assert forbidden not in source


def test_a_pipeline_with_no_table_config_contributes_no_checks(tmp_path, monkeypatch):
    defs, _ = _load(tmp_path, monkeypatch, {"p": _pipe()})
    assert not list(defs.asset_checks or [])
