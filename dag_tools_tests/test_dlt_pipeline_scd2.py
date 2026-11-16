"""``history: scd2`` on a ``table_config`` entry.

The behavioural tests load twice against duckdb and read the ROWS back: which
records are open is what must not change, whereas the SQL is dlt's and may.
dlt does not refuse ``primary_key`` + scd2 anywhere, so the failure mode of
emitting the wrong key is a wrong row, never an exception.

Gate cleared by measurement, not assumed (dlt 1.30.0): scd2 is core in
``dlt/common/schema/typing.py`` (``TLoaderMergeStrategy``); duckdb's strategy
selector in ``impl/duckdb/factory.py`` strips only ``upsert`` and
``insert-only``; ClickHouse's caps in ``impl/clickhouse/factory.py`` are
``["delete-insert", "scd2"]``. So no fallback observations table is built.
"""
import os
import sqlite3

import dagster as dg
import duckdb
import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")

from dag_tools.asset_wrappers import dlt_assets_parsing
from dag_tools.asset_wrappers.dlt_assets_factory import TableSpec, build_table_hints
from dag_tools_tests._dlt_component_helpers import load_component, make_source_db


def _pipe(**table):
    return {"p": {
        "name": "alpha", "dest_schema": "raw", "sources": ["widgets"],
        "table_config": {"widgets": {"primary_key": "id", **table}},
    }}


def _load(tmp_path, monkeypatch, **table):
    import dlt

    dest = dlt.destinations.duckdb((tmp_path / "w.duckdb").as_posix())
    monkeypatch.setattr(dlt_assets_parsing, "get_destination", lambda *a, **k: dest)
    monkeypatch.chdir(tmp_path)
    make_source_db(tmp_path)

    def run():
        # A fresh component per load, as a Dagster run process gets: a source
        # object run twice in one process fails in dlt whether or not history is
        # set (measured), which says nothing about scd2.
        defs = load_component(
            tmp_path, monkeypatch, tables=(), pipelines=_pipe(history="scd2", **table)
        )
        result = dg.materialize(list(defs.assets), resources=defs.resources, raise_on_error=False)
        assert result.success

    return run


def _source(tmp_path, sql):
    conn = sqlite3.connect(tmp_path / "source.db")
    conn.execute(sql)
    conn.commit()
    conn.close()


def _versions(tmp_path):
    con = duckdb.connect((tmp_path / "w.duckdb").as_posix(), read_only=True)
    try:
        return con.execute(
            "select id, name, _dlt_valid_to is null from raw.widgets order by id, _dlt_valid_from"
        ).fetchall()
    finally:
        con.close()


def test_two_loads_keep_one_open_version_per_key(tmp_path, monkeypatch):
    run = _load(tmp_path, monkeypatch)
    _source(tmp_path, "INSERT INTO widgets VALUES (2, 'b')")
    run()
    run()
    rows = _versions(tmp_path)
    assert rows == [(1, "a", True), (2, "b", True)]


def test_a_changed_value_closes_the_old_version_and_opens_a_new_one(tmp_path, monkeypatch):
    run = _load(tmp_path, monkeypatch)
    run()
    _source(tmp_path, "UPDATE widgets SET name = 'a2' WHERE id = 1")
    run()
    assert _versions(tmp_path) == [(1, "a", False), (1, "a2", True)]


def test_a_key_absent_from_the_second_load_is_not_retired(tmp_path, monkeypatch):
    run = _load(tmp_path, monkeypatch)
    _source(tmp_path, "INSERT INTO widgets VALUES (2, 'b')")
    run()
    _source(tmp_path, "DELETE FROM widgets WHERE id = 2")
    run()
    # Under a primary_key (no merge_key) dlt retires every active row absent from
    # the load; id 2 would read closed. With the key as a merge_key it stays open.
    assert (2, "b", True) in _versions(tmp_path)
    assert (1, "a", True) in _versions(tmp_path)


def test_the_primary_key_is_emitted_as_a_merge_key_under_scd2():
    hints = build_table_hints(
        {"t": TableSpec(primary_key=["a", "b"], history="scd2")}, {}
    )["t"]
    assert hints["merge_key"] == ["a", "b"]
    assert "primary_key" not in hints


def test_scd2_with_a_cursor_fails_the_load_naming_both_fields(tmp_path, monkeypatch):
    with pytest.raises(Exception) as err:
        load_component(
            tmp_path, monkeypatch,
            pipelines=_pipe(history="scd2", cursor="id"),
        )
    text, exc = "", err.value
    while exc is not None:
        text += str(exc)
        exc = exc.__cause__ or exc.__context__
    assert "widgets" in text and "history" in text and "cursor" in text


def test_a_table_without_history_is_loaded_exactly_as_before():
    spec = TableSpec(primary_key="id", cursor="updated", initial_value=5)
    assert build_table_hints({"t": spec}, {}) == {
        "t": {"primary_key": "id", "incremental": {"cursor_path": "updated", "initial_value": 5}}
    }
    assert build_table_hints({"t": TableSpec(primary_key="id")}, {"t": {"x": 1}}) == {
        "t": {"primary_key": "id", "x": 1}
    }
    assert TableSpec(primary_key="id").history is None


def test_only_the_strategy_is_emitted_and_no_column_name_is_named():
    disposition = build_table_hints(
        {"t": TableSpec(primary_key="id", history="scd2")}, {}
    )["t"]["write_disposition"]
    assert disposition == {"disposition": "merge", "strategy": "scd2"}


@pytest.mark.skipif(
    os.environ.get("DAGTOOLS_E2E_CLICKHOUSE") != "1",
    reason="needs a real ClickHouse; set DAGTOOLS_E2E_CLICKHOUSE=1 and the "
           "DAGTOOLS_E2E_CLICKHOUSE_* connection variables",
)
def test_clickhouse_scd2_two_loads_keep_one_open_version_per_key(tmp_path, monkeypatch):
    import dlt

    env = os.environ
    dest = dlt.destinations.clickhouse(credentials={
        "host": env["DAGTOOLS_E2E_CLICKHOUSE_HOST"],
        "port": int(env.get("DAGTOOLS_E2E_CLICKHOUSE_PORT", "9000")),
        "http_port": int(env.get("DAGTOOLS_E2E_CLICKHOUSE_HTTP_PORT", "8123")),
        "username": env.get("DAGTOOLS_E2E_CLICKHOUSE_USER", "default"),
        "password": env.get("DAGTOOLS_E2E_CLICKHOUSE_PASSWORD", ""),
        "database": env.get("DAGTOOLS_E2E_CLICKHOUSE_DATABASE", "default"),
    })
    monkeypatch.setattr(dlt_assets_parsing, "get_destination", lambda *a, **k: dest)
    monkeypatch.chdir(tmp_path)
    defs = load_component(tmp_path, monkeypatch, pipelines=_pipe(history="scd2"))
    _source(tmp_path, "INSERT INTO widgets VALUES (2, 'b')")
    for i in range(2):
        if i:
            defs = load_component(tmp_path, monkeypatch, tables=(), pipelines=_pipe(history="scd2"))
        assert dg.materialize(list(defs.assets), resources=defs.resources).success
    pipeline = dlt.pipeline("alpha_pipeline", destination=dest, dataset_name="raw")
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            "select id, name, _dlt_valid_to is null from widgets order by id, _dlt_valid_from"
        )
    assert [tuple(r) for r in rows] == [(1, "a", 1), (2, "b", 1)]
