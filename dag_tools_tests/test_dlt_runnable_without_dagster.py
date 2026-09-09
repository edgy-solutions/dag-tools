"""A dlt pipeline can be built and run without going through Dagster.

THE ASYMMETRY THIS CLOSES. A dbt project directory IS the artifact and dbt
ships its own runner, so Dagster only ever wraps something already
runnable -- which is why `dbt run` works standalone and the dev loop never
touches Dagster. A dlt pipeline here is YAML that only this factory knows
how to interpret, and the factory built the source and wrapped it in a
`@multi_asset` in one breath. Nothing outside Dagster could construct one.

The cost was the loop: iterating on a cursor, a hint or a column selection
meant a full definitions load -- 90-180s for a location carrying Dagster,
dbt, dlt and datahub -- and required every OTHER component in that
location to import cleanly first.

The tests below run a real dlt extraction end to end with no Dagster
involved, against a local DuckDB file.
"""
import pathlib

import pytest

pytest.importorskip("dlt")
pytest.importorskip("duckdb")

import dlt
import duckdb

from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from dag_tools.asset_wrappers.dlt_assets_parsing import (
    DltRunnable,
    build_dlt_runnables,
    create_dlt_assets,
)


@pytest.fixture
def source_db(tmp_path):
    """A sqlite database standing in for an operational source."""
    path = tmp_path / "source.db"
    con = duckdb.connect()  # only to prove duckdb is importable
    con.close()
    import sqlite3

    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, changed_on TEXT)")
    conn.executemany(
        "INSERT INTO widgets VALUES (?,?,?)",
        [(1, "a", "2026-01-01"), (2, "b", "2026-01-02"), (3, "c", "2026-01-03")],
    )
    conn.commit()
    conn.close()
    return path


def _configs(source_db, tmp_path):
    source_config = {
        "type": "sql_database",
        "drivername": "sqlite",
        "credentials": f"sqlite:///{source_db}",
        "database": "main",
        "schema": "main",
    }
    dest_config = {
        "type": "duckdb",
        "drivername": "duckdb",
        "credentials": str(tmp_path / "warehouse.duckdb"),
        "database": "warehouse",
        "schema": "raw",
    }
    return source_config, dest_config


def _config(**kw):
    return DltAssetGroupConfig(
        name="devloop",
        dest_schema="raw",
        hints={"widgets": {"primary_key": "id"}},
        pipeline_kwargs={"write_disposition": "replace"},
        **kw,
    )


def test_a_runnable_is_built_with_no_dagster_definitions_load(source_db, tmp_path):
    """The whole point: config in, (source, pipeline) out."""
    source_config, dest_config = _configs(source_db, tmp_path)
    runnables = build_dlt_runnables(
        ["widgets"], source_config, dest_config, _config(),
        destination_override=dlt.destinations.duckdb(str(tmp_path / "dev.duckdb")),
    )
    assert len(runnables) == 1
    unit = runnables[0]
    assert isinstance(unit, DltRunnable)
    assert unit.tables == ["widgets"]
    assert "widgets" in unit.source.resources


def test_it_actually_extracts_and_loads(source_db, tmp_path):
    """Not just constructed -- run. A builder that produces a source which
    cannot execute would pass every structural assertion."""
    warehouse = tmp_path / "dev.duckdb"
    source_config, dest_config = _configs(source_db, tmp_path)
    runnable = build_dlt_runnables(
        ["widgets"], source_config, dest_config, _config(),
        destination_override=dlt.destinations.duckdb(str(warehouse)),
    )[0]

    runnable.run()

    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        rows = con.execute("SELECT id, name FROM raw.widgets ORDER BY id").fetchall()
    finally:
        con.close()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


def test_extract_only_touches_no_destination_tables(source_db, tmp_path):
    """`--extract-only`: validate reflection, hints and cursors against the
    real source without writing anywhere."""
    warehouse = tmp_path / "extract-only.duckdb"
    source_config, dest_config = _configs(source_db, tmp_path)
    runnable = build_dlt_runnables(
        ["widgets"], source_config, dest_config, _config(),
        destination_override=dlt.destinations.duckdb(str(warehouse)),
    )[0]

    runnable.extract()

    if warehouse.exists():
        con = duckdb.connect(str(warehouse), read_only=True)
        try:
            tables = [
                r[0] for r in con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'raw'"
                ).fetchall()
            ]
        finally:
            con.close()
        assert "widgets" not in tables, tables


def test_the_row_limit_is_honoured(source_db, tmp_path):
    """The fast-loop lever: a sample rather than the whole table."""
    warehouse = tmp_path / "limited.duckdb"
    source_config, dest_config = _configs(source_db, tmp_path)
    runnable = build_dlt_runnables(
        ["widgets"], source_config, dest_config, _config(limit=1),
        destination_override=dlt.destinations.duckdb(str(warehouse)),
    )[0]

    runnable.run()

    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM raw.widgets").fetchone()[0]
    finally:
        con.close()
    assert count == 1, (
        f"limit=1 loaded {count} rows; dlt's default counts pages, not rows, "
        f"and the first page is not data"
    )


def test_the_destination_override_changes_only_where_data_lands(
    source_db, tmp_path,
):
    """Reflection, hints and cursors must stay exactly as configured, or
    the local run tests something other than what ships."""
    source_config, dest_config = _configs(source_db, tmp_path)

    configured = build_dlt_runnables(
        ["widgets"], source_config, dest_config, _config(),
    )[0]
    overridden = build_dlt_runnables(
        ["widgets"], source_config, dest_config, _config(),
        destination_override=dlt.destinations.duckdb(str(tmp_path / "o.duckdb")),
    )[0]

    assert configured.tables == overridden.tables
    assert set(configured.source.resources) == set(overridden.source.resources)
    assert configured.dest_schema == overridden.dest_schema


def test_both_paths_go_through_the_same_resolution(source_db, tmp_path, monkeypatch):
    """If they drifted, the dev loop would exercise a parallel reading of
    the config rather than the one that ships -- the failure mode a
    bolted-on local runner usually has.

    Asserted at the shared function rather than by comparing rendered
    asset keys, because those are subject to Dagster's name rules: a
    sqlite source whose "database" is a filesystem path produces a key
    Dagster rejects, which says nothing about whether the two paths agree.
    """
    import dag_tools.asset_wrappers.dlt_assets_parsing as mod

    calls = []
    original = mod._prepare_units
    monkeypatch.setattr(
        mod, "_prepare_units",
        lambda *a, **k: (calls.append(1), original(*a, **k))[1],
    )

    source_config, dest_config = _configs(source_db, tmp_path)
    mod.create_dlt_assets(["widgets"], source_config, dest_config, _config())
    assert calls, "create_dlt_assets does not use the shared resolution"

    calls.clear()
    mod.build_dlt_runnables(["widgets"], source_config, dest_config, _config())
    assert calls, "build_dlt_runnables does not use the shared resolution"


def test_the_row_limit_counts_ROWS_not_pages(source_db, tmp_path):
    """dlt's add_limit counts "yields/batches/pages" by default, not rows,
    and counts EMPTY pages too. With the sqlalchemy backend the first
    yield is not data, so `limit: 1` extracted nothing at all -- no rows,
    no table -- while `limit: 2` extracted the whole table.

    The field is documented as a row limit, so it is one now
    (count_rows=True). A knob whose "1" means zero and whose "2" means
    everything is worse than no knob: it reads as working.
    """
    warehouse = tmp_path / "rows.duckdb"
    source_config, dest_config = _configs(source_db, tmp_path)
    runnable = build_dlt_runnables(
        ["widgets"], source_config, dest_config, _config(limit=2),
        destination_override=dlt.destinations.duckdb(str(warehouse)),
    )[0]

    runnable.run()

    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM raw.widgets").fetchone()[0]
    finally:
        con.close()
    assert count == 2, f"limit=2 loaded {count} rows of a 3-row table"
