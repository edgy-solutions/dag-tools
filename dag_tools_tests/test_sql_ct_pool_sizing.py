"""The CT source must not exhaust its connection pool on wide pipelines.

Every generator in `_internal_ct_source` yields INSIDE `engine.connect()`,
so each table holds a connection until it is exhausted, and the source is
parallelized. On the default QueuePool (5 + 10 overflow) the 16th table
waited for a connection and timed out once the tables ahead of it ran
longer than pool_timeout -- the deployed failure was
`QueuePool limit of size 5 overflow 10 reached, connection timed out`,
surfacing as dlt's secondary `generator already executing`.

SQLite has no Change Tracking, so the table-level CT lookup fails and
every table takes the full-load generator -- the same connection shape.
"""
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("dlt")

import dlt
import sqlalchemy as sa

from dag_tools.asset_wrappers.sources import sql_ct_database as ct

N_TABLES = 20  # more than the default pool's 15-connection ceiling


@pytest.fixture
def wide_db(tmp_path: Path) -> sa.engine.Engine:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'src.db'}")
    with engine.begin() as conn:
        for i in range(N_TABLES):
            conn.execute(sa.text(f"create table t{i} (id integer primary key)"))
            conn.execute(sa.text(
                f"insert into t{i} values " + ",".join(f"({j})" for j in range(30))
            ))
    return engine


def _source(engine, credentials):
    @dlt.source(name="sql_database")
    def src():
        return list(ct._internal_ct_source(
            credentials=credentials,
            table_names=[f"t{i}" for i in range(N_TABLES)],
            chunk_size=5,  # several batches per table, so generators stay open
            engine=engine,
        ))
    return src()


def test_extraction_pool_holds_one_connection_per_table(wide_db, monkeypatch):
    seen = []
    real = ct._make_full_load_generator

    def spy(engine, table_obj, chunk_size):
        seen.append(engine)
        return real(engine, table_obj, chunk_size)

    monkeypatch.setattr(ct, "_make_full_load_generator", spy)
    _source(wide_db, credentials="sqlite://")

    assert len(seen) == N_TABLES
    assert len({id(e) for e in seen}) == 1, "every table must share one sized engine"
    assert seen[0].pool.size() >= N_TABLES


def test_a_caller_supplied_engine_is_not_resized(wide_db, monkeypatch):
    seen = []
    real = ct._make_full_load_generator
    monkeypatch.setattr(
        ct, "_make_full_load_generator",
        lambda e, t, c: (seen.append(e), real(e, t, c))[1],
    )
    _source(wide_db, credentials=wide_db)
    assert {id(e) for e in seen} == {id(wide_db)}


def test_wide_parallel_extract_does_not_exhaust_the_pool(wide_db, monkeypatch):
    # One worker makes the old failure deterministic: it blocks on checkout
    # while the connection holders are suspended with nobody to advance them.
    monkeypatch.setenv("EXTRACT__WORKERS", "1")
    pipeline = dlt.pipeline(
        pipeline_name="ct_pool_sizing", pipelines_dir=tempfile.mkdtemp()
    )
    info = pipeline.extract(_source(wide_db, credentials="sqlite://").parallelize())
    assert info is not None
