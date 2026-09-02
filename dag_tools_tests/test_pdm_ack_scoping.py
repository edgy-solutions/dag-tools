"""The acknowledgment reads only what landed since the last successful ack.

Unscoped, the dispatch asset ran `SELECT <pk> FROM <dest>.<table>` on
every cycle. Under `write_disposition: merge` the destination accumulates,
so the payload grew without bound: every cycle re-sent every primary key
ever ingested, and the stats row recorded an all-time count rather than
the batch. Idempotent, so nothing broke -- it was just the one part of
this flow whose cost grows with the data instead of with the work.

These drive the REAL asset against a real database (sqlite via
SQLAlchemy, which is all the read needs) with the Restate POST stubbed,
across a shared instance so the high-water mark actually round-trips
through materialization metadata the way it does in production.
"""
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("dagster_dlt")

import httpx
from dagster import AssetKey, DagsterInstance, materialize

from dag_tools.components.restate_dlt_sync.component import LAST_ACKED_LOAD_ID


TABLE = "PDM_PART"
DISPATCH = f"pdm_{TABLE}_ack_dispatch"

LOAD_1 = "1787253108.2521412"
LOAD_2 = "1787253109.9780922"


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "dest.sqlite"
    conn = sqlite3.connect(path)
    # `main` is sqlite's own name for the default schema, so the
    # component's `<schema>.<table>` query is valid here unchanged.
    conn.execute(
        f"CREATE TABLE main.{TABLE} (PART_ID INTEGER, _dlt_load_id TEXT)"
    )
    conn.commit()
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS", f"sqlite:///{path}")
    return conn


def _add(conn, ids, load_id):
    conn.executemany(
        f"INSERT INTO main.{TABLE} (PART_ID, _dlt_load_id) VALUES (?,?)",
        [(i, load_id) for i in ids],
    )
    conn.commit()


@pytest.fixture
def dispatch(db):
    """The real generated asset, pointed at sqlite's default schema."""
    import dag_tools_tests.test_pdm_component_build as build

    defs = build._component(pipeline={"dest_schema": "main"}).build_defs(None)
    asset = next(
        a for a in defs.assets if AssetKey([DISPATCH]) in getattr(a, "keys", [])
    )
    return asset


class _Posted:
    def __init__(self, fail=False):
        self.payloads = []
        self.urls = []
        self.fail = fail

    def install(self):
        outer = self

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def post(self, url, json=None, **kw):
                # The ack is now AWAITED rather than fired and forgotten,
                # so the caller checks the response. Under Restate's /send
                # form a failed handler still answered 202 and the mark
                # advanced past rows whose ack never landed.
                outer.urls.append(str(url))
                if outer.fail:
                    raise httpx.ConnectError("restate unreachable")
                outer.payloads.append(json)
                return httpx.Response(
                    200, json={"ok": True}, request=httpx.Request("POST", url),
                )

        return patch.object(httpx, "AsyncClient", _Client)

    @property
    def acked_ids(self):
        return [i for p in self.payloads for i in p["record_ids"]]


def _run(dispatch, instance, posted):
    with posted.install():
        result = materialize([dispatch], instance=instance, raise_on_error=False)
    mats = result.get_asset_materialization_events()
    assert mats, "the dispatch asset did not materialize"
    return dict(
        mats[0].step_materialization_data.materialization.metadata
    )


def _value(md, key):
    entry = md.get(key)
    return getattr(entry, "value", entry)


def test_first_cycle_acks_everything_and_records_the_mark(db, dispatch):
    _add(db, [1, 2, 3], LOAD_1)
    instance = DagsterInstance.ephemeral()
    posted = _Posted()

    md = _run(dispatch, instance, posted)

    assert sorted(posted.acked_ids) == [1, 2, 3]
    assert _value(md, "records_acked") == 3
    assert _value(md, LAST_ACKED_LOAD_ID) == LOAD_1


def test_second_cycle_acks_only_the_new_load(db, dispatch):
    """The whole point. Before this the second cycle re-sent rows 1-3
    alongside 4-5, and every cycle after that re-sent the lot."""
    instance = DagsterInstance.ephemeral()

    _add(db, [1, 2, 3], LOAD_1)
    _run(dispatch, instance, _Posted())

    _add(db, [4, 5], LOAD_2)
    posted = _Posted()
    md = _run(dispatch, instance, posted)

    assert sorted(posted.acked_ids) == [4, 5], "rows from load 1 were re-sent"
    assert _value(md, "records_acked") == 2
    assert _value(md, LAST_ACKED_LOAD_ID) == LOAD_2


def test_a_cycle_with_nothing_new_acks_nothing(db, dispatch):
    instance = DagsterInstance.ephemeral()
    _add(db, [1, 2], LOAD_1)
    _run(dispatch, instance, _Posted())

    posted = _Posted()
    md = _run(dispatch, instance, posted)

    assert posted.acked_ids == []
    assert _value(md, "records_acked") == 0
    # The mark must not regress to empty just because this cycle saw
    # nothing -- that would re-ack the whole table next time.
    assert _value(md, LAST_ACKED_LOAD_ID) == LOAD_1


def test_a_failed_dispatch_holds_the_mark(db, dispatch):
    """Advancing the mark past rows whose ack never arrived would strand
    them unacked forever: PDM would keep processed_flag='N' and we would
    never send them again."""
    instance = DagsterInstance.ephemeral()
    _add(db, [1, 2], LOAD_1)
    _run(dispatch, instance, _Posted())

    _add(db, [3, 4], LOAD_2)
    md = _run(dispatch, instance, _Posted(fail=True))

    assert _value(md, "chunks_failed") == 1
    assert _value(md, LAST_ACKED_LOAD_ID) == LOAD_1, "mark advanced past a failed ack"


def test_the_next_cycle_retries_what_the_failure_stranded(db, dispatch):
    instance = DagsterInstance.ephemeral()
    _add(db, [1, 2], LOAD_1)
    _run(dispatch, instance, _Posted())

    _add(db, [3, 4], LOAD_2)
    _run(dispatch, instance, _Posted(fail=True))

    posted = _Posted()
    md = _run(dispatch, instance, posted)

    assert sorted(posted.acked_ids) == [3, 4], "the stranded rows were not retried"
    assert _value(md, LAST_ACKED_LOAD_ID) == LOAD_2


def test_the_ack_is_awaited_not_fired_and_forgotten(db, dispatch):
    """Configured with Restate's /send suffix, a handler failure still
    answered 202 -- so the mark advanced past rows whose acknowledgment
    never actually reached the source."""
    _add(db, [1, 2], LOAD_1)
    posted = _Posted()
    _run(dispatch, DagsterInstance.ephemeral(), posted)

    assert posted.urls, "no call was made"
    for url in posted.urls:
        assert not url.rstrip("/").endswith("/send"), url
