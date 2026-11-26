"""Each (database, schema) unit gets its OWN destination credentials.

THE DEFECT. `_prepare_units` built one destination credentials object above
its loop and then mutated it inside: when the destination named no database,
the first unit filled `credentials.database` with its own, and every later
unit found it already filled and kept it. So a call carrying two units from
two source databases landed BOTH in the first one's -- same destination
database, a pipeline named after the first, `dest_database` reporting the
first -- with no error and no warning. The run read as a success; the second
database's rows were simply in the wrong place.

Two units in one call come only from AssetKey sources (`process_sources`),
and only differ in their source database when no `source_config` overrides
every key -- so the sources here resolve through registered credential
providers, one per prefix, exactly as a deployment would wire them.

For postgres and snowflake, `credentials.database` on the object handed to
`get_destination` IS the target database. DuckDB has no such notion, so the
test stands in a destination that is selected by that database: each unit is
sent to a DuckDB file named after the database it was handed, and the rows
are read back out of each file. "Each writes to its own" is then a statement
about where rows landed, not about a dict the factory returned.
"""
import sqlite3

import pytest

pytest.importorskip("dlt")
pytest.importorskip("duckdb")

import dlt
import duckdb
from dagster import AssetKey

from dag_tools.asset_wrappers import dlt_assets_parsing as parsing
from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
from dag_tools.utils import credentials as cred_registry

DATABASES = ("db_one", "db_two")


class _Provider(cred_registry.CredentialProvider):
    """A source database resolved by prefix, the way a deployment wires one."""

    def get_config(self, key):
        # Parts, not a DSN: a parsed sqlite DSN names a FILE PATH as its
        # database, and the point here is two plainly named databases.
        return {"drivername": "sqlite", "database": self.get_key(), "schema": "main"}


@pytest.fixture
def two_sources(tmp_path, monkeypatch):
    """Two sqlite databases, each holding a `widgets` table with its OWN rows."""
    monkeypatch.chdir(tmp_path)  # `sqlite:///db_one` resolves against cwd
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    for db in DATABASES:
        conn = sqlite3.connect(tmp_path / db)
        conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO widgets VALUES (1, ?)", (f"from_{db}",))
        conn.commit()
        conn.close()
        monkeypatch.setitem(cred_registry._providers, db, _Provider(db))
    return [AssetKey([db, "main", "widgets"]) for db in DATABASES]


@pytest.fixture
def database_selected_destination(tmp_path, monkeypatch):
    """`get_destination`, standing in for one that targets `credentials.database`.

    Records the database each call was handed AT CALL TIME -- the defect was a
    mutation, so reading the object afterwards would hide the order it happened
    in.
    """
    handed = []

    def _stub(credentials, config=None, vars=None, database=None):
        handed.append(credentials.database)
        return dlt.destinations.duckdb(str(tmp_path / f"{credentials.database}.duckdb"))

    monkeypatch.setattr(parsing, "get_destination", _stub)
    return handed


# No database: the case where each unit's own is filled in.
DEST = {"drivername": "duckdb", "schema": "raw"}


def _config():
    # No `name`: the default pipeline name is derived from the destination
    # database, which is one of the things the defect got wrong.
    return DltAssetGroupConfig(dest_schema="raw", pipeline_kwargs={"write_disposition": "replace"})


def _landed(tmp_path, db):
    path = tmp_path / f"{db}.duckdb"
    if not path.exists():
        return None
    with duckdb.connect(str(path), read_only=True) as con:
        return [r[0] for r in con.execute("select name from raw.widgets order by id").fetchall()]


def test_two_units_with_different_databases_each_write_to_their_own(
    two_sources, database_selected_destination, tmp_path
):
    runnables = parsing.build_dlt_runnables(two_sources, {}, DEST, _config())
    assert sorted(r.database for r in runnables) == sorted(DATABASES)

    for runnable in runnables:
        runnable.run()

    assert sorted(database_selected_destination) == sorted(DATABASES)
    for db in DATABASES:
        assert _landed(tmp_path, db) == [f"from_{db}"], (
            f"{db}'s rows did not land in {db}'s destination"
        )


def test_each_unit_names_its_own_destination_database_and_pipeline(
    two_sources, database_selected_destination
):
    _, units = parsing._prepare_units(two_sources, {}, DEST, _config())
    by_source = {u["database"]: u for u in units}

    assert set(by_source) == set(DATABASES)
    for db, unit in by_source.items():
        assert unit["dest_database"] == db
        assert unit["pipeline"].pipeline_name == f"{db}_raw_pipeline"


def test_control_one_shared_credentials_object_sends_the_second_unit_to_the_first(
    two_sources, database_selected_destination, tmp_path, monkeypatch
):
    """The old shape, planted: one destination credentials object for the call.

    Without this the test above would pass on any code that happened to hand
    `get_destination` the right names -- including code that never touched
    the loop. This proves the stand-in destination really does tell the two
    shapes apart, by measuring the old one land both units in one place.
    """
    real = parsing.config_to_credentials
    shared = {}

    def _once_per_config(config, kinds=None):
        if config is not DEST and config.get("drivername") == "duckdb":
            # `_prepare_units` deep-copies its dest_config; key on the copy.
            if "dest" not in shared:
                shared["dest"] = real(config, kinds)
            elif kinds is not None:
                real(config, kinds)
            return shared["dest"]
        return real(config, kinds)

    monkeypatch.setattr(parsing, "config_to_credentials", _once_per_config)
    _, units = parsing._prepare_units(two_sources, {}, DEST, _config())

    first = units[0]["database"]
    assert database_selected_destination == [first, first]
    assert {u["dest_database"] for u in units} == {first}


def test_a_destination_that_names_its_database_keeps_it_for_every_unit(
    two_sources, database_selected_destination
):
    """The fill-in only applies to an EMPTY database; an explicit one is the operator's."""
    _, units = parsing._prepare_units(
        two_sources, {}, {**DEST, "database": "warehouse"}, _config()
    )

    assert database_selected_destination == ["warehouse", "warehouse"]
    assert {u["dest_database"] for u in units} == {"warehouse"}
