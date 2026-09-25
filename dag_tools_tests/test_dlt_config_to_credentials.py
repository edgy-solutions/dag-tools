"""`config_to_credentials` must honour the destination `database` and
`schema` from YAML.

It used to skip both for every ConnectionStringCredentials, whether or
not a DSN was given: with no DSN the asset key got a None database and
instantiation failed; with a DSN the dataset fell back to the SOURCE
schema, so data landed in e.g. `dbo` instead of the configured schema.
"""
import pytest

pytest.importorskip("dlt")

from dag_tools.asset_wrappers.dlt_assets_factory import config_to_credentials


def test_fields_without_dsn_keep_database_and_schema():
    creds = config_to_credentials({
        "drivername": "postgresql", "host": "h", "port": 5432,
        "username": "u", "password": "p",
        "database": "analytics", "schema": "raw_zone",
    })
    assert creds.database == "analytics"
    assert creds.schema == "raw_zone"


def test_dsn_keeps_its_database_but_takes_config_schema():
    creds = config_to_credentials({
        "credentials": "postgresql://u:p@h:5432/from_dsn",
        "database": "from_config", "schema": "raw_zone",
    })
    # The DSN decides where we connect; the config database must not retarget it.
    assert creds.database == "from_dsn"
    assert creds.schema == "raw_zone"
    assert creds.to_native_representation() == "postgresql://u:p@h:5432/from_dsn"
