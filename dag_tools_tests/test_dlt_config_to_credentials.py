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


def test_a_snowflake_config_builds_snowflake_credentials():
    """The first snowflake coverage this function has ever had.

    `SnowflakeCredentials` was the one name in the dispatch below that was
    never imported, so this call raised `NameError: name
    'SnowflakeCredentials' is not defined` -- at the first line of the
    `snowflake` arm, before anything of the operator's config was read.
    Nothing in the suite drove a snowflake config through here, which is
    why a missing import survived every release.
    """
    creds = config_to_credentials({
        "drivername": "snowflake",
        "host": "h", "username": "u", "password": "p",
        "database": "analytics", "schema": "raw_zone",
        "warehouse": "wh", "role": "r",
    })
    assert type(creds).__name__ == "SnowflakeCredentials"
    assert creds.drivername == "snowflake"
    assert creds.database == "analytics"
    assert creds.schema == "raw_zone"
    # `warehouse` and `role` are not in STANDARD_ATTRS, so they ride in `query`.
    assert creds.query["warehouse"] == "wh"
    assert creds.query["role"] == "r"


def test_every_credentials_class_the_dispatch_names_is_actually_imported():
    """The seal for the defect above, read off the function's own source.

    A `NameError` in one arm of a seven-way dispatch is invisible until
    something takes that arm, and six of the seven were taken by tests.
    So this walks `config_to_credentials`'s AST for every `<X>Credentials()`
    construction and asserts the name resolves in the module that has to
    resolve it -- which is a property of the whole dispatch rather than of
    whichever arm a test happens to exercise.
    """
    import ast
    import inspect

    from dag_tools.asset_wrappers import dlt_assets_factory

    tree = ast.parse(inspect.getsource(dlt_assets_factory.config_to_credentials))
    names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id.endswith("Credentials")
    }
    # Vacuity guard: a walk that found nothing would pass silently, and a
    # later edit that moved the dispatch out of this function is exactly
    # the kind of change that would empty the set.
    assert len(names) >= 6, f"expected the whole dispatch, found {sorted(names)}"

    missing = sorted(n for n in names if not hasattr(dlt_assets_factory, n))
    assert not missing, f"named in the dispatch but not imported: {missing}"
