"""``get_destination`` must not hand back a destination nobody asked for.

Two defects, one of which hid the other.

The snowflake arm read ``credentials.staging``, and no credentials object
this library builds has ever carried that attribute -- so the arm raised
``AttributeError`` on every call it ever received. The surrounding
``except`` then PRINTED the error and fell through to ``return
"filesystem"``, so a snowflake load silently became a filesystem load,
ran, and reported success. dlt says so in one line of a log of
thousands, and says *"will be ignored"* about the destination the
operator actually asked for.

Nothing in the suite called this function for real: all six references
monkeypatch ``get_destination`` itself, because duckdb is not one of the
drivernames it builds. That is why both survived every release.
"""
import logging

import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster")

from dag_tools.asset_wrappers import dlt_assets_parsing
from dag_tools.asset_wrappers.dlt_assets_factory import config_to_credentials

SNOWFLAKE = {
    "drivername": "snowflake",
    "host": "h", "username": "u", "password": "p",
    "database": "analytics", "schema": "raw_zone",
}


def test_a_snowflake_config_builds_a_SNOWFLAKE_destination():
    """The whole path, from an operator's config dict to a destination.

    This is the call that used to come back as the string ``"filesystem"``
    -- not as an error, as a working destination pointing somewhere else.
    """
    destination = dlt_assets_parsing.get_destination(
        config_to_credentials(dict(SNOWFLAKE)), {}
    )
    assert destination != "filesystem"
    assert "snowflake" in type(destination).__module__


def test_a_recognised_destination_that_cannot_be_built_RAISES(monkeypatch):
    """The dangerous half, and the half no exception message could fix.

    A construction failure used to return a filesystem destination, so the
    load went somewhere real and the run went green. Raising is the only
    answer here: by the time this function returns there is nothing left
    that knows the destination is wrong.
    """
    def _boom(**_kwargs):
        raise ValueError("planted")

    monkeypatch.setattr(dlt_assets_parsing.dlt.destinations, "snowflake", _boom)

    with pytest.raises(RuntimeError) as excinfo:
        dlt_assets_parsing.get_destination(config_to_credentials(dict(SNOWFLAKE)), {})

    # The message names the destination that was asked for, because the
    # operator's own word for it is the one they can act on.
    assert "snowflake" in str(excinfo.value)
    assert "planted" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_control_the_old_shape_would_have_returned_a_destination(monkeypatch):
    """Without this, the test above would pass under any ``except`` at all.

    It plants the same failure and shows what the swallow produced: a
    value, not an exception -- which is the entire reason the defect was
    invisible.
    """
    def _boom(**_kwargs):
        raise ValueError("planted")

    def _old_shape(credentials, config):
        try:
            return _boom(credentials=credentials, **config)
        except Exception as exc:  # noqa: BLE001 - this IS the old body
            print(f"Warning instantiating destination: {exc}")
        return "filesystem"

    assert _old_shape(config_to_credentials(dict(SNOWFLAKE)), {}) == "filesystem"


def test_an_unrecognised_drivername_still_falls_back_but_WARNS(caplog):
    """The fallback is documented behaviour (AGENTS.md 4q) and is kept.

    duckdb is not one of the five destinations this function builds, and
    six test modules monkeypatch this function precisely because of that,
    so refusing here would be a different change. What it must not be is
    SILENT: until now the only clue was the data's absence from the
    destination the operator had written down.
    """
    creds = config_to_credentials({
        "drivername": "duckdb", "host": "h", "username": "u",
        "password": "p", "database": "d",
    })
    with caplog.at_level(logging.WARNING, logger=dlt_assets_parsing.logger.name):
        assert dlt_assets_parsing.get_destination(creds, {}) == "filesystem"

    assert any(
        record.levelno == logging.WARNING and "duckdb" in record.getMessage()
        for record in caplog.records
    ), [r.getMessage() for r in caplog.records]


def _recorder(monkeypatch):
    seen = {}

    def _capture(credentials=None, **config):
        seen["config"] = config
        return "DESTINATION"

    monkeypatch.setattr(dlt_assets_parsing.dlt.destinations, "snowflake", _capture)
    return seen


def test_a_credentials_object_carrying_staging_is_still_honoured(monkeypatch):
    """Section 6. Nothing in this library can set ``.staging``, but an
    external caller's own credentials subclass could, and that caller must
    keep working. The value now travels as the destination parameter it
    always was."""
    seen = _recorder(monkeypatch)
    creds = config_to_credentials(dict(SNOWFLAKE))
    creds.staging = "legacy_layout"

    assert dlt_assets_parsing.get_destination(creds, {}) == "DESTINATION"
    assert seen["config"]["staging_dataset_name_layout"] == "legacy_layout"


def test_an_explicit_config_key_wins_over_the_legacy_attribute(monkeypatch):
    """The operator's own spelling of the setting beats the attribute, so
    the compatibility fallback cannot override a document that names it."""
    seen = _recorder(monkeypatch)
    creds = config_to_credentials(dict(SNOWFLAKE))
    creds.staging = "legacy_layout"

    dlt_assets_parsing.get_destination(
        creds, {"staging_dataset_name_layout": "explicit"}
    )
    assert seen["config"]["staging_dataset_name_layout"] == "explicit"


def test_no_staging_attribute_means_no_such_key_is_sent(monkeypatch):
    """The ordinary case -- every credentials object this library builds.

    Sending the key as ``None`` would look harmless and would override
    whatever dlt's own default is, which is not this function's to decide.
    """
    seen = _recorder(monkeypatch)
    dlt_assets_parsing.get_destination(config_to_credentials(dict(SNOWFLAKE)), {})
    assert "staging_dataset_name_layout" not in seen["config"]


def test_the_callers_config_dict_is_not_mutated():
    """``get_destination`` adds defaults of its own, and the same dict is
    reused across tables by callers that build one config and loop."""
    config = {}
    dlt_assets_parsing.get_destination(config_to_credentials(dict(SNOWFLAKE)), config)
    assert config == {}
