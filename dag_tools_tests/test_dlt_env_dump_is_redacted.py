"""The dlt env dump must not write credentials into a working directory.

``ENV_VARS`` holds RESOLVED destination credentials -- entries like
``DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD`` -- because that is dlt's
own configuration convention. ``write_env_vars`` used to dump their values
to ``.env.dlt`` on every local ``dagster dev``.

The working directory there is the CONSUMER's repository root, and the
miss was specific: ``.dockerignore`` carried ``**/.env.dlt`` -- someone
knew the file was sensitive enough to keep out of an image -- while
``.gitignore`` carried only ``.env``, which does not match ``.env.dlt``.
Untracked, unignored, one ``git add -A`` from being committed.
"""
import os

import pytest

from dag_tools.asset_wrappers.dlt_assets_factory import (
    DLT_ENV_DUMP,
    DLT_ENV_FILE_VAR,
    ENV_VARS,
    write_env_vars,
)


SECRET = "super-secret-password"
KEY = "DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"


@pytest.fixture
def dev_mode(monkeypatch, tmp_path):
    """`dagster dev` in a scratch working directory."""
    monkeypatch.setenv("DAGSTER_IS_DEV_CLI", "1")
    monkeypatch.delenv(DLT_ENV_FILE_VAR, raising=False)
    monkeypatch.setitem(ENV_VARS, KEY, SECRET)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_the_default_dump_does_not_contain_the_secret(dev_mode):
    """The whole point."""
    write_env_vars()
    written = (dev_mode / DLT_ENV_DUMP).read_text()
    assert SECRET not in written, written


def test_the_default_dump_still_names_the_variables(dev_mode):
    """"Which destination variables did this config produce" is what
    anyone reading the file wanted; the values were never the answer."""
    write_env_vars()
    written = (dev_mode / DLT_ENV_DUMP).read_text()
    assert KEY in written
    assert "<redacted>" in written


def test_the_dump_says_how_to_get_real_values(dev_mode):
    write_env_vars()
    assert DLT_ENV_FILE_VAR in (dev_mode / DLT_ENV_DUMP).read_text()


def test_real_values_require_naming_a_path_not_flipping_a_flag(
    dev_mode, monkeypatch, tmp_path,
):
    """A boolean opt-in would put the secrets back in the repository root.
    Requiring a path means opting in cannot land them there by accident."""
    target = tmp_path / "elsewhere" / "dlt.env"
    target.parent.mkdir()
    monkeypatch.setenv(DLT_ENV_FILE_VAR, str(target))

    write_env_vars()

    assert SECRET in target.read_text()
    assert not (dev_mode / DLT_ENV_DUMP).exists(), (
        "the redacted dump was written as well, putting the file back in "
        "the working directory"
    )


def test_nothing_is_written_outside_dagster_dev(monkeypatch, tmp_path):
    monkeypatch.delenv("DAGSTER_IS_DEV_CLI", raising=False)
    monkeypatch.setitem(ENV_VARS, KEY, SECRET)
    monkeypatch.chdir(tmp_path)

    write_env_vars()

    assert not (tmp_path / DLT_ENV_DUMP).exists()


def test_the_dump_filename_is_gitignored_here():
    """Defence in depth for THIS repo. It cannot protect a consumer's --
    the file lands in whichever working directory `dagster dev` runs in --
    which is why the default is redacted rather than merely ignored."""
    import subprocess

    result = subprocess.run(
        ["git", "check-ignore", DLT_ENV_DUMP],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, (
        f"{DLT_ENV_DUMP} is not gitignored; `.env` does not match it"
    )
