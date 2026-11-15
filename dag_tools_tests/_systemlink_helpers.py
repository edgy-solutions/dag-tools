"""Shared helpers for the SystemLink tests: load the shipped example against a fake server."""
import json
import shutil
import uuid
from pathlib import Path

import yaml
from dagster.components import build_component_defs

EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "mfg_trace" / "systemlink"
KEY_ENV, PSEUDO_ENV = "SYSTEMLINK_API_KEY", "SYSTEMLINK_PSEUDONYM_KEY"
TEST_KEY, TEST_PSEUDO = "test-key-0000", "test-pseudonym-key-0000"


def load_example(tmp_path, monkeypatch, *, base_url="http://127.0.0.1:1", source_overrides=None,
                 pipeline_overrides=None, set_env=True):
    """Copy the shipped example under a unique package name and load it exactly as a deployment would."""
    pkg = "pkg_" + uuid.uuid4().hex[:8]
    comp_dir = tmp_path / pkg / "components"
    shutil.copytree(EXAMPLE, comp_dir / "systemlink")
    (tmp_path / pkg / "__init__.py").write_text("")
    path = comp_dir / "systemlink" / "component.yaml"
    doc = yaml.safe_load(path.read_text())
    attrs = doc["attributes"]
    attrs["source_config"].update(source_overrides or {})
    attrs["pipelines"]["sl_example"].update(pipeline_overrides or {})
    path.write_text(json.dumps(doc))
    # get_destination knows only the warehouse drivers; a local duckdb file stands in for the destination.
    import dlt
    from dag_tools.asset_wrappers import dlt_assets_parsing as parsing
    db = (tmp_path / "w.duckdb").as_posix()
    monkeypatch.setattr(parsing, "get_destination", lambda *a, **k: dlt.destinations.duckdb(db))
    monkeypatch.setenv("SYSTEMLINK_BASE_URL", base_url)
    monkeypatch.setenv("SYSTEMLINK_EXAMPLE_DB", (tmp_path / "w.duckdb").as_posix())
    monkeypatch.chdir(tmp_path)
    if set_env:
        monkeypatch.setenv(KEY_ENV, TEST_KEY)
        monkeypatch.setenv(PSEUDO_ENV, TEST_PSEUDO)
    monkeypatch.syspath_prepend(str(tmp_path))
    return build_component_defs(comp_dir)


TABLES = ("sl_results", "sl_steps", "sl_ingest_runs")


def run_job(defs, job_name, instance, tags=None):
    return defs.get_job_def(job_name).execute_in_process(
        instance=instance, tags=tags or {}, raise_on_error=False
    )


def table(tmp_path, name, columns="*", schema="raw"):
    import duckdb

    con = duckdb.connect((tmp_path / "w.duckdb").as_posix(), read_only=True)
    try:
        return con.execute(f"select {columns} from {schema}.{name}").fetchall()
    finally:
        con.close()


def all_text(tmp_path):
    """Every destination table rendered to one string, for leak checks."""
    import duckdb

    con = duckdb.connect((tmp_path / "w.duckdb").as_posix(), read_only=True)
    try:
        out = []
        for (schema, name) in con.execute(
            "select table_schema, table_name from information_schema.tables where table_schema not in ('information_schema','pg_catalog')"
        ).fetchall():
            out.append(f"{schema}.{name}")
            out.extend(str(r) for r in con.execute(f'select * from "{schema}"."{name}"').fetchall())
        return "\n".join(out)
    finally:
        con.close()
