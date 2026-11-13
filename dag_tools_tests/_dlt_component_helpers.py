"""Shared helper: load the dlt component through a tmp ``component.yaml``."""
import json
import sqlite3
import uuid
from pathlib import Path

from dagster.components import build_component_defs


def make_source_db(tmp_path: Path, tables=("widgets",)) -> Path:
    path = tmp_path / "source.db"
    conn = sqlite3.connect(path)
    for t in tables:
        conn.execute(f"CREATE TABLE {t} (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute(f"INSERT INTO {t} VALUES (1, 'a')")
    conn.commit()
    conn.close()
    return path


def load_component(tmp_path, monkeypatch, *, source_type="sql_database", pipelines,
                   tables=("widgets",), extra_attrs=None):
    """Write ``<pkg>/components/dlt/component.yaml`` (type + attributes only) and load it.

    The package name is unique per call: ``build_component_defs`` imports the
    directory as a module, so a reused name would hand back the first load.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = make_source_db(tmp_path, tables)
    pkg = "pkg_" + uuid.uuid4().hex[:8]
    comp_dir = tmp_path / pkg / "components"
    (comp_dir / "dlt").mkdir(parents=True)
    (tmp_path / pkg / "__init__.py").write_text("")
    attrs = {
        "source_config": {
            "type": source_type, "drivername": "sqlite",
            "credentials": f"sqlite:///{db.as_posix()}",
            "database": "main", "schema": "main",
        },
        "dest_config": {
            "type": "duckdb", "drivername": "duckdb",
            "credentials": (tmp_path / "w.duckdb").as_posix(),
            "database": "warehouse", "schema": "raw",
        },
        "pipelines": pipelines,
    }
    attrs.update(extra_attrs or {})
    doc = {"type": "dag_tools.components.dlt_pipeline.DltPipelineComponent", "attributes": attrs}
    # JSON is valid YAML, which keeps this free of a YAML dependency.
    (comp_dir / "dlt" / "component.yaml").write_text(json.dumps(doc))
    monkeypatch.syspath_prepend(str(tmp_path))
    return build_component_defs(comp_dir)
