"""``ingest_runs`` on the dlt pipeline component: one row per run, failed or not.

Everything goes through the real component YAML (``load_component``), because
``DltAssetGroupConfig`` has no ``extra="forbid"``: a flag that never became a
field would be silently ignored and these would read as configured.
"""
import hashlib

import dagster as dg
import duckdb
import pytest

pytest.importorskip("dlt")
pytest.importorskip("dagster_dlt")

from dag_tools.asset_wrappers import dlt_assets_parsing
from dag_tools_tests._dlt_component_helpers import load_component

SECRET = "hunter2-planted-secret-value"


class DistinctiveEngineError(Exception):
    pass


def _pipe(**kw):
    return {"p": {"name": "alpha", "dest_schema": "raw", "sources": ["widgets"], **kw}}


def _run(tmp_path, monkeypatch, **kw):
    # get_destination knows only the warehouse drivers; a local duckdb file stands in.
    import dlt

    dest = dlt.destinations.duckdb((tmp_path / "w.duckdb").as_posix())
    monkeypatch.setattr(dlt_assets_parsing, "get_destination", lambda *a, **k: dest)
    monkeypatch.chdir(tmp_path)
    defs = load_component(tmp_path, monkeypatch, pipelines=_pipe(**kw))
    result = dg.materialize(list(defs.assets), resources=defs.resources, raise_on_error=False)
    return defs, result


def _query(tmp_path, sql):
    con = duckdb.connect((tmp_path / "w.duckdb").as_posix(), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _tables(tmp_path):
    return {r[0] for r in _query(tmp_path, "select table_name from information_schema.tables")}


def _runs(tmp_path):
    return _query(
        tmp_path,
        "select status, error_type, error_digest, rows_total, row_counts, source_system, mode "
        "from raw.ingest_runs",
    )


def _make_resource_raise(monkeypatch, exc):
    real = dlt_assets_parsing.build_dlt_source

    def build(*a, **k):
        source = real(*a, **k)
        def boom(item):
            raise exc
        for r in source.resources.values():
            r.add_map(boom)
        return source

    monkeypatch.setattr(dlt_assets_parsing, "build_dlt_source", build)


def test_a_successful_run_is_recorded_with_its_row_counts(tmp_path, monkeypatch):
    _, result = _run(tmp_path, monkeypatch, ingest_runs=True)
    assert result.success
    rows = _runs(tmp_path)
    assert len(rows) == 1
    status, err_type, digest, total, counts, system, mode = rows[0]
    assert (status, err_type, digest) == ("SUCCESS", None, None)
    assert total == 1 and '"widgets": 1' in counts
    assert system == "alpha" and mode == "full"


def test_a_failed_run_is_recorded_and_the_failure_is_re_raised(tmp_path, monkeypatch):
    _make_resource_raise(monkeypatch, DistinctiveEngineError("nope"))
    _, result = _run(tmp_path, monkeypatch, ingest_runs=True)
    assert not result.success  # re-raised: the run is red
    rows = _runs(tmp_path)
    assert len(rows) == 1 and rows[0][0] == "FAILED"


def test_the_row_names_the_root_error_not_the_dlt_wrapper(tmp_path, monkeypatch):
    _make_resource_raise(monkeypatch, DistinctiveEngineError("nope"))
    _run(tmp_path, monkeypatch, ingest_runs=True)
    (row,) = _runs(tmp_path)
    assert row[1] == "DistinctiveEngineError"
    assert row[1] != "PipelineStepFailed"


def _everything(tmp_path):
    out = []
    for (t,) in _query(tmp_path, "select table_name from information_schema.tables where table_schema='raw'"):
        out += [str(c) for r in _query(tmp_path, f'select * from raw."{t}"') for c in r]
    return out


def test_no_exception_message_reaches_the_row(tmp_path, monkeypatch):
    _make_resource_raise(monkeypatch, DistinctiveEngineError(f"dsn=postgres://u:{SECRET}@h/db"))
    _run(tmp_path, monkeypatch, ingest_runs=True)
    (row,) = _runs(tmp_path)
    # a digest of the ROOT message; see the digest tests at the end of the module
    assert row[2] and len(row[2]) == 12 and int(row[2], 16) >= 0
    assert row[2] != hashlib.sha256(SECRET.encode()).hexdigest()[:12]
    assert not any(SECRET in c for c in _everything(tmp_path))


def test_control_the_scan_finds_a_planted_value(tmp_path, monkeypatch):
    _run(tmp_path, monkeypatch, ingest_runs=True)
    con = duckdb.connect((tmp_path / "w.duckdb").as_posix())
    con.execute(f"update raw.ingest_runs set error_type = '{SECRET}'")
    con.close()
    assert any(SECRET in c for c in _everything(tmp_path))

def test_a_failure_writing_the_row_re_raises_the_original_failure(tmp_path, monkeypatch):
    from dag_tools.asset_wrappers import ingest_runs

    _make_resource_raise(monkeypatch, DistinctiveEngineError("original"))

    def broken(pipeline, row):
        raise RuntimeError("recording failed")

    monkeypatch.setattr(ingest_runs, "write_row", broken)
    _, result = _run(tmp_path, monkeypatch, ingest_runs=True)
    assert not result.success
    text = " ".join(
        str(e.event_specific_data) for e in result.all_events if e.is_failure
    )
    assert "DistinctiveEngineError" in text and "recording failed" not in text


def test_ingest_runs_is_off_by_default_and_no_table_is_created(tmp_path, monkeypatch):
    _, result = _run(tmp_path, monkeypatch)
    assert result.success
    assert "ingest_runs" not in _tables(tmp_path)
    assert "widgets" in _tables(tmp_path)


def test_the_row_is_written_by_a_second_run_after_the_data(tmp_path, monkeypatch):
    from dag_tools.asset_wrappers import ingest_runs

    seen = {}
    real = ingest_runs.write_row

    def spy(pipeline, row):
        seen["tables_before"] = _tables(tmp_path)
        seen["counts"] = row["row_counts"]
        real(pipeline, row)

    monkeypatch.setattr(ingest_runs, "write_row", spy)
    _run(tmp_path, monkeypatch, ingest_runs=True)
    assert "widgets" in seen["tables_before"] and "ingest_runs" not in seen["tables_before"]
    assert '"widgets": 1' in seen["counts"]


def test_the_row_digests_the_ROOT_message_not_the_wrapper(tmp_path, monkeypatch):
    """The digest is of the root's own message, exactly, with no wrapper text in it."""
    _make_resource_raise(monkeypatch, DistinctiveEngineError("nope"))
    _run(tmp_path, monkeypatch, ingest_runs=True)
    (row,) = _runs(tmp_path)
    assert row[2] == hashlib.sha256(b"nope").hexdigest()[:12]


def _wrapped(load_id):
    """What dlt hands the asset: its own exception around what the resource raised.

    Raised rather than constructed, and raised ``from`` the inner error, because
    that is what `dlt/pipeline/pipeline.py` does at every one of its five raise
    sites -- a hand-built instance carries no cause chain at all, so a fixture
    that skipped this would report no root and prove nothing about real failures.
    """
    from dlt.pipeline.exceptions import PipelineStepFailed

    class _Pipe:
        pipeline_name = "alpha"
        has_pending_data = False

    try:
        raise DistinctiveEngineError("one root")
    except DistinctiveEngineError as exc:
        try:
            raise PipelineStepFailed(_Pipe(), "extract", load_id, exc) from exc
        except PipelineStepFailed as wrapper:
            return wrapper


def test_two_runs_of_one_root_failure_carry_the_SAME_digest():
    """The whole reason a digest stands in for a message nobody may store.

    A digest in an audit table exists so two rows recording the same failure can
    be grouped. dlt's wrapper embeds a per-run load id, so digesting what the
    asset actually catches makes the column a per-run random value -- which still
    looks like a working column. Measured before the fix: `1278fb1d13d6` and
    `046594cc097a` for one identical root failure.
    """
    from dag_tools.asset_wrappers import ingest_runs

    a, b = _wrapped("1700000000.1234567"), _wrapped("1800000000.7654321")
    # Anti-vacuity: the two wrappers really do differ, so the equality is earned.
    assert str(a) != str(b)
    assert ingest_runs.error_digest(a) == ingest_runs.error_digest(b)
    assert ingest_runs.error_digest(a) == hashlib.sha256(b"one root").hexdigest()[:12]


def test_the_two_error_columns_describe_ONE_exception():
    """`error_type` was already the root; a wrapper digest made the row name two errors."""
    from dag_tools.asset_wrappers import ingest_runs

    failure = _wrapped("1700000000.1234567")
    assert ingest_runs.root_error_name(failure) == "DistinctiveEngineError"
    assert ingest_runs.error_digest(failure) != hashlib.sha256(str(failure).encode()).hexdigest()[:12]
