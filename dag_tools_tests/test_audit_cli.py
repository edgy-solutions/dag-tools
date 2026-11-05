"""`dagtools audit` -- the thin entry point.

The component is the primary surface; this exists for the operator who has an
export and a terminal and no deployment yet. The tests therefore check the
plumbing and the two properties that matter at a console: the report is always
written to a file, and nothing here connects to anything.
"""

import json
from datetime import datetime, timezone

import pytest
from typer.testing import CliRunner

from dag_tools.audit.cli import audit_app
from dag_tools.audit.tag import QueryAttribution, encode, pipeline_principal

runner = CliRunner()

STATEMENT_ARGS = [
    "statement",
    "--relation",
    "db1.sch1.history_view",
    "--query-id-column",
    "c_qid",
    "--started-at-column",
    "c_start",
    "--principal-column",
    "c_user",
    "--query-tag-column",
    "c_tag",
    "--relations-column",
    "c_objects",
    "--since",
    "2026-10-01",
]


def test_the_audit_app_is_reachable_from_the_console_script():
    """It is registered on the `dagtools` app, which is what ships."""
    from dag_tools.qual.cli import app

    result = runner.invoke(app, ["audit", "--help"])
    assert result.exit_code == 0
    assert "reads" in result.output


def test_statement_prints_sql_and_parameters_separately():
    result = runner.invoke(audit_app, STATEMENT_ARGS)
    assert result.exit_code == 0

    payload = json.loads(result.output)
    assert payload["paramstyle"] == "named"
    assert "db1.sch1.history_view" in payload["statement"]
    assert payload["parameters"]["since"].startswith("2026-10-01")
    # The window bound, not written into the text.
    assert "2026-10-01" not in payload["statement"]


def test_statement_refuses_a_relation_that_is_not_an_identifier():
    args = list(STATEMENT_ARGS)
    args[args.index("--relation") + 1] = "db1.sch1.history_view; drop table t"

    result = runner.invoke(audit_app, args)
    assert result.exit_code == 2
    # The error names what was refused -- that is the diagnostic. What must
    # not exist is a statement built around it.
    assert "SELECT" not in (result.stdout or "")


def _rows():
    svc = pipeline_principal("daily_load")
    return [
        {
            "query_id": "q-1",
            "started_at": "2026-10-02T01:00:00+00:00",
            "principal": "svc_account",
            "query_tag": encode(
                QueryAttribution(principal=svc, asset_key="orders_clean")
            ),
            "relations": ["db1.sch1.t_a", "db1.sch1.t_b"],
        },
        {
            "query_id": "q-2",
            "started_at": "2026-10-02T02:00:00+00:00",
            "principal": "svc_account",
            "query_tag": None,
            "relations": ["db1.sch1.t_c"],
        },
    ]


@pytest.fixture()
def events_file(tmp_path):
    path = tmp_path / "events.json"
    path.write_text(json.dumps(_rows()), encoding="utf-8")
    return path


def test_reads_writes_the_report_to_a_file(events_file, tmp_path):
    out = tmp_path / "nested" / "report.json"
    result = runner.invoke(
        audit_app, ["reads", "--events", str(events_file), "--out", str(out)]
    )
    assert result.exit_code == 0, result.output

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["explained_relations"] == ["db1.sch1.t_a", "db1.sch1.t_b"]
    assert payload["unexplained_relations"] == ["db1.sch1.t_c"]
    assert payload["rows_read"] == 2
    assert payload["row_failures"] == []


def test_the_file_is_written_even_in_table_format(events_file, tmp_path):
    """`--format` chooses what is printed, never whether the report exists."""
    out = tmp_path / "report.json"
    result = runner.invoke(
        audit_app,
        ["reads", "--events", str(events_file), "--out", str(out), "--format", "table"],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()
    assert "no asset explains" in result.output


def test_run_windows_close_the_gap_when_supplied(events_file, tmp_path):
    runs = tmp_path / "runs.json"
    runs.write_text(
        json.dumps(
            [
                {
                    "run_id": "run-1",
                    "asset_keys": ["orders_enriched"],
                    "started_at": "2026-10-02T01:55:00+00:00",
                    "ended_at": "2026-10-02T02:10:00+00:00",
                }
            ]
        ),
        encoding="utf-8",
    )
    out = tmp_path / "report.json"
    result = runner.invoke(
        audit_app,
        [
            "reads",
            "--events",
            str(events_file),
            "--runs",
            str(runs),
            "--out",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["unexplained_relations"] == []
    assert payload["relations_per_asset"]["orders_enriched"] == ["db1.sch1.t_c"]


def test_a_malformed_run_window_is_reported_and_the_pass_still_completes(
    events_file, tmp_path
):
    runs = tmp_path / "runs.json"
    runs.write_text(json.dumps([{"run_id": "run-1"}]), encoding="utf-8")
    out = tmp_path / "report.json"

    result = runner.invoke(
        audit_app,
        ["reads", "--events", str(events_file), "--runs", str(runs), "--out", str(out)],
    )
    assert result.exit_code == 0, result.output

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert len(payload["row_failures"]) == 1
    assert payload["event_count"] == 2


def test_a_missing_events_file_exits_without_a_traceback(tmp_path):
    result = runner.invoke(
        audit_app, ["reads", "--events", str(tmp_path / "nope.json")]
    )
    assert result.exit_code == 2
    assert result.exception is None or isinstance(result.exception, SystemExit)


def test_the_audit_package_imports_neither_dagster_nor_the_qual_package():
    """It has to run where neither is installed.

    Checked by reading the sources rather than by import side effects, which
    a previously-imported module would hide.
    """
    import pathlib

    import dag_tools.audit as pkg

    root = pathlib.Path(pkg.__file__).parent
    for module in sorted(root.glob("*.py")):
        text = module.read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if not (stripped.startswith("import ") or stripped.startswith("from ")):
                continue
            assert "dagster" not in stripped, f"{module.name}: {stripped}"
            assert "dag_tools.qual" not in stripped, f"{module.name}: {stripped}"


def test_nothing_in_the_audit_package_names_an_object():
    """The standing rule, pinned.

    No relation, column or level literal lives in this code: the operator's
    names arrive as data and leave as data. The check is narrow on purpose --
    it looks for the column aliases being given warehouse-specific spellings
    as defaults, which is how a name would realistically creep in.
    """
    import inspect

    from dag_tools.audit import history

    signature = inspect.signature(history.HistoryColumns)
    # Every field is required; a default here would be a name in the repo.
    for name, parameter in signature.parameters.items():
        if name in {"self", "args", "kwargs"}:
            continue
        assert parameter.default is inspect.Parameter.empty, name


def test_statement_takes_a_window_end_when_given_one():
    args = STATEMENT_ARGS + ["--until", "2026-10-08"]
    result = runner.invoke(audit_app, args)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["parameters"]["until"].startswith("2026-10-08")


def test_statement_binds_the_principal_filter_when_given_one():
    args = STATEMENT_ARGS + ["--principal", "svc_account"]
    result = runner.invoke(audit_app, args)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["parameters"]["principal_filter"] == "svc_account"
    assert "svc_account" not in payload["statement"]


def test_the_report_carries_a_generated_at_a_reader_can_order_by():
    report_rows = _rows()
    from dag_tools.audit.history import rows_to_events
    from dag_tools.audit.reads import build_reads_report

    events, _ = rows_to_events(report_rows)
    report = build_reads_report(events, generated_at=datetime(2026, 11, 1, tzinfo=timezone.utc))
    assert report.generated_at == datetime(2026, 11, 1, tzinfo=timezone.utc)
