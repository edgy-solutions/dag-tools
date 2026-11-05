"""`dagtools audit` -- the read-only first step of the classification build.

Two commands, and the split between them is the point:

    dagtools audit statement ...   prints the history read to run
    dagtools audit reads ...       turns the rows it returns into the report

Nothing here connects to a warehouse. The operator runs the statement in their
own console, with their own credentials and their own audit trail, and hands
the rows back. That is not a limitation worked around -- it is the honest
shape for a first pass: this repo has no warehouse to test a connection
against, and a connection path that CI cannot exercise is a path that is
wrong the first time it matters. The part with the judgment in it -- what a
read means, and which asset explains it -- is `audit.reads`, and it is fully
covered without a driver installed.

The report is written to a file because that is what it is for: a number for
a board paper, diffable between windows. Machine-readable JSON by default,
per the recipe rule; `--format table` for reading at a terminal.

This package imports nothing from `dag_tools.qual` and nothing from Dagster.
It is deliberately cheap to run and deliberately able to run where neither is
installed.
"""
from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer


class OutputFormat(str, Enum):
    JSON = "json"
    TABLE = "table"


audit_app = typer.Typer(
    name="audit",
    help=(
        "Read-only audits of what the pipelines actually touch.\n\n"
        "`statement` prints the query-history read to run; `reads` turns its "
        "rows into a report of objects per asset, assets per object, and the "
        "objects no asset explains. Neither command connects to anything."
    ),
    no_args_is_help=True,
)


def _load_json_list(path: Path, what: str) -> List[Dict[str, Any]]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        typer.secho(f"cannot read {what} from {path}: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=2)
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        typer.secho(f"{path} is not JSON: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=2)
    if isinstance(parsed, dict) and isinstance(parsed.get(what), list):
        return parsed[what]
    if not isinstance(parsed, list):
        typer.secho(
            f"{path} should hold a JSON list of {what}, or an object with a "
            f"{what!r} key; got {type(parsed).__name__}",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)
    return parsed


@audit_app.command("statement")
def audit_statement(
    relation: str = typer.Option(
        ...,
        "--relation",
        help="The query-history relation to read, as your account spells it.",
    ),
    query_id_column: str = typer.Option(..., "--query-id-column"),
    started_at_column: str = typer.Option(..., "--started-at-column"),
    principal_column: str = typer.Option(..., "--principal-column"),
    query_tag_column: str = typer.Option(..., "--query-tag-column"),
    relations_column: str = typer.Option(
        ...,
        "--relations-column",
        help="The column holding the objects a statement touched.",
    ),
    since: datetime = typer.Option(..., "--since", formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    until: Optional[datetime] = typer.Option(
        None, "--until", formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]
    ),
    principal: Optional[str] = typer.Option(
        None,
        "--principal",
        help="Restrict to one identity. Unset reads every identity.",
    ),
    fmt: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
):
    """Print the history read to run, with its parameters bound separately.

    No relation or column name is built in -- every one is an argument,
    because they differ per account and because no object name belongs in this
    repo. Values are bound, never interpolated; the relation and the column
    names are checked against a plain-identifier grammar and refused if they
    are anything else.
    """
    from .history import HistoryColumns, HistorySource, UnsafeRelation, build_history_statement

    source = HistorySource(
        relation=relation,
        columns=HistoryColumns(
            query_id=query_id_column,
            started_at=started_at_column,
            principal=principal_column,
            query_tag=query_tag_column,
            relations=relations_column,
        ),
        principal_filter=principal,
    )

    try:
        statement, params = build_history_statement(source, since=since, until=until)
    except UnsafeRelation as exc:
        typer.secho(str(exc), fg=typer.colors.RED)
        raise typer.Exit(code=2)

    rendered = {
        key: (value.isoformat() if isinstance(value, datetime) else value)
        for key, value in params.items()
    }

    if fmt is OutputFormat.JSON:
        typer.echo(
            json.dumps(
                {"statement": statement, "parameters": rendered, "paramstyle": "named"},
                indent=2,
                sort_keys=True,
            )
        )
        return

    typer.echo(statement)
    typer.echo("")
    for key in sorted(rendered):
        typer.echo(f"  :{key} = {rendered[key]}")


@audit_app.command("reads")
def audit_reads(
    events: Path = typer.Option(
        ...,
        "--events",
        help="JSON rows returned by `audit statement`.",
    ),
    runs: Optional[Path] = typer.Option(
        None,
        "--runs",
        help=(
            "JSON run windows (run_id, asset_keys, started_at, ended_at). "
            "Without them nothing correlates by time and every untagged read "
            "stays in the gap."
        ),
    ),
    out: Path = typer.Option(
        Path("audit-reads.json"), "--out", help="Where the report is written."
    ),
    fmt: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
):
    """Report objects per asset, assets per object, and the unexplained rest.

    The report is always written to `--out`; `--format` only chooses what is
    printed to the terminal.
    """
    from .history import rows_to_events
    from .reads import RunWindow, build_reads_report

    rows = _load_json_list(events, "events")
    parsed_events, failures = rows_to_events(rows)

    windows: List[RunWindow] = []
    if runs is not None:
        for index, item in enumerate(_load_json_list(runs, "runs")):
            try:
                windows.append(RunWindow.model_validate(item))
            except Exception as exc:  # noqa: BLE001 -- one bad window, not a bad pass
                failures.append(f"run {index}: {type(exc).__name__}: {exc}")

    report = build_reads_report(parsed_events, windows)
    payload = json.loads(report.model_dump_json())
    payload["rows_read"] = len(rows)
    payload["row_failures"] = failures

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    if fmt is OutputFormat.JSON:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    coverage = report.coverage
    typer.echo(f"report written to {out}")
    typer.echo(f"  rows read            {len(rows)}")
    typer.echo(f"  events               {report.event_count}")
    typer.echo(f"  objects read         {report.relation_count}")
    typer.echo(f"  assets attributed    {len(report.relations_per_asset)}")
    typer.echo(
        "  explained            "
        f"{len(report.explained_relations)}"
        + ("" if coverage is None else f"  ({coverage:.0%})")
    )
    typer.echo(f"  no asset explains    {len(report.unexplained_relations)}")
    typer.echo(f"    ambiguous          {len(report.ambiguous_relations)}")
    typer.echo(f"    untagged           {len(report.untagged_relations)}")
    typer.echo(f"    not a pipeline     {len(report.non_pipeline_relations)}")
    if failures:
        typer.echo(f"  rows not read        {len(failures)}")
    typer.echo("")
    for method in sorted(report.method_counts):
        typer.echo(f"  {method:<20} {report.method_counts[method]}")
