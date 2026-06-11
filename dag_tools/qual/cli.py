"""The ``dagtools`` console-script entry point.

Top-level Typer app with sub-apps per concern. Phase rollout matches the
recipe — only what's already implemented is wired here, but the surface
is shaped so future sub-commands slot in without breaking flags:

  dagtools registry status [...]   ← Phase 1 step 2, implemented
  dagtools survey [...]            ← Phase 1 step 3, not yet wired
  dagtools canary [...]            ← Phase 1 optional, not yet wired
  dagtools qual {init|classes|...} ← Phase 2, not yet wired

Recipe rule (machine-readable by default):

  > Every command is idempotent and emits machine-readable JSON.

So every command has ``--format`` defaulting to ``json``. ``--format table``
is available for human consumption.
"""
from __future__ import annotations

import json
import sys
from datetime import timedelta
from enum import Enum
from typing import Optional

import typer

from .registry import (
    InventoryRegistry,
    StorageSettings,
    compute_staleness,
    layout,
)
from .registry.status import StalenessState


class OutputFormat(str, Enum):
    JSON = "json"
    TABLE = "table"


# --- top-level app ----------------------------------------------------------

app = typer.Typer(
    name="dagtools",
    help=(
        "Dagster Upgrade Regression & Qualification System.\n\n"
        "Configure the registry via --registry / --endpoint-url or the "
        "DAGTOOLS_REGISTRY / DAGTOOLS_S3_ENDPOINT environment variables."
    ),
    no_args_is_help=True,
    add_completion=False,
)


class CliSettings:
    """Carried on ``ctx.obj`` so sub-commands can resolve the registry."""

    def __init__(self, bucket: str, endpoint_url: Optional[str]):
        self.bucket = bucket
        self.endpoint_url = endpoint_url

    def storage_settings(self) -> StorageSettings:
        return StorageSettings(bucket=self.bucket, endpoint_url=self.endpoint_url)

    def registry(self) -> InventoryRegistry:
        return InventoryRegistry.from_settings(self.storage_settings())


@app.callback()
def main(
    ctx: typer.Context,
    registry: str = typer.Option(
        ...,
        "--registry",
        envvar="DAGTOOLS_REGISTRY",
        help="Registry URI, e.g. 's3://dag-tools' (or just the bucket name).",
    ),
    endpoint_url: Optional[str] = typer.Option(
        None,
        "--endpoint-url",
        envvar="DAGTOOLS_S3_ENDPOINT",
        help="S3 endpoint URL for MinIO. Leave unset for real AWS S3.",
    ),
) -> None:
    """Parse registry config and stash it on ctx.obj for sub-commands."""
    try:
        bucket = layout.parse_registry_uri(registry)
    except ValueError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    ctx.obj = CliSettings(bucket=bucket, endpoint_url=endpoint_url)


# --- registry sub-app -------------------------------------------------------

registry_app = typer.Typer(
    name="registry",
    help="Operate on the MinIO/S3 registry that backs the qualification system.",
    no_args_is_help=True,
)
app.add_typer(registry_app)


@registry_app.command("status")
def registry_status(
    ctx: typer.Context,
    max_age_hours: float = typer.Option(
        24.0,
        "--max-age-hours",
        help="Inventory older than this is flagged as stale.",
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.JSON, "--format", help="Output format."
    ),
    exit_nonzero_on_stale: bool = typer.Option(
        False,
        "--exit-nonzero-on-stale/--no-exit-nonzero-on-stale",
        help=(
            "When true, exit with code 2 if any repo is stale/missing/unreadable. "
            "Useful for piping into shell-driven fleet gates."
        ),
    ),
) -> None:
    """Report staleness of every repo's ``latest.json``.

    Pass criterion (per the recipe): every fleet repo has a ``latest.json``
    younger than ``--max-age-hours``. Anything else gets flagged.
    """
    settings: CliSettings = ctx.obj
    reg = settings.registry()
    report = compute_staleness(reg, max_age=timedelta(hours=max_age_hours))

    if format == OutputFormat.JSON:
        typer.echo(report.model_dump_json(indent=2))
    else:
        _print_status_table(report)

    if exit_nonzero_on_stale and (
        report.stale_count or report.missing_count or report.unreadable_count
    ):
        raise typer.Exit(code=2)


def _print_status_table(report) -> None:
    """Human-readable table form. Compact and dependency-free; we don't
    pull in rich/tabulate just for this."""
    typer.echo(
        f"registry status @ {report.generated_at.isoformat()} "
        f"(threshold {report.max_age_seconds:.0f}s)"
    )
    typer.echo(
        f"  total={report.repo_count}  fresh={report.fresh_count}  "
        f"stale={report.stale_count}  missing={report.missing_count}  "
        f"unreadable={report.unreadable_count}"
    )
    if not report.repos:
        typer.echo("  (no repos under inventory/)")
        return
    typer.echo("  REPO".ljust(40) + "STATE".ljust(12) + "AGE (s)".rjust(12) + "  SHA")
    for s in report.repos:
        age = f"{s.age_seconds:.0f}" if s.age_seconds is not None else "-"
        sha = s.pointer.git_sha[:12] if s.pointer else "-"
        typer.echo(
            f"  {s.repo[:38]:38s}{s.state.value:12s}{age:>12s}  {sha}"
        )


if __name__ == "__main__":
    app()
