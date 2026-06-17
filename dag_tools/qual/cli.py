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
from pathlib import Path
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


# --- survey command ---------------------------------------------------------


@app.command("survey")
def survey(
    ctx: typer.Context,
    introspect: bool = typer.Option(
        True,
        "--introspect/--no-introspect",
        help=(
            "Run inventory introspection. Currently the only mode; canary is a "
            "separate command. Default true."
        ),
    ),
    locations: str = typer.Option(
        ...,
        "--locations",
        help=(
            "Path to a workspace.yaml, a .py file, or a module spec "
            "'pkg.mod[:attr]'. Every code location is loaded."
        ),
    ),
    repo: str = typer.Option(..., "--repo", help="Repository name; primary registry partition key."),
    sha: str = typer.Option(..., "--sha", help="Git SHA of this build."),
    build: Optional[str] = typer.Option(
        None, "--build", help="CI build identifier (optional but recommended)."
    ),
    allow_overwrite: bool = typer.Option(
        False, "--allow-overwrite",
        help=(
            "Permit re-publishing the same SHA. Off by default — the recipe "
            "treats per-build keys as immutable. Useful for CI retries when "
            "you genuinely intend to replace the previous publish."
        ),
    ),
    skip_publish: bool = typer.Option(
        False, "--skip-publish",
        help=(
            "Run introspection and emit the result locally but do NOT write "
            "to the registry. Useful for dev iteration and dry runs."
        ),
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format", help="Output format."),
) -> None:
    """Publish a per-build structural inventory for one repo to the registry.

    Recipe rule: a load failure fails the Jenkins stage and **nothing is
    published** — the registry never contains an inventory for code that
    doesn't load. Exits non-zero on load failure.
    """
    # Lazy import — keeps `dagtools registry status` fast and free of
    # any dagster dependency.
    from .survey import run_survey
    from .survey.publisher import _detect_dagster_version, _detect_dagtools_version

    if not introspect:
        typer.secho(
            "error: --no-introspect disables the only currently-supported mode",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    outcome = run_survey(
        locations_spec=locations,
        repo=repo,
        git_sha=sha,
        registry=registry,
        build_id=build,
        dagster_version=_detect_dagster_version(),
        dagtools_version=_detect_dagtools_version(),
        allow_overwrite=allow_overwrite,
        skip_publish=skip_publish,
    )

    payload = {
        "published": outcome.published,
        "pointer_sha": outcome.pointer_sha,
        "artifacts_written": outcome.artifacts_written,
        "load_validation": outcome.load_validation.model_dump(mode="json"),
    }

    if format == OutputFormat.JSON:
        typer.echo(json.dumps(payload, indent=2, default=str))
    else:
        _print_survey_table(outcome)

    if not outcome.load_validation.loads:
        raise typer.Exit(code=2)


def _print_survey_table(outcome) -> None:
    lv = outcome.load_validation
    typer.echo(
        f"survey @ {lv.timestamp.isoformat()}  "
        f"loaded={len(lv.locations)}  failed={len(lv.failures)}  "
        f"warnings={len(lv.warnings)}  published={outcome.published}"
    )
    if lv.failures:
        typer.echo("  FAILURES:")
        for f in lv.failures:
            typer.echo(f"    - {f.name} ({f.source}): {f.error}")
    if lv.locations:
        typer.echo("  LOCATIONS:")
        for loc in lv.locations:
            typer.echo(
                f"    - {loc.name}  assets={loc.asset_count or 0}  "
                f"sensors={loc.sensor_count or 0}  schedules={loc.schedule_count or 0}  "
                f"checks={loc.asset_check_count or 0}"
            )
    if outcome.published:
        typer.echo(f"  PUBLISHED: pointer_sha={outcome.pointer_sha}")
        typer.echo(f"  ARTIFACTS: {', '.join(outcome.artifacts_written)}")


# --- qual sub-app -----------------------------------------------------------


qual_app = typer.Typer(
    name="qual",
    help=(
        "Orchestrate a Dagster upgrade qualification (Phase 2): manifest "
        "creation (Q0), equivalence-class matrix (Q1), baseline/candidate "
        "runs through the k8s test deployment (Q2/Q4), preflight checks "
        "(Q3), synthetic probes (Q5), and the verdict (Q6)."
    ),
    no_args_is_help=True,
)
app.add_typer(qual_app)


@qual_app.command("init")
def qual_init(
    ctx: typer.Context,
    qual_id: str = typer.Option(
        ..., "--id", help="Qualification identifier, e.g. '2026-06-15-dagster-1.12'."
    ),
    baseline_version: str = typer.Option(
        ..., "--baseline", help="Baseline Dagster version, e.g. '1.10.6'."
    ),
    candidate_version: str = typer.Option(
        ..., "--candidate", help="Candidate Dagster version, e.g. '1.12.1'."
    ),
    baseline_pins: Optional[Path] = typer.Option(
        None, "--baseline-pins",
        help="YAML file of baseline pins, e.g. {dagster-dbt: 0.27.0, dbt-core: 1.8.5}.",
    ),
    candidate_pins: Optional[Path] = typer.Option(
        None, "--candidate-pins",
        help="YAML file of candidate pins.",
    ),
    graphql_url: Optional[str] = typer.Option(
        None, "--graphql-url",
        help="Test deployment Dagster GraphQL endpoint. Recorded for Q2/Q3/Q4.",
    ),
    graphql_auth_env: Optional[str] = typer.Option(
        None, "--graphql-auth-env",
        help="Env var name containing the bearer token for the GraphQL endpoint.",
    ),
    staging_overrides: Optional[str] = typer.Option(
        None, "--staging-overrides",
        help="Pointer (typically s3://...) to the staging resource override config.",
    ),
    prefer_tag: str = typer.Option(
        "regression", "--prefer-tag",
        help="When picking representatives, prefer assets tagged with this.",
    ),
    reps_per_class: int = typer.Option(
        2, "--reps-per-class",
        help="Representatives to pick per equivalence class (Q1).",
    ),
    local_path: Optional[Path] = typer.Option(
        None, "--local-path",
        help="Override local manifest path (default ~/.dagtools/quals/<id>/manifest.yaml).",
    ),
    allow_overwrite: bool = typer.Option(
        False, "--allow-overwrite",
        help="Allow re-initializing an existing qual_id. Off by default.",
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Pin the registry snapshot + version pair into a qualification manifest."""
    # Lazy imports — keep registry-only commands lightweight.
    from .qualify import (
        Deployment,
        QualificationManifest,
        Selection,
        VersionTarget,
        create_qualification,
    )

    baseline_pins_dict = _load_pins_file(baseline_pins)
    candidate_pins_dict = _load_pins_file(candidate_pins)

    auth = f"env:{graphql_auth_env}" if graphql_auth_env else None

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        manifest = create_qualification(
            qual_id=qual_id,
            registry=registry,
            baseline=VersionTarget(dagster=baseline_version, pins=baseline_pins_dict),
            candidate=VersionTarget(dagster=candidate_version, pins=candidate_pins_dict),
            deployment=Deployment(graphql_url=graphql_url, auth=auth),
            staging_overrides=staging_overrides,
            selection=Selection(prefer_tag=prefer_tag, reps_per_class=reps_per_class),
            local_path=local_path,
            allow_overwrite=allow_overwrite,
        )
    except Exception as e:
        typer.secho(f"error: qual init failed: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    if format == OutputFormat.JSON:
        typer.echo(manifest.model_dump_json(indent=2, by_alias=True))
    else:
        _print_manifest_table(manifest)


def _load_pins_file(path: Optional[Path]) -> dict:
    """Parse a pins YAML file into a flat dict. Empty / missing returns {}."""
    if path is None:
        return {}
    import yaml
    try:
        with path.open() as f:
            doc = yaml.safe_load(f) or {}
    except Exception as e:
        typer.secho(
            f"error: failed to read pins file {path}: {e}",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)
    if not isinstance(doc, dict):
        typer.secho(
            f"error: pins file {path} must contain a mapping at top level",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)
    # All values to strings for stable diffing.
    return {str(k): str(v) for k, v in doc.items()}


def _print_manifest_table(manifest) -> None:
    typer.echo(f"qual init: {manifest.qual_id} @ {manifest.created_at.isoformat()}")
    typer.echo(
        f"  versions: baseline={manifest.baseline.dagster}  "
        f"candidate={manifest.candidate.dagster}"
    )
    typer.echo(f"  inventory pinned: {len(manifest.inventory_pins)} repo(s)")
    for pin in manifest.inventory_pins:
        sha = pin.git_sha[:12] if pin.git_sha else "-"
        typer.echo(f"    - {pin.repo:40s}  sha={sha}")
    if manifest.co_upgrade_risks:
        typer.echo(f"  co_upgrade_risks: {len(manifest.co_upgrade_risks)}")
        for r in manifest.co_upgrade_risks:
            typer.echo(
                f"    - [{r.severity:7s}] {r.lib}: "
                f"{r.from_version} -> {r.to_version}"
            )
    else:
        typer.echo("  co_upgrade_risks: none")
    if manifest.deployment.graphql_url:
        typer.echo(f"  deployment: {manifest.deployment.graphql_url}")


if __name__ == "__main__":
    app()
