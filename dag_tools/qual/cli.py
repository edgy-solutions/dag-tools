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
    location_name: Optional[str] = typer.Option(
        None, "--location-name",
        help=(
            "Code-location name the test deployment exposes for Q2/Q4 "
            "representative launches (e.g. the user-deployment name, or "
            "'demo_defs.py' for a local `dagster dev -f demo_defs.py`). "
            "Required for `qual run` to target the right location — the "
            "launcher falls back to 'default' otherwise, which real "
            "deployments never use."
        ),
    ),
    job_name: Optional[str] = typer.Option(
        None, "--job-name",
        help="Job to launch reps through. Defaults to Dagster's '__ASSET_JOB'.",
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
            deployment=Deployment(
                graphql_url=graphql_url, auth=auth,
                location_name=location_name, job_name=job_name,
            ),
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


@qual_app.command("classes")
def qual_classes(
    ctx: typer.Context,
    qual_id: str = typer.Option(
        ..., "--id", help="Qualification identifier (must already exist via 'qual init')."
    ),
    allow_overwrite: bool = typer.Option(
        False, "--allow-overwrite",
        help="Allow re-publishing the class matrix for this qual_id.",
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Build and publish the fleet equivalence-class matrix (Q1)."""
    # Lazy import — qual classes pulls the inventory schema, no need to
    # pay that on every `dagtools registry status`.
    from .classes import build_class_matrix, publish_class_matrix

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        matrix = build_class_matrix(qual_id, registry=registry)
        publish_class_matrix(
            matrix, registry=registry, allow_overwrite=allow_overwrite,
        )
    except FileNotFoundError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.secho(f"error: qual classes failed: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    if format == OutputFormat.JSON:
        typer.echo(matrix.model_dump_json(indent=2))
    else:
        _print_classes_table(matrix)


def _print_classes_table(matrix) -> None:
    typer.echo(
        f"classes for {matrix.qual_id} @ {matrix.generated_at.isoformat()}"
    )
    typer.echo(
        f"  assets={matrix.asset_count}  classes={matrix.class_count}  "
        + "  ".join(
            f"{k}={v}" for k, v in matrix.coverage_by_runnability.items()
        )
    )
    typer.echo()
    typer.echo("  HASH         SIZE  REPOS  REPS  COMPUTE      IO MANAGER")
    for cls in matrix.classes:
        io = (cls.key.io_manager_class or "—").rsplit(".", 1)[-1]
        compute = cls.key.compute_kind or "—"
        typer.echo(
            f"  {cls.class_hash}  "
            f"{cls.member_count:4d}  "
            f"{cls.member_repo_count:5d}  "
            f"{len(cls.representatives):4d}  "
            f"{compute[:11]:11s}  {io}"
        )


@qual_app.command("run")
def qual_run(
    ctx: typer.Context,
    qual_id: str = typer.Option(..., "--id", help="Qualification identifier."),
    side: str = typer.Option(
        ..., "--side",
        help="Which side to run: 'baseline' or 'candidate'.",
    ),
    retry_failed: bool = typer.Option(
        False, "--retry-failed",
        help="Re-launch representatives currently in FAILED state.",
    ),
    only_class: Optional[str] = typer.Option(
        None, "--only-class",
        help="Restrict the run to one equivalence class (the 12-char hash).",
    ),
    poll_interval: float = typer.Option(
        5.0, "--poll-interval",
        help="Seconds between run-status polls.",
    ),
    poll_timeout: float = typer.Option(
        1800.0, "--poll-timeout",
        help="Per-rep poll timeout in seconds.",
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Q2/Q4: launch representatives through the test deployment, poll
    to completion, persist run records, mirror state for resumability."""
    if side not in ("baseline", "candidate"):
        typer.secho(
            f"error: --side must be 'baseline' or 'candidate', got {side!r}",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)

    # Lazy imports — runs pulls dagster's graphql layer, no need to pay
    # that cost on registry-only commands.
    from .runs import run_side

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        outcome = run_side(
            qual_id=qual_id,
            side=side,
            registry=registry,
            poll_interval_seconds=poll_interval,
            poll_timeout_seconds=poll_timeout,
            retry_failed=retry_failed,
            only_class=only_class,
        )
    except FileNotFoundError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.secho(f"error: qual run failed: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    if format == OutputFormat.JSON:
        typer.echo(outcome.summary.model_dump_json(indent=2))
    else:
        _print_run_table(outcome)

    if outcome.summary.failed or outcome.summary.launched or outcome.summary.pending:
        # Non-zero exit on any non-terminal-or-failed state so CI can gate.
        raise typer.Exit(code=2)


def _print_run_table(outcome) -> None:
    s = outcome.summary
    typer.echo(
        f"qual run: {s.qual_id} side={s.side}  "
        f"total={s.rep_total}  passed={s.passed}  failed={s.failed}  "
        f"launched={s.launched}  pending={s.pending}  skipped={s.skipped}"
    )
    for rep in outcome.state.reps.values():
        ak = "/".join(rep.asset_key)
        status = rep.status.value if hasattr(rep.status, "value") else str(rep.status)
        typer.echo(
            f"  [{status:9s}] {rep.class_hash}  {rep.repo:30s}  {ak}  "
            f"run={rep.run_id or '-'}"
        )
        # Surface the failure reason inline — otherwise a failed/skipped rep
        # shows no cause and the operator has to dig into the state file.
        if rep.error:
            typer.echo(f"      └─ {rep.error}")


@qual_app.command("preflight")
def qual_preflight(
    ctx: typer.Context,
    qual_id: str = typer.Option(..., "--id", help="Qualification identifier."),
    side: str = typer.Option(
        ..., "--side",
        help="Which side to preflight: 'baseline' or 'candidate'.",
    ),
    sample_size: int = typer.Option(
        5, "--sample-size",
        help=(
            "Number of PASSED baseline runs to spot-check via GraphQL on "
            "the candidate side (event-log back-compat check). Ignored on "
            "the baseline side."
        ),
    ),
    allow_overwrite: bool = typer.Option(
        False, "--allow-overwrite",
        help="Allow re-publishing the preflight report after fixing failures.",
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Q3: gate the test deployment is ready for baseline/candidate work.

    Three checks:
      1. Deployment version matches the manifest's baseline/candidate.
      2. Every code location is loaded (fleet-wide load validation under
         the side's Dagster, on real infrastructure).
      3. (Candidate only) Sampled baseline runs still render — the
         event-log back-compat spot check.
    """
    if side not in ("baseline", "candidate"):
        typer.secho(
            f"error: --side must be 'baseline' or 'candidate', got {side!r}",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)

    from .preflight import publish_preflight_report, run_preflight

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        report = run_preflight(
            qual_id=qual_id, side=side,
            registry=registry,
            run_sample_size=sample_size,
        )
        publish_preflight_report(
            report, registry=registry, allow_overwrite=allow_overwrite,
        )
    except FileNotFoundError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.secho(
            f"error: qual preflight failed: {e}",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)

    if format == OutputFormat.JSON:
        typer.echo(report.model_dump_json(indent=2))
    else:
        _print_preflight_table(report)

    if not report.passed:
        raise typer.Exit(code=2)


def _print_preflight_table(report) -> None:
    typer.echo(
        f"qual preflight: {report.qual_id} side={report.side}  "
        f"version: deployment={report.deployment_version!r} expected={report.expected_version!r}  "
        f"passed={'YES' if report.passed else 'NO'}"
    )
    for check in report.checks:
        mark = "PASS" if check.passed else "FAIL"
        typer.echo(f"  [{mark}] {check.name}: {check.detail}")


@qual_app.command("report")
def qual_report(
    ctx: typer.Context,
    qual_id: str = typer.Option(..., "--id", help="Qualification identifier."),
    accept_co_upgrade_risks: bool = typer.Option(
        False, "--accept-co-upgrade-risks",
        help=(
            "Operator opt-in: every co_upgrade_risk in the manifest has been "
            "separately validated or pinned back. Without this flag, any "
            "risk in the manifest blocks GO."
        ),
    ),
    accept_synthetic_coverage_missing: bool = typer.Option(
        False, "--accept-synthetic-coverage-missing",
        help=(
            "Operator opt-in: there are SYNTHETIC_REQUIRED classes without "
            "probe coverage (Q5 not implemented). Required to GO when any "
            "synthetic class exists."
        ),
    ),
    accept_orchestration_deferred: bool = typer.Option(
        False, "--accept-orchestration-deferred",
        help=(
            "Operator opt-in: orchestration snapshot diffs are not "
            "produced yet. Required to GO until orchestration support lands."
        ),
    ),
    allow_overwrite: bool = typer.Option(
        False, "--allow-overwrite",
        help="Allow re-publishing the verdict for this qual_id.",
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Q6: build the GO/NO-GO verdict from all qualification artifacts."""
    from .verdict import GapAcceptance, build_verdict, publish_verdict

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        verdict = build_verdict(
            qual_id=qual_id,
            registry=registry,
            gaps=GapAcceptance(
                co_upgrade_risks=accept_co_upgrade_risks,
                synthetic_coverage_missing=accept_synthetic_coverage_missing,
                orchestration_deferred=accept_orchestration_deferred,
            ),
        )
        publish_verdict(
            verdict, registry=registry, allow_overwrite=allow_overwrite,
        )
    except FileNotFoundError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.secho(f"error: qual report failed: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    if format == OutputFormat.JSON:
        typer.echo(verdict.model_dump_json(indent=2))
    else:
        _print_verdict_table(verdict)

    # NO_GO -> exit 2 so CI / shell scripts can gate.
    if verdict.status.value != "go":
        raise typer.Exit(code=2)


def _print_verdict_table(verdict) -> None:
    headline = "GO" if verdict.status.value == "go" else "NO-GO"
    typer.echo(
        f"qual report: {verdict.qual_id}  "
        f"baseline={verdict.baseline_version}  candidate={verdict.candidate_version}  "
        f"=> {headline}"
    )
    typer.echo(
        f"  classes={verdict.class_count}  "
        f"runnable={verdict.runnable_classes_total} (red={len(verdict.runnable_classes_red)})  "
        f"synthetic={verdict.synthetic_classes_total}  "
        f"observe_only={verdict.observe_only_classes_total}"
    )
    if verdict.blocking_issues:
        typer.echo("  Blocking issues:")
        for issue in verdict.blocking_issues:
            typer.echo(f"    - {issue}")


@qual_app.command("synthetic")
def qual_synthetic(
    ctx: typer.Context,
    qual_id: str = typer.Option(..., "--id", help="Qualification identifier."),
    skip_publish: bool = typer.Option(
        False, "--skip-publish",
        help="Write probe modules locally but do NOT push to the registry.",
    ),
    skip_local: bool = typer.Option(
        False, "--skip-local",
        help="Push to registry but do NOT write a local copy.",
    ),
    local_path: Optional[Path] = typer.Option(
        None, "--local-path",
        help="Override local probes directory (default ~/.dagtools/quals/<id>/probes/).",
    ),
    allow_overwrite: bool = typer.Option(
        False, "--allow-overwrite",
        help="Allow re-publishing the probe bundle for this qual_id.",
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Q5: generate synthetic probe modules for every SYNTHETIC_REQUIRED
    class so the operator can deploy them to the dag-tools-probes
    code location.

    What runs:
      1. Read the manifest + class matrix.
      2. For each SYNTHETIC_REQUIRED class, generate a self-contained
         Python module: real IO manager FQN, deterministic dict payload,
         upstream + downstream asset pair, with a fallback to
         InMemoryIOManager so the code location always loads.
      3. Write the bundle to BOTH the registry and a local directory
         the operator pulls from.

    The probe deployment + run-through-Q2 + verdict-coverage-counting is
    the natural follow-up; v1 stops at generation so operators can start
    integrating the probe location into their test deployment.
    """
    from .synthetic import (
        default_local_probes_dir,
        generate_bundle,
        publish_bundle,
        write_local_bundle,
    )

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        bundle = generate_bundle(qual_id, registry=registry)
    except FileNotFoundError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.secho(f"error: qual synthetic failed: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    if not skip_publish:
        try:
            publish_bundle(bundle, registry=registry, allow_overwrite=allow_overwrite)
        except Exception as e:
            typer.secho(
                f"error: probe bundle publish failed: {e}",
                fg=typer.colors.RED, err=True,
            )
            raise typer.Exit(code=2)

    written_path: Optional[Path] = None
    if not skip_local:
        written_path = write_local_bundle(bundle, local_path=local_path)

    if format == OutputFormat.JSON:
        payload = bundle.manifest.model_dump(mode="json")
        payload["published_to_registry"] = not skip_publish
        payload["local_path"] = str(written_path) if written_path else None
        typer.echo(json.dumps(payload, indent=2, default=str))
    else:
        _print_synthetic_table(bundle, written_path=written_path, published=not skip_publish)


# --- qual probes sub-app ----------------------------------------------------


probes_app = typer.Typer(
    name="probes",
    help=(
        "Operate on the dag-tools-probes code location and its probe runs "
        "(Q5c). Operators deploy `dag_tools.probes_location.definitions` "
        "to the test deployment and point DAGTOOLS_PROBES_DIR at the "
        "bundle that `qual synthetic` produced; this sub-app launches "
        "each probe through the deployment and persists per-probe runs."
    ),
    no_args_is_help=True,
)
qual_app.add_typer(probes_app)


@probes_app.command("run")
def qual_probes_run(
    ctx: typer.Context,
    qual_id: str = typer.Option(..., "--id", help="Qualification identifier."),
    side: str = typer.Option(
        ..., "--side",
        help="Which side to run probes for: 'baseline' or 'candidate'.",
    ),
    retry_failed: bool = typer.Option(
        False, "--retry-failed",
        help="Re-launch probes currently in FAILED state.",
    ),
    only_class: Optional[str] = typer.Option(
        None, "--only-class",
        help="Restrict the run to one equivalence class (12-char hash).",
    ),
    poll_interval: float = typer.Option(
        5.0, "--poll-interval",
        help="Seconds between run-status polls.",
    ),
    poll_timeout: float = typer.Option(
        1800.0, "--poll-timeout",
        help="Per-probe poll timeout in seconds.",
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Q5c: launch each generated probe through the test deployment's
    dag-tools-probes location, poll to completion, persist per-probe
    RunRecords, mirror state for resumability."""
    if side not in ("baseline", "candidate"):
        typer.secho(
            f"error: --side must be 'baseline' or 'candidate', got {side!r}",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)

    from .probes import run_probes_side

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        outcome = run_probes_side(
            qual_id=qual_id, side=side, registry=registry,
            poll_interval_seconds=poll_interval,
            poll_timeout_seconds=poll_timeout,
            retry_failed=retry_failed,
            only_class=only_class,
        )
    except FileNotFoundError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.secho(
            f"error: qual probes run failed: {e}",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)

    if format == OutputFormat.JSON:
        typer.echo(outcome.summary.model_dump_json(indent=2))
    else:
        _print_probes_run_table(outcome)

    if outcome.summary.failed or outcome.summary.launched or outcome.summary.pending:
        raise typer.Exit(code=2)


@probes_app.command("status")
def qual_probes_status(
    ctx: typer.Context,
    qual_id: str = typer.Option(..., "--id", help="Qualification identifier."),
    exit_nonzero_on_gap: bool = typer.Option(
        False, "--exit-nonzero-on-gap/--no-exit-nonzero-on-gap",
        help=(
            "When true, exit code 2 if the dag-tools-probes location is "
            "not fully loaded (any missing or partially loaded probes, "
            "or the location itself is absent/erroring). Useful for "
            "operator scripts that gate the next step on a clean deploy."
        ),
    ),
    format: OutputFormat = typer.Option(OutputFormat.JSON, "--format"),
) -> None:
    """Q5d: cross-reference the probe manifest against the test
    deployment's dag-tools-probes location.

    Reports per probe: fully loaded vs partially loaded vs missing; and
    flags unexpected probe-shaped asset keys present in the location
    but absent from the current manifest (usually stale files)."""
    from .probes import check_probes_status

    settings: CliSettings = ctx.obj
    registry = settings.registry()

    try:
        report = check_probes_status(qual_id, registry=registry)
    except FileNotFoundError as e:
        typer.secho(f"error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.secho(
            f"error: qual probes status failed: {e}",
            fg=typer.colors.RED, err=True,
        )
        raise typer.Exit(code=2)

    if format == OutputFormat.JSON:
        typer.echo(report.model_dump_json(indent=2))
    else:
        _print_probes_status_table(report)

    if exit_nonzero_on_gap and not report.all_loaded:
        raise typer.Exit(code=2)


def _print_probes_status_table(report) -> None:
    typer.echo(
        f"qual probes status: {report.qual_id}  location={report.location_name}  "
        f"status={report.location_load_status}"
    )
    if report.location_error:
        typer.echo(f"  location error: {report.location_error}")
    typer.echo(
        f"  expected={report.expected_class_count}  "
        f"fully_loaded={report.fully_loaded_class_count}  "
        f"partial={len(report.partially_loaded)}  "
        f"missing={len(report.missing_class_hashes)}  "
        f"unexpected={len(report.unexpected_probe_asset_keys)}"
    )
    for ch in report.missing_class_hashes:
        typer.echo(f"  MISSING:    {ch}")
    for c in report.partially_loaded:
        bits = []
        if not c.upstream_loaded:
            bits.append("upstream missing")
        if not c.downstream_loaded:
            bits.append("downstream missing")
        typer.echo(f"  PARTIAL:    {c.class_hash}  ({', '.join(bits)})")
    for ak in report.unexpected_probe_asset_keys:
        typer.echo(f"  UNEXPECTED: {'/'.join(ak)}")


def _print_probes_run_table(outcome) -> None:
    s = outcome.summary
    typer.echo(
        f"qual probes run: {s.qual_id} side={s.side}  "
        f"total={s.probe_total}  passed={s.passed}  failed={s.failed}  "
        f"launched={s.launched}  pending={s.pending}  skipped={s.skipped}"
    )
    for probe in outcome.state.probes.values():
        status = probe.status.value if hasattr(probe.status, "value") else str(probe.status)
        typer.echo(
            f"  [{status:9s}] {probe.class_hash}  {probe.module_name}  "
            f"run={probe.run_id or '-'}"
        )


def _print_synthetic_table(bundle, *, written_path: Optional[Path], published: bool) -> None:
    m = bundle.manifest
    typer.echo(
        f"qual synthetic: {m.qual_id}  synthetic_classes={m.synthetic_class_count}  "
        f"probes={len(m.probes)}  skipped={len(m.skipped_class_hashes)}"
    )
    if published:
        typer.echo(f"  published to registry")
    if written_path is not None:
        typer.echo(f"  local: {written_path}")
    for p in m.probes:
        notes_str = f" ({len(p.notes)} note(s))" if p.notes else ""
        typer.echo(
            f"  - {p.class_hash}  {p.module_name}  "
            f"io_manager={p.io_manager_class or '(none)'}{notes_str}"
        )
    for ch in m.skipped_class_hashes:
        typer.echo(f"  SKIPPED: {ch}")


if __name__ == "__main__":
    app()
