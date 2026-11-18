"""``dagtools documents`` -- profile document bundles before designing ingestion."""
from __future__ import annotations

import logging
from typing import List, Optional

import typer

documents_app = typer.Typer(
    name="documents",
    help="Document bundle tools: profile zips or directory trees of XML/JSON documents and images.",
    no_args_is_help=True,
)


def _require_documents_extra() -> None:
    try:
        import lxml  # noqa: F401
        import PIL  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as e:
        typer.secho(
            f"The documents tools need optional dependencies: {e}\n"
            "    pip install 'edgy-dag-tools[documents]'",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)


@documents_app.command("profile")
def profile(
    source: Optional[str] = typer.Option(
        None, "--source", help="Local directory or s3://bucket/prefix holding the bundles."),
    endpoint_url: Optional[str] = typer.Option(None, "--endpoint-url", help="S3-compatible endpoint."),
    out: str = typer.Option("./profile", "--out", help="Output directory (also holds the checkpoint)."),
    bundle_depth: int = typer.Option(0, "--bundle-depth", help="Directory depth that defines a bundle."),
    sample: int = typer.Option(0, "--sample", help="Stop after N new bundles (0 = all)."),
    examples: int = typer.Option(3, "--examples", help="Example values kept per path."),
    redact: bool = typer.Option(False, "--redact", help="Replace example values with hashes."),
    max_bundle_bytes: float = typer.Option(0, "--max-bundle-bytes", help="Skip bundles larger than this."),
    max_docs: int = typer.Option(0, "--max-docs", help="Documents profiled per bundle (0 = all)."),
    max_images: int = typer.Option(2000, "--max-images", help="Image headers read per bundle."),
    heartbeat: float = typer.Option(30, "--heartbeat", help="Seconds between progress lines (0 = off)."),
    anon_bundles: bool = typer.Option(False, "--anon-bundles", help="Anonymise bundle names in the report."),
    scrub: Optional[List[str]] = typer.Option(None, "--scrub", help="Regex to scrub from report text; repeatable."),
    fresh: bool = typer.Option(False, "--fresh", help="Discard the checkpoint and start over."),
    report_only: bool = typer.Option(False, "--report-only", help="Rebuild the report from the checkpoint."),
) -> None:
    """Profile bundles into summary.md, CSVs, parquet, shapes.json and paths/<family>.txt."""
    _require_documents_extra()
    from dag_tools.documents.profiler import ProfileError, run_profile
    from dag_tools.documents.sources import SourceError

    if not report_only and not source:
        typer.secho("--source is required unless --report-only", fg=typer.colors.RED)
        raise typer.Exit(code=2)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        res = run_profile(
            source=source, out=out, endpoint_url=endpoint_url, bundle_depth=bundle_depth,
            sample=sample, examples=examples, redact=redact, max_bundle_bytes=max_bundle_bytes,
            max_docs=max_docs, max_images=max_images, heartbeat=heartbeat,
            anon_bundles=anon_bundles, scrub=scrub or (), fresh=fresh, report_only=report_only)
    except (SourceError, ProfileError) as e:
        typer.secho(str(e), fg=typer.colors.RED)
        raise typer.Exit(code=2)
    typer.echo(f"report: {res.summary_path}  bundles: {res.bundles_in_checkpoint}  errors: {res.errors}")
    if res.exit_code:
        raise typer.Exit(code=res.exit_code)
