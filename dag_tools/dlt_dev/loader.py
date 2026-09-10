"""Read a component's defs.yaml and build its dlt pipelines, without Dagster
loading the code location.

WHAT THIS IS AND IS NOT. It does not avoid importing Dagster -- the dlt
factory imports it -- and it does not need to. The expensive thing is
loading YOUR definitions module, which pulls Dagster, dbt, dlt, datahub and
every component in the location, takes 90-180 seconds, and fails entirely
if any ONE of those components fails to import. This reads the YAML for one
pipeline and builds that pipeline.

THE ENV-VAR SUBSTITUTION IS DELIBERATELY SMALL. Component YAML resolves
``{{ env.NAME }}`` through Dagster's component loader; this resolves the
same form itself, because pulling in the loader would pull in the thing
being avoided. Anything richer -- conditionals, filters, other Jinja --
is not supported and says so rather than rendering something surprising.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


ENV_TEMPLATE = re.compile(r"\{\{\s*env\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")

#: Jinja that is NOT a bare `{{ env.NAME }}`. Rendering it here would give a
#: different answer than Dagster does, which is worse than refusing.
UNSUPPORTED_TEMPLATE = re.compile(r"\{\{(?!\s*env\.[A-Za-z_][A-Za-z0-9_]*\s*\}\})")


class DefsError(Exception):
    """The defs file cannot be turned into a runnable pipeline."""


def referenced_env_vars(raw: str, doc: Any = None) -> List[str]:
    """Every environment variable this defs file needs, in both forms.

    Config declares credentials two ways -- ``{{ env.NAME }}`` in the YAML
    text and ``{env: NAME}`` as a mapping the factory resolves -- so
    "what do I need to set" is derivable rather than something to
    hand-maintain in a .env.example that drifts.
    """
    found = list(dict.fromkeys(ENV_TEMPLATE.findall(raw)))

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            name = node.get("env")
            if isinstance(name, str) and name not in found:
                found.append(name)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(doc if doc is not None else yaml.safe_load(raw))
    return found


def missing_env_vars(names: List[str]) -> List[str]:
    return [n for n in names if not os.environ.get(n)]


def load_defs(path: str) -> Tuple[Dict[str, Any], List[str]]:
    """Parse a defs.yaml with ``{{ env.NAME }}`` resolved.

    Returns ``(document, referenced_env_var_names)``. Unset variables
    substitute as empty rather than raising here, so ``dagtools dlt env``
    can report the whole list at once instead of one per run.
    """
    text = Path(path).read_text(encoding="utf-8")

    unsupported = UNSUPPORTED_TEMPLATE.search(text)
    if unsupported:
        line = text[: unsupported.start()].count("\n") + 1
        raise DefsError(
            f"{path}:{line} uses templating beyond '{{{{ env.NAME }}}}'. Only "
            f"that form is resolved here; anything else would render "
            f"differently than Dagster does, and a config that means one "
            f"thing locally and another when deployed is worse than a "
            f"refusal."
        )

    names = referenced_env_vars(text)
    resolved = ENV_TEMPLATE.sub(lambda m: os.environ.get(m.group(1), ""), text)
    doc = yaml.safe_load(resolved) or {}
    return doc, names


def pipeline_names(doc: Dict[str, Any]) -> List[str]:
    attributes = doc.get("attributes") or {}
    return list((attributes.get("pipelines") or {}).keys())


def build_runnables_from_defs(
    path: str,
    pipeline: Optional[str] = None,
    destination: Optional[str] = None,
    limit: Optional[int] = None,
    tables: Optional[List[str]] = None,
):
    """Turn one pipeline in a defs.yaml into ``DltRunnable`` objects.

    Imported lazily so ``dagtools`` subcommands that have nothing to do
    with dlt keep working in an install without the orchestrator extra.
    """
    from dag_tools.asset_wrappers.dlt_assets_factory import DltAssetGroupConfig
    from dag_tools.asset_wrappers.dlt_assets_parsing import build_dlt_runnables

    doc, _names = load_defs(path)
    attributes = doc.get("attributes") or {}
    pipelines = attributes.get("pipelines") or {}

    if not pipelines:
        raise DefsError(f"{path} declares no pipelines under attributes.pipelines")

    if pipeline is None:
        if len(pipelines) > 1:
            raise DefsError(
                f"{path} declares {len(pipelines)} pipelines "
                f"({', '.join(sorted(pipelines))}); name one with --pipeline"
            )
        pipeline = next(iter(pipelines))
    if pipeline not in pipelines:
        raise DefsError(
            f"{path} has no pipeline {pipeline!r}. Available: "
            f"{', '.join(sorted(pipelines)) or '<none>'}"
        )

    attrs = dict(pipelines[pipeline])
    sources = list(tables) if tables else list(attrs.pop("sources", []) or [])
    if not sources:
        raise DefsError(f"pipeline {pipeline!r} declares no sources")

    # table_config is a component-level convenience that generates dlt
    # hints; fold it in so a local run sees the same index and cursor the
    # deployed asset does.
    raw_table_config = attrs.pop("table_config", None) or {}
    if raw_table_config:
        from dag_tools.components.restate_dlt_sync.component import build_table_hints
        from dag_tools.components.restate_dlt_sync.config import TableSpec

        specs = {
            name: TableSpec.model_validate(spec)
            for name, spec in raw_table_config.items()
            if name in sources
        }
        attrs["hints"] = build_table_hints(specs, attrs.get("hints") or {})

    # Anything that is not a DltAssetGroupConfig field is component
    # machinery -- control tables, sensors, ack settings. Filtering by the
    # model's own fields means a new component key does not break this.
    allowed = set(DltAssetGroupConfig.model_fields)
    kwargs = {k: v for k, v in attrs.items() if k in allowed}
    kwargs.setdefault("name", pipeline)
    if limit is not None:
        kwargs["limit"] = limit

    destination_override = _destination(destination)

    return build_dlt_runnables(
        sources,
        attributes.get("source_config") or {},
        attributes.get("dest_config") or {},
        DltAssetGroupConfig(**kwargs),
        staging_config=attributes.get("staging_config"),
        destination_override=destination_override,
    )


def _destination(spec: Optional[str]):
    """Resolve ``--destination`` into a dlt destination.

    A bare path or ``duckdb:///path`` gives a local DuckDB file, which is
    the point: run the REAL source -- real reflection, real hints, real
    cursors -- and land it somewhere disposable, so the fast loop needs no
    warehouse credential at all.
    """
    if not spec:
        return None

    import dlt

    if spec.startswith("duckdb://"):
        return dlt.destinations.duckdb(spec.replace("duckdb://", "", 1).lstrip("/"))
    if spec.endswith(".duckdb") or spec == ":memory:":
        return dlt.destinations.duckdb(spec)
    raise DefsError(
        f"unrecognised --destination {spec!r}. Use a DuckDB path "
        f"(./dev.duckdb, duckdb:///tmp/dev.duckdb, or :memory:); other "
        f"destinations come from the defs file so a local run cannot "
        f"accidentally point at one that needs credentials."
    )
