"""Declarative component: ClickHouse telemetry -> ordered API calls via Restate.

Shape of a pipeline, per `AGENTS.md` §2 (Declarative Component First):

* an extraction ``@multi_asset`` — dlt reads ClickHouse into the staging
  destination (skipped entirely when ``staged: false``);
* a ``<pipeline>_dispatch`` ``@asset`` — reads rows back, groups them into
  execution groups, renders each group's ordered call plan, and posts it
  to the group-keyed Restate VirtualObject.

Rendering happens here rather than in the worker so that a dry run shows
the exact payloads in the Dagster UI and a mapping change never needs a
worker redeploy. Execution happens in Restate so that a partially
completed group is resumed, not restarted.
"""
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Definitions,
    MetadataValue,
    asset,
    asset_check,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model
from dagster_dlt import DagsterDltResource  # noqa: F401  (re-exported by consumers)
from ..dlt_resource import dlt_resources

from dag_tools.components.otel_api_sync.ledger import (
    LEDGER_METADATA_KEY,
    DispatchLedger,
    flush_to_sql,
    load_from_dagster,
    load_from_sql,
)
from dag_tools.components.otel_api_sync.schema import OtelApiSyncRunConfig
from dag_tools.otel_api_sync.render import render_plans
from dag_tools.otel_api_sync.spec import OtelApiSyncSpec, load_spec

logger = logging.getLogger(__name__)

# Restate ingress path for the group-keyed plan executor. `/send` makes the
# invocation one-way: Dagster hands off the plan and Restate owns
# completion from there, which is the whole point of the split.
_INGRESS_TEMPLATE = "{ingress}/ApiCallPlanService/{group_key}/execute_plan/send"

# Shared between the dispatch asset (which writes it) and the completion
# check (which reads it back from the materialization event) so the two
# never drift on the metadata key name.
DISPATCHED_METADATA_KEY = "dispatched"


def _executable_asset_keys(items: List[Any]) -> List[AssetKey]:
    """Only AssetsDefinition entries are materializable; AssetSpecs are not."""
    keys: List[AssetKey] = []
    for item in items:
        if isinstance(item, AssetsDefinition):
            keys.extend(item.keys)
    return keys


def _staging_engine(dest_config: Dict[str, Any]):
    """SQLAlchemy engine for the staging destination.

    Mirrors how the existing Restate components resolve the destination
    credential, so a deployment configures one env var, not two.
    """
    import sqlalchemy as sa
    from sqlalchemy.engine.url import URL

    credentials = dest_config.get("credentials")
    if isinstance(credentials, str) and credentials:
        return sa.create_engine(credentials)

    driver = dest_config.get("drivername", "postgres")
    env_name = f"DESTINATION__{driver.upper()}__CREDENTIALS"
    url = os.environ.get(env_name) or os.environ.get("DESTINATION__POSTGRES__CREDENTIALS")
    if url:
        return sa.create_engine(url)

    if dest_config.get("host"):
        return sa.create_engine(
            URL.create(
                drivername=driver,
                username=dest_config.get("username"),
                password=dest_config.get("password"),
                host=dest_config.get("host"),
                port=dest_config.get("port"),
                database=dest_config.get("database"),
            )
        )

    raise ValueError(
        f"Cannot reach the staging destination: set {env_name}, dest_config.credentials, "
        "or host/username/password in dest_config."
    )


def _read_staged_rows(
    dest_config: Dict[str, Any], schema: Optional[str], table: str, limit: int
) -> List[Dict[str, Any]]:
    import sqlalchemy as sa

    engine = _staging_engine(dest_config)
    qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'
    query = f"SELECT * FROM {qualified}"
    if limit:
        query += f" LIMIT {int(limit)}"
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(sa.text(query)).mappings()]


def _read_clickhouse_rows(
    source_config: Dict[str, Any], resource_config: Dict[str, Any], limit: int
) -> List[Dict[str, Any]]:
    """Direct read for unstaged pipelines."""
    from dag_tools.asset_wrappers.sources.clickhouse_query import _base_query, _client

    sql = f"SELECT * FROM ({_base_query(resource_config)}) AS _src"
    if limit:
        sql += f" LIMIT {int(limit)}"
    client = _client(source_config)
    try:
        result = client.query(sql)
        columns = list(result.column_names)
        return [dict(zip(columns, row)) for row in result.result_rows]
    finally:
        client.close()


def _json_safe(value: Any) -> Any:
    """Make rendered payloads JSON-serializable without changing their types."""
    import datetime as dt
    import decimal
    import uuid

    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _reconcile_dispatch_completion(
    context: Any,
    dispatch_key: AssetKey,
    ingress: str,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> AssetCheckResult:
    """Poll Restate for what the dispatch asset only ever fired-and-forgot.

    `/send` dispatch means HTTP 200 only says "accepted into the journal" —
    nothing about whether the plan's calls actually succeeded. This reads
    the dispatch asset's latest materialization, extracts the group/plan
    hashes it recorded under `DISPATCHED_METADATA_KEY`, and polls each
    group's read-only `get_status` handler until every plan resolves or the
    timeout elapses.

    Kept as a plain module-level function (rather than inlined in the
    `@asset_check`-decorated closure) so it can be unit tested directly
    with a fake `context` object, without going through Dagster's asset
    check execution machinery.
    """
    event = context.instance.get_latest_materialization_event(dispatch_key)
    materialization = getattr(event, "asset_materialization", None) if event else None
    if materialization is None:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.ERROR,
            metadata={"note": "no materialization to reconcile against"},
        )

    raw = (materialization.metadata or {}).get(DISPATCHED_METADATA_KEY)
    dispatched = getattr(raw, "data", raw) if raw is not None else None
    if not dispatched:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.ERROR,
            metadata={"note": "nothing was dispatched (dry run or empty batch)"},
        )

    pending = {(str(d["group_key"]), str(d["plan_hash"])) for d in dispatched}
    completed: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []

    deadline = time.monotonic() + timeout_seconds
    with httpx.Client(timeout=30.0) as client:
        while pending:
            for group_key, plan_hash in list(pending):
                url = f"{ingress}/ApiCallPlanService/{quote(group_key, safe='')}/get_status"
                try:
                    response = client.post(url)
                    response.raise_for_status()
                    status = response.json()
                except Exception as exc:
                    failed.append(
                        {"group_key": group_key, "plan_hash": plan_hash, "error": str(exc)}
                    )
                    pending.discard((group_key, plan_hash))
                    continue

                # Order matters: COMPLETED_WITH_ERRORS DOES get its hash
                # recorded in completed_plan_hashes, so checking that list
                # first would report a partially-failed plan as a pass.
                last = status.get("last_result") or {}
                if last.get("plan_hash") == plan_hash:
                    pending.discard((group_key, plan_hash))
                    if last.get("status") == "COMPLETED":
                        completed.append({"group_key": group_key, "plan_hash": plan_hash})
                    else:
                        failed.append(
                            {
                                "group_key": group_key,
                                "plan_hash": plan_hash,
                                "status": last.get("status"),
                                "failures": last.get("failures"),
                            }
                        )
                elif plan_hash in (status.get("completed_plan_hashes") or []):
                    pending.discard((group_key, plan_hash))
                    completed.append({"group_key": group_key, "plan_hash": plan_hash})
                # else: still executing — poll again next round.

            if not pending:
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(poll_interval_seconds)

    still_pending = [{"group_key": g, "plan_hash": p} for g, p in pending]

    # A timeout with groups still pending is a failed check — "we never
    # confirmed it landed" is exactly the false-green this check exists to
    # close.
    return AssetCheckResult(
        passed=not failed and not still_pending,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "groups_checked": len(dispatched),
            "groups_completed": len(completed),
            "groups_failed": MetadataValue.json(failed),
            "groups_still_running": MetadataValue.json(still_pending),
        },
    )


class OtelApiSyncComponent(Component, Resolvable, Model):
    """Push any ClickHouse telemetry to any ordered set of API endpoints."""

    source_config: Dict[str, Any]
    """ClickHouse connection configuration."""

    restate_endpoint: str
    """Restate ingress base URL."""

    dest_config: Dict[str, Any] = {}
    """Staging destination; required when a pipeline is staged."""

    staging_config: Dict[str, Any] = {}
    """Optional intermediate staging (bucket/filesystem) configuration."""

    pipelines: Dict[str, Any] = {}
    """Map of pipeline key -> pipeline configuration."""

    # --- spec loading ------------------------------------------------------

    def _load_mapping(
        self, context: ComponentLoadContext, pipeline_key: str, attrs: Dict[str, Any]
    ) -> OtelApiSyncSpec:
        """Resolve and validate the mapping document at definition-load time.

        Validating here means a malformed mapping fails the code location
        load with a pointed error, rather than at 2am inside a dispatch.
        """
        inline = attrs.get("mapping")
        if inline:
            raw: Dict[str, Any] = dict(inline)
        else:
            mapping_file = attrs.get("mapping_file")
            if not mapping_file:
                raise ValueError(
                    f"pipeline '{pipeline_key}' must declare either 'mapping' (inline) "
                    "or 'mapping_file'"
                )

            import yaml

            path = os.path.join(str(context.path), mapping_file)
            if not os.path.exists(path):
                path = mapping_file
            with open(path, "r", encoding="utf-8") as handle:
                raw = dict(yaml.safe_load(handle) or {})

        self._apply_api_override(pipeline_key, raw, attrs.get("api"))
        return load_spec(raw)

    @staticmethod
    def _apply_api_override(
        pipeline_key: str, raw: Dict[str, Any], override: Optional[Dict[str, Any]]
    ) -> None:
        """Merge the component's `api:` override into the mapping's `api` block.

        component.yaml attributes get `{{ env.X }}` resolution; mapping.yaml
        cannot be, since it is full of mapping-engine Jinja. So non-secret,
        per-deployment API config (base URL, static headers) is layered on
        here, before `load_spec` validates the merged document.
        """
        if not override:
            return

        api = dict(raw.get("api") or {})

        base_url = override.get("base_url")
        if base_url:
            api["base_url"] = base_url
            # _resolve_api lets base_url_env override base_url on the
            # worker; leaving it in place would silently keep the worker
            # env var winning over this override.
            api.pop("base_url_env", None)

        timeout_seconds = override.get("timeout_seconds")
        if timeout_seconds is not None:
            api["timeout_seconds"] = timeout_seconds

        headers = dict(api.get("headers") or {})
        headers.update(override.get("headers") or {})
        plan_carried_headers = dict(override.get("plan_carried_headers") or {})
        headers.update(plan_carried_headers)
        if headers:
            api["headers"] = headers

        if plan_carried_headers:
            logger.warning(
                "pipeline '%s': header(s) %s are rendered into the call plan and will be "
                "persisted in the Restate journal; use api.header_env in the mapping to "
                "keep credentials worker-side",
                pipeline_key,
                sorted(plan_carried_headers.keys()),
            )

        raw["api"] = api

    # --- definitions -------------------------------------------------------

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        generated_assets: List[Any] = []
        generated_checks: List[AssetChecksDefinition] = []

        for pipeline_key, raw_attrs in self.pipelines.items():
            attrs = dict(raw_attrs)
            spec = self._load_mapping(context, pipeline_key, attrs)

            sources = [dict(s) for s in attrs.get("sources", [])]
            if not sources:
                raise ValueError(f"pipeline '{pipeline_key}' declares no sources")

            staged = bool(attrs.get("staged", True))
            dest_schema = attrs.get("dest_schema")
            group_name = attrs.get("group_name")
            dispatch_from = attrs.get("dispatch_from") or sources[0]["name"]
            dispatch_config = next(
                (s for s in sources if s["name"] == dispatch_from), sources[0]
            )
            ledger_config = dict(attrs.get("ledger") or {})
            check_config = dict(attrs.get("completion_check") or {})

            upstream_keys: List[AssetKey] = []
            if staged:
                if not self.dest_config:
                    raise ValueError(
                        f"pipeline '{pipeline_key}' is staged but no dest_config was provided"
                    )
                extraction_assets = self._build_extraction_assets(
                    pipeline_key, attrs, sources, dest_schema, group_name
                )
                generated_assets.extend(extraction_assets)
                upstream_keys = _executable_asset_keys(extraction_assets)

            dispatch_asset = self._build_dispatch_asset(
                pipeline_key=pipeline_key,
                spec=spec,
                staged=staged,
                dest_schema=dest_schema,
                dispatch_config=dispatch_config,
                upstream_keys=upstream_keys,
                ledger_config=ledger_config,
                group_name=group_name,
                max_plan_bytes=int(attrs.get("max_plan_bytes", 4 * 1024 * 1024)),
            )
            generated_assets.append(dispatch_asset)

            if check_config.get("enabled", True):
                dispatch_key = list(dispatch_asset.keys)[0]
                generated_checks.append(
                    self._build_completion_check(pipeline_key, dispatch_key, check_config)
                )

        return Definitions(
            assets=generated_assets, asset_checks=generated_checks, resources=dlt_resources()
        )

    def _build_extraction_assets(
        self,
        pipeline_key: str,
        attrs: Dict[str, Any],
        sources: List[Dict[str, Any]],
        dest_schema: Optional[str],
        group_name: Optional[str],
    ) -> List[Any]:
        """dlt ClickHouse -> staging destination."""
        import dlt

        from dag_tools.asset_wrappers.dlt_assets_factory import (
            CustomDagsterDltTranslator,
            config_to_credentials,
        )
        from dag_tools.asset_wrappers.dlt_assets_parsing import (
            dlt_assets_with_io_managers,
            get_destination,
        )
        from dag_tools.asset_wrappers.sources.clickhouse_query import clickhouse_query

        pipeline_name = attrs.get("name") or pipeline_key
        effective_schema = dest_schema or self.dest_config.get("schema") or "public"
        src_database = self.source_config.get("database", "clickhouse")
        src_schema = self.source_config.get("schema") or src_database

        source = clickhouse_query(connection=self.source_config, resources=sources)

        dest_kinds: List[str] = []
        destination_credentials = config_to_credentials(self.dest_config, dest_kinds)
        destination = get_destination(
            destination_credentials, self.dest_config.get("destination", {})
        )

        dlt_pipeline = dlt.pipeline(
            pipeline_name=f"{pipeline_name}_pipeline",
            dataset_name=effective_schema,
            destination=destination,
            progress="log",
        )

        # Without an explicit translator the generated asset keys collapse
        # to dlt///<resource>, which breaks lineage and any downstream
        # selection by database/schema.
        translator = CustomDagsterDltTranslator(
            source_keys={},
            dest_database=getattr(destination_credentials, "database", "") or "",
            dest_schema=effective_schema,
            src_database=src_database,
            src_schema=src_schema,
            src_platform="clickhouse",
            dest_driver=getattr(destination_credentials, "drivername", None),
            kinds=dest_kinds,
            destination=dlt_pipeline.destination,
        )

        @dlt_assets_with_io_managers(
            dlt_source=source,
            dlt_pipeline=dlt_pipeline,
            name=f"dlt_{pipeline_name}_clickhouse_asset",
            group_name=group_name,
            io_manager_key=attrs.get("io_manager_key", "io_manager"),
            dagster_dlt_translator=translator,
        )
        def extraction_asset(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context, **attrs.get("pipeline_kwargs", {}))

        return [extraction_asset]

    def _build_dispatch_asset(
        self,
        pipeline_key: str,
        spec: OtelApiSyncSpec,
        staged: bool,
        dest_schema: Optional[str],
        dispatch_config: Dict[str, Any],
        upstream_keys: List[AssetKey],
        ledger_config: Dict[str, Any],
        group_name: Optional[str],
        max_plan_bytes: int,
    ) -> AssetsDefinition:
        """Factory-built dispatch asset.

        A factory (rather than a closure defined inline in the loop) is
        required: Dagster 1.12 introspects an asset function's parameters
        as inputs, so loop variables must be bound as defaults of the
        enclosing factory instead.
        """
        source_config = self.source_config
        dest_config = self.dest_config
        ingress = self.restate_endpoint.rstrip("/")

        ledger_enabled = ledger_config.get("enabled", True)
        ledger_backend = ledger_config.get("backend") or ("sql" if staged else "dagster")
        ledger_table = ledger_config.get("table", "otel_api_dispatch_ledger")
        ledger_schema = ledger_config.get("schema") or ledger_config.get("schema_name") or dest_schema

        @asset(
            name=f"{pipeline_key}_dispatch",
            deps=upstream_keys or None,
            group_name=group_name,
        )
        async def dispatch_asset(context: AssetExecutionContext, config: OtelApiSyncRunConfig):
            # 1. Read.
            if staged:
                rows = _read_staged_rows(
                    dest_config, dest_schema, dispatch_config["name"].lower(), config.limit
                )
                origin = f"{dest_schema}.{dispatch_config['name'].lower()}"
            else:
                rows = _read_clickhouse_rows(source_config, dispatch_config, config.limit)
                origin = f"clickhouse:{dispatch_config.get('table') or 'query'}"
            context.log.info("Read %s rows from %s", len(rows), origin)

            # 2. Group, gate, render.
            plans, deferred = render_plans(rows, spec)

            if config.ignore_readiness and deferred:
                from dag_tools.otel_api_sync.render import build_plan, group_rows

                groups = group_rows(rows, spec)
                for group_key, reason in deferred:
                    context.log.warning(
                        "ignore_readiness: dispatching group %s anyway (%s)", group_key, reason
                    )
                    plans.append(build_plan(group_key, groups[group_key], spec))
                deferred = []

            if config.only_group:
                plans = [p for p in plans if p["group_key"] == config.only_group]

            # 3. Ledger — do not re-send what has already been dispatched.
            ledger = DispatchLedger(ledger_backend)
            engine = None
            if ledger_enabled:
                if ledger_backend == "sql":
                    engine = _staging_engine(dest_config)
                    ledger = load_from_sql(engine, ledger_schema, ledger_table)
                else:
                    ledger = load_from_dagster(context, context.asset_key)

            pending, duplicates = [], []
            for plan in plans:
                if (
                    ledger_enabled
                    and not config.ignore_ledger
                    and ledger.contains(plan["group_key"], plan["plan_hash"])
                ):
                    duplicates.append(plan["group_key"])
                    continue
                pending.append(plan)

            if config.max_groups:
                pending = pending[: config.max_groups]

            call_total = sum(
                len(step["calls"]) for plan in pending for step in plan["steps"]
            )

            # Size guard. A single large execution group is one plan to one
            # object key, so capping the number of groups cannot help; better
            # to fail here, naming the group, than to have the ingress reject
            # a multi-megabyte body partway through a dispatch loop.
            oversized = []
            if max_plan_bytes:
                for plan in pending:
                    size = len(json.dumps(_json_safe(plan), default=str).encode())
                    if size > max_plan_bytes:
                        oversized.append((plan["group_key"], size))
            if oversized:
                detail = ", ".join(
                    f"{key} ({size / 1048576:.1f} MiB)" for key, size in oversized
                )
                raise ValueError(
                    f"rendered plan(s) exceed max_plan_bytes "
                    f"({max_plan_bytes / 1048576:.1f} MiB): {detail}. Narrow the "
                    "source query, split the execution group, or raise "
                    "max_plan_bytes together with the Restate ingress body limit."
                )

            # 4. Dispatch (or don't).
            sent, failed = [], []
            if config.dry_run:
                context.log.info(
                    "DRY RUN: %s group(s), %s call(s) rendered and NOT sent.",
                    len(pending),
                    call_total,
                )
                for plan in pending:
                    context.log.info(
                        "plan %s:\n%s",
                        plan["plan_id"],
                        json.dumps(_json_safe(plan), indent=2, default=str),
                    )
            else:
                sent, failed = await self._dispatch_plans(context, ingress, pending)
                for plan in sent:
                    ledger.record(plan["group_key"], plan["plan_hash"])
                if ledger_enabled and ledger_backend == "sql" and engine is not None:
                    flush_to_sql(
                        engine,
                        ledger_schema,
                        ledger_table,
                        ledger.added,
                        {p["plan_hash"]: len(p["steps"]) for p in sent},
                    )

            context.add_output_metadata(
                {
                    "rows_read": len(rows),
                    "groups_rendered": len(plans),
                    "groups_dispatched": len(sent),
                    "groups_deferred_not_ready": len(deferred),
                    "groups_skipped_duplicate": len(duplicates),
                    "calls_planned": call_total,
                    "dry_run": config.dry_run,
                    "deferred_reasons": MetadataValue.json(
                        [{"group": str(g), "reason": r} for g, r in deferred[:50]]
                    ),
                    "failed_dispatches": MetadataValue.json(failed[:50]),
                    "plans": MetadataValue.json(
                        _json_safe(pending[:5] if config.dry_run else [])
                    ),
                    DISPATCHED_METADATA_KEY: MetadataValue.json(
                        [
                            {"group_key": str(p["group_key"]), "plan_hash": p["plan_hash"]}
                            for p in sent
                        ]
                    ),
                    LEDGER_METADATA_KEY: ledger.serialize()
                    if (ledger_enabled and ledger_backend == "dagster")
                    else "",
                }
            )

            if failed:
                raise RuntimeError(
                    f"{len(failed)} execution group(s) could not be handed to Restate: {failed[:3]}"
                )

        return dispatch_asset

    def _build_completion_check(
        self, pipeline_key: str, dispatch_key: AssetKey, check_config: Dict[str, Any]
    ) -> AssetChecksDefinition:
        """Factory-built check reconciling dispatch against Restate's own record.

        `/send` dispatch means HTTP 200 only says "accepted into the
        journal" — nothing about whether the plan's calls actually
        succeeded. This check polls each dispatched group's read-only
        `get_status` handler and fails when a plan failed or never
        confirmed. Same factory-function discipline as
        `_build_dispatch_asset`: loop variables are bound as factory
        locals, never as asset-function parameters.

        The actual reconciliation logic lives in the module-level
        `_reconcile_dispatch_completion` so tests can drive it directly with
        a fake context, without going through Dagster's asset-check
        machinery.
        """
        ingress = self.restate_endpoint.rstrip("/")
        blocking = bool(check_config.get("blocking", True))
        poll_interval_seconds = float(check_config.get("poll_interval_seconds", 2.0))
        timeout_seconds = float(check_config.get("timeout_seconds", 300.0))

        @asset_check(
            asset=dispatch_key,
            name="restate_plans_completed",
            blocking=blocking,
            description="Polls each dispatched group's Restate object and fails if any plan failed.",
        )
        def completion_check(context) -> AssetCheckResult:
            return _reconcile_dispatch_completion(
                context=context,
                dispatch_key=dispatch_key,
                ingress=ingress,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
            )

        return completion_check

    async def _dispatch_plans(
        self, context: AssetExecutionContext, ingress: str, plans: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Hand each plan to its group's Restate object.

        The plan hash rides along as the ingress idempotency key, so a
        retried send is collapsed by Restate itself rather than becoming a
        second invocation.
        """
        sent: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        async with httpx.AsyncClient(timeout=60.0) as client:
            for plan in plans:
                url = _INGRESS_TEMPLATE.format(
                    ingress=ingress, group_key=quote(str(plan["group_key"]), safe="")
                )
                try:
                    response = await client.post(
                        url,
                        json=_json_safe(plan),
                        headers={"idempotency-key": plan["plan_id"]},
                    )
                    response.raise_for_status()
                    context.log.info(
                        "Dispatched group %s (%s calls) -> %s",
                        plan["group_key"],
                        sum(len(s["calls"]) for s in plan["steps"]),
                        url,
                    )
                    sent.append(plan)
                except Exception as exc:
                    context.log.error("Failed to dispatch group %s: %s", plan["group_key"], exc)
                    failed.append({"group_key": plan["group_key"], "error": str(exc)})

        return sent, failed
