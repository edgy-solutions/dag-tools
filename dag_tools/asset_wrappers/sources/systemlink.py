"""The ``systemlink_test_monitor`` source kind: raw SystemLink Test Monitor tables through dlt.

Two sources are built from one definition, so the resource names and table names (which the
translator turns into asset keys) cannot disagree:

* the DEFINITION-TIME source (``build_source``) does no IO and reads no environment variable; its
  resources yield nothing. A code location loads with no network and no key.
* the RUN-TIME source (built inside ``execute``) carries the live client, the known ids and the seeds.

The API key is read from ``os.environ`` in ``execute`` and held only in the httpx client's headers;
no dlt resource argument, state entry or config receives it.

No module-level ``from __future__ import annotations``: dlt inspects resource signatures.
"""
import hashlib
import json
import logging
import os
from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional, Sequence

import dlt
from dagster import AssetKey, AssetSpec, MaterializeResult

from dag_tools.asset_wrappers.source_kinds import SourceKind, register_source_kind
from dag_tools.systemlink import events as ev
from dag_tools.systemlink import queries
from dag_tools.systemlink.client import SystemLinkClient, SystemLinkError
from dag_tools.systemlink.incremental import (
    EXCLUDED,
    apply_operator,
    known_from_rows,
    result_version,
    scan_pages,
    wants_steps,
)
from dag_tools.systemlink.paging import StepBatcher, fetch_steps, iter_pages
from dag_tools.systemlink.spec import KIND_NAME, SystemLinkConfig, parse_source_config

logger = logging.getLogger(__name__)

SOURCE_NAME = "systemlink"
MODE_TAG = "dagtools/systemlink_mode"
RESTART_TAG = "dagtools/systemlink_restart"
CONCURRENCY_TAG = "dagster/concurrency_key"
VERSION_UPDATED = "_version_updated_at"
VERSION_STATUS = "_version_status"
RESULTS, STEPS, PRODUCTS, INGEST = "results", "steps", "products", "ingest_runs"
DATA_RESOURCES = (RESULTS, STEPS, PRODUCTS)
_STEP_JSON_COLUMNS = ("inputs", "outputs")


class Page(list):
    """A page of result rows that remembers which ids want steps (survives into the transformer)."""

    fetch: Optional[List[str]] = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_table(name: str) -> str:
    from dlt.common.normalizers.naming.snake_case import NamingConvention

    return NamingConvention().normalize_table_identifier(name)


def table_name(prefix: str, resource: str) -> str:
    return f"{prefix}{resource}"


def resource_names(sources: Sequence[Any], cfg: SystemLinkConfig) -> List[str]:
    """The resource names this pipeline builds, validated against ``sources``."""
    wanted = [str(s) for s in sources]
    allowed = {RESULTS, STEPS} | ({PRODUCTS} if cfg.products.enabled else set())
    bad = sorted(set(wanted) - allowed - {INGEST})
    if bad:
        raise ValueError(
            f"sources {bad} are not resources of {KIND_NAME} (allowed: {sorted(allowed)}; "
            f"{PRODUCTS} needs products.enabled; {INGEST} is always added)"
        )
    if STEPS in wanted and RESULTS not in wanted:
        raise ValueError("source 'steps' is a transformer of 'results'; list 'results' too")
    if not wanted:
        raise ValueError(f"{KIND_NAME} needs sources: at least [results]")
    names = [n for n in (RESULTS, STEPS, PRODUCTS) if n in wanted]
    if cfg.products.enabled and PRODUCTS not in names:
        names.append(PRODUCTS)
    return names + [INGEST]


# --------------------------------------------------------------------------- seeds


def load_seeds(seed) -> List[str]:
    if seed.kind == "inline":
        raw = list(seed.values or [])
    elif seed.kind == "file":
        import csv

        with open(seed.path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if seed.column not in (reader.fieldnames or []):
                raise ValueError(f"seed file has no column {seed.column!r}")
            raw = [row[seed.column] for row in reader]
    else:
        from dag_tools.asset_wrappers.sources.clickhouse_query import _client

        client = _client(dict(seed.connection or {}))
        try:
            raw = [row[0] for row in client.query(seed.query).result_rows]
        finally:
            close = getattr(client, "close", None)
            if close:
                close()
    parts: List[str] = []
    for value in raw:
        text = str(value).strip() if value is not None else ""
        if text and text not in parts:
            parts.append(text)
    if not parts:
        raise ValueError("the seed produced no part numbers; refusing a run over nothing")
    return parts


# --------------------------------------------------------------------------- run-time state


class Runtime:
    """Everything one run needs. Built in ``execute``; never stored on the kind."""

    def __init__(
        self,
        cfg: SystemLinkConfig,
        prefix: str,
        client: SystemLinkClient,
        parts: List[str],
        known: Dict[str, Any],
        mode: str,
        operator_key: Optional[bytes],
        completed_before: Optional[List[str]] = None,
    ) -> None:
        self.cfg, self.prefix, self.client = cfg, prefix, client
        self.parts, self.known, self.mode = parts, known, mode
        self.operator_key = operator_key
        self.completed_before = completed_before or []
        self.extracted_at = _now()
        self.stats: Counter = Counter()
        self.batcher = StepBatcher(cfg.limits.step_batch_default, cfg.limits.step_batch_max)

    @property
    def events(self) -> ev.EventLog:
        return self.client.events

    def stamp(self, row: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        row["_source_system"] = self.cfg.source_id
        row["_extracted_at"] = self.extracted_at
        row["_request_id"] = request_id
        return row

    def updated_after(self) -> Optional[str]:
        if self.mode != "incremental" or not self.cfg.incremental.use_server_time_filter:
            return None
        stamps = [v[0] for v in self.known.values() if v[0]]
        return max(stamps) if stamps else None


def _scan(rt: Runtime, part: str):
    cfg = rt.cfg
    return scan_pages(
        rt.client, part,
        order_by=cfg.incremental.order_by,
        page_size=cfg.limits.page_size,
        workspace_id=cfg.connection.workspace_id,
        known=rt.known,
        terminal_statuses=cfg.filters.terminal_statuses,
        lookback_pages=cfg.incremental.lookback_pages,
        program_include=cfg.filters.program_include,
        program_exclude=cfg.filters.program_exclude,
        updated_after=rt.updated_after(),
        stop_on_known=rt.mode == "incremental",
    )


def _results_items(rt: Runtime, parts: List[str], record_completion: bool):
    cfg = rt.cfg
    for part in parts:
        for page in _scan(rt, part):
            out, fetch = Page(), []
            for result, classification, rid in page:
                if classification in (ev.KNOWN_UNCHANGED, EXCLUDED):
                    continue
                rt.stats[classification] += 1
                row = dict(result)
                updated, status = result_version(result)
                row[VERSION_UPDATED], row[VERSION_STATUS] = updated, status
                apply_operator(row, cfg.operator.store, rt.operator_key)
                out.append(rt.stamp(row, rid))
                if wants_steps(
                    result, classification,
                    step_detail_for=cfg.filters.step_detail_for,
                    failed_statuses=cfg.filters.failed_statuses,
                ):
                    fetch.append(str(result["id"]))
            if out:
                out.fetch = fetch
                rt.stats["results_stored"] += len(out)
                yield out
        if record_completion:
            state = dlt.current.resource_state(RESULTS)
            state["completed_parts"] = list(rt.completed_before) + [part]
            rt.completed_before = state["completed_parts"]


def _steps_items(rt: Runtime, page: Page):
    ids = getattr(page, "fetch", None)
    if not ids:
        return
    cfg = rt.cfg
    rows = fetch_steps(
        rt.client, ids,
        form=cfg.limits.step_filter,
        take=cfg.limits.page_size,
        batcher=rt.batcher,
        workspace_id=cfg.connection.workspace_id,
        max_concurrency=cfg.limits.max_concurrency,
    )
    out = []
    for step, rid in rows:
        if any(step.get(k) is None for k in cfg.steps_key):
            rt.events.emit(ev.SCHEMA_DRIFT, missing_field="|".join(cfg.steps_key), route="query-steps")
            continue
        out.append(rt.stamp(dict(step), rid))
    rt.stats["steps_stored"] += len(out)
    yield out


def _products_items(rt: Runtime):
    cfg = rt.cfg
    size = cfg.limits.step_batch_default
    for i in range(0, len(rt.parts), size):
        chunk = rt.parts[i : i + size]
        for items, rid in iter_pages(
            rt.client, "query-products",
            lambda token, c=chunk: queries.products_query(c, take=cfg.limits.page_size, token=token),
            "products", setting="products",
        ):
            rows = [rt.stamp(dict(p), rid) for p in items if isinstance(p, dict)]
            rt.stats["products_stored"] += len(rows)
            if rows:
                yield rows


def _ingest_items(row: Optional[Dict[str, Any]]):
    if row:
        yield row


# --------------------------------------------------------------------------- source builder


def build_systemlink_source(
    cfg: SystemLinkConfig,
    prefix: str,
    names: Sequence[str],
    rt: Optional[Runtime] = None,
    *,
    parts: Optional[List[str]] = None,
    record_completion: bool = False,
    ingest_row: Optional[Dict[str, Any]] = None,
):
    """One source over ``names``; ``rt=None`` is the no-IO definition-time shape."""
    json_cols = {c: {"data_type": "json"} for c in cfg.keep_json_columns}
    result_cols = {
        VERSION_UPDATED: {"data_type": "text"},
        VERSION_STATUS: {"data_type": "text"},
        **json_cols,
    }
    step_cols = {c: h for c, h in json_cols.items() if c in _STEP_JSON_COLUMNS}
    use = parts if parts is not None else (rt.parts if rt else [])

    @dlt.resource(
        name=RESULTS, table_name=table_name(prefix, RESULTS), write_disposition="merge",
        primary_key="id", columns=result_cols,
    )
    def results():
        if rt is not None:
            yield from _results_items(rt, use, record_completion)

    @dlt.transformer(
        data_from=results, name=STEPS, table_name=table_name(prefix, STEPS),
        write_disposition="merge", primary_key=list(cfg.steps_key), columns=step_cols,
    )
    def steps(page):
        if rt is not None:
            yield from _steps_items(rt, page)

    @dlt.resource(
        name=PRODUCTS, table_name=table_name(prefix, PRODUCTS), write_disposition="merge",
        primary_key="id",
    )
    def products():
        if rt is not None:
            yield from _products_items(rt)

    @dlt.resource(
        name=INGEST, table_name=table_name(prefix, INGEST), write_disposition="append",
        primary_key=["run_id", "source_system"],
    )
    def ingest_runs():
        yield from _ingest_items(ingest_row)

    available = {RESULTS: results, STEPS: steps, PRODUCTS: products, INGEST: ingest_runs}
    chosen = [available[n] for n in names]

    @dlt.source(name=SOURCE_NAME)
    def systemlink_source():
        return chosen

    return systemlink_source()


# --------------------------------------------------------------------------- the kind


def _decorate(events: Iterator[Any], extra: Dict[AssetKey, Dict[str, Any]]):
    for event in events:
        key = getattr(event, "asset_key", None)
        if isinstance(event, MaterializeResult) and key in extra:
            yield MaterializeResult(asset_key=key, metadata={**(event.metadata or {}), **extra[key]})
        else:
            yield event


def _key_for(context: Any, tbl: str) -> Optional[AssetKey]:
    wanted = _normalize_table(tbl)
    found = [k for k in context.selected_asset_keys if str(k.path[-1]).lower().endswith(wanted)]
    return found[0] if len(found) == 1 else None


def _selected_names(context: Any, prefix: str, names: Sequence[str]) -> List[str]:
    return [n for n in names if _key_for(context, table_name(prefix, n)) is not None]


def read_known(pipeline: Any, tbl: str, events: ev.EventLog) -> Dict[str, Any]:
    """``(id, updatedAt, status)`` already stored. A missing table is a first run; anything else raises."""
    from dlt.destinations.exceptions import DatabaseUndefinedRelation

    name = _normalize_table(tbl)
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(
                f"SELECT id, {VERSION_UPDATED}, {VERSION_STATUS} "
                f"FROM {client.make_qualified_table_name(name)}"
            )
    except DatabaseUndefinedRelation:
        events.info("known_ids", first_run=True, known=0)
        return {}
    known = known_from_rows(rows or [])
    events.info("known_ids", first_run=False, known=len(known))
    return known


def _env_value(name: str, what: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"environment variable {name!r} ({what}) is not set")
    return value


def _root_error_name(failure: BaseException) -> str:
    """dlt wraps whatever a resource raised; name the engine's own error when one is in the chain."""
    seen, exc = set(), failure
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, SystemLinkError):
            return type(exc).__name__
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return type(failure).__name__


class SystemLinkKind(SourceKind):
    # ------------------------------------------------------------------ load-time
    def validate(self, source_config: Dict[str, Any]) -> None:
        parse_source_config(source_config)

    def validate_group(self, group_config: Any) -> None:
        missing = [f for f in ("table_prefix", "dest_schema") if not getattr(group_config, f, None)]
        if missing:
            raise ValueError(f"{KIND_NAME} requires {missing} on every pipeline")
        if (group_config.pipeline_kwargs or {}).get("table_name"):
            raise ValueError(
                f"{KIND_NAME} names its tables from table_prefix; pipeline_kwargs.table_name "
                "would give every resource the same name"
            )

    def localize(self, source_config: Dict[str, Any], component_dir: Any) -> Dict[str, Any]:
        seed = (source_config or {}).get("seed")
        if not isinstance(seed, dict) or seed.get("kind") != "file" or not seed.get("path"):
            return source_config
        path = str(seed["path"])
        if os.path.isabs(path) or component_dir is None:
            return source_config
        return {**source_config, "seed": {**seed, "path": os.path.join(str(component_dir), path)}}

    def prepare_units(
        self, sources, source_config, dest_config, config, staging=None,
        staging_config=None, destination_override=None,
    ):
        from dag_tools.asset_wrappers import dlt_assets_parsing as parsing

        cfg = parse_source_config(source_config)
        names = resource_names(sources, cfg)
        dest_config = parsing.update_from_env(dest_config, True)
        kinds: List[str] = []
        credentials = parsing.config_to_credentials(dest_config, kinds)
        if staging and staging_config:
            staging = parsing.get_destination(
                parsing.config_to_credentials(parsing.update_from_env(staging_config, True)),
                staging_config.get("destination", {}), vars=parsing.ENV_VARS,
            )
        database = getattr(credentials, "database", None) or SOURCE_NAME
        destination = destination_override or parsing.get_destination(
            credentials, dest_config.get("destination", {}), vars=parsing.ENV_VARS,
            database=database,
        )
        pipeline = dlt.pipeline(
            pipeline_name=f"{config.name}_pipeline",
            dataset_name=config.dest_schema,
            destination=destination,
            staging=staging,
            progress="log",
            export_schema_path="schemas/export",
        )
        upstream = AssetKey([SOURCE_NAME, cfg.source_id, config.name])
        prefix = config.table_prefix
        base = {
            "tables": [table_name(prefix, n) for n in names],
            "creds": SimpleNamespace(drivername=SOURCE_NAME, database=SOURCE_NAME, schema=cfg.source_id),
            "source": {n: list(upstream.path) for n in names},
            "source_config": source_config,
            "resource_names": names,
        }
        default_kwargs = (
            {"loader_file_format": "parquet"}
            if dest_config.get("drivername") in ["filesystem", "databricks"] else {}
        )
        default_kwargs.update(config.pipeline_kwargs)
        unit = {
            "base": base, "database": SOURCE_NAME, "schema": cfg.source_id,
            "pipeline": pipeline, "dest_database": database, "dest_schema": config.dest_schema,
            "dest_driver": credentials.drivername, "kinds": kinds,
            "default_pipeline_kwargs": default_kwargs,
        }
        return [AssetSpec(key=upstream, kinds={SOURCE_NAME})], [unit]

    def build_source(self, base, database, schema, query_callback, config, dest_driver, defer_table_reflect=True):
        cfg = parse_source_config(base["source_config"])
        return build_systemlink_source(cfg, config.table_prefix, base["resource_names"])

    # ------------------------------------------------------------------ definitions
    def schedule_job_name(self, group_name: str) -> Optional[str]:
        return f"{group_name}_incremental"

    def extra_defs(self, group_name, asset_keys, source_config, group_config):
        from dagster import (
            AssetCheckResult, AssetCheckSeverity, AssetSelection, asset_check, define_asset_job,
        )

        cfg = parse_source_config(source_config)
        if not asset_keys:
            return {}
        selection = AssetSelection.assets(*asset_keys)
        tags = {CONCURRENCY_TAG: group_name}
        if getattr(group_config, "pool", None):
            tags["dagster/pool"] = group_config.pool
        incremental = define_asset_job(f"{group_name}_incremental", selection=selection, tags=tags)
        backfill = define_asset_job(
            f"{group_name}_backfill", selection=selection, tags={**tags, MODE_TAG: "backfill"}
        )
        ingest_tbl = _normalize_table(table_name(group_config.table_prefix, INGEST))
        ingest_keys = [k for k in asset_keys if str(k.path[-1]).lower().endswith(ingest_tbl)]
        checks: List[Any] = []
        if len(ingest_keys) == 1:
            key = ingest_keys[0]

            def latest(context) -> Optional[Dict[str, Any]]:
                event = context.instance.get_latest_materialization_event(key)
                if event is None or event.asset_materialization is None:
                    return None
                return {k: getattr(v, "value", v) for k, v in event.asset_materialization.metadata.items()}

            def verdict(context, name, ok, detail):
                meta = latest(context)
                if meta is None:
                    return AssetCheckResult(
                        passed=False, severity=AssetCheckSeverity.WARN,
                        description="no ingest run has been recorded yet",
                    )
                passed, text = ok(meta)
                return AssetCheckResult(passed=passed, description=text, metadata={name: meta.get(name)})

            @asset_check(asset=key, name="no_auth_failed")
            def no_auth_failed(context):
                return verdict(context, ev.AUTH_FAILED, lambda m: (
                    int(m.get(ev.AUTH_FAILED) or 0) == 0, "auth_failed events in the latest run"), "")

            @asset_check(asset=key, name="not_visible_within_threshold")
            def not_visible(context):
                limit = cfg.limits.not_visible_warn_threshold
                return verdict(context, ev.NOT_VISIBLE, lambda m: (
                    int(m.get(ev.NOT_VISIBLE) or 0) <= limit,
                    f"not_visible events must be <= {limit}"), "")

            @asset_check(asset=key, name="step_batch_within_max")
            def batch_within_max(context):
                limit = cfg.limits.step_batch_max
                return verdict(context, "max_step_batch_sent", lambda m: (
                    int(m.get("max_step_batch_sent") or 0) <= limit,
                    f"largest step batch sent must be <= {limit}"), "")

            checks = [no_auth_failed, not_visible, batch_within_max]
        return {"jobs": [incremental, backfill], "asset_checks": checks}

    # ------------------------------------------------------------------ run
    def execute(self, context, dlt_resource, *, base, pipeline, config, run_kwargs):
        cfg = parse_source_config(base["source_config"])
        prefix = config.table_prefix
        names = base["resource_names"]
        tags = getattr(getattr(context, "run", None), "tags", None) or {}
        mode = "backfill" if tags.get(MODE_TAG) == "backfill" else "incremental"
        restart = str(tags.get(RESTART_TAG, "")).lower() == "true"

        api_key = _env_value(cfg.connection.api_key_env, "SystemLink API key")
        operator_key = None
        if cfg.operator.store == "pseudonym":
            operator_key = _env_value(cfg.operator.pseudonym_key_env, "operator pseudonym key").encode()
        events = ev.EventLog()
        client = SystemLinkClient(
            cfg.connection.base_url, api_key,
            api_version_path=cfg.connection.api_version_path,
            verify_tls=cfg.connection.verify_tls,
            timeout_s=cfg.limits.request_timeout_s,
            max_retries=cfg.limits.max_retries,
            max_concurrency=cfg.limits.max_concurrency,
            events=events,
        )
        del api_key
        selected = _selected_names(context, prefix, names)
        data_names = [n for n in selected if n != INGEST]
        started = _now()
        failure: Optional[BaseException] = None
        rt: Optional[Runtime] = None
        try:
            parts = load_seeds(cfg.seed)
            run_pipeline = pipeline
            if mode == "backfill":
                run_pipeline = dlt.pipeline(
                    pipeline_name=f"{config.name}_backfill",
                    dataset_name=pipeline.dataset_name,
                    destination=pipeline.destination,
                    staging=pipeline.staging,
                    progress="log",
                )
            known = read_known(run_pipeline, table_name(prefix, RESULTS), events)
            rt = Runtime(cfg, prefix, client, parts, known, mode, operator_key)
            if data_names:
                if mode == "backfill":
                    yield from self._backfill(context, dlt_resource, run_pipeline, rt, config, run_kwargs,
                                              data_names, restart)
                else:
                    yield from self._incremental(context, dlt_resource, run_pipeline, rt, config, run_kwargs,
                                                 data_names)
        except Exception as exc:  # recorded, then re-raised after the ingest row is written
            failure = exc
        finally:
            client.close()

        if INGEST in selected:
            row = self._ingest_row(context, cfg, rt, events, mode, started, failure)
            try:
                yield from self._write_ingest(context, dlt_resource, pipeline, cfg, prefix, config, run_kwargs, row)
            except Exception:
                if failure is None:
                    raise
                logger.exception("could not record the ingest run of a failed run")
        if failure is not None:
            raise failure

    def _incremental(self, context, dlt_resource, pipeline, rt, config, run_kwargs, data_names):
        cfg, prefix = rt.cfg, rt.prefix
        source = build_systemlink_source(cfg, prefix, data_names, rt)
        events = dlt_resource.run(
            context=context, dlt_source=source, dlt_pipeline=pipeline, **run_kwargs
        )
        extra = self._child_metadata(pipeline, prefix, context)
        yield from _decorate(events, extra)

    @staticmethod
    def _child_metadata(pipeline, prefix, context) -> Dict[AssetKey, Dict[str, Any]]:
        key = _key_for(context, table_name(prefix, STEPS))
        if key is None:
            return {}
        parent = _normalize_table(table_name(prefix, STEPS))
        try:
            children = sorted(
                t for t in pipeline.default_schema.data_table_names() if t.startswith(f"{parent}__")
            )
        except Exception:
            children = []
        return {key: {"child_tables": children or f"{parent}__data__parameters"}}

    def _backfill(self, context, dlt_resource, pipeline, rt, config, run_kwargs, data_names, restart):
        cfg, prefix = rt.cfg, rt.prefix
        if pipeline.config.restore_from_destination:
            pipeline.drop()
        pipeline.sync_destination()
        completed: List[str] = []
        if not restart:
            try:
                state = pipeline.state["sources"][SOURCE_NAME]["resources"][RESULTS]
                completed = list(state.get("completed_parts", []))
            except KeyError:
                completed = []
        rt.completed_before = completed

        counts: Dict[str, Any] = {}
        for part in rt.parts:
            data, _ = rt.client.post(
                "query-results",
                queries.count_query(part, workspace_id=cfg.connection.workspace_id),
                setting="limits.page_size",
            )
            counts[part] = data.get("totalCount")
        rt.events.info("backfill_sizing", counts=counts, total=sum(c for c in counts.values() if isinstance(c, int)))

        rows: Counter = Counter()
        done = skipped = 0
        for part in rt.parts:
            if part in completed and not restart:
                skipped += 1
                continue
            source = build_systemlink_source(cfg, prefix, data_names, rt, parts=[part], record_completion=True)
            pipeline.run(source, **run_kwargs)
            done += 1
            counts_by_table = pipeline.last_trace.last_normalize_info.row_counts
            for n in data_names:
                rows[n] += int(counts_by_table.get(_normalize_table(table_name(prefix, n)), 0))
        if PRODUCTS in data_names:
            source = build_systemlink_source(cfg, prefix, [PRODUCTS], rt)
            pipeline.run(source, **run_kwargs)
            rows[PRODUCTS] += int(
                pipeline.last_trace.last_normalize_info.row_counts.get(
                    _normalize_table(table_name(prefix, PRODUCTS)), 0)
            )
        child = self._child_metadata(pipeline, prefix, context)
        for n in data_names:  # exactly one materialization per selected key
            key = _key_for(context, table_name(prefix, n))
            meta: Dict[str, Any] = {
                "mode": "backfill", "rows_loaded": rows[n],
                "parts_total": len(rt.parts), "parts_loaded": done, "parts_skipped": skipped,
            }
            meta.update(child.get(key, {}))
            yield MaterializeResult(asset_key=key, metadata=meta)

    def _ingest_row(self, context, cfg, rt, events, mode, started, failure) -> Dict[str, Any]:
        summary = events.summary()
        stats = rt.stats if rt is not None else Counter()
        digest = hashlib.sha256(str(failure).encode()).hexdigest()[:12] if failure else None
        return {
            "run_id": getattr(context, "run_id", None) or "local",
            "source_system": cfg.source_id,
            "mode": mode,
            "status": "FAILED" if failure else "SUCCESS",
            "error_type": _root_error_name(failure) if failure else None,
            "error_digest": digest,
            "started_at": started,
            "finished_at": _now(),
            "requests": events.requests,
            "max_step_batch_sent": events.max_step_batch_sent,
            "results_stored": int(stats.get("results_stored", 0)),
            "steps_stored": int(stats.get("steps_stored", 0)),
            "new_terminal": int(stats.get(ev.NEW_TERMINAL, 0)),
            "changed_terminal": int(stats.get(ev.CHANGED_TERMINAL, 0)),
            "in_progress": int(stats.get(ev.IN_PROGRESS, 0)),
            **summary,
        }

    def _write_ingest(self, context, dlt_resource, pipeline, cfg, prefix, config, run_kwargs, row):
        source = build_systemlink_source(cfg, prefix, [INGEST], ingest_row=row)
        metadata = {k: v for k, v in row.items() if isinstance(v, (int, str)) or v is None}
        metadata = {k: v for k, v in metadata.items() if v is not None}
        key = _key_for(context, table_name(prefix, INGEST))
        events = dlt_resource.run(context=context, dlt_source=source, dlt_pipeline=pipeline, **run_kwargs)
        yield from _decorate(events, {key: metadata})


SYSTEMLINK_KIND = SystemLinkKind()
register_source_kind(KIND_NAME, SYSTEMLINK_KIND)
