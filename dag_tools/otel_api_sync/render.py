"""Turn ClickHouse rows into ordered, pre-rendered call plans.

Rendering happens here — on the Dagster side — so that a dry run shows
the exact bytes that will hit the API, and so mapping edits never
require redeploying the Restate worker. The consequence is that the
handler has no template engine, which forces one useful discipline:
every fallback body must be renderable *before* anyone knows which call
failed. See ``_render_step`` for how aggregate fallbacks satisfy that
with per-item fragments.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dag_tools.otel_api_sync.environment import (
    build_environment,
    build_path_environment,
    render_query,
    render_structure,
    render_value,
)
from dag_tools.otel_api_sync.functions import row_get
from dag_tools.otel_api_sync.plan import PLAN_FORMAT_VERSION
from dag_tools.otel_api_sync.spec import OtelApiSyncSpec, StepSpec


def _as_datetime(value: Any) -> Optional[dt.datetime]:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    if isinstance(value, str):
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
    return None


def group_rows(rows: Iterable[Dict[str, Any]], spec: OtelApiSyncSpec) -> Dict[Any, List[Dict[str, Any]]]:
    """Bucket rows into execution groups, preserving arrival order."""
    env = build_environment(spec.attribute_columns, spec.strict_undefined)
    groups: Dict[Any, List[Dict[str, Any]]] = {}
    for row in rows:
        key = render_value(spec.group_by, {"row": row, "item": row}, env)
        if key is None or (isinstance(key, str) and not key.strip()):
            continue
        groups.setdefault(key, []).append(row)
    return groups


def group_readiness(
    rows: List[Dict[str, Any]],
    spec: OtelApiSyncSpec,
    now: Optional[dt.datetime] = None,
) -> Tuple[bool, str]:
    """Decide whether a group is complete enough to dispatch.

    Returns ``(ready, reason)``; the reason is logged either way so a
    group sitting undispatched is always explainable.
    """
    readiness = spec.readiness
    now = now or dt.datetime.now(dt.timezone.utc)

    if readiness.complete_when:
        env = build_environment(spec.attribute_columns, spec.strict_undefined)
        if bool(render_value(readiness.complete_when, {"rows": rows, "now": now}, env)):
            return True, "complete_when matched"

    timestamps = [_as_datetime(row_get(row, readiness.timestamp_column)) for row in rows]
    timestamps = [t for t in timestamps if t is not None]

    if not timestamps:
        # Nothing to reason about — a group with no usable timestamps
        # cannot be gated, so let it through rather than strand it.
        return True, "no usable timestamps; quiet period not applicable"

    newest, oldest = max(timestamps), min(timestamps)

    if readiness.max_age_seconds is not None:
        if (now - oldest).total_seconds() >= readiness.max_age_seconds:
            return True, f"group older than max_age_seconds ({readiness.max_age_seconds}s)"

    if readiness.quiet_period_seconds:
        quiet_for = (now - newest).total_seconds()
        if quiet_for < readiness.quiet_period_seconds:
            return False, (
                f"still filling — newest row is {quiet_for:.0f}s old, "
                f"quiet period is {readiness.quiet_period_seconds}s"
            )
        return True, f"quiet for {quiet_for:.0f}s"

    if readiness.complete_when:
        return False, "complete_when not matched and no quiet period configured"

    return True, "no readiness gate configured"


_MIN_TIME = dt.datetime.min.replace(tzinfo=dt.timezone.utc)


def _row_digest(row: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(row, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


def canonical_rows(rows: List[Dict[str, Any]], spec: OtelApiSyncSpec) -> List[Dict[str, Any]]:
    """Order rows canonically before anything derives from them.

    Every collection helper preserves *input* order, and SQL guarantees
    no order without ORDER BY — so the same group re-read from ClickHouse
    can yield a differently-ordered artifact list, a different body, a
    different ``call_key``, and an unchanged call gets re-sent. That
    defeats the per-call delivery suppression exactly when it matters.

    Sorting by event time (with a content digest as tiebreaker) makes
    rendering a pure function of the row *set*, not the row *sequence*,
    and has the side benefit that per-event steps fan out chronologically.
    """
    timestamp_column = spec.readiness.timestamp_column

    def sort_key(row: Dict[str, Any]):
        moment = _as_datetime(row_get(row, timestamp_column))
        return (moment is None, moment or _MIN_TIME, _row_digest(row))

    return sorted(rows, key=sort_key)


def _derive(rows: List[Dict[str, Any]], group_key: Any, spec: OtelApiSyncSpec, env) -> Dict[str, Any]:
    """Evaluate the derive block in declaration order.

    Each derived name is visible to those declared after it, which is
    what lets a per-item lookup table be built on top of a distinct list.
    """
    context: Dict[str, Any] = {"rows": rows, "group_key": group_key}
    for name, expression in spec.derive.items():
        context[name] = render_value(expression, dict(context), env)
    return context


def _item_label(value: Any, index: int) -> str:
    """A short, deterministic label for a fanned-out item.

    This ends up in the durable Restate step name, so it must be stable
    across retries and unique within the step — an index alone is not
    enough (plans are re-rendered) and a raw value is not always safe.
    """
    if isinstance(value, (str, int, float, bool)):
        text = str(value)
    else:
        text = json.dumps(value, sort_keys=True, default=str)
    text = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in text)
    if len(text) > 48:
        digest = hashlib.sha256(text.encode()).hexdigest()[:8]
        text = f"{text[:39]}-{digest}"
    return text or f"item{index}"


def _render_path(template: str, scope: Dict[str, Any], path_env, query, env) -> str:
    """Render a path template with percent-encoded substitutions."""
    path = path_env.from_string(template).render(**scope)
    query_string = render_query(query, scope, env) if query else ""
    return f"{path}?{query_string}" if query_string else path


def _call_key(step_id: str, item_key: str, method: str, path: str, body: Any) -> str:
    """Content identity of a single call.

    Includes the body, so a call whose payload legitimately changed
    (an artifact list that grew) is a *different* call and runs again,
    while a call already delivered verbatim is recognised and skipped.
    This is what stops a re-dispatch carrying a superset of rows from
    re-sending append-style calls for the rows it already covered.
    """
    canonical = json.dumps(
        [step_id, item_key, method, path, body], sort_keys=True, default=str, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _render_call(
    step: StepSpec, scope: Dict[str, Any], env, path_env, item_key: str
) -> Dict[str, Any]:
    path = _render_path(step.path, scope, path_env, step.query, env)
    body = render_structure(step.payload, scope, env) if step.payload is not None else None
    method = step.method.upper()

    call: Dict[str, Any] = {
        "item_key": item_key,
        "method": method,
        "path": path,
        "headers": {k: render_value(v, scope, env) for k, v in step.headers.items()},
        "body": body,
        "call_key": _call_key(step.id, item_key, method, path, body),
        "dedupe": step.dedupe,
        "on_status": {},
        "fragments": {},
    }

    for status, fallback in step.on_status.items():
        if fallback.mode == "item":
            # Rendered in the *failing item's* scope, so it can only ever
            # reference that item. A fallback that could see the whole
            # group would be able to overwrite state for items that
            # succeeded — which, against replace-semantics bulk
            # endpoints, destroys data that was never in the telemetry.
            call["on_status"][str(status)] = {
                "method": fallback.method.upper(),
                "path": _render_path(fallback.path, scope, path_env, None, env),
                "headers": {k: render_value(v, scope, env) for k, v in fallback.headers.items()},
                "body": render_structure(fallback.payload, scope, env)
                if fallback.payload is not None
                else None,
            }
        else:
            call["fragments"][str(status)] = render_structure(fallback.fragment, scope, env)

    return call


def _render_step(
    step: StepSpec, group_context: Dict[str, Any], env, path_env
) -> Optional[Dict[str, Any]]:
    """Render one step into its calls plus any aggregate fallbacks."""
    calls: List[Dict[str, Any]] = []

    if step.for_each:
        items = render_value(step.for_each, dict(group_context), env)
        if items is None:
            items = []
        if isinstance(items, dict):
            items = list(items.keys())
        seen_labels: Dict[str, int] = {}
        for index, element in enumerate(items):
            scope = dict(group_context)
            scope["item"] = element
            scope["loop_index"] = index
            if step.skip_when and bool(render_value(step.skip_when, scope, env)):
                continue
            raw_key = render_value(step.item_key, scope, env) if step.item_key else element
            label = _item_label(raw_key, index)
            # Two items can legitimately render the same label; disambiguate
            # deterministically so durable step names stay unique.
            if label in seen_labels:
                seen_labels[label] += 1
                label = f"{label}-{seen_labels[label]}"
            else:
                seen_labels[label] = 0
            calls.append(_render_call(step, scope, env, path_env, label))
    else:
        scope = dict(group_context)
        if step.skip_when and bool(render_value(step.skip_when, scope, env)):
            return None
        calls.append(_render_call(step, scope, env, path_env, "single"))

    if not calls:
        return None

    aggregate_fallbacks = []
    for status, fallback in step.on_status.items():
        if fallback.mode != "aggregate":
            continue
        aggregate_fallbacks.append(
            {
                "status": int(status),
                "method": fallback.method.upper(),
                "path": _render_path(fallback.path, dict(group_context), path_env, None, env),
                "headers": {
                    k: render_value(v, dict(group_context), env)
                    for k, v in fallback.headers.items()
                },
                # The container is rendered with its collection slot as
                # written; the handler replaces the slot at collect_into
                # with the fragments of the calls that actually failed.
                "body": render_structure(fallback.payload, dict(group_context), env)
                if fallback.payload is not None
                else {},
                "collect_into": fallback.collect_into,
            }
        )

    return {
        "id": step.id,
        "continue_on_error": step.continue_on_error,
        "calls": calls,
        "aggregate_fallbacks": aggregate_fallbacks,
    }


def compute_plan_hash(steps: List[Dict[str, Any]]) -> str:
    """Stable digest of the rendered calls.

    Identity is the payload, not the run: re-rendering the same rows
    yields the same hash, which is what makes the dispatch ledger and the
    ingress idempotency key able to recognise a re-dispatch.
    """
    canonical = json.dumps(steps, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def build_plan(group_key: Any, rows: List[Dict[str, Any]], spec: OtelApiSyncSpec) -> Dict[str, Any]:
    """Render one execution group into a complete, ordered call plan."""
    env = build_environment(spec.attribute_columns, spec.strict_undefined)
    path_env = build_path_environment(spec.attribute_columns, spec.strict_undefined)
    # Canonical first: the plan (and every call_key in it) must depend on
    # which rows are present, never on the order the source returned them.
    rows = canonical_rows(rows, spec)
    group_context = _derive(rows, group_key, spec, env)

    steps: List[Dict[str, Any]] = []
    for step in spec.steps:
        rendered = _render_step(step, group_context, env, path_env)
        if rendered is not None:
            steps.append(rendered)

    plan_hash = compute_plan_hash(steps)
    return {
        "format_version": PLAN_FORMAT_VERSION,
        "plan_id": f"{group_key}:{plan_hash}",
        "group_key": str(group_key),
        "plan_hash": plan_hash,
        "api": {
            "base_url": spec.api.base_url,
            "base_url_env": spec.api.base_url_env,
            "headers": dict(spec.api.headers),
            "idempotency_header": spec.api.idempotency_header,
            "header_env": dict(spec.api.header_env),
            "timeout_seconds": spec.api.timeout_seconds,
            "retry_statuses": list(spec.api.retry_statuses),
        },
        "steps": steps,
        "row_count": len(rows),
    }


def render_plans(
    rows: Iterable[Dict[str, Any]],
    spec: OtelApiSyncSpec,
    now: Optional[dt.datetime] = None,
) -> Tuple[List[Dict[str, Any]], List[Tuple[Any, str]]]:
    """Group, gate, and render.

    Returns ``(plans, deferred)`` where ``deferred`` lists
    ``(group_key, reason)`` for groups held back by the readiness gate.
    """
    groups = group_rows(rows, spec)
    plans: List[Dict[str, Any]] = []
    deferred: List[Tuple[Any, str]] = []

    for group_key in sorted(groups, key=lambda k: str(k)):
        group = groups[group_key]
        ready, reason = group_readiness(group, spec, now)
        if not ready:
            deferred.append((group_key, reason))
            continue
        plans.append(build_plan(group_key, group, spec))

    return plans, deferred
