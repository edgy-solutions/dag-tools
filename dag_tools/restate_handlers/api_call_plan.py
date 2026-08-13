"""Durable executor for pre-rendered API call plans.

This service knows nothing about telemetry, entities, or any other
domain concept: it receives an ordered plan (see
``dag_tools.otel_api_sync.render``) and executes it exactly once, in
order, with per-call durability.

It is a **VirtualObject keyed by the execution group**, so two dispatches
touching the same group serialise instead of interleaving their step
sequences, while different groups still run fully in parallel.

Three behaviours here are not obvious and are the difference between
working and quietly-broken:

**Status is data, not an exception.** ``ctx.run`` retries whatever
raises. If the HTTP call raised on every non-2xx, a 404 — the thing a
fallback exists to handle — would be retried with backoff forever and
the fallback would never run. So the closure returns the status and the
classification happens outside: 2xx succeeds, a status with a fallback
runs the fallback, retryable statuses (5xx/429/…) raise so Restate
retries, and anything else is terminal.

**Keying serialises but does not deduplicate.** A later dispatch of the
same group is a *new* invocation and would re-run append-style calls.
The object therefore records the plan hashes it has completed in its own
durable state and short-circuits a repeat.

**Fallbacks are pre-rendered.** There is no template engine here. An
aggregate fallback arrives as a container body plus one fragment per
call; this handler splices in only the fragments of the calls that
actually failed. That constraint is deliberate — it makes it structurally
impossible to send a bulk request covering items that succeeded.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Tuple

import restate

# Only the dependency-free wire contract. Depending on the renderer would
# put a template engine within reach of this handler and invite lazy,
# execution-time rendering of fallback bodies — which is exactly what the
# pre-rendered plan exists to prevent.
from dag_tools.otel_api_sync.plan import PLAN_FORMAT_VERSION, set_path

logger = logging.getLogger(__name__)

service = restate.VirtualObject(name="ApiCallPlanService")

# Durable state keys on the group-keyed object.
_COMPLETED_KEY = "completed_plan_hashes"
_COMPLETED_CALLS_KEY = "completed_call_keys"
_LAST_RESULT_KEY = "last_result"

# How many completed plan hashes to remember per group. Bounded so a
# long-lived group key cannot grow its state without limit.
_COMPLETED_HISTORY = 50

# Both histories evict oldest-first (a plain FIFO window, not LRU): the
# lists are appended in delivery order and trimmed from the front. Past
# the cap, the earliest-delivered calls stop being recognised and would
# be re-sent if a plan still referenced them — so the cap must exceed the
# largest number of calls one execution group can accumulate across all
# its dispatches. See `purge` for the retention story.
#
# How many individual delivered calls to remember per group. This is the
# guard for the *overlapping* re-dispatch: a group force-dispatched on
# age, then re-dispatched when late rows arrive, renders a NEW plan hash
# whose per-row step legitimately covers the earlier rows too. Upserts
# tolerate that; append-style calls duplicate. Remembering delivered
# calls closes the window that the plan-hash check cannot see.
# ~17 bytes per entry, so the cap is well under Restate's state limits.
_COMPLETED_CALLS_HISTORY = 50_000

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand_env(template: str) -> str:
    """Expand ``${VAR}`` references from the worker's environment.

    Auth material is referenced by name in the plan and resolved here, so
    credentials never cross the Restate ingress inside a payload.
    """

    def _replace(match: "re.Match[str]") -> str:
        name = match.group(1)
        value = os.environ.get(name)
        if value is None:
            raise restate.TerminalError(
                f"call plan references environment variable {name!r}, which is not "
                "set on the Restate worker"
            )
        return value

    return _ENV_REF.sub(_replace, template)


def _resolve_api(
    plan: Dict[str, Any],
) -> Tuple[str, Dict[str, str], float, List[int], str]:
    api = plan.get("api") or {}

    base_url = api.get("base_url")
    if api.get("base_url_env"):
        base_url = os.environ.get(api["base_url_env"], base_url)
    if not base_url:
        raise restate.TerminalError(
            "call plan has no API base URL: set api.base_url in the mapping or "
            f"{api.get('base_url_env')!r} on the worker"
        )

    headers: Dict[str, str] = {"Content-Type": "application/json"}
    headers.update({k: str(v) for k, v in (api.get("headers") or {}).items()})
    for name, template in (api.get("header_env") or {}).items():
        headers[name] = _expand_env(str(template))

    timeout = float(api.get("timeout_seconds") or 30.0)
    retry_statuses = [int(s) for s in (api.get("retry_statuses") or [])]
    idempotency_header = api.get("idempotency_header") or ""
    return base_url.rstrip("/"), headers, timeout, retry_statuses, idempotency_header


def _make_request(
    method: str,
    url: str,
    body: Any,
    headers: Dict[str, str],
    timeout: float,
    retry_statuses: List[int],
):
    """Build the closure handed to ``ctx.run``.

    Returns ``{"status": int, "body": Any}`` for anything the plan might
    want to branch on; raises only for conditions a retry could fix, so
    Restate's backoff does the right thing.
    """

    def _execute() -> Dict[str, Any]:
        import requests

        response = requests.request(
            method, url, json=body, headers=headers, timeout=timeout
        )

        if response.status_code in retry_statuses:
            # Raising hands control back to Restate's retry policy —
            # correct for transient server errors and throttling.
            raise RuntimeError(
                f"{method} {url} -> {response.status_code} (retryable): "
                f"{response.text[:500]}"
            )

        try:
            parsed: Any = response.json()
        except ValueError:
            parsed = response.text[:2000]

        return {"status": response.status_code, "body": parsed}

    return _execute


def _is_success(status: int) -> bool:
    return 200 <= status < 300


async def _run_call(
    ctx: restate.ObjectContext,
    step_name: str,
    method: str,
    base_url: str,
    path: str,
    body: Any,
    headers: Dict[str, str],
    timeout: float,
    retry_statuses: List[int],
) -> Dict[str, Any]:
    url = f"{base_url}{path if str(path).startswith('/') else '/' + str(path)}"
    logger.info("plan step %s: %s %s", step_name, method, url)
    return await ctx.run(
        step_name,
        _make_request(method, url, body, headers, timeout, retry_statuses),
    )


@service.handler()
async def execute_plan(ctx: restate.ObjectContext, plan: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one rendered call plan for this object's execution group."""

    version = int(plan.get("format_version") or 0)
    if version != PLAN_FORMAT_VERSION:
        # Refuse rather than half-execute a plan shape we do not know.
        raise restate.TerminalError(
            f"unsupported call plan format_version {version}; this worker "
            f"speaks version {PLAN_FORMAT_VERSION}"
        )

    plan_hash = str(plan.get("plan_hash") or "")
    group_key = str(plan.get("group_key") or ctx.key())

    completed: List[str] = (await ctx.get(_COMPLETED_KEY)) or []
    if plan_hash and plan_hash in completed:
        logger.info(
            "group %s already executed plan %s — skipping re-dispatch", group_key, plan_hash
        )
        return {
            "status": "SKIPPED_DUPLICATE",
            "group_key": group_key,
            "plan_hash": plan_hash,
        }

    base_url, base_headers, timeout, retry_statuses, idempotency_header = _resolve_api(plan)

    delivered: List[str] = (await ctx.get(_COMPLETED_CALLS_KEY)) or []
    delivered_set = set(delivered)
    newly_delivered: List[str] = []

    executed = 0
    skipped = 0
    fallbacks_run = 0
    failures: List[Dict[str, Any]] = []

    for step_index, step in enumerate(plan.get("steps") or []):
        step_id = step.get("id") or f"step{step_index}"
        continue_on_error = bool(step.get("continue_on_error"))
        aggregate_fallbacks = step.get("aggregate_fallbacks") or []
        aggregate_statuses = {int(a["status"]): a for a in aggregate_fallbacks}
        # status -> fragments of the calls that failed with it, and the
        # call keys that contributed them. The aggregate call delivers the
        # effect on behalf of those calls, so a successful aggregate must
        # mark them delivered too — otherwise a later overlapping dispatch
        # re-sends work that has already landed.
        collected: Dict[int, List[Any]] = {status: [] for status in aggregate_statuses}
        collected_keys: Dict[int, List[str]] = {status: [] for status in aggregate_statuses}

        for call in step.get("calls") or []:
            item_key = call.get("item_key") or "single"
            # Deterministic and unique: the same plan re-executed after a
            # crash resolves to the same journal entries.
            step_name = f"{step_index:02d}-{step_id}-{item_key}"

            call_key = call.get("call_key")
            if call.get("dedupe", True) and call_key and call_key in delivered_set:
                # Already delivered verbatim to this group by an earlier
                # dispatch — re-sending would duplicate an append-style
                # call and is pointless for an upsert.
                skipped += 1
                continue

            headers = dict(base_headers)
            headers.update({k: str(v) for k, v in (call.get("headers") or {}).items()})
            if idempotency_header and call_key:
                # Closes the at-least-once gap in ctx.run: a replayed side
                # effect carries the same key, so a server that honours it
                # recognises the retry instead of appending a duplicate.
                headers[idempotency_header] = f"{plan_hash}-{call_key}"

            result = await _run_call(
                ctx,
                step_name,
                str(call.get("method") or "POST"),
                base_url,
                call.get("path") or "/",
                call.get("body"),
                headers,
                timeout,
                retry_statuses,
            )
            executed += 1
            status = int(result.get("status", 0))

            if _is_success(status):
                if call_key:
                    delivered_set.add(call_key)
                    newly_delivered.append(call_key)
                continue

            # 1. Aggregate fallback: bank the fragment, act after the fan-out.
            if status in aggregate_statuses:
                fragment = (call.get("fragments") or {}).get(str(status))
                if fragment is not None:
                    collected[status].append(fragment)
                    if call_key:
                        collected_keys[status].append(call_key)
                    continue

            # 2. Per-item fallback: retry just this item elsewhere.
            fallback = (call.get("on_status") or {}).get(str(status))
            if fallback is not None:
                fb_headers = dict(base_headers)
                fb_headers.update(
                    {k: str(v) for k, v in (fallback.get("headers") or {}).items()}
                )
                if idempotency_header and call_key:
                    fb_headers[idempotency_header] = f"{plan_hash}-{call_key}-fb{status}"
                fb_result = await _run_call(
                    ctx,
                    f"{step_name}-fallback-{status}",
                    str(fallback.get("method") or "POST"),
                    base_url,
                    fallback.get("path") or "/",
                    fallback.get("body"),
                    fb_headers,
                    timeout,
                    retry_statuses,
                )
                fallbacks_run += 1
                fb_status = int(fb_result.get("status", 0))
                if _is_success(fb_status):
                    if call_key:
                        delivered_set.add(call_key)
                        newly_delivered.append(call_key)
                    continue
                failure = {
                    "step": step_id,
                    "item": item_key,
                    "status": fb_status,
                    "phase": "fallback",
                    "body": fb_result.get("body"),
                }
            else:
                failure = {
                    "step": step_id,
                    "item": item_key,
                    "status": status,
                    "phase": "call",
                    "body": result.get("body"),
                }

            failures.append(failure)
            if not continue_on_error:
                raise restate.TerminalError(
                    f"group {group_key} step '{step_id}' item '{item_key}' failed with "
                    f"{failure['status']} and no fallback matched: {failure['body']}"
                )

        # 3. One aggregate call per status, carrying only what failed.
        for status, fragments in collected.items():
            if not fragments:
                continue
            aggregate = aggregate_statuses[status]
            body = set_path(aggregate.get("body") or {}, aggregate["collect_into"], fragments)
            agg_headers = dict(base_headers)
            agg_headers.update({k: str(v) for k, v in (aggregate.get("headers") or {}).items()})
            if idempotency_header:
                agg_headers[idempotency_header] = (
                    f"{plan_hash}-{step_id}-aggregate-{status}"
                )
            agg_result = await _run_call(
                ctx,
                f"{step_index:02d}-{step_id}-aggregate-{status}",
                str(aggregate.get("method") or "POST"),
                base_url,
                aggregate.get("path") or "/",
                body,
                agg_headers,
                timeout,
                retry_statuses,
            )
            fallbacks_run += 1
            agg_status = int(agg_result.get("status", 0))
            if _is_success(agg_status):
                for key in collected_keys[status]:
                    if key not in delivered_set:
                        delivered_set.add(key)
                        newly_delivered.append(key)
            if not _is_success(agg_status):
                failures.append(
                    {
                        "step": step_id,
                        "item": f"aggregate-{status}",
                        "status": agg_status,
                        "phase": "aggregate_fallback",
                        "body": agg_result.get("body"),
                    }
                )
                if not continue_on_error:
                    raise restate.TerminalError(
                        f"group {group_key} step '{step_id}' aggregate fallback for "
                        f"{status} failed with {agg_status}: {agg_result.get('body')}"
                    )

    summary = {
        "status": "COMPLETED" if not failures else "COMPLETED_WITH_ERRORS",
        "group_key": group_key,
        "plan_hash": plan_hash,
        "calls_executed": executed,
        "calls_skipped_already_delivered": skipped,
        "fallbacks_run": fallbacks_run,
        "failures": failures,
    }

    if plan_hash:
        ctx.set(_COMPLETED_KEY, (completed + [plan_hash])[-_COMPLETED_HISTORY:])
    if newly_delivered:
        ctx.set(
            _COMPLETED_CALLS_KEY,
            (delivered + newly_delivered)[-_COMPLETED_CALLS_HISTORY:],
        )
    ctx.set(_LAST_RESULT_KEY, summary)

    return summary


@service.handler(kind="shared")
async def get_status(ctx: restate.ObjectSharedContext) -> Dict[str, Any]:
    """Read-only view of what this group has already executed."""
    return {
        "group_key": ctx.key(),
        "completed_plan_hashes": (await ctx.get(_COMPLETED_KEY)) or [],
        "delivered_call_count": len((await ctx.get(_COMPLETED_CALLS_KEY)) or []),
        "last_result": (await ctx.get(_LAST_RESULT_KEY)),
    }


@service.handler()
async def purge(ctx: restate.ObjectContext) -> Dict[str, Any]:
    """Forget everything this execution group has recorded.

    Retention is an explicit operational decision, not an accident.
    Every execution group is a distinct VirtualObject key, and Restate
    keeps an object's state until something clears it — so a pipeline
    keyed per run accumulates keys indefinitely. The state per key is
    small (a bounded hash list), but "small forever" is still forever.

    Call this once a group can no longer receive late telemetry — a
    natural fit for a scheduled sweep over groups older than the
    mapping's ``readiness.max_age_seconds``. Purging a group that *does*
    get re-dispatched is safe but re-enables duplicate delivery for it,
    so purge behind the same horizon that governs readiness.
    """
    group_key = ctx.key()
    delivered = len((await ctx.get(_COMPLETED_CALLS_KEY)) or [])
    plans = len((await ctx.get(_COMPLETED_KEY)) or [])
    ctx.clear_all()
    logger.info("purged state for group %s", group_key)
    return {
        "status": "PURGED",
        "group_key": group_key,
        "forgotten_plan_hashes": plans,
        "forgotten_calls": delivered,
    }
