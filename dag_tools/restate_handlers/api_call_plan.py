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

**A failed plan is recorded, never raised.** ``execute_plan`` never lets
a plan-level failure escape as ``restate.TerminalError``. Two reasons,
both hard requirements: Dagster dispatches through ``/send``, the
one-way ingress, so nothing ever observes a raised exception — a
TerminalError here is simply invisible. And Restate rolls back
uncommitted state on a terminal failure, which would destroy the very
failure record the Dagster asset check reads back through
``get_status``. So a halted plan is written to object state and
returned normally with ``status: "FAILED"``. The trade-off this buys:
the Restate invocation itself always completes successfully, so
failures must be read from object state, never inferred from
``restate invocations list``.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import re
import threading
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import restate

# Only the dependency-free wire contract. Depending on the renderer would
# put a template engine within reach of this handler and invite lazy,
# execution-time rendering of fallback bodies — which is exactly what the
# pre-rendered plan exists to prevent.
from dag_tools.otel_api_sync.plan import PLAN_FORMAT_VERSION, set_path

logger = logging.getLogger(__name__)

_INACTIVITY_ENV = "API_CALL_PLAN_INACTIVITY_TIMEOUT_SECONDS"
_ABORT_ENV = "API_CALL_PLAN_ABORT_TIMEOUT_SECONDS"

# Restate's server defaults, used by the permit-wait guard when the env vars
# above are unset (so the worker assumes the server is running its defaults).
_DEFAULT_INACTIVITY_SECONDS = 60.0
_DEFAULT_ABORT_SECONDS = 600.0


def _env_seconds(name: str) -> Optional[float]:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return None
    try:
        value = float(raw)
    except ValueError:
        logger.warning("ignoring %s=%r: not a number of seconds", name, raw)
        return None
    if value <= 0:
        logger.warning("ignoring %s=%r: must be positive", name, raw)
        return None
    return value


def _service_options() -> Dict[str, Any]:
    """Service-level timeouts from the worker env, or nothing.

    Unset means the kwarg is not passed at all. SDK 0.14 still negotiates
    discovery v2, and ``compute_discovery_json`` raises ``ValueError`` for
    these fields at v2, which would fail registration and leave the worker
    serving nothing. So the default path must stay byte-identical.
    """
    options: Dict[str, Any] = {}
    inactivity = _env_seconds(_INACTIVITY_ENV)
    if inactivity is not None:
        options["inactivity_timeout"] = timedelta(seconds=inactivity)
    abort = _env_seconds(_ABORT_ENV)
    if abort is not None:
        options["abort_timeout"] = timedelta(seconds=abort)
    return options


service = restate.VirtualObject(name="ApiCallPlanService", **_service_options())

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


def _resolve_limits(plan: Dict[str, Any]) -> Tuple[Optional[int], Dict[str, Any]]:
    """Optional concurrency cap and retry bound carried by the plan.

    Every field is optional and unset means today's behaviour: no cap and
    an unbounded ``ctx.run`` retry.
    """
    api = plan.get("api") or {}
    cap = api.get("max_concurrency")
    cap = int(cap) if cap else None
    if cap is not None and cap < 1:
        cap = None
    run_options: Dict[str, Any] = {}
    if api.get("max_attempts"):
        run_options["max_attempts"] = int(api["max_attempts"])
    if api.get("max_retry_seconds"):
        run_options["max_retry_duration"] = timedelta(seconds=float(api["max_retry_seconds"]))
    return cap, run_options


# --- connection pool: one Session per executor thread -----------------------

_thread_local = threading.local()


def _new_session(pool_maxsize: int):
    """Build the Session a worker thread keeps for its lifetime.

    ``Session.request`` still merges environment settings (``trust_env``),
    so ``REQUESTS_CA_BUNDLE`` keeps applying -- the private-CA invariant
    rests on that.
    """
    import requests
    from requests.adapters import HTTPAdapter

    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=max(10, int(pool_maxsize)))
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _thread_session(pool_maxsize: int):
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = _new_session(pool_maxsize)
        _thread_local.session = session
    return session


# --- per-API concurrency cap -------------------------------------------------

_DEFAULT_POOL_SIZE = min(32, (os.cpu_count() or 1) + 4)
_runtime_lock = threading.Lock()
_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
_executor_size = 0
# base_url -> (cap, loop, semaphore). A semaphore is bound to the loop it
# first contends on, so an entry is rebuilt if the running loop changes.
_semaphores: Dict[str, Tuple[int, Any, asyncio.Semaphore]] = {}
_warned_deadline: set = set()


def _reset_runtime() -> None:
    """Drop the executor, semaphores and warning memory (tests)."""
    global _executor, _executor_size
    with _runtime_lock:
        if _executor is not None:
            _executor.shutdown(wait=False)
        _executor = None
        _executor_size = 0
        _semaphores.clear()
        _warned_deadline.clear()
    _thread_local.__dict__.clear()


def _get_executor(cap: Optional[int]) -> concurrent.futures.ThreadPoolExecutor:
    """The module's own pool, sized to the largest cap seen.

    The floor is the default pool's size, so API calls neither shrink below
    what they had nor compete with other handlers' sync actions, and a cap
    above 32 is real.
    """
    global _executor, _executor_size
    wanted = max(_DEFAULT_POOL_SIZE, cap or 0)
    with _runtime_lock:
        if _executor is None or wanted > _executor_size:
            old = _executor
            _executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=wanted, thread_name_prefix="api-call-plan"
            )
            _executor_size = wanted
            if old is not None:
                old.shutdown(wait=False)  # queued and running work drains
        return _executor


def _get_semaphore(base_url: str, cap: int) -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    with _runtime_lock:
        entry = _semaphores.get(base_url)
        if entry is None or entry[0] != cap or entry[1] is not loop:
            # A different cap replaces the semaphore for new acquirers;
            # calls holding the old one drain, so the overshoot is bounded
            # by the old value and transient.
            entry = (cap, loop, asyncio.Semaphore(cap))
            _semaphores[base_url] = entry
        return entry[2]


def _permit_deadline_seconds(base_url: str, timeout: float) -> float:
    """How long the action may wait for a permit and still finish its request.

    ``inactivity + abort - timeout_seconds - margin``. The action must never
    *start* a request it cannot finish before Restate aborts the attempt:
    the abandoned thread's request would complete anyway and the retry
    would send it again. A non-positive result is treated as "no wait",
    never as "no deadline".
    """
    inactivity = _env_seconds(_INACTIVITY_ENV) or _DEFAULT_INACTIVITY_SECONDS
    abort = _env_seconds(_ABORT_ENV) or _DEFAULT_ABORT_SECONDS
    bound = inactivity + abort
    margin = max(5.0, 0.1 * bound)
    deadline = bound - timeout - margin
    if deadline <= 0:
        if base_url not in _warned_deadline:
            _warned_deadline.add(base_url)
            logger.warning(
                "api_call_plan: timeout_seconds=%s leaves no permit-wait budget inside "
                "inactivity+abort=%ss for %s; calls that cannot start at once are "
                "refused and retried. Raise %s or lower timeout_seconds.",
                timeout,
                bound,
                base_url,
                _ABORT_ENV,
            )
        return 0.0
    return deadline


class PermitWaitExpired(RuntimeError):
    """Raised (retryable) before sending when no permit arrived in time."""


def _make_request(
    method: str,
    url: str,
    body: Any,
    headers: Dict[str, str],
    timeout: float,
    retry_statuses: List[int],
    base_url: str = "",
    max_concurrency: Optional[int] = None,
):
    """Build the async action handed to ``ctx.run``.

    Returns ``{"status": int, "body": Any}`` for anything the plan might
    want to branch on; raises only for conditions a retry could fix, so
    Restate's backoff does the right thing.

    The permit wait is inside the action: the SDK calls it only when the
    step actually executes, so a replay returns the journaled result
    without ever waiting. The permit is released when the HTTP *thread*
    finishes (its future's done-callback), not when this coroutine ends:
    the SDK cancels the task when an attempt ends, and cancelling a task
    awaiting ``run_in_executor`` does not stop the thread.
    """

    def _execute() -> Dict[str, Any]:
        session = _thread_session(max_concurrency or 0)
        response = session.request(method, url, json=body, headers=headers, timeout=timeout)

        if response.status_code in retry_statuses:
            # Raising hands control back to Restate's retry policy --
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

    async def _action() -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        semaphore: Optional[asyncio.Semaphore] = None
        if max_concurrency:
            semaphore = _get_semaphore(base_url, max_concurrency)
            deadline = _permit_deadline_seconds(base_url, timeout)
            started = loop.time()
            # Not asyncio.wait_for: before 3.12 it returns the inner result and
            # drops an outer cancel that lands in the tick the permit is granted,
            # so an abandoned attempt would go on to send a request its retry
            # sends again.
            acquiring = asyncio.ensure_future(semaphore.acquire())
            try:
                done, _ = await asyncio.wait({acquiring}, timeout=deadline)
            except asyncio.CancelledError:
                if acquiring.done() and not acquiring.cancelled():
                    semaphore.release()
                else:
                    acquiring.cancel()
                raise
            if not done:
                acquiring.cancel()
                try:
                    await acquiring
                except asyncio.CancelledError:
                    pass
                else:  # granted while cancelling
                    semaphore.release()
                raise PermitWaitExpired(
                    f"no permit for {base_url} within {deadline:.0f}s (cap {max_concurrency}); "
                    "refused before sending so the attempt cannot overrun Restate's abort "
                    "timeout. Retrying."
                )
            logger.info(
                "api_call_plan permit_wait base_url=%s seconds=%.3f",
                base_url,
                loop.time() - started,
            )

        try:
            future = _get_executor(max_concurrency).submit(_execute)
        except BaseException:
            if semaphore is not None:
                semaphore.release()
            raise

        if semaphore is not None:
            held = semaphore

            def _release(_done: "concurrent.futures.Future") -> None:
                try:
                    loop.call_soon_threadsafe(held.release)
                except RuntimeError:  # loop already closed; nothing waits any more
                    pass

            future.add_done_callback(_release)
        return await asyncio.wrap_future(future)

    return _action


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
    max_concurrency: Optional[int] = None,
    run_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = f"{base_url}{path if str(path).startswith('/') else '/' + str(path)}"
    logger.info("plan step %s: %s %s", step_name, method, url)
    return await ctx.run(
        step_name,
        _make_request(
            method, url, body, headers, timeout, retry_statuses, base_url, max_concurrency
        ),
        **(run_options or {}),
    )


def _plan_level_failure(group_key: str, plan_hash: str, exc: Exception) -> Dict[str, Any]:
    """Summary for a failure that happens before any call is attempted.

    Recorded the same way as a mid-plan halt: no ``step``/``item`` scope
    exists yet, so the failure carries only ``phase: "plan"``.
    """
    return {
        "status": "FAILED",
        "halted": True,
        "group_key": group_key,
        "plan_hash": plan_hash,
        "calls_executed": 0,
        "calls_skipped_already_delivered": 0,
        "fallbacks_run": 0,
        "failures": [{"phase": "plan", "status": 0, "error": str(exc)}],
    }


@service.handler()
async def execute_plan(ctx: restate.ObjectContext, plan: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one rendered call plan for this object's execution group."""

    plan_hash = str(plan.get("plan_hash") or "")
    group_key = str(plan.get("group_key") or ctx.key())

    version = int(plan.get("format_version") or 0)
    if version != PLAN_FORMAT_VERSION:
        # Refuse rather than half-execute a plan shape we do not know —
        # but record the refusal, because a raise here would be rolled
        # back and the asset check would see nothing at all.
        summary = _plan_level_failure(
            group_key,
            plan_hash,
            restate.TerminalError(
                f"unsupported call plan format_version {version}; this worker "
                f"speaks version {PLAN_FORMAT_VERSION}"
            ),
        )
        ctx.set(_LAST_RESULT_KEY, summary)
        return summary

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

    try:
        base_url, base_headers, timeout, retry_statuses, idempotency_header = _resolve_api(plan)
    except restate.TerminalError as exc:
        summary = _plan_level_failure(group_key, plan_hash, exc)
        ctx.set(_LAST_RESULT_KEY, summary)
        return summary

    max_concurrency, run_options = _resolve_limits(plan)

    def _call_failed_terminally(exc: "restate.TerminalError", step_id: str, item: str,
                                phase: str) -> Dict[str, Any]:
        # The call ended terminally: usually the retry bound
        # (api.max_attempts / max_retry_seconds) ran out, but an operator
        # cancelling the invocation or a terminal raised from the closure
        # lands here too. A raise here would roll back object state, so it is recorded and the
        # plan halts through the same path as any other unrecoverable call.
        logger.error(
            "group %s step '%s' item '%s': call ended terminally (%s); halting plan %s",
            group_key, step_id, item, exc, plan_hash,
        )
        return {
            "step": step_id,
            "item": item,
            "status": 0,
            "phase": phase,
            "error": str(exc),
            "body": None,
        }

    delivered: List[str] = (await ctx.get(_COMPLETED_CALLS_KEY)) or []
    delivered_set = set(delivered)
    newly_delivered: List[str] = []

    executed = 0
    skipped = 0
    fallbacks_run = 0
    failures: List[Dict[str, Any]] = []
    halted = False

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

            try:
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
                    max_concurrency,
                    run_options,
                )
            except restate.TerminalError as exc:
                failures.append(_call_failed_terminally(exc, step_id, item_key, "call"))
                halted = True
                break
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
                try:
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
                        max_concurrency,
                        run_options,
                    )
                except restate.TerminalError as exc:
                    failures.append(
                        _call_failed_terminally(exc, step_id, item_key, "fallback")
                    )
                    halted = True
                    break
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
                logger.error(
                    "group %s step '%s' item '%s' failed with %s and no fallback matched; "
                    "halting plan %s",
                    group_key,
                    step_id,
                    item_key,
                    failure["status"],
                    plan_hash,
                )
                halted = True
                break

        if halted:
            break

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
            try:
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
                    max_concurrency,
                    run_options,
                )
            except restate.TerminalError as exc:
                failures.append(
                    _call_failed_terminally(
                        exc, step_id, f"aggregate-{status}", "aggregate_fallback"
                    )
                )
                halted = True
                break
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
                    logger.error(
                        "group %s step '%s' aggregate fallback for %s failed with %s: %s; "
                        "halting plan %s",
                        group_key,
                        step_id,
                        status,
                        agg_status,
                        agg_result.get("body"),
                        plan_hash,
                    )
                    halted = True
                    break

        if halted:
            break

    if halted:
        status_value = "FAILED"
    elif failures:
        status_value = "COMPLETED_WITH_ERRORS"
    else:
        status_value = "COMPLETED"

    summary = {
        "status": status_value,
        "halted": halted,
        "group_key": group_key,
        "plan_hash": plan_hash,
        "calls_executed": executed,
        "calls_skipped_already_delivered": skipped,
        "fallbacks_run": fallbacks_run,
        "failures": failures,
    }

    # Calls that already landed must be banked even on a halted plan — a
    # raise used to discard this and a corrected re-dispatch would re-send
    # calls that already succeeded.
    if newly_delivered:
        ctx.set(
            _COMPLETED_CALLS_KEY,
            (delivered + newly_delivered)[-_COMPLETED_CALLS_HISTORY:],
        )
    # A halted plan is incomplete: don't mark its hash completed, so a
    # corrected re-dispatch of the same hash is allowed through. Per-call
    # dedupe (above) protects the calls that already landed.
    if plan_hash and not halted:
        ctx.set(_COMPLETED_KEY, (completed + [plan_hash])[-_COMPLETED_HISTORY:])
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
