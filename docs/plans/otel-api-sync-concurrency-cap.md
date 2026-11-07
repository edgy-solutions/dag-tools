# OTel → API sync: cap concurrent calls to the target API

Status: approved 2026-11-06 (D1 unset, D2 ship off, D3 mapping/component),
with two conditions, folded in below:

1. The Restate timeout measurement is the gate, not a note (§4, Gate).
2. The per-process × replicas arithmetic, with the effective-cap formula,
   goes in `docs/RECIPE.md` and the component's docstring (Docs).

Measure, then PR. Rides 0.12.x.

## Problem

A sync that dispatches many execution groups at once gets slower per record
as the group count rises, and slower in total throughput too:

| groups in flight | records done | wall time | throughput |
|---|---|---|---|
| 1 | 1 | ~0.5 s | ~2 / s |
| 65 | 65 | ~80 s | ~0.8 / s |
| ~300 | 95 | 300 s (check timeout) | ~0.3 / s |

Pure queueing keeps throughput flat; this falls as load rises, so something
degrades under concurrency and lowering concurrency will raise throughput.
The 300 s is only the completion check's stopwatch
(`restate_plans_completed`'s default timeout); the work is durable and
continues, which is why a restarted worker resumed the remainder.

## What the code does today

- Every call is a bare `requests.request(...)` inside the `ctx.run` closure
  (`api_call_plan.py::_make_request`). No session, so every call opens a
  new TCP + TLS connection and loads the CA bundle (the merged bundle, when
  `caCerts` is on).
- SDK 0.14 runs a sync action through `loop.run_in_executor(None, ...)`, so
  the only limit on calls in flight is the default thread pool,
  `min(32, os.cpu_count() + 4)` per worker process. On Kubernetes
  `os.cpu_count()` is the node's count, not the pod's limit. That limit is
  accidental, unconfigurable, and shared with every other handler in the
  worker that runs a sync `ctx.run` action.
- `ctx.run` is called with no retry bound, and `timeout_seconds` defaults
  to 30. Once the API's latency under load passes 30 s, every call times
  out, raises, and is retried while the server may still be processing the
  abandoned request: load on the API grows as useful throughput falls.
  This is the most likely reason throughput *falls* rather than flattens.
  It is a hypothesis; the rollout section says how to confirm it.

## Change

### 1. `api.max_concurrency`: a per-API cap in the worker

- New optional field on `ApiSpec` (mapping.yaml) and on the component's
  `api:` override (component.yaml), merged exactly like `timeout_seconds`:
  the component value wins. Rendered into `plan["api"]`, so `dry_run` shows
  it and changing it needs no worker redeploy.
- Worker: a process-wide registry of `asyncio.Semaphore`s keyed by the
  resolved `base_url`. The `ctx.run` action becomes an `async def` that
  acquires the semaphore and then runs the blocking request on an executor.
- The acquire is **inside the action**, not around `await ctx.run(...)`.
  The SDK invokes the action only when it actually executes the step
  (`run_coros_to_execute`); a replay returns the journaled result without
  calling it. So replays never wait for a permit. Moving the wait *outside*
  the durable step (the approval's alternative) does not escape Restate's
  timers: the attempt is just as idle to the server, the wait would run
  again on every replay, and a suspension would be noticed only at the
  next context call. Inside the action is the better place; §4 is what
  makes it safe.
- **The permit is held until the HTTP thread finishes, not until the
  coroutine ends.** When an attempt ends, the SDK cancels the action's task
  (`Tasks.cancel()` in `on_attempt_finished`), and cancelling a task that
  awaits `run_in_executor` cancels the asyncio future but not the thread:
  the request carries on. A permit released by `async with` would be handed
  to the next call while the abandoned request is still in flight, and the
  cap would overshoot. So: `executor.submit(...)` for a
  `concurrent.futures.Future`, release through its `add_done_callback`
  (via `loop.call_soon_threadsafe`), await `asyncio.wrap_future(...)`. A
  call that was queued on the executor but never started is cancelled
  outright and its callback releases the permit at once.
- No cap set means no semaphore, so behaviour is unchanged (D1).
- A plan carrying a different cap for a `base_url` already registered
  replaces the semaphore for new acquirers; calls already holding the old
  one drain. The overshoot is transient and bounded by the old value; this
  is documented rather than engineered away.
- Calls run on a dedicated `ThreadPoolExecutor` owned by the module, not the
  loop's default. It is sized to the largest cap seen, with a floor of the
  default pool's size, so a cap above 32 is real and API calls stop
  competing with other handlers' sync actions.
- The cap is **per worker process**. One hypercorn process serves the whole
  worker (`serve.py`), one process per pod, so the effective limit at the
  API is

      effective concurrency = max_concurrency × restateWorker.replicas

  A cluster-wide limit (a permit-issuing VirtualObject whose waiters
  suspend on awakeables, so no timer applies while they wait) is out of
  scope until there is more than one worker pod.

### 2. One connection pool per thread

- Replace `requests.request` with a `requests.Session` held in a
  `threading.local`, created on first use in each executor thread, with
  an `HTTPAdapter` whose `pool_maxsize` covers the cap. Per thread, because
  requests does not document `Session` as thread-safe; reuse still happens
  because the executor's threads are long-lived.
- `Session.request` still merges environment settings (`trust_env`), so
  `REQUESTS_CA_BUNDLE` keeps applying. The CA invariant in AGENTS §4 rests
  on exactly that, so it gets a regression test: a session built the way
  the handler builds it verifies against a bundle named only by
  `REQUESTS_CA_BUNDLE`.
- Satisfies the AGENTS rule against building a heavy client per call.

### 3. Optional retry bound (D2)

- New optional `api.max_attempts` and `api.max_retry_seconds`, passed to
  `ctx.run` as `max_attempts` / `max_retry_duration`. Unset means unbounded,
  as today.
- When a bound is exhausted the SDK raises `TerminalError` at the `await`.
  Today nothing catches a `TerminalError` around `_run_call`; it would escape
  `execute_plan`, roll back object state and break the "a failed plan is
  recorded, never raised" invariant. Each of the three call sites (primary,
  item fallback, aggregate fallback) catches it and halts the plan through
  the existing halt path: `status: "FAILED"`, delivered calls banked in
  `completed_call_keys`, plan hash withheld so a corrected re-dispatch is
  let through.

### 4. Restate's timeouts (approval condition 1)

**Measured 2026-11-06**, with Restate 1.6.2 (the version the deployments
pin) and 1.8.1, SDK 0.14.2, a `ctx.run` action that waits and then POSTs
to a fake API that logs every request, and the service's timeouts shortened
to inactivity 3 s / abort 6 s (harness:
`scratchpad/restate-probe/probe.py`):

| action | what Restate did | requests at the API |
|---|---|---|
| waits 1 s, sends | normal | 1 |
| waits 5 s (past inactivity, before inactivity + abort), sends | asks the invocation to suspend; the action keeps running, its completion is journaled, a fresh attempt replays it | **1** |
| waits 15 s (past inactivity + abort) | aborts the attempt at 9 s (1.6.2; ~14 s on 1.8.1); the SDK cancels the action's task, so it **never sends**; the retry starts the wait from zero and is aborted again, every attempt | **0**, and it never completes |
| sends at once, request takes 15 s | aborts mid-request; the abandoned thread's request completes anyway and each retry sends again | **5+ (duplicates)** |

Both versions also apply a default invocation retry policy of
`max_attempts: 70, on_max_attempts: Pause`, so the never-completing row
ends paused: neither done nor recorded FAILED.

What this means:

- Crossing the **inactivity** timeout alone is harmless. The bound that
  matters is **inactivity + abort** (60 s + 600 s by default).
- A call still **parked** on the semaphore when the abort fires sends
  nothing: no duplicate, but it loses its place in the queue and, while
  the backlog stays above the bound, never completes.
- The duplicate is a **request in flight** when the abort fires. Under a
  cap that happens when a long wait ends just before the abort deadline
  and the request is still in flight when it passes. That is the
  approval's concern, and it is real.
- Gate arithmetic: per-call wait ≈ groups in flight × per-call latency ÷
  effective concurrency. At cap 1, 300 groups and 0.5 s that is ~150 s:
  past the 1-min inactivity timeout (so the condition applies), under the
  11-min default bound, but only for latencies under ~2.2 s. That is too
  thin a margin to leave to Restate's defaults.

So both of these land in this PR:

**4a. The service's timeouts, set in the service config.**
`ApiCallPlanService` is built with `inactivity_timeout` / `abort_timeout`
from the worker env vars `API_CALL_PLAN_INACTIVITY_TIMEOUT_SECONDS` /
`API_CALL_PLAN_ABORT_TIMEOUT_SECONDS`, read when the module is imported.
Unset means the kwarg is not passed and discovery is byte-identical to
today. That is deliberate: SDK 0.14.2 still negotiates discovery v2, and
`compute_discovery_json` raises `ValueError` for these fields at v2, which
would fail registration and leave the worker serving nothing. The chart
sets them: new values `restateWorker.apiCallPlan.inactivityTimeoutSeconds`
(default unset, since the table shows crossing it is harmless) and
`restateWorker.apiCallPlan.abortTimeoutSeconds` (default `3600`), rendered
into the worker's env only when set. The values comment names the bound
(below) and the server versions measured.

**4b. A bounded permit wait: the duplicate guard.** The action never
*starts* a request it cannot finish before the abort deadline:

    permit deadline = inactivity + abort − timeout_seconds − margin

Use the env values above, or Restate's defaults (60 s / 600 s) when unset.
The margin is `max(5 s, 10 %)`. The deadline is measured from the start of
the action, which follows the attempt's last message to Restate. If no
permit is acquired by then, the action raises a retryable error **before
sending**. Restate retries the attempt with backoff, and the fresh attempt
resets its timers. A cap-induced wait therefore never puts a request across
the abort deadline. A deadline ≤ 0 (a timeout too long for the bound) is
logged at WARNING once per `base_url` and treated as "no wait allowed past
`timeout_seconds`", never as "no deadline". The guard reads the worker's
env, not the server's, so a server-wide default changed only in Restate's
own config is invisible to it. The docs say to set these timeouts through
the env vars, not server-wide.

The operator's sizing rule, which goes in the docs verbatim:

    groups in flight × per-call latency ÷ (max_concurrency × replicas)
        < inactivity + abort − timeout_seconds

Above it, calls are refused before sending and retried (no duplicates),
but sustained overload ends in Restate's `Pause` after 70 attempts. The fix
is a larger `abortTimeoutSeconds` or a higher cap.

## Not changed

- Dispatch stays one `/send` per group with no Dagster-side batching. It
  would bound only one Dagster run, hold that run open for the whole sync,
  and stretch the completion check across every wave.
- `PLAN_FORMAT_VERSION` is not bumped. The new fields are optional reads; a
  bump would make every older worker refuse every plan. The cost is that an
  older worker ignores the cap silently, so the CHANGELOG says the cap
  needs the new worker image.
- The pre-existing duplicate path (a single HTTP call longer than
  inactivity + abort) is out of scope; `timeout_seconds` (default 30)
  already keeps a call far inside the bound.

## Decisions (approved)

- **D1.** `max_concurrency` defaults to unset (no cap).
- **D2.** The retry bound ships, default off.
- **D3.** The cap is plan-carried, owned by the mapping/component author.

## Tests

- **Executor (fake ctx, fake HTTP).** Today the fake patches
  `requests.request`; it moves to patching the session factory. The fake
  server counts calls in flight.
  - `test_cap_bounds_calls_in_flight_across_groups`: 50 groups, cap 3,
    asserts the observed maximum in flight is 3.
  - `test_no_cap_is_unbounded_as_before`
  - `test_replay_does_not_wait_for_a_permit`: a journaled result returns
    while every permit is held.
  - `test_cap_is_per_base_url`
  - `test_a_cancelled_action_keeps_its_permit_until_the_request_finishes`:
    cancel the action mid-request; the next waiter acquires only after the
    fake request returns.
  - `test_a_parked_call_cancelled_before_its_permit_sends_nothing`
  - `test_permit_wait_past_the_deadline_raises_retryable_before_sending`
  - `test_permit_deadline_reads_the_worker_timeout_env`
  - `test_exhausted_retry_bound_halts_and_is_recorded_not_raised`: one test
    per call site.
  - `test_unbounded_by_default_passes_no_retry_options`
- **Service config.**
  - `test_service_timeouts_unset_leave_discovery_unchanged`: with the env
    unset, the discovery JSON at **v2** still renders (no `ValueError`) and
    equals today's.
  - `test_service_timeouts_from_env_reach_discovery`: at v3, both fields
    are present in milliseconds.
  - Chart: `abortTimeoutSeconds` renders the env var; unset
    `inactivityTimeoutSeconds` renders nothing.
- **Config.**
  - `test_component_max_concurrency_overrides_the_mapping`
  - `test_dry_run_plan_carries_max_concurrency`
- **CA.** `test_session_requests_still_honour_REQUESTS_CA_BUNDLE`
- **Opt-in live regression** (`DAGTOOLS_E2E_RESTATE=1`, needs Docker, like
  the dbt e2e): a real Restate 1.6.2 container, the real
  `ApiCallPlanService` (the worker container gets a local editable install),
  and a fake API that logs a key for every request. 30 groups, cap 1, and
  timeouts of 3 s / 6 s set through the env vars, so waits cross both
  bounds. Asserts:
  - no request key is received twice;
  - at most `cap` requests are ever in flight at the API;
  - every group ends COMPLETED or is still retrying, and none received a
    request that was then re-sent.

### Gate (before the PR; results go in the PR body)

Real Restate 1.6.2, the shared worker image built from the branch,
**default Restate timeouts plus the chart's `abortTimeoutSeconds`**, and a
fake API at 0.5 s per call, then a second fake API whose latency grows
with its in-flight count. Dispatch 300 groups with the cap unset, then at
1, 2, 5 and 10. For each run, record:

- wall time;
- the maximum in flight at the API;
- the maximum permit wait;
- Restate attempts per invocation;
- paused invocations;
- **duplicate requests at the API**, counted by call key.

**Pass:**

- zero duplicates in every capped run;
- the cap holds at the API;
- no paused invocations;
- every capped run beats the uncapped one on the collapsing API.

If cap 1 / 300 groups crosses the bound even with the chart default, the
default is raised until it does not, and the PR records the measured
numbers.

- Both full suites (1.13.1 and the 1.10.19 floor, with the compat job's
  exact install), skip list unchanged against master.

## Docs

- **Component docstring** (`OtelApiSyncComponent`): the effective-cap
  formula (`max_concurrency × restateWorker.replicas`), the sizing rule,
  and the two env vars.
- **`docs/RECIPE.md`**: a new section "OTel → API sync: concurrency and
  Restate's timeouts" covering the effective-cap formula, the sizing rule,
  the measured table above, and why the wait lives inside the step.
- **AGENTS §4a**: a new bullet covering:
  - the wait is inside the action;
  - the permit is held until the thread finishes;
  - the cap is per process, multiplied by replicas;
  - the inactivity timeout is harmless, inactivity + abort is the real
    bound, and an in-flight request is the duplicate;
  - the deadline guard;
  - discovery stays unchanged when the env is unset (v2);
  - one session per thread, with the CA test.
- **README and the example's README**: `max_concurrency` in the `api:`
  block, how to choose it (sweep; start low), and the sizing rule.
- **CHANGELOG**: Added, under the next 0.12.x. Note that the cap needs the
  new worker image, and that the chart now sets `abortTimeoutSeconds` (the
  old behaviour returns with the value unset).

## Rollout (the deployment, not this PR)

1. Confirm the amplification hypothesis first. Check Restate's
   invocation attempt counts for these groups, and the API's logs for
   repeated requests.
2. After the new worker image is deployed, set `api.max_concurrency` in
   component.yaml, and set the abort timeout env var (or the chart value)
   to cover the sizing rule. Sweep 1, 2, 5, 10 against ~300 groups and keep
   the fastest.
3. If attempts were above one, also set `timeout_seconds` above the API's
   loaded latency, or `max_attempts`.
