# OpenTelemetry (ClickHouse) → any ordered set of API endpoints

A generic pipeline: **dlt** extracts telemetry from ClickHouse, a **YAML
mapping** turns it into API payloads, and **Restate** dispatches them
durably. The example implements four endpoints; the engine has no idea
how many there are.

```
ClickHouse (otel_traces)
   │  dlt extraction asset          incremental on Timestamp + lookback
   ▼
Postgres staging  ──►  <pipeline>_dispatch asset
                          │  group rows into execution groups
                          │  gate on readiness
                          │  render one ordered CallPlan per group
                          ▼
                    Restate ingress  /ApiCallPlanService/<group>/execute_plan/send
                          │  VirtualObject keyed by execution group
                          ▼
                     Target API   (PATCH → 404 → bulk POST, upserts, per-event records)
```

## What the mapping expresses

[`mapping.yaml`](dagster_home/components/otel_sync/mapping.yaml) is the
whole domain model. Four ordered steps:

| # | Step | Scope | Call |
|---|------|-------|------|
| 1 | `entity_artifacts` | per entity | `PATCH /api/EntityMaintenance/{id}`, 404 → aggregated bulk `POST` |
| 2 | `upsert_items` | per group | `POST /api/ProcessItemDetails/BulkUpdate` |
| 3 | `item_entity_map` | per item | `POST /api/ProcessItemDetails/{item}/EntityMapping` |
| 4 | `record_execution` | per event | `POST /api/RecordExecution` |

Adding a fifth endpoint is another entry under `steps:`. No Python, no
worker redeploy.

## The parts that are easy to get wrong

**Fallbacks are scoped to what failed.** Step 1's 404 fallback uses
`mode: aggregate`: each call carries a pre-rendered `fragment`, and the
handler splices only the fragments of the calls that actually 404'd into
the bulk body at `collect_into`. The bulk endpoint has replace
semantics — a fallback that resent every entity in the group would
overwrite server-side state for entities whose PATCH had just succeeded.
Use `mode: item` (the default) when the fallback should retry a single
item against a different endpoint instead.

**Status is data, not an exception.** The Restate handler returns the
HTTP status from inside `ctx.run` and classifies it outside: 2xx done,
a status with a fallback runs the fallback, 5xx/429 raise so Restate
retries with backoff, any other 4xx is terminal. If the HTTP call raised
on every non-2xx, a 404 would be retried forever and the fallback would
never run.

**JSON types survive.** Mapping expressions render through a combined
native + sandboxed Jinja environment, so a template that is a single
expression returns a real Python value. Attribute maps are
`Map(String, String)`, so use `as_int` / `as_float` / `as_bool` /
`split` / `metrics_from_prefix` for any field that is not a string.

**Path substitutions are percent-encoded — with one hard limit.**
Identifiers are often sentences, so `{{ item }}` in a `path:` is encoded
(`Login flow 'smoke' & retry` → `Login%20flow%20%27smoke%27%20%26%20retry`).
Literal separators the author writes are untouched, and `raw()` opts a
value out when it is meant to span segments.

What encoding *cannot* fix: an identifier containing a literal `/`.
Most servers — Starlette/FastAPI included — decode the path before
routing, so `%2F` becomes a real separator and the request no longer
matches a single-segment route. This was confirmed against the mock API
here: such a call returns 404 and the plan fails loudly rather than
misrouting silently. If your identifiers can contain slashes, carry them
in a `query:` parameter or the body rather than a path segment.

**Attribute maps are pinned to JSON through staging.** Left alone, dlt
flattens a nested map into `span_attributes__item_name` columns and
lower-cases every part — which loses the original attribute casing, so
`metric.NUM_ERROR` returns as `num_error`. The ClickHouse source hints
those columns as JSON by default. Lookups are *also* tolerant of both the
snake-cased and the flattened shapes, so a mapping file works staged or
direct, but the JSON hint is what preserves fidelity.

**Side effects are at-least-once at the crash boundary.** Restate makes
the *journal* exactly-once, but a call inside `ctx.run` that lands before
its result is journaled re-runs on replay. Killing the worker mid-plan
during an 802-call run produced exactly one extra call. `api.idempotency_header`
stamps a stable per-call key (`<plan hash>-<call key>`) so the server can
recognise the retry; the mock API here honours it and reports
`replays_suppressed`. Set it whenever the target API supports it — the
readiness gate, the ledger and the object state all guard *re-dispatch*,
but only the server can make the *crash-boundary* retry idempotent.

**Groups are gated before dispatch.** Spans for one execution group
arrive across several extraction runs. `readiness:` holds a group back
until it has been quiet for 5 minutes, releases it early on a terminal
marker (`complete_when`), and force-dispatches after `max_age_seconds`
so a lost marker cannot strand data.

**Re-dispatch is suppressed twice.** The Dagster-side ledger skips
sending a `(group, plan hash)` it has already sent; the group-keyed
Restate object independently refuses a plan hash it has completed.
Upsert-shaped calls tolerate duplication, but per-event records do not.

## Verified against the real stack

`docker compose up -d` then the checks below were run live, not simulated:

| Layer | Result |
|---|---|
| Worker self-registration via `RESTATE_SERVICES=api_call_plan` | `ApiCallPlanService` discovered as a VirtualObject with 3 handlers |
| Four endpoints, real HTTP | 8 calls, 1 aggregate fallback, bulk create carried only the 404'd entity |
| Duplicate dispatch | `SKIPPED_DUPLICATE`, no new records |
| Unknown `format_version` | terminal in 0.0s — no retry storm |
| **Worker killed mid-plan** (802 calls, killed after 22) | resumed, completed, API applied exactly 802 |
| Per-event records after replay | 400, each exactly once |
| `purge` | clears retention state |

## Run it

```bash
docker compose up -d                     # clickhouse + postgres + restate + mock API
export RESTATE_INGRESS_URL=http://localhost:8080
export CLICKHOUSE_HOST=localhost
export POSTGRES_DSN=postgresql://admin:password@localhost:5433/telemetry
export TARGET_API_BASE_URL=http://localhost:9100
export TARGET_API_TOKEN=dev-token

dagster dev -w workspace.yaml
```

Register the worker's handlers by selecting the service:

```bash
RESTATE_SERVICES=api_call_plan \
RESTATE_ADMIN_URL=http://localhost:9070 \
RESTATE_ADVERTISED_URI=http://restate-worker:9080 \
python -m dag_tools.restate_handlers.serve
```

### Review a mapping change before it goes live

Materialize `ci_dispatch` with `dry_run: true`:

```yaml
ops:
  ci_dispatch:
    config:
      dry_run: true
```

Every plan is rendered, logged and attached to the asset materialization
as metadata — exact URLs, exact bodies — and nothing is sent.

Other run-time knobs: `limit`, `max_groups`, `only_group`,
`ignore_readiness` (dispatch a still-filling group), `ignore_ledger`
(re-send an already-dispatched group).

## Configuration reference

Deploy-time shape lives in
[`component.yaml`](dagster_home/components/otel_sync/component.yaml):
ClickHouse connection, staging destination, the extraction queries, and
which mapping file to use. `staged: false` skips dlt entirely and reads
ClickHouse directly in the dispatch asset — lighter, but it gives up the
replayable staged copy, the incremental cursor, and the SQL ledger
(which falls back to Dagster asset metadata).
