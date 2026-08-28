# ADR-0003 — `/health` always answers 200; `/ready` refuses until the load is clean

**Status:** Accepted
**Date:** 2026-08-27
**Deciders:** Platform team
**Related:** [ADR-0001](ADR-0001-mesh-publishing-protocol.md) — the load whose outcome these report.

## Context

The domain broker imports a whole user deployment — Dagster, dbt, dlt, datahub and their
transitive dependencies — which takes **90–180 seconds** on a real deployment and can fail
outright on an `ImportError` several layers down.

Pointing both Kubernetes probes at one endpoint gets one of them wrong, and the two wrongs
have different costs.

## Decision

**`/health` is liveness and always returns 200**, reporting state in the body:
`loading` / `error` (with `definitions_error`) / `ok`.

**`/ready` is readiness and returns 503 until the definitions load cleanly.**

The questions differ. Liveness asks *"should this pod be restarted?"* — and the broker's
characteristic failure, a `Definitions` import raising, is **not something a restart fixes**.
Failing liveness on it crash-loops the pod and buries the actual error in a restart count.
Readiness asks *"should this pod receive traffic?"* — and a broker that could not load has
nothing truthful to resolve.

Three consequences follow, and each is load-bearing:

**A failed load does NOT register with the gateway.** Registering would push an empty URN list,
which the gateway stores as this broker's authoritative claim: *"I own nothing."* Every lookup
then 404s as "no active domain broker" — the asset appears not to exist. Staying silent lets
the previous registration age out on its TTL and keeps a healthy replica authoritative, so a
broken rollout **degrades instead of erasing** the routing table.

**`{"status": "ok", "assets": 0}` must mean what it says.** An empty `LOCAL_ASSETS` is
ambiguous on its own — "no mesh assets" and "the import blew up" look identical. Recording
`definitions_error` is what separates them.

**`/health` carries posture as numbers, not prose.** `adr0044.echoed_credentials_dropped`,
`unprotected_source_types`, `non_fqdn_hosts`, `unadvertised`. A WARNING in a stream nobody
reads is the failure mode this repo has hit most; a field an operator can poll is not.

## Consequences

A broker that cannot load stays up, stays out of the routing table, and says why — the three
things an operator needs, and the opposite of a crash loop.

`startupProbe` must cover the long import (`failureThreshold: 60` at `periodSeconds: 10`), or
readiness kills the pod mid-load. **The specific trap:** a broker Deployment copied from the
Dagster user-deployment chart inherits `dagster api grpc-health-check`, which runs against a
process that is hypercorn, not gRPC. It can never pass, so the pod sits at `0/1` forever
regardless of the broker's actual health.

## Alternatives considered

- **Both probes on `/ready`.** Rejected: crash-loops on an import error, which no restart
  fixes, and hides the error behind a restart count.
- **Both on `/health`.** Rejected: a broker that loaded nothing would receive traffic and
  answer 404 for assets it should own.
- **Fail liveness after N failed loads.** Rejected: a deterministic import error fails
  identically every time; N restarts produce N identical stack traces and no new information.

## Indicators for revisiting

- The import becomes fast (lazy `Definitions`, a cached inventory), making the loading state
  rare enough that the split stops earning its complexity.
- A failure mode appears that a restart genuinely does fix — a leaked connection, a wedged
  thread — which would justify a liveness condition that is not "the process exists."
- The `/health` posture fields grow past what a human reads, wanting a metrics endpoint
  instead.
