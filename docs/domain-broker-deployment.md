# Deploying the Domain Broker alongside a user deployment

The broker runs as a second container (or a second Deployment) using the
**same image** as your Dagster user deployment, because it imports the
same `Definitions` to learn what this deployment owns.

```bash
CENTRAL_GATEWAY_URL=http://central-gateway.<ns>.svc.cluster.local:8090 \
BROKER_URL=http://<this-service>.<ns>.svc.cluster.local:8000 \
DAGSTER_DEFS_MODULE=mfg \
hypercorn dag_tools.domain_broker.main:app --bind 0.0.0.0:8000
```

## `DAGSTER_DEFS_MODULE`

Use the same target you start Dagster with.

| How you run Dagster | Set |
| --- | --- |
| `dagster api grpc --package-name mfg` | `DAGSTER_DEFS_MODULE=mfg` |
| `dagster api grpc -m mfg.definitions` | `DAGSTER_DEFS_MODULE=mfg.definitions` |
| more than one `Definitions` on the module | `DAGSTER_DEFS_MODULE=mfg:defs` |

A bare name makes the broker discover the single `Definitions` on that
module, exactly as `--package-name` does. It refuses to guess when a
module exposes several — picking one would advertise half a deployment
and look like it worked.

## Probes

**This is the part that bites.** If the broker Deployment was copied from
the Dagster user-deployment chart, it inherited that chart's readiness
probe:

```yaml
readinessProbe:
  exec:
    command: ["dagster", "api", "grpc-health-check", "-p", "3030"]
```

That runs a gRPC health check against a process that is hypercorn, not a
gRPC server. It can never pass, so the pod sits at `0/1` forever and a
rollout never completes — regardless of whether the broker itself is
perfectly healthy.

Replace it:

```yaml
# Covers the long import. A real user deployment carrying Dagster + dbt +
# dlt + datahub takes 90-180s to import, and the broker cannot answer
# anything until it finishes. While startupProbe is running, the readiness
# and liveness probes are suspended, so a slow start cannot be mistaken
# for a failure.
startupProbe:
  httpGet: {path: /ready, port: 8000}
  periodSeconds: 10
  failureThreshold: 60        # ~10 minutes before giving up

readinessProbe:
  httpGet: {path: /ready, port: 8000}
  periodSeconds: 10
  failureThreshold: 3

# NOT /ready. See below.
livenessProbe:
  httpGet: {path: /health, port: 8000}
  periodSeconds: 20
  failureThreshold: 3
```

### Why liveness and readiness use different paths

They answer different questions, and pointing both at the same endpoint
gets one of them wrong.

`/health` is **always 200** and reports the state in its body. It is the
liveness probe because the broker's characteristic failure — the
`Definitions` import raising — is not something a restart fixes. Failing
liveness on it would crash-loop the pod and bury the error in a restart
count.

`/ready` returns **503 until the definitions load cleanly**. That is the
routing question, and a broker that could not load has nothing truthful
to resolve.

| State | `/health` | `/ready` |
| --- | --- | --- |
| still importing | `200 {"status": "loading"}` | `503` |
| import failed | `200 {"status": "error", "definitions_error": "..."}` | `503` |
| loaded | `200 {"status": "ok", "assets": N}` | `200` |

`assets: 0` with `status: ok` is a real, legitimate state — a deployment
that advertises nothing. It is deliberately distinguishable from
`status: error`, which used to look identical from outside.

### Diagnosing without reading logs

```bash
kubectl exec deploy/<broker> -- curl -s localhost:8000/health
```

`status: error` carries `definitions_error` with the reason. A broker in
that state has **not** registered with the gateway, and says so
(`registered: false`) — it will not advertise an empty asset list, because
the gateway stores that as "this broker owns nothing" and every lookup
then 404s as though the data does not exist.

## Registration

`CENTRAL_GATEWAY_URL` must resolve from inside the pod; the default
(`central-gateway.default.svc.cluster.local`) is almost never right.
`BROKER_URL` is what the gateway will call back on, so it must be this
pod's own Service, reachable from the gateway's namespace.

Routes carry a TTL (5 minutes) and the broker re-pushes every
`BROKER_REGISTER_INTERVAL_SEC` (120 by default), giving two to three
attempts per window. A gateway `404 No active domain broker found` is
therefore a **liveness** signal about the owning deployment, not a
statement that the asset does not exist.

## `BROKER_UNMINTED_POSTURE`

invincible-agent ADR-0044's minting is per backend. For a backend the broker
has no minter for, a ticket carries the **producer's own** credential — a write
credential, scoped to no asset, expiring with nothing — because without a
minter that is the only thing that would let a reader read.

`observe` (the default) is unchanged behaviour: every read that is served today
is still served, and the broker additionally counts per backend what it would
refuse:

```bash
kubectl exec deploy/<broker> -- curl -s localhost:8000/health \
  | jq .adr0044.unminted_credentials
```

```json
{"posture": "observe", "would_refuse": {"some_backend": 4}, "refused": {}}
```

`enforce` refuses those reads with `403 unminted_credential`, which arrives at
a `cortex_data` reader as `NotEntitled`. The two numbers are never one: under
`observe` a would-refusal is still served, so summing them would report
refusals for reads that succeeded.

Roll it out by reading `would_refuse` until it lists only backends you are
willing to lose reads of. A backend that appears there wants a **minter**; the
posture is how you stop serving its producer's credential in the meantime, not
the fix. A ticket carrying no credential at all is never refused — there is
nothing exposed to refuse. The reader's side of this is in
[`docs/cortex-data-client.md`](cortex-data-client.md).
