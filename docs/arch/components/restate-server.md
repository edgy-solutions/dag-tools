# Restate server (CO-RESTATE)

The Restate server is the durable execution engine that the mesh's write-back work runs on. A Dagster asset hands a job to the server's ingress port. The server records each step in its journal and calls the [Restate worker](restate-worker.md), where the handler code runs. If a step fails, the server retries it, and after a restart it replays the job from the journal. The dag-tools chart can run the server itself, but by default it does not: it expects a Restate run elsewhere and asks the deployment for its address.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **The chart's own server is off by default.** `restateServer.enabled` is false, and the chart's header says the usual layout uses an external Restate. With it off, the chart deploys no server, and everything below about the in-chart server applies only to a deployment that turns it on. `built`
2. **The defaults alone do not render.** The worker is on by default and needs the server's admin address. That address comes from the in-chart server when it is on, else from `externalRestate.adminUrl`. If neither is set, rendering stops with a message naming both settings, so a deployment must choose one before anything installs. The chart tests turn the in-chart server on for every render for this reason. `built`
3. **The in-chart server is one stateful pod with its own volume.** It is a StatefulSet with one replica by default, and it mounts a ReadWriteOnce claim of 10Gi at its data directory. The storage class is left to the cluster unless one is set. The journal lives on that volume. `built`
4. **Two ports: ingress for callers, admin for registration and operations.** Ingress is 8080 and admin is 9070. Both bind on every interface, and a Service exposes both inside the cluster. The chart creates no Ingress for either. Liveness and readiness are both an HTTP GET of `/health` on the admin port. `built`
5. **No sign-in on either port, as far as the chart and code reach.** The chart sets no identity, signing or authentication setting on the server. The worker registers itself on the admin port without a credential ([worker, finding 5](restate-worker.md)). The three Dagster components that send work post to the ingress port without an authorization header. The one header they send is an idempotency key, from the OpenTelemetry API sync. What a caller that reaches either port can do is then up to Restate itself and to the network, and neither was read here. `not built`
6. **The image is upstream Restate at tag `latest`.** The registry, repository and tag can be set. With no tag set, the chart's image helper falls back to `latest` as well. Nothing pins the server to a version or a digest. `built`
7. **The chart reads two addresses for an external Restate, and uses one.** `externalRestate.adminUrl` feeds the worker's registration. `externalRestate.ingressUrl` has a helper that resolves it, but no template uses that helper. The Dagster components that send work take the ingress address as their own setting (`restate_endpoint`). The chart does not pass it to them. `built`
8. **Timeouts the server applies to the API-call service are set from the worker side.** The service declares its own inactivity and abort timeouts when the worker's environment carries them. The chart sets the abort timeout to an hour by default and leaves inactivity at Restate's own default. The chart's comment records that these need a server whose discovery protocol supports them, and that this was measured on two Restate versions. That measurement is the chart comment's, not a recorded run in this record. `built`

## Settings and defaults

| Setting | Chart default | What it does |
|---|---|---|
| `restateServer.enabled` | `false` | deploy the server in this release |
| `restateServer.replicas` | `1` | StatefulSet replicas |
| `restateServer.image.registry` / `.repository` / `.tag` | upstream Restate, tag `latest` | the server image; an empty tag also means `latest` |
| `restateServer.ingressPort` / `.adminPort` | `8080` / `9070` | the two ports, bound on every interface |
| `restateServer.storage.size` / `.storageClass` | `10Gi` / the cluster's default | the journal's volume |
| `restateServer.resources` | requests and limits set | container resources |
| `externalRestate.adminUrl` | empty | the admin address of a Restate run elsewhere; required when the in-chart server is off |
| `externalRestate.ingressUrl` | empty | read by a helper that no template uses |

## Gaps

- **Nothing on the server's ports establishes who is calling** (finding 5). Any process that can reach the ingress port can start a job on a registered handler, and any process that can reach the admin port can register a deployment. The chart ships no restriction, and network policy was not read. Remedy: [roadmap row 13](../ROADMAP.md) (not filed).
- **The server image is not pinned** (finding 6). Two installs of one chart version can run different Restate versions, and the chart's own comment ties a setting to the server's version (finding 8). Remedy: [roadmap row 14](../ROADMAP.md) (not filed).
- **A setting is read by nothing** (finding 7). `externalRestate.ingressUrl` looks like the address the senders use, and it is not passed to them. Remedy: [roadmap row 14](../ROADMAP.md) (not filed).

## Not established from code

- What an external Restate, the default layout, is configured with: version, authentication, and who can reach it. The chart only takes its address.
- Whether any network policy limits who reaches the ingress and admin ports. None is in the dag-tools chart, and a deployment's own was not read.
- What the admin port lets a caller read or change beyond registration, such as journal or state contents. That is Restate's own behaviour, and it was not read here.
- How the journal volume is backed up, and how long the journal is kept.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | durable execution for write-back and delivery jobs, which are retried and replayed from a journal | — |
| How the mesh wires it | external by default and named by address; or a StatefulSet in this chart; the worker registers itself on the admin port; Dagster components post jobs to the ingress | — |
| Sign-in | none set by the chart or sent by any caller in dag-tools | [NATO R7](../alignment/nato-dcra.md) |
| Data path | job payloads and step results pass through it and are kept in its journal; the bytes a handler reads or writes go between the worker and the backend, not through the server | [DoD S7](../alignment/dod-dmra.md) |
| Plane | infrastructure that the operational and data management planes run on | [NATO L1](../alignment/nato-dcra.md) |
| Security role | none: it decides and enforces nothing about who may read | — |
| Services served | the delivery of rendered API calls to a consumer's endpoint, as a registered and retried exchange | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Chart paths are under `helm/dag-tools/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| In-chart server off by default; external by default | `values.yaml:1-5`, `:31-32` | not stated | — | `built` |
| Defaults alone stop the render | `templates/_helpers.tpl:53-61`; `templates/restate-worker.yaml:51-52` | not stated | `test_chart_ca_certs.py` and `test_chart_api_call_plan.py` set `restateServer.enabled=true` on every render, and say why | `built` |
| One stateful pod, one volume | `templates/restate-server.yaml:1-3`, `:59`, `:76-82`; `values.yaml:33`, `:41-43` | not stated | no test renders the server | `built` |
| Two ports, both on every interface; health probes | `templates/restate-server.yaml:43-54`, `:62-69`, `:95-99`; `values.yaml:39-40` | not stated | no test | `built` |
| No sign-in set or sent | `templates/restate-server.yaml:47-54`; `dag_tools/restate_handlers/serve.py:75-112`; `dag_tools/components/otel_api_sync/component.py:58`, `:218-224`, `:760-770`; `dag_tools/components/restate_dlt_sync/component.py:290`, `:342`; `dag_tools/components/restate_api_sync/component.py:130-160` | not stated | no test | `not built` |
| Image at `latest` | `values.yaml:34-38`; `templates/_helpers.tpl:43-46` | not stated | no test | `built` |
| Ingress address read by no template | `values.yaml:22-24`; `templates/_helpers.tpl:66-74` | not stated | — | `built` |
| API-call service timeouts from the worker's environment | `values.yaml:91-110`; `templates/restate-worker.yaml:57-66` | AGENTS.md §4j, the concurrency bullet | `test_chart_api_call_plan.py`: `test_abort_timeout_renders_by_default_and_unset_inactivity_renders_nothing`, `test_clearing_the_abort_timeout_restores_restates_default` | `built` |
| Remedies | [the roadmap note](../ROADMAP.md), rows 13 and 14 | none | — | `proposed` |
