# Restate worker (CO-RESTATE-WORKER)

The Restate worker is where the mesh's durable handlers run. It is one shared image. Every durable handler dag-tools ships is registered in it, and a deployment picks which ones a pod hosts through an environment variable. On startup the worker registers itself with the [Restate server](restate-server.md). The server then calls it for each step of each job. The handlers write back to source databases and deliver records to external APIs, using credentials the pod is given in its environment.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **One image hosts every handler, and environment variables select them.** Five handlers are registered by key: an Oracle acknowledgement service, an Oracle control-request service, a generic API sync, the API-call plan executor and a SAP induction service. Each is imported only when selected. A deployment adds a handler by adding its key to the registry, not by writing a new worker; the house rule against per-project workers is in AGENTS.md §4. `built`
2. **The chart hosts one handler by default, and an empty selection hosts all of them.** The chart's default selection is the Oracle acknowledgement service alone. The chart's comment says the control cycle needs both Oracle services, so a deployment running it adds the second one. An empty selection hosts every registered handler. An unknown key stops startup with an error that names the valid keys. Tests cover a valid chart default, the two-service selection, and refusal of an unknown key. No test covers an empty selection. `built`
3. **It is a Deployment with no health probes.** It runs one replica by default and listens on port 9080 on every interface, behind a Service inside the cluster. The pod declares no liveness or readiness probe, so it is reported ready once its container starts. `built`
4. **The image is built by this repository and named by version.** By default the image is `restate-worker`, under the chart's global registry and prefix, tagged with the chart's app version. The repository and tag can be set. The chart pins no digest. How a release tag maps to a commit's build is set out in AGENTS.md §8. `built`
5. **Registration is per pod, sends no credential, and never fails the pod.** When an admin address is set, each pod posts its advertised address to the server's admin port with `force` set. The chart always sets the admin address and the advertised address, and derives the latter from the worker's Service. Each attempt waits up to ten seconds, and a failed attempt is retried every three seconds. After two minutes the worker logs an error and stops trying, but goes on serving. If an admin address is set with no advertised address, startup stops. With no admin address, the worker skips registration and logs that it must be registered by hand. The chart's notes say every replica registers the same address, and suggest a one-off job for setups with many replicas. `built`
6. **The worker's port checks no caller.** The application is built from the selected services and nothing else, with no identity setting. The chart adds none. `not built`
7. **Each handler's credentials come from the pod's environment.**

   | Handler | What it reaches | Credential read from |
   |---|---|---|
   | Oracle acknowledgement | an Oracle database | DSN, user and password variables |
   | Oracle control requests | an Oracle database | the same three variables |
   | Generic API sync | an HTTP API, then a SQL Server database | an API base address and key; a SQL Server DSN |
   | API-call plan | the HTTP endpoints named in each plan | the plan's own headers; non-secret settings come from the Dagster side (AGENTS.md §4a) |
   | SAP induction | a SAP OData service, a callback API and a database | prefixed settings: OData user and password, a callback header, database parts |

   The chart passes plain values through `restateWorker.env` and references through `restateWorker.envFrom`. AGENTS.md §4 records that a variable changes only on restart, so a rotated secret takes effect only at the next restart. It also records that absent credentials are refused at once, not retried. `built`
8. **These are direct connections, not mesh reads.** Every handler opens its own connection, to a database or to an HTTP endpoint. The [central gateway](central-gateway.md) and the [domain broker](domain-broker.md) are not on the path, so no ticket is issued and nothing is narrowed. `built`
9. **Database steps are bounded, and fail at once where a retry would do damage; HTTP steps are not bounded.** Each step that opens a database connection carries a retry bound. That step also stops at once on the errors whose retry would spend the account's failed-login attempts. The generic API sync's HTTP step has no bound, and only the handler's own refusals stop it at once. A payload the handler cannot act on is refused inside the step. The reasons are recorded in AGENTS.md §4. `built`
10. **The API-call plan executor keeps state per group, and records failure instead of raising it.** It is a keyed object. It records the calls it has completed, so a repeated dispatch does not repeat them. A halted plan is stored as failed, and the invocation returns normally. Its per-process concurrency cap and the timeouts it declares to the server are described in AGENTS.md §4j and [the server record, finding 8](restate-server.md). `built`
11. **A private certificate authority is added to the system roots, not swapped in for them.** When `caCerts` is on, an init container builds one bundle from the system roots, the Python roots and the mounted certificates. Three environment variables point at that bundle: one for each of the two HTTP libraries the handlers use, and one for curl. `built`

## Settings and defaults

| Setting | Chart default | What it does |
|---|---|---|
| `restateWorker.enabled` | `true` | deploy the worker |
| `restateWorker.replicas` | `1` | Deployment replicas; each registers itself |
| `restateWorker.image.repository` / `.tag` | the chart's `restate-worker` image / the chart's app version | the worker image |
| `restateWorker.port` | `9080` | the port the server calls, bound on every interface |
| `restateWorker.services` | the Oracle acknowledgement service alone | handlers to host; empty hosts every registered one |
| `restateWorker.advertisedUri` | empty, derived from the worker's Service | the address registered with the server |
| `restateWorker.env` / `.envFrom` | empty | handler credentials and settings |
| `restateWorker.apiCallPlan.inactivityTimeoutSeconds` / `.abortTimeoutSeconds` | unset / one hour | timeouts the API-call plan service declares to the server |
| `restateWorker.caCerts` | off | the merged trust bundle above |
| `RESTATE_BIND` (code) | every interface, port 9080 | the listen address when the chart does not set one |

## Gaps

- **The worker's port and its registration carry no identity** (findings 5 and 6). The server cannot tell its own worker from another process posting the same address. The worker cannot tell the server from any other caller that reaches its port. Remedy: [roadmap row 13](../ROADMAP.md) (not filed).
- **A worker that is not registered still looks healthy** (findings 3 and 5). With no probe, the pod is ready once it starts. A registration that gave up shows only as an error log line, while the server routes nothing to the pod. Remedy: [roadmap row 15](../ROADMAP.md) (not filed).

## Not established from code

- Which handlers any deployment selects, and with which credentials.
- Whether a deployment passes credentials as plain values in `env`, which puts them in the pod specification, or as references to secrets in `envFrom`.
- Which callers can reach the worker's port. The chart ships no network policy, and a deployment's own was not read.
- What the image contains. Its build file was not read for this record.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-RESTATE | calls | HTTP | none | its advertised address, posted for registration | `dag_tools/restate_handlers/serve.py:43`, `:75-112`, `:115-139`; no test | `built` |
| CO-RESTATE | is called by | HTTP | none: the worker's port checks no caller | handler invocations carrying a job payload | `dag_tools/restate_handlers/serve.py:72`; `helm/dag-tools/templates/restate-worker.yaml:47-56`; no test | `built` |
| none: a SQL database | writes | SQL through a database driver | user and password from the environment; absent credentials refused at once | acknowledgements, control rows and requests written back | `dag_tools/restate_handlers/oracle_ack.py:98-100`, `:115`, `:151-154`; `dag_tools/restate_handlers/api_sync.py:146-155`; `test_a_wrong_password_causes_one_login_attempt_not_a_loop` | `built` |
| none: the target API | calls | HTTP | not established | a delivered record or a rendered call | `dag_tools/restate_handlers/api_sync.py:115-116`, `:137`, `:164`; `dag_tools/restate_handlers/api_call_plan.py:110`; `test_the_acking_step_carries_a_bound_and_the_http_step_does_not` | `built` |
| none: a source system's OData service | calls | HTTP | user and password from the environment | an induction request | `dag_tools/restate_handlers/sap_induction/config.py:29-31`, `:76-91`; no test | `built` |
| none: a callback endpoint | calls | HTTP | not established | a state change pushed to a callback API | `dag_tools/restate_handlers/sap_induction/config.py:29-31`, `:76-91`; no test | `built` |
| none: a SQL database | writes | SQL through a database driver | not established | the local record of each state change | `dag_tools/restate_handlers/sap_induction/db_client.py:14`; no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | runs the durable handlers: acknowledgements and control requests written back to source databases, and records delivered to external APIs | — |
| How the mesh wires it | one shared image, handlers selected by environment, registered with the server by the worker itself on startup | — |
| Sign-in | none on its own port; to each backend, with a credential from its environment | [NATO R7](../alignment/nato-dcra.md) |
| Data path | direct between the worker and each backend; the gateway and broker are not on it | [DoD S7](../alignment/dod-dmra.md) |
| Plane | operational, where producers' systems are written back to | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none for mesh reads; it refuses malformed payloads and absent credentials, which is input validation, not access control | — |
| Services served | delivery of records to a consumer's API as a registered, retried exchange; the plan executor's rendering of telemetry into a consumer's request shape is done on the Dagster side | [DoD S7](../alignment/dod-dmra.md), [S14](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179`. Chart paths are under `helm/dag-tools/`; handler paths under `dag_tools/restate_handlers/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| One image, a registry of five handlers, lazy imports | `serve.py:35-41`, `:60-72`; `oracle_ack.py:9`; `oracle_control.py:35`; `api_sync.py:11`; `api_call_plan.py:110`; `sap_induction/service.py:23` | AGENTS.md §4 | `test_restate_worker_services.py`: `test_the_registry_is_not_empty`, `test_every_registered_service_imports_and_exposes_a_service`, `test_every_registered_service_is_documented_in_the_chart` | `built` |
| One handler by default; empty hosts all; unknown key refused | `values.yaml:67-82`; `serve.py:46-56` | not stated | `test_restate_worker_services.py`: `test_the_chart_default_is_a_valid_selection`, `test_selecting_the_pdm_pair_builds`, `test_an_unknown_key_is_refused_by_name`; no test of an empty selection | `built` |
| Deployment, port, no probes | `templates/restate-worker.yaml:1-3`, `:47-56`, `:88-98`; `values.yaml:60-61`, `:66` | not stated | — | `built` |
| Image named by version | `values.yaml:62-65`; `templates/_helpers.tpl:31-38` | AGENTS.md §8 | `test_image_release_retag.py` | `built` |
| Registration per pod, no credential, never fails the pod | `serve.py:43`, `:75-112`, `:115-139`; `templates/_helpers.tpl:53-61`, `:80-86`; `templates/NOTES.txt:25-29` | not stated | no test | `built` |
| No caller check on the worker's port | `serve.py:72`; `templates/restate-worker.yaml:47-56` | not stated | — | `not built` |
| Credentials from the environment | `oracle_ack.py:98-100`; `oracle_control.py:64-66`; `api_sync.py:115-116`, `:164`; `sap_induction/config.py:29-31`, `:76-91`; `values.yaml:89`, `:111`; `db_retry.py:63` | AGENTS.md §4 | `test_restate_db_retry.py` | `built` |
| Direct connections | `oracle_ack.py:115`; `api_sync.py:137`, `:173`; `sap_induction/db_client.py:14` | not stated | — | `built` |
| Bounded database steps; unbounded HTTP step; refusal inside the step | `oracle_ack.py:151-154`; `oracle_control.py:354-357`, `:411-414`; `api_sync.py:146-155`, `:187-190`; `sap_induction/service.py:101`; `db_retry.py:78` | AGENTS.md §4 | `test_restate_db_retry.py`: `test_a_wrong_password_causes_one_login_attempt_not_a_loop`, `test_every_connecting_step_in_the_handlers_carries_a_bound`; `test_restate_payload_refusal.py`: `test_a_malformed_payload_costs_one_invocation_not_a_loop`; `test_restate_api_sync_refusal.py`: `test_the_acking_step_carries_a_bound_and_the_http_step_does_not` | `built` |
| Plan executor keeps state; failure recorded | `api_call_plan.py:110`, `:196-201` | AGENTS.md §4a, §4j | `test_otel_api_sync_executor.py`: `test_unhandled_client_error_halts_and_is_recorded_not_raised` | `built` |
| Merged trust bundle; three variables name it | `values.yaml:140`; `templates/restate-worker.yaml:36-40`, `:71`, `:76-86`; `templates/_helpers.tpl:195-204` | AGENTS.md §4 | `test_chart_ca_certs.py`: `test_enabling_sets_both_env_vars` asserts the two library variables; `test_off_by_default_nothing_is_added` asserts all three are absent when off; no test asserts the third is set when on | `built` |
| Remedies | [the roadmap note](../ROADMAP.md), rows 13 and 15 | none | — | `proposed` |
