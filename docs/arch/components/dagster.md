# Dagster (CO-DAGSTER)

Dagster is the orchestrator every dag-tools component is written for. Each component builds Dagster definitions, and the qualification tooling drives a Dagster deployment through its GraphQL API. The dag-tools chart does not deploy Dagster. The invincible-agent chart deploys the webserver, the daemon and the instance, and runs the dag-tools image as one of its code locations.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **The dag-tools chart deploys no part of Dagster.** Its templates are the gateway, the Restate server and worker, and the CA bundle. Dagster appears in the chart only as a keyword. `not built`
2. **The invincible-agent chart deploys the webserver, the daemon and one instance configuration.** Both control pods mount one instance ConfigMap. It sets the default run launcher, which starts each run as a process inside the code-location pod. It sets a queued run coordinator with a global limit and a per-code-location limit. Run storage is Postgres, and its credentials are read from environment variables rather than written into the file. Compute logs and artifacts are local directories, and telemetry is off. `built`
3. **The instance bounds a started run.** Run monitoring is on by default, with a start timeout, a cancel timeout, a poll interval and a maximum run time of one hour. A maximum of zero turns the bound off, and the bound is then left out of the rendered file. `built`
4. **The dag-tools code location is one entry in that chart's list of user deployments.** The entry runs the shipped image's gRPC server on the code location's module at port 4000, and the chart adds one gRPC server per enabled entry to the workspace, named after the entry. The image's own default command is the same server on the same module and port. The [domain broker](domain-broker.md) runs from the same image with a different command. `built`
5. **The shipped code location is empty unless demo mode is on, and the invincible-agent chart turns demo mode on by default.** In code, an unset or unrecognised demo-mode setting means off, and off yields no assets. That chart's values set it to `"true"` for the code location and for the broker, so a deployment that keeps the chart default runs the demo surfaces. `built`
6. **Two further surfaces are gated by the environment, independently of demo mode.** The Grist surface is added only when `DAG_TOOLS_GRIST_CONFIG` names a file that enables it (see [the Grist record](grist-ingest.md)). The catalog lineage sensor is added only when `DATAHUB_SERVER` is set and the plugin imports, and then it starts running (see [the lineage record](datahub-lineage.md), finding 2). `built`
7. **The supported floor is Dagster 1.10.19, its CI job does not block a merge, and it cannot be reached through the orchestrator extra.** The orchestrator extra names `dagster` without a version, but its Dagster AWS integration requires 0.28.18 or later, which requires Dagster 1.12.18 or later. A separate CI job therefore does not install that extra: it installs the other extras plus Dagster 1.10.19 and integrations of the matching 0.26.19 line, and runs the suite with failures allowed. A deployment that installs the orchestrator extra cannot be on the floor. The floor's limits on component files are in AGENTS.md §4j. `built`
8. **The qualification client signs in with an optional bearer token and does not follow redirects.** The token is an environment variable named in the qualification manifest, or a literal, or nothing, in which case no authorization header is sent. A redirect is reported as a sign-in gate in front of the API rather than followed. The qualification modules are the only callers of the API found in this repo. `built`
9. **The chart enforces no sign-in on the webserver.** The webserver's ingress is off by default. When it is turned on, the template passes the annotations a deployment sets and adds nothing of its own. `not built`
10. **A second code location for synthetic probes is built in code and deployed by neither chart.** It loads whatever probe modules a directory holds and is empty when the directory is unset. `built` Neither chart has an entry for it. `not built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `dagster.maxConcurrentRuns` (invincible-agent chart) | 6 | runs the coordinator lets run at once |
| `dagster.runTagConcurrencyLimits` (invincible-agent chart) | one slot for the dag-tools code location | runs per code location |
| `dagster.runMonitoring.maxRuntimeSeconds` (invincible-agent chart) | 3600; 0 turns it off | the longest a started run may last |
| `dagster.webserver.ingress.enabled` and `annotations` (invincible-agent chart) | off; none | exposure of the webserver, and anything placed in front of it |
| `userDeployments.dag-tools` (invincible-agent chart) | enabled, port 4000, the shipped module | the dag-tools code location |
| demo mode | off in code; `"true"` in the invincible-agent chart | adds the demo surfaces |
| `DAG_TOOLS_GRIST_CONFIG` | unset | adds the Grist surface |
| `DATAHUB_SERVER`, `DATAHUB_SENSOR_STATUS` | unset; `RUNNING` | adds the lineage sensor and its starting state |
| `DAGTOOLS_PROBES_DIR` | unset | the probe modules the probes code location loads |
| qualification manifest `deployment.auth` | none | the bearer token the qualification client sends |

## Gaps

- **A deployment that keeps the invincible-agent chart default runs the demo surfaces** (finding 5). Remedy: not on the roadmap.
- **Nothing in either chart puts a sign-in in front of the webserver** (finding 9). Whoever reaches the webserver can launch runs. Remedy: not on the roadmap.
- **The floor is tested by a job that may fail without blocking** (finding 7). A change that breaks the floor can merge. Remedy: not on the roadmap.

## Not established from code

- Whether a deployment puts a sign-in in front of the webserver, and which.
- Whether a deployment runs the probes code location.
- Which Dagster version a deployment runs.
- Whether anything besides the qualification tooling calls the GraphQL API.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| XE-DAGSTER-STORAGE | writes | SQL through a database driver | credentials read from the environment | run and event records | invincible-agent `helm/invincible-agent/templates/dagster-instance.yaml:20-22`, `:24-36`; invincible-agent `tests/test_a_started_run_whose_worker_died_is_failed.py`: `test_THE_RENDERED_STORAGE_READS_ENV_REFS_NEVER_LITERALS` | `built` |
| XE-DAGSTER-STORAGE | reads | SQL through a database driver | credentials read from the environment | run and event records | invincible-agent `helm/invincible-agent/templates/dagster-instance.yaml:20-22`, `:24-36`; invincible-agent `tests/test_a_started_run_whose_worker_died_is_failed.py`: `test_THE_RENDERED_STORAGE_READS_ENV_REFS_NEVER_LITERALS` | `built` |
| XE-CODE-LOCATION | calls | gRPC | none in the chart | the code location's definitions and run launches | invincible-agent `helm/invincible-agent/templates/user-deployments.yaml:93-104`, `:239-240`; invincible-agent `tests/test_a_started_run_whose_worker_died_is_failed.py`: `test_EVERY_RENDERED_CODE_LOCATION_HAS_ITS_OWN_RUN_SLOTS` | `built` |
| RL-OPERATOR | is called by | HTTP | none enforced in the chart; a deployment may add annotations on the ingress | the web interface | invincible-agent `helm/invincible-agent/templates/ingress.yaml:87-124`; invincible-agent `helm/invincible-agent/values.yaml:452-454`; no test | `built` |
| XE-QUAL-TOOLING | is called by | HTTP, GraphQL | optional bearer token; a redirect is reported as a sign-in gate, not followed | launch, status and event-log queries | `dag_tools/qual/graphql/client.py:112-129`, `:195-200`, `:254-270`; `test_graphql_client.py`: `test_post_sends_auth_header_when_token_set`, `test_post_omits_auth_header_when_no_token` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | runs the components' assets, sensors, checks and schedules | — |
| How the mesh wires it | deployed by the invincible-agent chart; the dag-tools image is one of its code locations | — |
| Sign-in | none enforced on the webserver by chart default; the qualification client sends an optional bearer token | [NATO R7](../alignment/nato-dcra.md) |
| Data path | runs move data between sources and stores; the instance holds run and event records, not data | — |
| Plane | `data-management`: the shared lifecycle services run in it. Producers' pipelines run in it too, so it also carries operational-data work | [NATO L3](../alignment/nato-dcra.md) |
| Security role | none built; the run bound limits a stuck run, not access | — |
| Services served | the source of lineage, of the materializations the read audit reconciles, and of asset checks | [DoD S4](../alignment/dod-dmra.md), [DoD S10](../alignment/dod-dmra.md), [DoD S13](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. Invincible-agent chart paths are under `helm/invincible-agent/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No Dagster in the dag-tools chart | `helm/dag-tools/templates/` (no Dagster template); `helm/dag-tools/Chart.yaml:3-9`, `:19` | not stated | — | `not built` |
| Webserver, daemon, instance; launcher, coordinator, storage | invincible-agent `templates/dagster.yaml:4`, `:36`, `:112`, `:148`, `:188`; `templates/dagster-instance.yaml:10-17`, `:20-22`, `:24-36`, `:38-48`, `:50-58`, `:60-61`; `values.yaml:420-425`, `:438-478` | not stated | invincible-agent `tests/test_a_started_run_whose_worker_died_is_failed.py`: `test_EVERY_RENDERED_CODE_LOCATION_HAS_ITS_OWN_RUN_SLOTS`, `test_THE_RENDERED_STORAGE_READS_ENV_REFS_NEVER_LITERALS`, `test_EVERY_DAGSTER_CONTROL_POD_MOUNTS_THE_RENDERED_INSTANCE` | `built` |
| Run bound | invincible-agent `templates/dagster-instance.yaml:69-76`; `values.yaml:426-437` | not stated | invincible-agent `tests/test_a_started_run_whose_worker_died_is_failed.py`: `test_THE_RENDERED_INSTANCE_BOUNDS_A_STARTED_RUN`, `test_A_ZOMBIE_PAST_THE_RENDERED_BOUND_IS_FAILED` | `built` |
| dag-tools code location as a user deployment; image command; broker from the same image | invincible-agent `values.yaml:1289-1299`; `templates/user-deployments.yaml:93-104`, `:239-240`; `templates/dagster-user-code.yaml:156-162`; `dag_tools/user_deployment/Dockerfile:13-17`, `:128`, `:133` | not stated | — | `built` |
| Empty unless demo mode; chart default on | `dag_tools/user_deployment/definitions.py:48`, `:61-67`, `:96-106`, `:231-273`; invincible-agent `values.yaml:1305`, `:1353` | AGENTS.md §4k-1 | `test_demo_ev_refusal.py`: `test_a_deployment_with_demo_mode_off_does_not_have_them`, `test_it_merges_with_the_demo_surface`; `test_demo_dbt_surface.py`: `test_no_dbt_assets_when_demo_mode_is_off` | `built` |
| Grist and lineage gates | `dag_tools/user_deployment/definitions.py:53`, `:126-151`, `:183-228` | not stated | `test_user_deployment_grist.py`: `test_grist_disabled_by_default`, `test_grist_enabled_false_is_disabled`, `test_grist_merged_into_combined_defs`; `test_datahub_catalog_sensor.py`: `test_no_sensor_when_datahub_server_unset`, `test_sensor_registered_and_running_when_configured`, `test_sensor_status_is_overridable`, `test_catalog_failure_does_not_break_the_code_location` | `built` |
| Floor 1.10.19, non-blocking; not reachable through the orchestrator extra | `pyproject.toml:89-100`; `.github/workflows/test.yml:117-125`, `:145-152`, `:177-187`; `uv.lock:1185-1190` | AGENTS.md §4j | the CI job itself | `built` |
| Qualification client: optional bearer, no redirects | `dag_tools/qual/graphql/client.py:112-129`, `:195-200`, `:245-247`, `:254-270`; `dag_tools/qual/cli.py:502-503`; `dag_tools/qual/qualify/manifest.py:133` | AGENTS.md §4f | `test_graphql_client.py`: `test_resolve_auth_token_none_returns_none`, `test_post_302_redirect_raises_clear_auth_error_not_json_error`, `test_post_sends_auth_header_when_token_set`, `test_post_omits_auth_header_when_no_token` | `built` |
| No sign-in in the chart | invincible-agent `templates/ingress.yaml:87-124`; `values.yaml:452-454` | not stated | — | `not built` |
| Probes code location built, not deployed | `dag_tools/probes_location/definitions.py:13`, `:34`, `:65`; no entry in either chart | AGENTS.md §4g1 | `test_probes_location_definitions.py`: `test_definitions_module_yields_empty_defs_when_env_unset` | `built`; the absence `not built` |
