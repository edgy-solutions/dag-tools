# Grist (CO-GRIST)

Grist is a spreadsheet-database: people edit tables in a browser, and the tables are reachable through a REST API. The mesh reads Grist tables into the warehouse through the [Grist ingest component](grist-ingest.md). Neither chart deploys Grist itself. The invincible-agent chart can hand the dag-tools code location a Grist ingest configuration, and that is off by default. This record covers the tool and its wiring; the ingest record has the detail of what the component does.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **Neither chart deploys Grist.** The dag-tools chart does not mention it, and no invincible-agent chart template deploys a Grist server. A deployment points the ingest component at a Grist server it runs or reaches some other way. `not built`
2. **The invincible-agent chart can configure the ingest per code location, and it is off by default.** The switch is per user-deployment entry: the template renders only for an entry that is enabled and carries a `grist` block with `enabled` true. The shipped values declare that block once, inside the dag-tools entry, with `enabled` false; the pub-tools entry carries none. When a deployment turns it on, the chart renders a ConfigMap per enabled entry holding `enabled: true` and the operator's `grist.config` as the component's attributes, mounts it read-only in the code-location pod, and sets `DAG_TOOLS_GRIST_CONFIG` to the mounted file. `built`
3. **Secrets reach the pod as environment variables the deployment names.** Each entry of `grist.secretEnv` becomes an environment variable read from a named key of a named Kubernetes Secret, optional if the entry says so. The chart names no Secret and no key of its own. `built`
4. **The code location adds the Grist surface only when that file enables it.** With the variable unset, the path not a file, the file unparseable, `enabled: false` or no attributes, the surface is left out and the load goes on (see [the ingest record](grist-ingest.md), finding 10). `built`
5. **Sign-in to Grist is a static token from the component's configuration.** The component sends it to the Grist API on every call, and nothing in the mesh mints, rotates or scopes it (see [the ingest record](grist-ingest.md), findings 8 and 9). `built`
6. **A Grist read does not pass through the mesh gateway.** The component calls the Grist API directly and writes to the warehouse through its own IO manager. The tables it writes are reached through the mesh like any other once they land. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `userDeployments.<name>.grist.enabled` (invincible-agent chart) | false for the dag-tools entry | renders and mounts the Grist ingest configuration |
| `userDeployments.<name>.grist.config` (invincible-agent chart) | unset | the component's attributes, written into the ConfigMap |
| `userDeployments.<name>.grist.secretEnv` (invincible-agent chart) | unset | environment variables read from named Secret keys |
| `DAG_TOOLS_GRIST_CONFIG` | unset in code; set by the chart when the block is enabled | the file the code location reads to add the surface |

## Gaps

- **The Grist token is static and outside the mesh's identity** (finding 5). Remedy: not on the roadmap. Status: `built`.
- **The read from Grist is not decided or audited by the mesh** (finding 6). Remedy: not on the roadmap. Status: `built`.

## Not established from code

- Whether a deployment runs a Grist server, and where.
- Which Grist account the token stands for, what it may read, and how long it lives.
- Whether a deployment fills the referenced Secrets from Vault or by other means.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| CO-GRIST-INGEST | is called by | HTTP | static bearer token from the component's configuration | table and document reads over the REST API | `dag_tools/resources/grist.py:29-36`, `:56-57`; `test_user_deployment_grist.py`: `test_grist_builds_and_resolves_env` | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | tables people edit by hand, read into the warehouse | — |
| How the mesh wires it | an external service; the invincible-agent chart can mount an ingest configuration into the dag-tools code location, off by default | — |
| Sign-in | a static token from the component's configuration | [NATO R7](../alignment/nato-dcra.md) |
| Data path | Grist API to the code location to the warehouse; not through the gateway | — |
| Plane | `operational-data`: a source of data people produce | [NATO L2](../alignment/nato-dcra.md) |
| Security role | none built | — |
| Services served | data exchange from a source into the warehouse | [DoD S7](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. Invincible-agent chart paths are under `helm/invincible-agent/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No Grist server in either chart | `helm/dag-tools/` (no match); invincible-agent `templates/` (only the ingest ConfigMap and the code-location wiring) | not stated | — | `not built` |
| Per-entry ConfigMap, mount, variable; off by default | invincible-agent `values.yaml:1324-1325`; `templates/user-deployment-grist.yaml:15-30`; `templates/user-deployments.yaml:149-154`, `:168-178` | not stated | — | `built` |
| Secrets as named environment variables | invincible-agent `templates/user-deployments.yaml:155-164` | not stated | — | `built` |
| Code-location gate | `dag_tools/user_deployment/definitions.py:126-151` | not stated | `test_user_deployment_grist.py`: `test_grist_disabled_by_default`, `test_grist_enabled_false_is_disabled`, `test_grist_merged_into_combined_defs` | `built` |
| Static token; direct calls | see [the ingest record](grist-ingest.md), findings 8 and 9 | not stated | see that record | `built` |
