# Vault (CO-VAULT)

Vault is named in the census as a tool the mesh may use for secrets. At the component-records pin, nothing in this repo reads from a Vault, and neither chart deploys one. This record says how secrets reach dag-tools workloads instead, so that a deployment that fills them from a Vault knows where they land.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **No code or chart in dag-tools refers to a Vault.** A search of the code and the chart at the pin finds no Vault client, address or template. `not built`
2. **The invincible-agent chart deploys no Vault either.** In that chart's templates and default values, the matches for the word name an identity store inside that application, with its own lifetime setting, not a Vault service. A deployment's own values may name a host of that kind; that is outside what this record reads. That chart is read at its own pin. `not built`
3. **Secrets reach the gateway and the worker as environment variables from Kubernetes Secrets.** Each has an `envFrom` list in the chart values, which a deployment fills with its own Secrets or ConfigMaps. The gateway's policy-engine key and the worker's database and API credentials arrive this way. `built`
4. **Certificates arrive as a mounted Secret, a ConfigMap or inline values, exactly one.** The chart's CA setting takes one of the three and refuses more. An init container merges the certificates with the public roots. The reasons are in AGENTS.md §4. `built`
5. **A secret delivered as a variable changes only when the process restarts.** The worker treats absent credentials as terminal for that reason. A secret that must change in place has to arrive as a mounted file and be read from that file. The reasons are in AGENTS.md §4. `built`
6. **Image pull credentials are a list a deployment sets.** The chart passes `global.imagePullSecrets` through as given. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `centralGateway.envFrom` | empty | Secrets or ConfigMaps loaded into the gateway's environment |
| `restateWorker.envFrom` | empty | Secrets or ConfigMaps loaded into the worker's environment |
| `restateWorker.caCerts`, `centralGateway.caCerts` | off | a Secret, a ConfigMap or inline certificates, exactly one |
| `global.imagePullSecrets` | empty | pull credentials for the images |

## Gaps

- **A rotated secret does not reach a running pod** (finding 5). Every secret the chart wires is a variable. Remedy: not on the roadmap.

## Not established from code

- Whether a deployment fills its Kubernetes Secrets from a Vault, and how.
- Which secrets a deployment places in each `envFrom` source.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|

No connection is built at this pin: nothing in the code or the chart refers to this component. `not built`

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | none in this repo; the record shows where secrets land instead | — |
| How the mesh wires it | not wired; secrets come from Kubernetes Secrets a deployment provides | — |
| Sign-in | not applicable | — |
| Data path | none | — |
| Plane | infrastructure: secret delivery | [NATO L1](../alignment/nato-dcra.md) |
| Security role | none built; credential storage and rotation are the deployment's | [NATO R5](../alignment/nato-dcra.md), [NATO R8](../alignment/nato-dcra.md) |
| Services served | none at the pin | — |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No Vault in dag-tools | no match for the word under `dag_tools/` or `helm/` | not stated | — | `not built` |
| No Vault in the invincible-agent chart | invincible-agent `helm/invincible-agent/templates/frontend.yaml:152-155`; `helm/invincible-agent/values.yaml:1890-1904` | not stated | — | `not built` |
| Secrets through `envFrom` | `helm/dag-tools/templates/central-gateway.yaml:57-58`; `helm/dag-tools/templates/restate-worker.yaml:72-73`; `helm/dag-tools/values.yaml:111-113`, `:181`, `:225` | not stated | — | `built` |
| Certificates: exactly one source, merged | `helm/dag-tools/templates/_helpers.tpl:125-153`; `helm/dag-tools/values.yaml:142-155` | AGENTS.md §4 | `test_chart_ca_certs.py`: `test_enabling_sets_both_env_vars`, `test_the_env_vars_name_the_MERGED_bundle_not_the_mounted_secret`, `test_the_merge_keeps_the_public_roots` | `built` |
| Variable secrets change on restart; absent is terminal | `dag_tools/restate_handlers/db_retry.py` (`MissingCredentials`) | AGENTS.md §4 | `test_restate_db_retry.py` | `built` |
| Pull credentials passed through | `helm/dag-tools/values.yaml:12` | not stated | — | `built` |
