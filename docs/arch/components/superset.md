# Superset (CO-SUPERSET)

Superset is a business-intelligence tool that runs SQL against a database and saves the results as charts and dashboards. dag-tools neither deploys it nor calls it. The invincible-agent chart passes its address and an empty token to that repo's agent engines, and one of those engines calls Superset's API. Neither chart deploys Superset itself.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **dag-tools does not reach Superset.** No component, resource, chart template or setting in dag-tools names it. Where the word appears in the code, it means a superset of rows, not the tool. `not built`
2. **Neither chart deploys Superset.** The dag-tools chart does not mention it, and no invincible-agent chart template deploys it. `built`
3. **The invincible-agent chart passes an address and an empty token to the agent engines.** The address is a key in the fleet environment block, rendered into the release's ConfigMap. The token is a key in the fleet secrets block, empty by default, rendered into the release's Secret. Every engine loads both the ConfigMap and the Secret as environment variables. `built`
4. **One invincible-agent engine calls Superset's API with that token as a bearer token.** The header is sent even when the token is empty. The restate analyst offers a tool that runs a query through Superset's SQL Lab and, when asked to publish, creates a dataset and a chart from it. This is invincible-agent code; it is cited here only to show where the address and token are read. `built`
5. **A Superset read does not pass through the mesh gateway.** The engine's calls go to Superset directly, and Superset reads its own database connections. Nothing at either pin connects such a read to the gateway's decision, the broker's ticket or the read audit. `not built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `agentFleet.env.SUPERSET_URL` (invincible-agent chart) | a default address in the values, and another in the engine code | the address the engines call |
| `agentFleet.secrets.SUPERSET_ACCESS_TOKEN` (invincible-agent chart) | empty | the bearer token the engines send |

## Gaps

- **A read through Superset is outside the mesh's access path** (finding 5). Such a read is not decided by the gateway, not ticketed by the broker and not reconciled by the read audit. Remedy: not on the roadmap.
- **dag-tools has no Superset wiring** (finding 1). Remedy: not on the roadmap.

## Not established from code

- Whether a deployment runs Superset, and where.
- What role or account Superset's token stands for, and how long it lives.
- Which databases Superset connects to, and with which credentials.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | queries and charts for people who make decisions | — |
| How the mesh wires it | an external service; the invincible-agent chart passes its address and an empty token to the agent engines | — |
| Sign-in | a bearer token from a chart secret, empty by default | [NATO R7](../alignment/nato-dcra.md) |
| Data path | Superset reads its own database connections; nothing passes through the gateway | — |
| Plane | analytical: insight for decisions | [NATO L4](../alignment/nato-dcra.md) |
| Security role | none built | — |
| Services served | none of the fifteen at the pin | — |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. Invincible-agent chart paths are under `helm/invincible-agent/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No Superset wiring in dag-tools | whole repo (matches for `superset` mean a superset of rows, for example `dag_tools/otel_api_sync/render.py:178`); `helm/dag-tools/` (no match) | not stated | — | `not built` |
| No Superset template in either chart | `helm/dag-tools/templates/`; invincible-agent `templates/` (no match) | not stated | — | `built` |
| Address and empty token to the engines | invincible-agent `values.yaml:535`, `:653`, `:655`, `:662`; `templates/configmap.yaml:402`; `templates/secrets.yaml:163-167`; `templates/engines.yaml:84-88` | not stated | — | `built` |
| The restate analyst calls Superset | invincible-agent `agent_fleet/restate_analyst/main.py:1009-1075`, `:1102`; `values.yaml:686-687` | not stated | — | `built` |
| No gateway on that path | the calls above go to the Superset address; no gateway call in them | not stated | — | `not built` |
