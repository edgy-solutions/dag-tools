# Superset (CO-SUPERSET)

Superset is a business-intelligence tool that runs SQL against a database and saves the results as charts and dashboards. dag-tools neither deploys it nor calls it. The invincible-agent chart passes its address and an empty token to that repo's agent engines, and one of those engines calls Superset's API. Neither chart deploys Superset itself.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **dag-tools does not reach Superset.** No component, resource, chart template or setting in dag-tools names it. Where the word appears in the code, it means a superset of rows, not the tool. `built`
2. **Neither chart deploys Superset.** The dag-tools chart does not mention it, and no invincible-agent chart template deploys it. `built`
3. **The invincible-agent chart passes an address and an empty token to the agent engines.** The address is a key in the fleet environment block, rendered into the release's ConfigMap. The token is a key in the fleet secrets block, empty by default, rendered into the release's Secret. Every engine loads both the ConfigMap and the Secret as environment variables. `built`
4. **One invincible-agent engine calls Superset's API with that token as a bearer token.** The header is sent even when the token is empty. The restate analyst offers a tool that runs a query through Superset's SQL Lab and, when asked to publish, creates a dataset and a chart from it. This is invincible-agent code; it is cited here only to show where the address and token are read. `built`
5. **A Superset read does not pass through the mesh gateway.** The engine's calls go to Superset directly, and Superset reads its own database connections. Nothing at either pin connects such a read to the gateway's decision, the broker's ticket or the read audit. `built`
6. **As a point of use, it is protected by its own role-based access alone, and its reads produce no gateway receipt.** Nothing in the mesh is on the path from an agent engine to Superset (finding 5), so what protects a read there is what Superset's own roles permit. No code at either pin configures those roles. This is the condition today, recorded as it is; the remedies are roadmap rows 21 and 24, each `proposed` ([need to know, finding 7](../flows/need-to-know.md)). `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `agentFleet.env.SUPERSET_URL` (invincible-agent chart) | one default address, written twice: in the values and in the engine code, where the two can drift apart | the address the engines call |
| `agentFleet.secrets.SUPERSET_ACCESS_TOKEN` (invincible-agent chart) | empty | the bearer token the engines send |

## Gaps

- **A read through Superset is outside the mesh's access path** (finding 5). Such a read is not decided by the gateway, not ticketed by the broker and not reconciled by the read audit. Remedy: [roadmap rows 21 and 24](../ROADMAP.md). Status: `built`.
- **dag-tools has no Superset wiring** (finding 1). Remedy: not on the roadmap. Status: `built`.

## Not established from code

- Whether a deployment runs Superset, and where.
- What role or account Superset's token stands for, and how long it lives.
- Which databases Superset connects to, how it signs in to them, and with which credentials; no code at either pin shows it.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|
| XE-AGENT-ENGINE | is called by | HTTP | bearer token from a chart secret, empty by default and sent even when empty | SQL queries; a dataset and a chart created from the result | invincible-agent `agent_fleet/restate_analyst/main.py:1009-1075`, `:1102`; invincible-agent `helm/invincible-agent/values.yaml:662`; no test | `built` |
| CO-GATEWAY | calls | none | none | nothing; a read through Superset is not decided, ticketed or audited | invincible-agent `agent_fleet/restate_analyst/main.py:1009-1075` (no gateway call in it); no test | `built` |

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | queries and charts for people who make decisions | — |
| How the mesh wires it | an external service; the invincible-agent chart passes its address and an empty token to the agent engines | — |
| Sign-in | a bearer token from a chart secret, empty by default | [NATO R7](../alignment/nato-dcra.md) |
| Data path | Superset reads its own database connections; nothing passes through the gateway | — |
| Plane | `analytical-data`: insight for decisions | [NATO L4](../alignment/nato-dcra.md) |
| Security role | none built | — |
| Protected by | its own role-based access; its reads produce no gateway receipt | [need to know, finding 7](../flows/need-to-know.md) |
| Services served | none of the fifteen at the pin | — |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. Invincible-agent chart paths are under `helm/invincible-agent/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No Superset wiring in dag-tools | whole repo (matches for `superset` mean a superset of rows, for example `dag_tools/otel_api_sync/render.py:178`); `helm/dag-tools/` (no match) | not stated | — | `built` |
| No Superset template in either chart | `helm/dag-tools/templates/`; invincible-agent `templates/` (no match) | not stated | — | `built` |
| Address and empty token to the engines | invincible-agent `values.yaml:535`, `:653`, `:655`, `:662`; `templates/configmap.yaml:402`; `templates/secrets.yaml:163-167`; `templates/engines.yaml:84-88` | not stated | — | `built` |
| The restate analyst calls Superset | invincible-agent `agent_fleet/restate_analyst/main.py:1009-1075`, `:1102`; `values.yaml:689` | not stated | — | `built` |
| No gateway on that path | the calls above go to the Superset address; no gateway call in them | not stated | — | `built` |
| Protected by its own roles, with no gateway receipt | finding 5; [need to know, finding 7](../flows/need-to-know.md) | not stated | — | `built` |
