# Hasura (CO-HASURA)

Hasura is a GraphQL engine that serves an API over a relational database. It is on the list of tools this set of records covers. Nothing at either pin wires it into the mesh. This record says so, so that the gap is visible rather than missing.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the dag-tools pin listed there (`503607d`). The record shape is provisional.

## Findings

1. **No code, chart or configuration names Hasura at either pin.** A case-insensitive search of the dag-tools repo finds no mention. The same search of the invincible-agent chart finds none, and neither does a search of the rest of that repo. `built`
2. **As a point of use, an API layer is protected only by its own role-based access, and its reads produce no gateway receipt.** Nothing at either pin wires one (finding 1), so nothing places the gateway on its path. A deployment that adds an API layer adds a read path whose only protection is that layer's own roles. This is the condition today, recorded as it is; the remedy is roadmap row 22, `proposed` ([need to know, finding 7](../flows/need-to-know.md)). `built`

## Settings and defaults

None. Neither chart nor any component has a setting for it.

## Gaps

- **Nothing wires Hasura into the mesh** (finding 1). Remedy: [roadmap row 22](../ROADMAP.md). Status: `built`.

## Not established from code

- What role Hasura is meant to play in the mesh, and which services it would serve.
- Whether any deployment runs it alongside the mesh.

## Connections

One row per connection, read from this component's side. Evidence is at this record's pin. Prose above says how each connection behaves; this table is what a diagram draws.

| Other party | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|

No connection is built at this pin: nothing in the code or the chart refers to this component. `built`

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | not established | — |
| How the mesh wires it | not wired | — |
| Sign-in | none | — |
| Data path | none | — |
| Plane | not established: nothing wires it into the mesh at either pin, so no plane is assigned | — |
| Security role | none | — |
| Protected by | its own role-based access, where a deployment adds one; no gateway receipt | [need to know, finding 7](../flows/need-to-know.md) |
| Services served | none at the pin | — |

## Sources

All paths are at `503607dd321cd6627509bfc93955357b8566597f` unless an invincible-agent path is named, which is at `950b3eb912e2cbede454eea31ceb23a317c13fcc`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No mention in dag-tools | whole repo (no match for `hasura`, any case) | not stated | — | `built` |
| No mention in invincible-agent | invincible-agent `helm/invincible-agent/` and the rest of the repo (no match for `hasura`, any case) | not stated | — | `built` |
| Protected by its own roles, with no gateway receipt | finding 1; [need to know, finding 7](../flows/need-to-know.md) | not stated | — | `built` |
