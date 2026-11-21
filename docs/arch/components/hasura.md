# Hasura (CO-HASURA)

Hasura is a GraphQL engine that serves an API over a relational database. It is on the list of tools this set of records covers. Nothing at either pin wires it into the mesh. This record says so, so that the gap is visible rather than missing.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **No code, chart or configuration names Hasura at either pin.** A case-insensitive search of the dag-tools repo finds no mention. The same search of the invincible-agent chart finds none, and neither does a search of the rest of that repo. `not built`

## Settings and defaults

None. Neither chart nor any component has a setting for it.

## Gaps

- **Nothing wires Hasura into the mesh** (finding 1). Remedy: not on the roadmap.

## Not established from code

- What role Hasura is meant to play in the mesh, and which services it would serve.
- Whether any deployment runs it alongside the mesh.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | not established | — |
| How the mesh wires it | not wired | — |
| Sign-in | none | — |
| Data path | none | — |
| Plane | not established | — |
| Security role | none | — |
| Services served | none at the pin | — |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No mention in dag-tools | whole repo (no match for `hasura`, any case) | not stated | — | `not built` |
| No mention in invincible-agent | invincible-agent `helm/invincible-agent/` and the rest of the repo (no match for `hasura`, any case) | not stated | — | `not built` |
