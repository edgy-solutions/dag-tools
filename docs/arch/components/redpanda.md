# Redpanda (CO-REDPANDA)

Redpanda is a message broker that speaks the Kafka protocol: producers publish records to topics, and subscribers read them in order. It is on the list of tools this set of records covers as a proposal. Nothing at either pin deploys it or calls it. The mesh's publish and subscribe phase is `proposed`, and this record says what exists today so that the proposal is not read as built.

This record covers what the chart and code provide by default, and what a deployment may set. It describes no particular deployment.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md). Line numbers are at the component-records pin listed there (`add5bc4`). The record shape is provisional.

## Findings

1. **dag-tools has no message-broker client and no broker in its chart.** No component, resource or chart template names Redpanda or a Kafka client. `not built`
2. **No invincible-agent chart template deploys a broker.** No template at the pin names Redpanda or Kafka. `not built`
3. **The mesh's only asynchronous hand-off is not a subscription.** The OTel API sync component sends a call plan to Restate one way, and Restate runs it as a durable workflow. Nothing subscribes to it, and nothing registers it as a published object (see [the DoD alignment](../alignment/dod-dmra.md), row S9). `built`
4. **Invincible-agent names Redpanda as a planned successor to an interim mechanism in its projector.** Its plans and the projector's code mention it; they are cited by path only. Nothing at either pin builds it. `proposed`

## Settings and defaults

None. Neither chart nor any component has a setting for it.

## Gaps

- **The mesh has no publish and subscribe service** (findings 1 to 3). Delivery is a read a consumer asks for, not a record pushed to a subscriber, and no published object is registered for subscription under access control. Remedy: not on the roadmap.

## Not established from code

- What role Redpanda would play in the mesh, and which datasets or events it would carry.
- How a subscription would be authorized, and by which decision point.
- Whether a published object would be registered in the catalog.

## Place in the mesh

| Aspect | What the chart and code show | Alignment row |
|---|---|---|
| Purpose | `proposed`: brokered asynchronous delivery | — |
| How the mesh wires it | not wired | — |
| Sign-in | none | — |
| Data path | none | — |
| Plane | `proposed`: data management, delivery between producers and consumers | [NATO L3](../alignment/nato-dcra.md) |
| Security role | none | — |
| Services served | none at the pin; pub/sub is `not built`, and the mesh's pub/sub phase is `proposed` | [DoD S9](../alignment/dod-dmra.md) |

## Sources

All paths are at `add5bc48021fe6d2c79c24c31072142fc93c8179` unless an invincible-agent path is named, which is at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`. Invincible-agent chart paths are under `helm/invincible-agent/`.

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| No broker client or template in dag-tools | whole repo (no match for `redpanda`, any case; no Kafka client among the dependencies in `pyproject.toml`); `helm/dag-tools/templates/` | not stated | — | `not built` |
| No broker template in invincible-agent | invincible-agent `templates/` (no match for `redpanda` or `kafka`, any case) | not stated | — | `not built` |
| One-way send to Restate | `dag_tools/components/otel_api_sync/component.py:58` | AGENTS.md §4a | see [the OTel API sync record](otel-api-sync.md) | `built` |
| Planned in invincible-agent | invincible-agent `docs/plans/projector-build-plan.md`; `src/iagent/projector/__init__.py`; `src/iagent/projector/apply_loop.py` | not stated | — | `proposed` |
