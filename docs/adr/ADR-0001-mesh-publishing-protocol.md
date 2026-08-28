# ADR-0001 — `physical_coordinates()` is the advertisement contract

**Status:** Accepted
**Date:** 2026-08-27
**Deciders:** Platform team
**Related:**
  - invincible-agent **ADR-0044** — a routing ticket carries only broker-minted credentials.
    That ADR governs what the ticket may CONTAIN; this one governs how a producer DESCRIBES
    one. The division is deliberate and is stated in Decision below.
  - [ADR-0002](ADR-0002-physical-urn-is-the-catalog-urn.md) — the identity derived from this ticket.
  - `docs/publishing-io-managers-to-the-mesh.md` — the how-to this ADR is the reasoning behind.

## Context

An asset becomes readable through the mesh when the domain broker can tell a consumer where
its bytes are. The broker learns that by calling **`physical_coordinates(asset_key_path)`**
on whatever object sits in `Definitions(resources=...)` under the asset's `io_manager_key`,
and checking `hasattr`.

That is duck typing, and it is the right choice: an IO manager is the only party that knows
where it put the data, and requiring a dag-tools base class would exclude every third-party
and in-house manager in the fleet.

**But the protocol was never written down.** It existed as four independent implementations —
`arrow.py`, `duckdb.py`, `sql.py`, `delta.py` — and a producer wanting to publish had to read
one and infer which parts generalised. Two failures followed, neither caught by review or
tests, because there was nothing to check an implementation against:

1. **A newer path omitted a property nobody had recorded as required.** The
   mesh-publishing path returned the producer's own writing credential, bypassing the scoped
   STS minting the broker's fallback had always performed. The scoping was not removed; it
   was bypassed by a path that never had it. (invincible-agent ADR-0044.)

2. **A vendored copy carried the class name and none of the contract.** A deployment's IO
   managers were forked from dag-tools' before `physical_coordinates` existed —
   `orch.resources.arrow.ConfigurableArrowIOManager`, identical in name, no shared ancestry.
   142 assets registered, none readable, and the symptom presented as a URN-naming puzzle.

Both are the same shape: **a contract that exists only as an implementation is one a copy can
silently omit.**

## Decision

**`physical_coordinates(asset_key_path) -> dict | None` is the mesh-publishing contract, and
it is stated here rather than demonstrated.**

It returns:

| key | meaning |
|---|---|
| `source_type` | which read path the consumer takes; must be one the client dispatches on |
| `physical_uri` | **where the writer wrote** — not where the asset key suggests |
| `mode` | `mint-sts` for object stores, `producer-credential-unprotected` where no minter exists yet |
| `scope` | bucket/prefix a minted credential is confined to; optional, derived from the URI when absent |
| non-secret coordinates | `endpoint_url`, `region`, `catalog_uri`, `table_identifier`, `database` |

**Returning `None` is a first-class answer, not a failure.** An asset on local disk, or in a
format no consumer can read, has no location another process can use. **An
advertised-but-unreadable location is worse than an unadvertised asset**, because the gateway
routes consumers to it with full confidence.

**It returns no credentials.** Per ADR-0044 the broker mints, per request, scoped to the asset
and expiring with the access window. An IO manager runs in a pipeline pod and knows neither
the caller nor the window; giving every user deployment minting authority would spread
assume-role privilege across the fleet to replace one credential with dozens. **Advertising
and authorising are different jobs.**

**`dag_tools.io_managers.MeshPublishable` is the reference implementation, not a
requirement.** Duck typing stands — any object with the method participates. The mixin exists
so the contract has a single executable statement, and so a producer writes only the part that
genuinely differs (`mesh_uri`). It offers no credentials hook, structurally.

## Consequences

**Wins.** A third party publishes by writing one method. The contract has one place to change
and one place to read. `MeshPublishable` refuses unknown `source_type`s and empty asset keys at
the producer, where the failure is attributable, instead of in the consumer's process where
the client raises on dispatch.

**Costs we accept.** Duck typing means a manager that misspells the method fails silently as
"not advertised" rather than loudly as "wrong signature." Mitigated, not removed, by the broker
naming the declining precondition per asset — including the **module**, since a vendored copy
and the original are indistinguishable by class name.

`physical_coordinates` is a **load-time** call: it runs once at broker startup and its result
is cached for the process lifetime. Nothing time-bound or caller-specific may be computed
there. This is the constraint that forced ADR-0044's amendment, and it is a property of this
decision, not an implementation detail of that one.

**The rule cuts both ways.** Producers that decline to advertise unreadable locations are
matched by a broker that declines to register assets with no physical identity. Deployments
relying on the old dagster-URN fallback see their advertised count drop; that count was never
real.

## Alternatives considered

- **Require a dag-tools base class.** Rejected: excludes third-party and in-house managers,
  and the broker only ever needed one method. It would also not have prevented failure 2 — a
  fork of the base class has the same problem.
- **Infer the location from the asset key.** Rejected: it is a guess, and wrong precisely when
  something other than the IO manager performed the write (a dlt pipeline with its own
  filesystem destination). Wrong in a way nothing detects until a read returns nothing.
- **A formal ABC or Protocol with runtime checking.** Rejected for now: it converts a silent
  miss into an import-time error, which is better, but forces every producer to import from
  dag-tools — reintroducing the coupling duck typing exists to avoid. Revisit if silent misses
  recur (see below).
- **Let producers supply credentials, and have the broker narrow them.** Rejected: the broker
  cannot narrow a credential it did not mint, and possession is the whole risk.

## Indicators for revisiting

- **Silent misses recur** — producers intending to publish and not publishing, found only by
  the diagnostic. That would argue for a `typing.Protocol` with `runtime_checkable`, or an
  opt-in registration call, accepting the coupling.
- A backend needs coordinates that do not fit the ticket's shape, suggesting the flat dict has
  outgrown itself.
- `physical_coordinates` needs to become **per-request** — e.g. coordinates that vary by
  caller. It is load-time cached today, and that assumption is load-bearing for ADR-0044.
- The `mode` vocabulary grows a value whose handling differs in the CONSUMER rather than the
  broker, which would break the property that mode is transport-only.
