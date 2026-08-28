# ADR-0005 — Immutable artifacts, one mutable pointer, written last

**Status:** Accepted
**Date:** 2026-08-27
**Deciders:** Platform team
**Related:** [ADR-0004](ADR-0004-inventory-schema-evolves-additively-only.md) — why those artifacts must stay readable forever.

## Context

The qualification registry stores per-build inventories and qualification results in
object storage, read concurrently by CLI invocations, CI jobs and operators. Object stores
offer no transactions and no cross-key atomicity: a multi-key write is observable **halfway
done**.

A reader that lands mid-publish sees a manifest referencing artifacts that are not there yet,
or a partial set that looks complete. Both produce a wrong answer confidently — the failure
mode this system exists to prevent, since its entire output is a GO/NO-GO on a Dagster
upgrade.

## Decision

**Every artifact key is immutable. Exactly one key per scope is mutable, and it is written
LAST.**

```
inventory/<repo>/<sha>/…      immutable — a build's artifacts, never rewritten
inventory/<repo>/latest.json  the ONLY mutable key; written after the above
qualifications/<qual_id>/…    immutable; the probe manifest is written last within it
```

The ordering is the mechanism. A reader following `latest.json` reaches a complete set,
because the pointer does not exist — or still names the previous build — until every artifact
it references is durably written. **A crash mid-publish leaves the previous build current,
not a broken one.**

Immutability makes it work: because `<sha>/` is never rewritten, a reader that resolved the
pointer a moment ago is still reading a coherent snapshot even as a new build publishes
alongside.

## Consequences

Readers never observe a partial publish, with no locking, no transactions, and no coordination
between writers.

Storage grows monotonically; old builds are never overwritten. That is the cost, and it buys
the property that a qualification months old can still be read — which
[ADR-0004](ADR-0004-inventory-schema-evolves-additively-only.md) depends on.

**The invariant is an ordering, and orderings are easy to break by accident.** A future
optimisation that writes the pointer earlier "to reduce a window", or in parallel with
artifacts, silently reintroduces the partial read. There is no test that fails at the moment
of the mistake — only a rare wrong answer later, under concurrency. That is why this is an ADR
rather than a comment.

## Alternatives considered

- **Write the pointer first, then artifacts.** Rejected: exactly the partial-read window, made
  the common case instead of the rare one.
- **A lock or lease.** Rejected: adds a coordination service to a system whose readers are
  ad-hoc CLI invocations, to solve a problem write ordering already solves.
- **Mutate artifacts in place, no pointer.** Rejected: a reader mid-write sees a mixture of two
  builds with nothing marking the boundary.
- **Manifest with checksums, verified on read.** Rejected as the primary mechanism — it detects
  a partial read rather than preventing one, and leaves the reader with no valid snapshot to
  fall back to. Reasonable as defence in depth.

## Indicators for revisiting

- The backing store gains real atomicity across keys (a transactional catalog, a metadata
  layer), making the ordering discipline unnecessary rather than merely sufficient.
- Storage growth becomes the binding constraint and a retention policy is needed — which must
  preserve the invariant that anything `latest.json` can reach still exists.
- Publishing becomes concurrent per repo, where two writers could interleave pointer updates
  and the ordering alone stops being sufficient.
