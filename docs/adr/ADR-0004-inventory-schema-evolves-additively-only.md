# ADR-0004 — The inventory contract evolves additively only

**Status:** Accepted
**Date:** 2026-08-27
**Deciders:** Platform team
**Related:** [ADR-0001](ADR-0001-mesh-publishing-protocol.md), [ADR-0005](ADR-0005-registry-writes-the-pointer-last.md)

## Context

`AssetRecord` is read by parties that upgrade **independently and at different times**:

- the **runtime domain broker**, which classifies IO managers and derives identities;
- the **`dagtools survey` CLI**, which publishes per-build inventory to MinIO;
- the **qualification system**, which reads inventories published by earlier versions, from
  the registry, months later.

That last consumer is the constraint. A qual run compares a baseline snapshot against a
candidate, and the baseline was written by whatever version was current when it ran. **A reader
must be able to consume a record written by a version it has never seen** — older or newer.

Renaming a field breaks every stored artifact retroactively. There is no migration, because the
artifacts are immutable by design ([ADR-0005](ADR-0005-registry-writes-the-pointer-last.md)).

## Decision

**Evolution is additive-only, and `SCHEMA_VERSION` is monotonic.**

1. Never rename a field. Never remove one. Only add.
2. Every added field carries a default, so an old record deserialises under a new reader.
3. Bump `SCHEMA_VERSION` by 1 **in the same commit** as the change.
4. Each record stamps the version it was written with.

A field that becomes wrong is **deprecated in its docstring and left in place**, not deleted.
The cost is a schema that accretes; the alternative is stored artifacts that cannot be read.

## Consequences

Any reader consumes any record. A broker on an old release keeps working against a survey from
a new one, which matters because the fleet is never uniformly upgraded — a fact this repo has
been reminded of repeatedly.

The schema grows monotonically and will eventually carry fields nothing reads. That is the
accepted price, and it is cheaper than a migration over immutable history.

**The version stamp is only useful if it is honest.** An additive change without the bump is
worse than no versioning: a reader trusts a number that no longer identifies the shape. The
same-commit rule exists because a bump deferred to "the release" is a bump that gets forgotten.

## Alternatives considered

- **Semantic versioning with breaking majors.** Rejected: implies a migration path, and there
  is none for artifacts that are immutable by contract.
- **Migrate stored artifacts on read.** Rejected: every reader would carry every historical
  shape — the complexity the version stamp exists to avoid, relocated.
- **Version only when convenient.** Rejected explicitly. A version that sometimes lies is worse
  than none, because it is trusted.

## Indicators for revisiting

- Accreted dead fields make the record genuinely hard to read, suggesting a numbered successor
  type rather than an in-place rename.
- A change is required that cannot be expressed additively — a field whose *meaning* inverts,
  where adding a second field would leave both present and contradictory.
- Registry artifacts gain a retention policy short enough that old shapes age out, which would
  make a breaking change survivable.
