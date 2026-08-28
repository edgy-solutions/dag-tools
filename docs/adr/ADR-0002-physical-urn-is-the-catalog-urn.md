# ADR-0002 — An asset's mesh identity is the CATALOG's identity, derived the catalog's way

**Status:** Accepted
**Date:** 2026-08-27
**Deciders:** Platform team
**Related:** [ADR-0001](ADR-0001-mesh-publishing-protocol.md) — the ticket this identity is derived from.

## Context

The broker registers routes keyed by DataHub URN, and consumers ask by URN. **If the two
strings differ by one character the route does not exist**, and the gateway answers `404 No
active domain broker found` — which reads as "that asset does not exist" rather than "we named
it differently."

The broker originally took its key from `record.urn`, whose derivation forces
`platform="dagster"`. That argument does not merely mislabel the platform: the converter
selects the **name layout** from it. `dagster` is absent from `FILESYSTEM_PLATFORMS`, so it
takes the `".".join(asset_key)` branch. An asset key of
`minio-svc/publog-lake/publog/p_cage` therefore became:

```
registered   ...(dagster, minio-svc.publog-lake.publog.p_cage, PROD)
catalogued   ...(s3,      minio-svc.publog-lake/publog/p_cage, PROD)
```

Not a spelling difference. The dotted form destroys the boundary between platform instance,
bucket and key prefix that the convention exists to encode — and the instance segment is
load-bearing, because one S3 path on two servers is two tables.

Nothing a resolver produced could match a route registered that way, so every read 404'd
against a routing table that looked fully populated.

## Decision

**The mesh identity is the physical one, derived exactly as the catalog derives it.**

Quoting the sensor that owns the rule (`datahub_lineage/component.py`):

> An asset that materializes an S3 table and the S3 table are the same real-world object, so
> they get ONE catalog entity — the physical one, named exactly as a DataHub source crawler
> would discover it. Assets with no physical location (a staging step, a source stub) keep a
> dagster-platform entity, because there is no table to point at.

`physical_urn_for()` mirrors that resolution rather than inventing a third. It reads the
platform the asset **declared** — via `physical_coordinates()`'s `source_type` — and passes it
to the same converter the sensor uses.

Precedence, most authoritative first:

1. **An explicit `datahub/urn` tag** — someone stated it; nothing overrides a statement.
2. **The physical URN**, derived as above.
3. **Not advertised.**

**There is no dagster-platform rung.** A dagster URN means the asset has no physical location,
which is exactly when there is nothing to hand a reader. Registering one advertised a route
whose ticket resolved to a bucket that does not exist.

## Consequences

Producer and catalog agree by construction, because they run the same derivation from the same
declared platform.

**Deployments lose routes they appeared to have.** An advertised count drops to whatever is
genuinely publishable. That is a correction, not a regression — those routes never resolved to
readable data.

**A known gap, recorded rather than hidden:** the sensor resolves through its component's
`platform_mappings`; the broker has no component config and resolves without them. A
deployment that remaps a platform in YAML drifts again here. Wiring that config through is
outstanding.

## Alternatives considered

- **Make consumers ask by dagster URN.** Rejected: it fixes the lookup and leaves the ticket
  pointing at a placeholder bucket. The name was a symptom.
- **Have the broker register both forms.** Rejected: two identities for one object is the
  problem restated, and it makes the catalog ambiguous about which is canonical.
- **Derive identity from the endpoint or bucket.** Rejected: the URN's instance segment comes
  from the ASSET KEY, a naming convention independent of where the store currently answers.
  Changing an endpoint to an FQDN must not move an asset's identity — and does not.

## Indicators for revisiting

- The sensor's derivation changes and this stops mirroring it. These two must move together;
  if they can drift silently, a reconciliation guard is owed.
- `platform_mappings` starts being used in the fleet, making the gap above live rather than
  latent.
- An asset legitimately has two physical locations (a migration, a mirror), which this
  one-identity model cannot express.
