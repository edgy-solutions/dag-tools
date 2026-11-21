# Mesh architecture records

These records **describe the mesh as it is**. They do not propose. Where a record names something that is not built, it gives that thing a status of `accepted` or `proposed` and says so. Nothing unbuilt is called built. The one exception is [the roadmap note](ROADMAP.md): it holds the remedies proposed for gaps the records found. Every row in it is `proposed` except the one the architect has accepted, which the note marks.

The records are written for arch-kit, pinned at tag `v0.1.0` (`e0b9b3c40520440a09a4e0e6050ea5017de05848`). arch-kit v0.1.0 has no record type for a *flow* or for *alignment to an outside reference*. Until the kit or its loader declares one, these files follow the kit's view form: a title line, findings first, and a Sources table at the end. **That shape is provisional.**

## Status vocabulary

Every claim carries exactly one status.

| Status | Means | Evidence it needs |
|---|---|---|
| `measured` | A recorded run against a running system showed it. | The record of the run. Having a test that could measure it is not enough. |
| `built` | Code at the pinned commit does it. | A file and line. A test that pins it, when one exists; when none does, the row says so. |
| `accepted` | A decision record accepts it, and no code does it yet. | The decision record. |
| `proposed` | It is written down as intent, but not accepted or built. | Where it is written. |

A gap is a claim the reference expects and the mesh does not meet. Its status is the status of whatever partial thing exists. A gap with nothing built is written `not built`.

## Pins

All evidence is read at these commits. A line number is valid only at its pin.

| Source | Pin | Read status |
|---|---|---|
| dag-tools | `9d89568a70a54482ef9ee0b3d6502555f6b0c4d1` | read directly |
| dag-tools (component records) | `add5bc48021fe6d2c79c24c31072142fc93c8179` | read directly |
| invincible-agent | `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` | read directly, file by file, for the claims cited |
| iagent-mesh-sdk | `d6a737837c3d0609ace95017c6b4b45b500ea755` | facts supplied by the owning session; not read here |
| pub-tools | `90fba4a` | facts supplied by the owning session; not read here |

## References

- **NATO, *Data Centric Reference Architecture for the Alliance*, version 2 (AC/322-D(2025)0056), publicly disclosed.** Its Appendix 9 (Data-Centric Security) frames the need-to-know flow.
- **Chief Digital and Artificial Intelligence Office (CDAO), *Data Mesh Reference Architecture*, version 2.6, March 2024.** It is the primary alignment for the service and principle records.

Both are public. The records cite them by element name and section or paragraph number only. They copy no figure and no passage, and they state each definition in their own words. Neither document is committed here.

## Alignment records

An alignment record sets the mesh against an outside reference. It follows the shape the OpenDDIL lane gives:

1. **The reference:** name, edition, and the terms under which it is cited. A record may cite more than one reference.
2. **One row per reference element**, with four fields:
   - the element's name, with its section or paragraph number;
   - what corresponds in the mesh;
   - how closely it corresponds;
   - the evidence.

These records add a fifth field to each row: the status of what corresponds, in the vocabulary above. The shape gives none, and this README requires one for every claim.

How closely is one of:

| How closely | Means |
|---|---|
| `close` | The mesh does what the element describes, in the role the element gives it. |
| `partial` | The mesh does part of it. The row names the missing part. |
| `different` | The mesh meets the same need another way. The row says how. |
| `none` | Nothing in the mesh corresponds. |

`how closely` describes the correspondence; `status` describes the thing that corresponds. A row can be `close` and `proposed`, if the intent matches and nothing is built.

## Records

| Record | Kind | State |
|---|---|---|
| [flows/need-to-know.md](flows/need-to-know.md) | flow | draft |
| [flows/big-data.md](flows/big-data.md) | flow | draft |
| [flows/agent-capabilities.md](flows/agent-capabilities.md) | flow | draft |
| [components/central-gateway.md](components/central-gateway.md) | component | draft |
| [components/classification-propagation.md](components/classification-propagation.md) | component | draft |
| [components/contract-mapping.md](components/contract-mapping.md) | component | draft |
| [components/datahub-lineage.md](components/datahub-lineage.md) | component | draft |
| [components/dbt-project.md](components/dbt-project.md) | component | draft |
| [components/dlt-pipeline.md](components/dlt-pipeline.md) | component | draft |
| [components/document-tables.md](components/document-tables.md) | component | draft |
| [components/domain-broker.md](components/domain-broker.md) | component | draft |
| [components/grist-ingest.md](components/grist-ingest.md) | component | draft |
| [components/mesh-data-client.md](components/mesh-data-client.md) | component | draft |
| [components/otel-api-sync.md](components/otel-api-sync.md) | component | draft |
| [components/read-audit.md](components/read-audit.md) | component | draft |
| [components/restate-api-sync.md](components/restate-api-sync.md) | component | draft |
| [components/restate-dlt-sync.md](components/restate-dlt-sync.md) | component | draft |
| [components/restate-server.md](components/restate-server.md) | component | draft |
| [components/restate-worker.md](components/restate-worker.md) | component | draft |
| [components/s3-sensor.md](components/s3-sensor.md) | component | draft |
| [components/systemlink-source.md](components/systemlink-source.md) | component | draft |
| [components/tagged-warehouse.md](components/tagged-warehouse.md) | component | draft |
| [components/warehouse-source.md](components/warehouse-source.md) | component | draft |
| [ROADMAP.md](ROADMAP.md) | roadmap note (the kit's format) | draft |
| [alignment/dod-dmra.md](alignment/dod-dmra.md) | alignment: the DoD reference's principles and services | draft |
| [alignment/nato-dcra.md](alignment/nato-dcra.md) | alignment: the NATO reference's security roles and planes | draft |
