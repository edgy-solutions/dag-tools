# Mesh architecture records

These records **describe the mesh as it is**. They do not propose. Where a record names something that is not built, it gives that thing a status of `accepted` or `proposed` and says so. Nothing unbuilt is called built.

The records are written for [arch-kit](https://github.com/edgy-solutions/arch-kit), pinned at tag `v0.1.0` (`e0b9b3c40520440a09a4e0e6050ea5017de05848`). arch-kit v0.1.0 has no record type for a *flow* or for *alignment to an outside reference*. Until the kit or its loader declares one, these files follow the kit's view form: a title line, findings first, and a Sources table at the end. **That shape is provisional.**

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
| invincible-agent | `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` | read directly, file by file, for the claims cited |
| iagent-mesh-sdk | `d6a737837c3d0609ace95017c6b4b45b500ea755` | facts supplied by the owning session; not read here |
| pub-tools | `90fba4a` | facts supplied by the owning session; not read here |

## References

- **NATO, *Data Centric Reference Architecture for the Alliance*, version 2 (AC/322-D(2025)0056), publicly disclosed.** It frames the need-to-know flow (its data-centric security appendix) and the data-product characteristics. These records name its elements and copy none of its figures.
- **A second data mesh reference architecture** is the primary alignment for the service records. It is cited by title only once its public citation is confirmed.

Neither reference document is committed here.

## Records

| Record | Kind | State |
|---|---|---|
| [flows/need-to-know.md](flows/need-to-know.md) | flow | draft |
