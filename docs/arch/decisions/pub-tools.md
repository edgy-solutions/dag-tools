# pub-tools decisions: a pointer

pub-tools records its own decisions, as numbered decision records in its own repository. This record points to them and restates none. Where pub-tools' records and any line here disagree, pub-tools' records hold. [The decisions index](README.md) explains how each decision is addressed.

**This record describes; it decides nothing.** Statuses are those of [the README](../README.md). The record shape is provisional.

## Where they are

| | |
|---|---|
| Repository | pub-tools |
| Commit | `ad981b119dc00dc34ab2d187456bf6f4b4205bbb`, the head of its stacked pull requests #6 and #7, which are not yet merged. The stack was rebased on its default branch; the `docs/arch` tree is `e1a8a6a6525ad96c10adc49ebde0fc9809a9077b` at this commit and at `21bc4db758f2465d35122e3a9f9ee8aeb4fab1f9`, where it was first read. After the merge, the paths on its default branch are the same. |
| Index | `docs/arch/decisions/README.md`, one line per record, giving the record's number, title and status |
| Records | `docs/arch/decisions/ADR-NNNN-*.md`, ADR-0001 to ADR-0045. Each has a status, the decision, why, its consequences and its evidence. |
| Evidence pins | Each record cites pub-tools code at `90fba4a`, the pub-tools pin in [the README](../README.md), with a test name or "no test". dag-tools references are at `a99779f`, and invincible-agent references at `6b3e8efd`. |

The ADR numbers are pub-tools' own series. A bare decision number in a dag-tools record means the dag-tools series, so a pub-tools record is always cited with its repository, as "pub-tools ADR-NNNN".

## Statuses

| Status | Records |
|---|---|
| `built` | 36 |
| `accepted` | 8 |
| `proposed` | 1 |
| `measured` | 0 |

pub-tools records a run that is reported only in a commit message as `built`, with a note. That is the same rule as in [the README](../README.md): `measured` needs a recorded run.

## Not established

- Whether pub-tools' records match the code at `90fba4a` line by line. They are not checked here. The owning session reports its term check as clean.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| 45 records and one index at the commit | pub-tools `docs/arch/decisions/` at `ad981b1`, file list read through the hosting service's API | — | 45 `ADR-*` files and `README.md` | — |
| Status counts | pub-tools `docs/arch/decisions/README.md` at `ad981b1`, status markers counted | — | 36, 8 and 1 | — |
| Evidence pins and the commit-message rule | supplied by the owning session, 2026-11-19 | — | — | — |
