# Decisions from the census

These records list the decisions that dag-tools states in `AGENTS.md`, and the decisions of the other repositories the mesh draws on (see [other repositories](#other-repositories)). Each decision is given in plain words, with its reason, one status and its evidence at a pinned commit.

**These records describe; they decide nothing.** Each decision already stands in the census. Where a numbered decision record or a ruling already holds a decision, the entry points to it and does not restate it. Statuses are those of [the README](../README.md). Gaps go to [the roadmap note](../ROADMAP.md).

## How a decision is addressed

`AGENTS.md` is the census. It is cited by path, at the dag-tools pin (`503607dd321cd6627509bfc93955357b8566597f`), as listed in [the README](../README.md).

A decision is addressed by its census section and its bullet's position in that section. For example, `AGENTS.md §4, bullet 3` is the third top-level bullet of section 4 at the pin. Headings in these files carry the same address in short form, such as `§4.3`. Where one entry states two census bullets, its Source cell names both, such as `bullets 3-4`, and the entries after it keep consecutive heading numbers, so from there on a heading's number is one lower than its bullet. The Source cell is the census address, and the bullets it names add up to the section's count.

The census has 307 top-level bullets at the pin, across 35 sections. A 36th, section 7, has none: it states one rule as a numbered list, and is recorded as one entry, `§7.1`.

The count in the index below is the number of entries in each record. Where an entry records two bullets, or states no decision, the bracket says so. Two entries state none, each for a different reason: §4g2.6 lists tests, and §4f.10 points to another section.

A status is qualified only where part of the decision is not built. A qualified status appears in both places, the entry's Status line and the table's Status cell, though the wording may differ. A missing test is evidence, not status: it is written in the table's Evidence cell, and the status stays `built`.

Two sections carry the heading `4a`: the OTel to API sync section and the shared inventory contract section. They are addressed as `§4a (OTel)` and `§4a (inventory)`.

## Why these are not numbered

dag-tools already has one numbered series, [`docs/adr/`](../../adr/README.md). Its README says three things:

- `AGENTS.md` is guidance, not a record.
- No decision is recorded twice.
- A bare decision number means that series.

[`docs/rulings.md`](../../rulings.md) keeps its entries unnumbered on purpose. A single numbered log is due from the architecture kit, and numbering now would make a second series to merge.

So these records add no numbers. They index decisions where the census states them, and point to the numbered record or ruling when there is one.

The shape is provisional. It changes when the kit's single log or lane 02's shapes arrive.

## Records

| Record | Census sections | Decisions |
|---|---|---|
| [dag-tools-s1-s3.md](dag-tools-s1-s3.md) | §1 reusability, §2 declarative components, §3 durable state and double writes | 11 |
| [dag-tools-s4-restate.md](dag-tools-s4-restate.md) | §4 Restate worker deployment | 11 |
| [dag-tools-s5-s6.md](dag-tools-s5-s6.md) | §5 configuration design, §6 backwards compatibility | 5 |
| [dag-tools-s4a-otel.md](dag-tools-s4a-otel.md) | §4a (OTel) OTel to API sync | 15 |
| [dag-tools-s4o2-s8.md](dag-tools-s4o2-s8.md) | §4o-2 gateway image signature support, §8 release images | 13 (one bullet of §8 lists tests and states no decision) |
| [dag-tools-s4p.md](dag-tools-s4p.md) | §4p dlt source kinds and per-pipeline schedules | 16 |
| [dag-tools-s4q.md](dag-tools-s4q.md) | §4q SystemLink source kind | 15 |
| [dag-tools-s4r-s4l-s4m.md](dag-tools-s4r-s4l-s4m.md) | §4r curated slice, §4l classification dimensions file, §4m warehouse source | 37 |
| [dag-tools-s4s.md](dag-tools-s4s.md) | §4s documents to tables | 33 |
| [dag-tools-s4n-s4o-s4k1.md](dag-tools-s4n-s4o-s4k1.md) | §4n classification propagation, §4o ticket identity, §4o-1 refusal reasons, §4k-1 refusal fixture | 31 |
| [dag-tools-s4k-s4j.md](dag-tools-s4k-s4j.md) | §4k contract mapping, §4j read audit and tagged warehouse | 37 |
| [dag-tools-s4i-s4h-s4g.md](dag-tools-s4i-s4h-s4g.md) | §4i dbt to DataHub publishing, §4h verdict, §4g-1 probe status, §4g0 probe runner, §4g0a synthetic coverage rollup, §4g2 round-trip probe | 33 (§4g2.3 records two bullets; one, §4g2.6, lists tests and states no decision) |
| [dag-tools-s4g1-s4g-s4f.md](dag-tools-s4g1-s4g-s4f.md) | §4g1 synthetic probes, §4g preflight, §4f run execution | 28 (one bullet of §4f points to §4g2 and states no decision) |
| [dag-tools-s4e-s4a-s7.md](dag-tools-s4e-s4a-s7.md) | §4e equivalence-class matrix, §4d qualification manifest, §4c survey, §4b qualification registry, §4a (inventory) shared inventory contract, §7 the documentation trifecta | 22 (21 bullets, and the one rule of §7) |

## Where the numbered series already holds a decision

| Census section | Numbered record or ruling |
|---|---|
| §4a (inventory) shared inventory contract (evolution rules) | [ADR-0004](../../adr/ADR-0004-inventory-schema-evolves-additively-only.md) |
| §4b registry (pointer written last) | [ADR-0005](../../adr/ADR-0005-registry-writes-the-pointer-last.md) |
| §4k, §4l, §4m asset of record | [ADR-0002](../../adr/ADR-0002-physical-urn-is-the-catalog-urn.md) |
| §1 no names from below | [rulings.md](../../rulings.md), 2026-11-04, widened 2026-11-05 |
| §2, §5 everything is a component configured from YAML | [rulings.md](../../rulings.md), 2026-11-04 |

Every census section now has a record.

## Other repositories

Each repository's decisions are addressed in its own way. A decision number is always cited with its repository, because each repository numbers its own series.

| Repository | Record | How a decision is addressed | Decisions |
|---|---|---|---|
| invincible-agent | [ADR-0001 to ADR-0024](invincible-agent-adr-0001-0024.md), [ADR-0025 to ADR-0048](invincible-agent-adr-0025-0048.md), [ADR-0049 to ADR-0072](invincible-agent-adr-0049-0072.md) | by its own ADR number, as "invincible-agent ADR-NNNN" | 68 ADR files; four numbers in the range have none |
| iagent-mesh-sdk | [sdk.md](sdk.md) | `SDK.N`, numbered in that record only; the SDK has no committed census | 8 |
| pub-tools | [pub-tools.md](pub-tools.md), a pointer | by its own ADR number, as "pub-tools ADR-NNNN", in its own records | 45, recorded there |
