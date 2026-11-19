# Decisions from the census

These records list the decisions that dag-tools states in `AGENTS.md`. Each decision is given in plain words, with its reason, one status and its evidence at a pinned commit.

**These records describe; they decide nothing.** Each decision already stands in the census. Where a numbered decision record or a ruling already holds a decision, the entry points to it and does not restate it. Statuses are those of [the README](../README.md). Gaps go to [the roadmap note](../ROADMAP.md).

## How a decision is addressed

`AGENTS.md` is the census. It is cited by path, at the component-records pin (`add5bc48021fe6d2c79c24c31072142fc93c8179`), as listed in [the README](../README.md).

A decision is addressed by its census section and its bullet's position in that section. For example, `AGENTS.md §4, bullet 3` is the third top-level bullet of section 4 at the pin. Headings in these files carry the same address in short form, such as `§4.3`.

The census has 307 top-level bullets at the pin, across 35 sections (section 7 has none).

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

## Where the numbered series already holds a decision

| Census section | Numbered record or ruling |
|---|---|
| §4a shared inventory contract (evolution rules) | [ADR-0004](../../adr/ADR-0004-inventory-schema-evolves-additively-only.md) |
| §4b registry (pointer written last) | [ADR-0005](../../adr/ADR-0005-registry-writes-the-pointer-last.md) |
| §4l, §4m asset of record | [ADR-0002](../../adr/ADR-0002-physical-urn-is-the-catalog-urn.md) |
| §1 no names from below | [rulings.md](../../rulings.md), 2026-11-04, widened 2026-11-05 |
| §2, §5 everything is a component configured from YAML | [rulings.md](../../rulings.md), 2026-11-04 |

Records for the sections after §6 follow in later batches.
