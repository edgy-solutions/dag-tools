# Architecture Decision Records

Short, immutable documents capturing decisions in `dag-tools`, their context,
and the indicators that would lead us to revisit.

## Why this series exists, and the specific thing that started it

Commit messages explain *what changed*. ADRs explain *what we decided* and
*why* — for decisions with long half-lives, spanning many commits, that a future
contributor without the original context could plausibly re-litigate.

This series was opened in August 2026 after a defect whose root cause was the
absence of exactly this. **The mesh-publishing protocol was real, load-bearing,
and unwritten** — it existed only as four independent implementations inside
`arrow`, `duckdb`, `sql` and `delta`. A newer path could therefore satisfy the
shape while omitting a property nobody had recorded as required, and a vendored
copy of an IO manager could carry the same class name with none of the contract.
Neither was caught by review or by tests, because there was nothing to check
against.

**A contract that exists only as an implementation is one a copy can silently
omit.** That is the thesis of this directory. ADR-0001 is that protocol, written
down.

## Scope — what is owned here, and what is cited

**Decisions about the data infrastructure are owned and recorded here.** How a
producer advertises a dataset, what identity a catalog registers it under, what a
probe answers, how the inventory contract evolves, how the registry orders its
writes: those are this repository's to decide, and this series is the record of
them. They are not proposals awaiting ratification elsewhere.

**Governance decisions are owned by `invincible-agent` and stay there.** Who may
read what, how a credential is minted, the classification vocabulary, which plane
a read crosses: those are decided and recorded in that repository and **cited**
from here, by repo and commit. A citation is not a copy.

**No decision is recorded twice.** One record, in the repository that owns it.
Where the two meet, each side records its own half and cites the other's:
invincible-agent ADR-0044 governs what a routing ticket may carry, and ADR-0001
here governs how a producer describes one. Neither restates the other, and
neither is the place to look for the other's reasoning.

Guidance is not a record. `AGENTS.md` says what a decision means for the code it
sits beside and cites the record rather than standing in for it; a bullet that
paraphrases a decision is guidance, however long it runs. The rule above governs
**records** — this series and [`../rulings.md`](../rulings.md) — which is why that
file cites the upstream amendment to the classification ruling instead of
restating it.

If a decision is purely local (renamed a variable, picked the obvious library,
fixed a bug) — commit message. If it shapes how future work gets done — ADR.

### Citing one

**An ADR number without its repo is not a citation.** Two series share a number
range: `ADR-0001` here is the mesh publishing protocol, `ADR-0001` in
`invincible-agent` is something else entirely, and a third series (OpenDDIL)
is cited by both. So:

- A bare `ADR-NNNN` in this repository means **this repository's** series, and
  nothing else may be written bare.
- Every citation of another repository's series **names that repository** —
  `invincible-agent ADR-0064`, `OpenDDIL ADR-0055` — and, where the text being
  relied on could move, the commit that carries it.
- A number someone announced before the record existed is **provisional** and
  must not be cited as if allocated. One already moved: see
  [`../rulings.md`](../rulings.md), 2026-11-05.

## Layout

`ADR-NNNN-short-slug.md`, zero-padded, never reused. Immutable once accepted; a
reversal is a new ADR linking back, with the old one's **Status** updated.

```
# ADR-NNNN — Short imperative title

**Status:** Proposed | Accepted | Superseded by ADR-XXXX | Deprecated
**Date:** YYYY-MM-DD
**Deciders:** name(s)
**Related:** ADR-XXXX (cross-links, including invincible-agent's series)

## Context
## Decision
## Consequences
## Alternatives considered
## Indicators for revisiting
```

If you cannot write an indicator for revisiting, the decision probably is not
ADR-worthy — it is permanent, or it is a preference.
