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

## Scope — this repo, not the mesh

`dag-tools` is a library. Decisions about the mesh as a whole — authorization
subjects, credential minting, the two planes — live in `invincible-agent`'s ADR
series, which is where the cross-component contracts belong. When a decision
here implements one of those, it links rather than restates. ADR-0044 there
governs what a routing ticket may carry; ADR-0001 here governs how a producer
describes one.

If a decision is purely local (renamed a variable, picked the obvious library,
fixed a bug) — commit message. If it shapes how future work gets done — ADR.

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
