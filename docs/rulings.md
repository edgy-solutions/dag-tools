# Rulings

Decisions that were ruled in conversation and, until 2026-11-16, existed nowhere
in this repository. Each entry says **when** it was ruled, **who** ruled it,
**where** it was said, what it changed here, and where the contract it produced
is written down now.

**Nothing here is numbered, deliberately.** A single decision log with one
numbering is coming from the architecture kit; numbering these today would create
a second series to merge, which is the problem, not the fix. When that format
arrives, these entries move into it and this file becomes a redirect.

**Citations, and one record per decision.** An ADR number without its repo is not
a citation — see [`adr/README.md`](adr/README.md). Every number below names its
series, and where the text being relied on could move, the commit that carries
it. A conversational ruling has no commit to cite: the source named is the
conversation and the packet or issue it answered. And a decision is recorded
**once**, in the repository that owns it: where an entry below rests on a
governance decision `invincible-agent` owns, it cites that record rather than
restating it.

**What this file is not.** It is not a record of the reasoning — that lives in
[`../AGENTS.md`](../AGENTS.md) beside the code each ruling governs, which is where
someone about to edit that code will actually read it. This file exists so a
ruling has a date and an author, which a guidance bullet does not carry.

---

## 2026-11-03 — This line holds `dag-tools/seat/owner` for gateway, broker and image work

**Ruled by:** the architect, via Chris.
**Where:** granted in conversation when the seat was assigned.

Work on the central gateway, the domain broker and the release images is taken
directly by this seat rather than proposed and queued. Chris merges.

**What it changed:** a seat is not a lane. Work done under it carries no `Lane:`
trailer, and a branch does not wait on a lane assignment before starting.

**Where it is written:** here only. It governs process, not code, so no guidance
bullet corresponds to it.

---

## 2026-11-04 — Anything built in `dag-tools` is a component configurable from YAML

**Ruled by:** Chris.
**Where:** in conversation, while the read audit (step 1 of invincible-agent
ADR-0064) was being built as a `dagtools audit` CLI command.

Verbatim: *"remember when you build anything in dagtools it must be component
based and configurable from yaml"*.

**What it changed:** the CLI was the wrong primary surface. The read audit was
rebuilt as `ReadAuditComponent`, and the CLI kept as a thin extra door over the
same engine. Every component since has been built this way: `Component,
Resolvable, Model` with `build_defs()`, every knob a YAML field, and the engine a
pure module that the component, any Restate handler and the tests all share.

**Why it needed ruling at all:** it was already the written rule — `AGENTS.md` §2
("Declarative Component First") and §5 ("Normalized Configuration Design"). The
correction was that the rule was not being applied, not that it was missing.

**Where it is written:** `AGENTS.md` §2, §5; `.cursorrules` §2, §5.

---

## 2026-11-04, widened 2026-11-05 — Nothing from below is named in `dag-tools` code

**Ruled by:** Chris.
**Where:** in conversation, as the preamble to invincible-agent ADR-0064
(2026-11-04), and widened as the preamble to the mapping work queued on
2026-11-05.

No database, schema, table, column, classification dimension or classification
value appears in this library — not as a constant, not as a default, not in a
comment. Configuration that needs such a name is required, with no default,
because a *wrong* default is worse than a missing one: it reads the wrong column
and reports the result as a clean answer.

**What it changed:** `HistoryColumns` has five required fields and no fallbacks.
`warehouse_source` discovers which classification dimension is hierarchical from
the mounted document's own `hierarchical` flag rather than from any name it
carries. The SystemLink kind's `filters.terminal_statuses` is required with no
default, and the empty list has a refusal of its own.

**Where it is written:** `AGENTS.md` §4j, §4l, §4m, §4q, and `.cursorrules`
likewise. The rule itself was already in the repo; only its date and author were
not.

---

## 2026-11-05 — One classification file, owned by policy; `dag-tools` ships none

**Ruled by:** Chris, with two amendments (the second recorded upstream).
**Where:** on the forks governance raised on `invincible-agent` issue #268, in a
gov packet dated 2026-11-05 addressed to this seat, copied to the architect.

Verbatim: *"(a) one levels file, owned by policy
(invincible-agent/policy/classification/levels.yaml); dag-tools never ships its
own -- it reads the deployment's copy (ConfigMap from the chart), so code reads
what the server serves. (b) Levels are ordered; a grant at a level is a ceiling
and implies every less restrictive one. (c) Relation: `cleared_for:<level>` --
`can_read` stays what it is."*

### Amendment, the same day (2026-11-05), by Chris

Verbatim: *"the levels mount becomes the dimensions file, same path;
warehouse_source column labels become {dimension: values} with the hierarchical
one required."*

The single ordered list of levels becomes a **set of dimensions**, exactly one of
which is hierarchical. The mount is unchanged — "same path" — because the
hierarchical dimension *is* the former levels list, so what governance rebuilds is
the ConfigMap's content, not its wiring.

### Amendment, 2026-11-06 — recorded upstream, cited here

Deliberately **not** restated below. It is a governance decision this repository
does not own, and a decision is recorded once, in the repository that owns it
([`adr/README.md`](adr/README.md)). It is recorded in `invincible-agent`
ADR-0064, whose Status reads *"Amended 2026-11-06 to align with OpenDDIL
ADR-0055"* (invincible-agent commit `b29d4b96`, 2026-11-07); read it there.

In one line, so this entry is navigable: it is the amendment that makes
membership on a non-hierarchical dimension a **declared kind** rather than one
fixed rule. What that obliges the code here to do is written as guidance, beside
the code, in `AGENTS.md` §4l, §4m and §4n.

**What it changed:**

- **Step 3 of invincible-agent ADR-0064 shrank.** As first handed over it was
  `warehouse_source` component instances *plus* a `dag-tools`
  `classification/levels.yaml`. The file half is ruled out; it was never created.
- The mount stayed where it was: env `DAGTOOLS_CLASSIFICATION_LEVELS`, default
  `/etc/dagtools/classification/levels.yaml`.
- The composition engine here reads it as **one partial order, four ways** rather
  than as four rules — which works out as `max` on the ladder, union on a
  restrictive or exclusionary set, and **intersection** on a permissive one. That
  shape is this repository's decision, and it was taken because the permissive
  case runs opposite to the other three: a wider permissive set admits more
  readers, not fewer, so written as four independent branches one of them would
  end up pointing the wrong way.
- An absent or empty document means every column is most restrictive in every
  dimension, which in practice means nothing is served. Not "unclassified,
  therefore readable".
- `cleared_for:<level>` must now carry the dimension too. That spelling is
  policy's call; it was flagged to governance rather than invented here, and is
  still outstanding.

**Where it is written:** `AGENTS.md` §4l (the rules every reader is built to),
§4m (`warehouse_source`), §4n (propagation); `.cursorrules` §4l.

---

## 2026-11-05 — `contract_mapping` and `graph_mapping` are queued behind work in flight

**Ruled by:** the architect, via Chris.
**Where:** in conversation, in the brief that followed the classification ruling
above.

Two pieces of work were described and explicitly queued rather than started:
`contract_mapping` (contracts as data — a mapping document reconciled against a
governed contract) and `graph_mapping`. The theme given was obsolescence as a
process, and contracts as data.

### The numbers in that brief were provisional, and one of them has moved

This is the entry that shows why a bare number is not a citation.

The brief allocated provisional **ADR-0065** to `contract_mapping` and
**ADR-0066** to `graph_mapping`. Neither holds today:

- `invincible-agent` **ADR-0065** is *"Obsolescence is a process; PCN/PDN notices
  are its intake"* (commit `831d8d06`). Its own Status says: *"The number was
  provisional in the architect's 2026-11-06 brief and is allocated by this
  record."*
- `invincible-agent` has no **ADR-0066**. That series has gaps at 0058, 0059 and
  0066, and 0067 is allocated.

So `contract_mapping` and `graph_mapping` have **no number** in that series, and a
bare "ADR-0065" written in this repository to mean `contract_mapping` would now
resolve to a different decision. Neither is to be cited by number until a record
allocates one.

**What it changed:** `contract_mapping` is built and shipped
(`dag_tools/contract_mapping/` + `ContractMappingComponent`), referred to by name
rather than by number. `graph_mapping` is not built, and nothing in this
repository claims it is.

**Where it is written:** `AGENTS.md` §4k for what was built. The numbering
correction is written here and nowhere else.

---

## 2026-11-12 — Output describing a work server is never committed, not even redacted

**Ruled by:** the dag-tools owner, amending the third of the mfg-trace rulings.
**Where:** in conversation, on the plan for the SystemLink spike.

`dag-tools` is a **public** repository. Anything that describes a work server — a
spike or probe dump, a host, a server version string — is never committed, not
even with the host redacted. A redacted file still describes the employer's server
in public. The person who runs the probe brings the file over locally.

Only the settings **derived** from it are committed, in a commit whose message
says the values were confirmed by a spike on a given date and names no server
version.

**What it changed:** an earlier plan to commit a redacted copy was overruled.
`docs/plans/mfg-trace-spike/` is git-ignored apart from its README. The SystemLink
kind's measured settings (`DEFAULTS` in `dag_tools/systemlink/spec.py`) are the
derived half; every one a deployment may have measured differently is overridable
in YAML, and no status value appears in that file at all.

**Where it is written:** `AGENTS.md` §4q; the ignore rule in `.gitignore`.
