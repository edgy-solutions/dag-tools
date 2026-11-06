# Contract mapping

A runnable example of `ContractMappingComponent`: a data contract on one side,
a raw source table on the other, and a YAML file saying which source column
feeds which contract column. The component renders the dbt model that satisfies
the contract, plus the tests that prove it still does.

It runs with no warehouse, no dbt project and no credentials. `dry_run: true`
renders everything into the materialization's metadata and writes nothing, which
is also how a mapping change gets reviewed before it reaches a project.

## Run it

```bash
cd examples/contract_mapping/dagster_home && dagster dev
```

Materialize `ev_fixture_contract_mapping`. The metadata is the report:

| | |
|---|---|
| `contract_id` / `contract_version` | `ev_fixture` / `1.0.0` |
| `asset_of_record` | the contract's own dataset URN |
| `source_dataset` | the raw table's URN, attached as lineage |
| `columns_produced` | 11 |
| `tests_emitted` | 11 |
| `tests_under_undeclared_rules` | 0 |
| `declared_not_produced` | 2 |
| `plan_hash` | `0d042688311f` |
| `rendered` | 10 files, in full |
| `files_written` | 0 — this example is a dry run |

## The two URNs, and why they are different

`asset_of_record` is the **physical platform's** dataset URN, taken from the
contract. The mapped model is what a governed read is served from, so that is
what the catalog has to register, and a broker authorizes a read of that object.
`source_dataset` is the raw table's URN and goes on as lineage. Neither is a
`dagster` platform URN: the Dagster asset is how the rows get made, not the
thing being read, and it attaches as lineage like any other producer.

## What the transforms are for

The source here is synthetic — a raw landing table for the earned-value fixture
dag-tools already ships for the sandbox, written the way a raw table usually is.
Every transform in `mapping.yaml` exists because the raw table disagrees with
the contract about one thing:

* **Padded strings**, so `trim`. The contract declares the column a string and
  means it; the padding is the landing table's.
* **A column that is sometimes null with a fallback beside it**, so `coalesce`
  over an ordered list. First non-null wins, which is the only order that is
  ever what anyone means.
* **Case the contract does not want**, so `upper`.
* **Amounts in cents**, so `scale` with `factor: "0.01"`. The factor is read as
  a decimal, never a float — a unit conversion of money through a binary float
  is how a total stops reconciling. The contract declares those columns integer,
  so a scaled value that is not whole is refused rather than rounded.
* **Two columns the contract declares and nothing produces yet**
  (`produced_since: null`), so the mapping leaves them out and the plan reports
  them under `declared_not_produced`. A contract declares columns ahead of their
  producer so readers can be written against them; a mapping is not wrong for
  not having them yet, but the forward edge has to stay visible.

## What the plan refuses, and what it only reports

The line is whether the emitted artifact would be **wrong**. A mapping that
reads a column the source does not have, or fails to produce a column a
governed read selects, or disagrees with the contract about write cadence,
fails the **code location load** — before any run. A mapping that cannot get a
test written, because the contract names its calendar rather than listing it or
because no warehouse type is declared, loads and says so in `notes`: the model
is correct, there is just less proof of it than there could be.

Nothing is guessed to fill a gap. There is no default warehouse type, no default
cast, no default non-finite predicate, and no inferred period calendar, because
a wrong guess at any of them emits SQL that fails at the warehouse for a reason
that has nothing to do with the data.

## The 11 tests

Three of them are the model contract itself — column presence, declared types
and not-null — rendered into `ev_fixture.yml` under `config.contract.enforced`,
which dbt checks before the model lands rather than after. `enforced: true` is
only emitted when **every** column has a declared warehouse type; a half-typed
contract is not a contract.

The other eight are singular tests under `tests/`, each named
`<model>__<rule_id>__<columns>.sql` and each carrying the rule id it fails
under, in a comment and as a selected column. A failure that names an id the
contract does not declare cannot be acted on by anyone reading the contract, so
`tests_under_undeclared_rules` is a number on every materialization and the key
uniqueness id is derived from the contract's own grain rather than written down
in this library.

## Writing into a dbt project

Replace the `emit:` block with a project directory, as the comments in
`component.yaml` show. `prune: true` additionally deletes generated tests this
plan no longer emits; it is off by default because the component writes into a
directory it does not own, and deleting a test an operator wrote by hand is
worse than leaving a stale one. Only files named `<model>__...` are ever
considered for deletion.

A run-config `dry_run: true` overrides a configured target for one run, so a
mapping change can be rendered and read without touching the project.

## What this example cannot show

**There is no warehouse, so the rendered SQL is never executed.** The row route
and the SQL route are proven to agree on types by
`dag_tools_tests/test_contract_mapping_proof.py`, which runs this mapping over
rows derived from the shipped fixture and asserts it reproduces the fixture
exactly, types included — but that proves the *plan*, not your adapter.

One asymmetry is worth knowing before you ship a scaled integer column: the row
route **refuses** a scaled value that is not whole, while a SQL `cast` to an
integer type **rounds** in most dialects. The cast is the operator's own SQL
from `cast_templates` and this library does not own anyone's dialect, so it is
documented rather than papered over. If silent rounding is unacceptable, the
cast is where to put the check.

## Provenance of the vendored contract

`contracts/ev_fixture.yaml` is governed in the agent repo (`invincible-agent`,
`contracts/ev_fixture.yaml`, from PR #275). dag-tools vendors it **byte for
byte, with no added header**, so the example and the proof run with no second
checkout — which makes drift possible, so the canary in
`test_contract_mapping_proof.py` compares the two whenever the upstream copy is
on the machine. Point `DAGTOOLS_UPSTREAM_EV_CONTRACT` at a path, or keep an
`invincible-agent` checkout beside this one. With neither it **skips**: a canary
that silently passes when it cannot find its target is worse than no canary.

Every object name in this example belongs to that fixture. In a deployment the
names are the operator's own, in the operator's own YAML, with no default
anywhere in the library.
