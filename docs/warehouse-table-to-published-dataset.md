# One warehouse table to a published dataset

Status: describes the code at `503607d`. Every name in every example below is
invented or belongs to that example's own sqlite file.

For finance analysts and the engineer who runs your deployment: what an
operator actually does to take one warehouse table from "nobody knows who
reads it" to a curated, contracted, classified, published dataset — using only
examples this repository already ships and runs — and where those examples stop
short of the real thing.

Each step is a component configured from YAML. Every example runs with **no
warehouse, no catalog and no credentials**, so the whole path can be walked
before any grant is requested.

---

## The seven steps

| # | What the operator does | The example that runs it |
|---|---|---|
| 1 | Measure who reads the table today | [`examples/read_audit`](../examples/read_audit) |
| 2 | Make every pipeline read say which asset it is for | [`examples/tagged_warehouse`](../examples/tagged_warehouse) |
| 3 | Label the source table's columns | [`examples/warehouse_source`](../examples/warehouse_source) |
| 4 | Let everything downstream inherit those labels | the same example's second half |
| 5 | Put the table under a data contract | [`examples/contract_mapping`](../examples/contract_mapping) |
| 6 | Derive the curated tables readers actually query | [`examples/mfg_trace`](../examples/mfg_trace) |
| 7 | Publish to the catalog | the `catalog:` block in steps 3-5 |

### 1. Measure the reads (read-only)

`ReadAuditComponent` reads your warehouse's query history — or an export of
it — and reports which asset explains each object read, and **which reads
nothing explains**. The gap is the deliverable, and it is published broken into
three parts, because they go to three different people: an *untagged* read is
closed by step 2, an *ambiguous* one by narrowing a run's selection, and a
*non-pipeline* read (a person) is not an attribution gap at all.

The shipped example reports 9 objects read, 4 explained, 5 in the gap. It
executes nothing itself: it builds the SELECT and hands it to a resource as SQL
plus bound parameters. Point it at an export first — the account that can read
query history is usually not the account the orchestrator holds, and
measurement should not wait on a grant.

Nothing in this library names a relation or a column. All five history column
names and the relation are required, with no defaults, because a wrong default
would audit the wrong column and report an empty window as a clean audit.

### 2. Tag the reads

`TaggedWarehouseComponent` is a warehouse resource that states, on every query
it sends, which asset the read was for — and **refuses to send one it cannot
say that about**. One line in an asset sharpens the tag from "this run" to
"this asset":

```python
rows = context.resources.warehouse.for_asset(context).fetch_rows(stmt, params)
```

A read made for a person is bound per request with `for_user(authz_id)` — the
id the gateway verified, never a header a client sent.

Step 1's component is reused **unchanged**: its `history:` block now names
`resource_key: warehouse` instead of a file. The seam between the two steps is
one method, `fetch_rows`. As the resource rolls out, step 1's untagged count
trends to zero; that transition is the whole argument for the tag.

### 3. Label the source

`WarehouseSourceComponent` declares one dataset in YAML with a classification
label per column, and **observes rather than copies**: it counts rows, names
columns, records an as-of. No row of the source reaches the orchestrator, the
catalog or the process.

Two things an operator must supply before this step runs at all:

* **The classification document.** dag-tools ships none. It is policy's, mounted
  as a file, named by `DAGTOOLS_CLASSIFICATION_LEVELS`. It declares a set of
  dimensions, exactly one of them hierarchical; the component learns which is
  the ladder from the document's own flag, never from a name in this library.
* **The physical dataset URN.** The asset of record is the warehouse's own
  dataset, not the orchestrator's asset. An orchestrator-platform URN is
  refused, not discouraged: the catalog would describe one object and a crawler
  would discover a second.

A column you forget to label comes out **most restrictive in every dimension**,
counted and warned about. A label naming a column the source no longer has
fails loudly instead — the two cases must stay distinguishable.

### 4. Propagate downstream

`ClassificationPropagationComponent` labels everything downstream, and nobody
declares those labels: the label of a derived column is a consequence of what it
read. It reads each derived asset's own column lineage, composes each output
column's label from the source columns it came from, and publishes the result.

Composition only ever tightens. A column with no lineage entry publishes the
whole table's ceiling — correct and coarse; declaring one more lineage entry is
what buys the difference. The one thing that can widen a column is a **declared
downgrade**, which requires an `id`, an `owner` and a `reason`, because it is
the only rule in the system that publishes a column wider than it derives.

The sensor ships **stopped**. Publishing classifications is not something a
deployment should start doing because a code location loaded.

### 5. Contract the table

`ContractMappingComponent` takes a data contract on one side, the raw table on
the other, and a YAML file saying which source column feeds which contract
column. It renders the dbt model that satisfies the contract plus the tests that
prove it still does. `dry_run: true` renders everything into the run's metadata
and writes nothing — which is also how a mapping change is reviewed before it
reaches a project.

The transforms exist because a raw table disagrees with a contract about exactly
one thing each: padding (`trim`), a fallback column (`coalesce`), case (`upper`),
amounts in cents (`scale`, with the factor read as a decimal — a unit conversion
of money through a binary float is how a total stops reconciling).

Every emitted test fails under a rule the contract itself declares, so a failure
names something a reader of the contract can act on.

### 6. Derive the curated tables

[`examples/mfg_trace`](../examples/mfg_trace) is the one example that shows the
two-stage shape a curated layer needs:

* **Stage 1, conform.** One `contract_mapping` instance per raw table, mapping
  the landing tables onto conformed contracts. This is the **only** place
  anything from the source is named by name — a file an operator edits when a
  source renames a field, not a library release.
* **Stage 2, derive.** A shipped dbt package (`mfg_trace_curated`: three models,
  five tests, one macro) that derives the curated tables and names nothing from
  below. It declares a default for **no** required variable — the two conformed
  relations, the exact numeric type for tolerance arithmetic, and the comparator
  table are all your deployment's, and an unset one fails `dbt parse` rather
  than guessing.

### 7. Publish

Steps 3-5 each take a `catalog:` block (`server`, `token`). Published: the
dataset's properties, a glossary term per column per label value on
`editableSchemaMetadata`, and the lineage edge from the physical dataset to the
orchestrator's asset. Deliberately **not** a schema aspect: these components read
column *names*, not types, and claiming a type would write a guess into the
catalog as fact.

A catalog failure **fails** the run, on purpose: the published label set is the
deliverable, and reporting success without publishing it would leave the catalog
describing a dataset it never described.

---

## Where the examples fall short

Read this part before planning the work.

1. **The labels decide nothing today.** Publishing a label set changes nothing
   about what any reader may currently read. Nothing in this library consults a
   classification to answer an access question; the brokering those labels exist
   for is **intended, not built**. Steps 3 and 4 buy a catalog that describes the
   data correctly — not enforcement.
2. **No example carries one table the whole way.** Five examples, five
   vocabularies: invented identifiers in step 1, one sqlite file in step 2,
   another in step 3, an earned-value landing table in step 5, a
   manufacturing-shaped source in step 6. Only steps 1 to 3 are actually wired
   together in shipped code. An operator stitches the rest.
3. **Nothing publishes to a real catalog.** Every example is a dry run with
   `catalog_proposals: 0`. The authentication posture is documented and is not
   exercised by any example.
4. **Chained propagation is not supported.** A derived dataset cannot be another
   dataset's source, so a two-hop mart has no propagated label at its second hop.
5. **The classification document does not exist until policy writes it.** The
   one in the example is a stand-in with placeholder ids (`rung_*`, `facet_*`).
   Step 3 cannot run against a real document until there is one.
6. **Automation has two traps that pass silently.** Selecting "all assets" does
   **not** include a source asset, so a job built that way observes nothing and
   reports success; and the YAML route for attaching a schedule is rejected
   outright by the orchestrator version production runs, so the schedule has to
   be set from Python.
7. **Two checks that look like proof and are not.** A SQL cast rounds where the
   contract's row-level check refuses — the cast is your SQL and this library
   owns no dialect, so that is where a check goes. And building dbt with
   `--empty` makes its tests pass on empty relations, so contract-test parity
   under `--empty` says nothing about the data.
8. **Step 6 carries two stated unknowns.** The flattened spelling of its nested
   status column is unverified against a real server, and six of its result
   columns are declared in the mapping but have never been observed in real
   data. Both are marked in the example rather than papered over.
9. **Your vocabularies are yours, in two places that must agree.** The set of
   statuses that counts as terminal, and the comparator table that says which
   operator bounds which limit, carry no defaults. An operator the comparator
   table does not cover is kept as text and **counted** — at warning severity,
   deliberately, so the pressure is on adding a row rather than dropping one.

## What to do first

Step 1 against an export. It needs no grant beyond a query-history read, it
writes nothing anywhere, and the number it produces — reads nothing explains —
is what sizes every step after it.
