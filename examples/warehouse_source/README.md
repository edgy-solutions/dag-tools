# A classified source, and everything downstream of it

A runnable example of two components, and the pair is the point.

`WarehouseSourceComponent` labels a **source**: one dataset, declared in YAML
with a classification label on each column, published to the catalog under the
dataset's own URN — and **observed, not copied**. Materializing it counts the
rows, names the columns and records an as-of. No row of the source reaches
Dagster, the catalog, or the process.

`ClassificationPropagationComponent` labels everything **downstream**. Nobody
declares those labels: the label of a derived column is a consequence of what
it read. It builds one sensor, reads each derived asset's own column lineage,
composes every output column's label from the source columns it came from, and
publishes the result — with a declared downgrade where a human decided the
aggregate is not the thing it was computed from.

It runs with no warehouse, no catalog, no credentials and no extra dependency.
**sqlite stands in for the warehouse**, `seed.py` creates the source relation,
and `classification/dimensions.yaml` stands in for the document policy owns —
dag-tools ships none.

## Run it

```bash
cd examples/warehouse_source
python seed.py                     # prints the db path and the two export lines
export WAREHOUSE_SOURCE_DB="$PWD/source.db"
export DAGTOOLS_CLASSIFICATION_LEVELS="$PWD/classification/dimensions.yaml"
cd dagster_home && dagster dev
```

Then **Observe** `party_extract` — it is a source asset, so the button says
observe, not materialize, and a job that wants it has to name it in its
selection (`AssetSelection.assets(...)`; `AssetSelection.all()` does not
include source assets). Then **Materialize** `marts/party_exposure`, and turn
on the `classification_propagation` sensor — it ships stopped, because
publishing classifications is not something a deployment should start doing
because a code location loaded.

The observation's metadata is the whole result:

| | |
|---|---|
| `row_count` | 5 |
| `column_count` | 5 |
| `defaulted_column_count` | 1 |
| `defaulted_columns` | `['loaded_at']` |
| `as_of` | the watermark the `as_of_statement` projected |
| `schema_hash` | digest of the projected columns, order included |
| `data_version` | moves when the size, the shape or the as-of moves |
| `classification_source` | the document the labels were validated against |
| `classification_digest` | of that document's bytes |
| `unratified_values` | `['example_facet.facet_y']` |
| `catalog_proposals` | 0 — no `catalog.server` is configured here |
| `columns` | every column's label, terms and predicate |

## What the fixture is for

* **Four labelled columns, one unlabelled.** `loaded_at` is deliberately
  missing from `columns:`, and it comes out labelled
  `example_ladder: rung_restricted`, `example_facet: [facet_x, facet_y]` with
  `defaulted: true` — the most restrictive value on the ladder, and *every*
  value on the unordered dimension, because a label's values there are
  conjunctive. That is deny by default: an incomplete list under-serves rather
  than over-serves, and the run says so as a warning and a number.
* **One cell predicate.** `exposure` carries `state = 'open'`. It is published
  as part of the label and never interpreted — this library owns no dialect.
* **One unratified value.** `facet_y` carries no `ratified_by` in the stand-in
  document. It is reported, not refused; set `require_ratified: true` to refuse
  instead.
* **An as-of that is not a clock.** `as_of_statement` projects the relation's
  own watermark, recorded as written. Drop the statement and the as-of is the
  time of the look, which says when the source was observed and nothing about
  the data.

## The dataset URN is the asset of record

`source.dataset_urn` names the **physical** platform. A `dagster` platform URN
is refused by the component, not merely discouraged: the catalog would describe
one object, a crawler would discover a second, and a broker asked to authorize
a read of the physical one would find no labels on it. The Dagster asset
attaches to the physical dataset as lineage instead
(`catalog.emit_lineage_from_asset`), in the same fabric as the source — read
off your own URN, since the fabric is your deployment's word for its
environment.

## The observation reads through the tagged warehouse resource

`components/warehouse/` is the step-2 component **unchanged**. It is here
because the observation is a read: a schema observation that arrived untagged
would be counted as unexplained by step 1's audit. The seam is
`fetch_columns(statement)` alongside `fetch_rows(statement)` — a separate
method because the schema statement matches no rows, and the column names come
off the result's own metadata rather than off a row there is none of.

## Pointing it at a real catalog

```yaml
catalog:
  server: "{{ env('DATAHUB_GMS_URL') }}"
  token: "{{ env('DATAHUB_TOKEN') }}"
```

A token is required whenever the metadata service runs with
`METADATA_SERVICE_AUTH_ENABLED=true`, which is the correct posture. Published:
the dataset's properties (including `classification`, `classification_origin`,
`access`, `owner` and the observation), a glossary term per column per label
value on `editableSchemaMetadata`, and the lineage edge. **Not** a
`schemaMetadata` aspect: this component reads column *names*, not types, and
claiming a type for each would write a guess into the catalog as fact.

A catalog failure **fails** the materialization, deliberately. The published
label set is the deliverable: if it does not land, a broker finds no labels on
the dataset and denies every read of it, and reporting success would turn an
outage into a silent embargo.

To see what would be published without publishing it, set `dry_run: true`. The
metadata is identical; `catalog_proposals` stays 0.

## What a derived column publishes

`marts/party_exposure` aggregates the source by state. It is **not** a
dag-tools asset — `dagster_home/derived.py` is ordinary Python, and what builds
a derived table is the deployment's own business: dbt, dlt, a Python asset.
All the propagation needs from it is the `dagster/column_lineage` Dagster
already carries.

Its three columns are the three outcomes:

| column | basis | publishes | why |
|---|---|---|---|
| `state` | `column_lineage` | `rung_open` | reads the source's `state`, which is labelled openly |
| `exposure_total` | `column_lineage` | `rung_internal`, facet cleared | reads `exposure` (`rung_restricted` + `facet_x`), then a **declared downgrade** applies |
| `party_count` | `table_max` | `rung_restricted`, `[facet_x, facet_y]` | declares no lineage, so it publishes the whole table's ceiling |

`party_count` is the fallback working. With no lineage entry, a column
publishes the composition of *every* labelled column of every source it read —
which here includes the unlabelled `loaded_at`, published deny-by-default. It
is correct and it is coarse, and declaring one more lineage entry is what buys
the difference.

**Composition only ever tightens.** The hierarchical dimension takes the
highest rung of its inputs; a restrictive or exclusionary set unions; a
*permissive* set **intersects**, because a wider permissive set admits more
readers. Nothing is widened except by a rule.

### The downgrade is declared, with a name on it

```yaml
downgrades:
  - id: state_total_is_not_a_party
    owner: example-board
    reason: >-
      A state-level sum does not identify a party, so the aggregate is not
      the party-level exposure it was computed from.
    columns: [exposure_total]
    label:
      example_ladder: rung_internal
      example_facet: []
```

`id`, `owner` and `reason` are all required, because this rule is the only
thing in the system that can publish a column wider than it derives. The id
travels on the published decision, so *which rule widened this column, and who
stood behind it* has an answer that survives the YAML being reorganized. Two
rules reaching one column are refused rather than ordered — which one the
record credits decides who is accountable.

The empty `example_facet: []` **clears** that dimension: an empty set is a
value, not an omission. A dimension the rule says nothing about keeps whatever
the data derived, which is what `example_facet` would do if that line were
deleted. A rule that would make any dimension *more* restrictive is refused:
that is a re-label, not a downgrade.

### The sources are declared, not inferred

```yaml
sources:
  - party_extract
```

Which upstreams carry labels is a policy fact, not a graph fact — not every dep
publishes a label set. Inferred, an upstream that publishes none would look
like nothing to propagate from, the ceiling would be composed from a subset of
the inputs, and the answer would look exactly as correct as the real one. So a
lineage dep naming a source that is not listed here is **refused**, and so is
a listed source that has published no label set.

A derived dataset cannot yet be another's source: this component records its
decisions as observations against the derived asset, and reads its sources'
published label sets. Chaining propagation is not supported.

## Nothing here is dag-tools' vocabulary

Every relation, column, dimension and value in this example belongs to this
example. The ids in `classification/dimensions.yaml` are shaped like
placeholders — `rung_*`, `facet_*` — because policy owns that document and
dag-tools names nothing in it: the component discovers which dimension is the
ladder from the document's own `hierarchical: true`.
