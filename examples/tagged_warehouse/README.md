# Tagged warehouse reads

A runnable example of `TaggedWarehouseComponent`: a warehouse resource that
says, on every query it sends, which asset the read was for — and refuses to
send one it cannot say that about.

It runs with no warehouse, no credentials and no extra dependency. **sqlite
stands in for the warehouse**, and `seed.py` brings the one thing sqlite does
not have: a table shaped like a query-history view.

## Run it

```bash
cd examples/tagged_warehouse
python seed.py                     # prints the db path and the export line
export TAGGED_WAREHOUSE_DB="$PWD/warehouse.db"
cd dagster_home && dagster dev
```

Materialize `read_audit`. The materialization's metadata is the report:

| | |
|---|---|
| `objects_read` | 4 |
| `explained` | 2 |
| `no_asset_explains` | 2 |
| `gap_untagged` | 1 |
| `gap_not_a_pipeline` | 1 |
| `coverage` | 0.5 |

## What the three fixture rows are for

* **The explained read** was issued through this resource. Its tag names the
  asset outright, so the report attributes both objects it touched with no
  time correlation and no guessing. The tag in the fixture is not typed by
  hand — `seed.py` reads it off a plan the shipped resource built, so if the
  two halves of the attribution contract ever drift apart, this example stops
  explaining its read, loudly, as a number.
* **The untagged read** is a pipeline not yet on the resource. It is the
  number this step exists to drive to zero, and it is counted as a gap rather
  than quietly correlated away.
* **The read with somebody else's tag** decodes, but names a kind this
  contract does not know. It is denied rather than attributed: query history
  is full of tags written by other tools, and guessing one onto an asset would
  be worse than counting it as unexplained.

## The two components, and why only one of them is new

`components/warehouse/` contributes the resource under the key `warehouse`.
`components/read_audit/` is the step-1 component **unchanged**: the only
difference from the step-1 example is that its `history:` block now names
`resource_key: warehouse` instead of an exported file. The seam between them
is one method, `fetch_rows(statement, parameters)` — the read audit still
builds the statement and executes nothing itself.

An asset that reads the warehouse for its own purposes gets a sharper tag by
naming itself, which is one line:

```python
rows = context.resources.warehouse.for_asset(context).fetch_rows(statement, params)
```

Without it the tag carries the run rather than the asset, and the report can
resolve that to an asset only when the run selected exactly one. A read made
for a specific person is bound per request instead, with
`warehouse.for_user(authz_id)` — the authz_id the gateway verified, never a
header a client sent.

## What this example cannot show

sqlite keeps no query history, so the audit's own read does not appear in the
table it reads. Against a real warehouse it would: the audit reads through the
tagged resource like everything else, so its own reads are attributed too.
With `comment` placement the tag is in the statement text, which is where a
warehouse's history records it.

`comment` is also the placement a connection pool wants, because the next
statement may land on a different session. `session` placement runs the
operator's own statement first, on the same connection, with the tag bound as
`:tag`; that is the form most warehouses document, and the statement is the
operator's because this library does not know anyone's dialect and will not
guess one.

Every object name in this example belongs to its sqlite file. In a deployment
the names are the operator's own, in the operator's own YAML, with no default
anywhere in the library.
