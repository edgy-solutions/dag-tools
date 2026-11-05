# Which assets read which objects

The first step of classification work, and the only one that is read-only: a
report of what the pipelines' identity actually reads, which asset explains
each read, and **which reads nothing explains**. The gap is the point. It is
what sizes the classification registry, and it is the number the query tag
exists to drive down.

```
query history  ──►  read_audit asset  ──►  report
  (or an export)        │                    tables per asset
                        │                    assets per table
  run windows  ────────►│                    tables no asset explains
  (instance or file)
```

Nothing is granted, nothing is written to the warehouse, and no object name
from your estate lives in this repository. The names arrive as data, through
YAML, and leave as data, in the report.

## Run it

```bash
cd examples/read_audit
DAGSTER_HOME=$PWD/dagster_home uv run dagster dev -w dagster_home/workspace.yaml
```

It runs with no warehouse and no credentials. The shipped
[`component.yaml`](dagster_home/components/read_audit/component.yaml) reads a
synthetic export, so you can materialize `read_audit` immediately and see the
report shape before deciding what to point it at. Every identifier in
`history_rows.json` and `run_windows.json` is invented.

Materializing it yields 9 objects read, 4 explained by an asset, 5 in the
gap — and the metadata breaks that 5 down three ways, because the three have
different owners (below).

## What the YAML says

| Block | What it is |
|---|---|
| `history.rows_file` | an export you already have — the first pass, usually |
| `history.resource_key` | a resource that runs the read instead, with `relation` + all five `columns` |
| `history.principal_filter` | narrow to the one identity being audited |
| `history.lookback_hours` | how far back; overridable per run via run config |
| `runs.runs_file` / `runs.from_instance` | run windows, used only for reads that carry no tag |

**The resource topology names nothing by default.** `relation` and all five
column names are required, with no fallbacks. Every warehouse spells its
query history differently, your account may expose it through a view of its
own, and a default would be wrong in the one direction this tool must not be
wrong in: it would audit the wrong column and report an empty window as a
clean audit. The commented block in `component.yaml` shows them arriving as
`{{ env.* }}`.

**The component executes nothing itself.** It builds the SELECT and hands it
to the resource as SQL plus bound parameters — one method, `fetch_rows`. That
is the same seam the tagged warehouse resource implements, so step 2 drops in
underneath without touching this component, and it is why no driver and no
credential handling enters this repository or its tests.

## How a read gets attributed

Six outcomes, in order of how much they are worth:

| Method | What happened |
|---|---|
| `asset_key` | the query carried a tag naming the asset — the only one that needs no inference |
| `run_id` | the tag named a run, and that run selected exactly one asset |
| `correlation` | no tag, but exactly one single-asset run window covers the read |
| `ambiguous` | something covers it, but more than one thing could |
| `unattributed` | no tag, and no window explains it |
| `not_a_pipeline` | the tag names a person (the gateway's verified `on_behalf_of`) |

The last three are not failures of the report; they are the report. An object
in the gap stays in `unexplained_relations` **and** is broken out into
`ambiguous_relations` / `untagged_relations` /
`non_pipeline_relations`, because the three go to different people: an
untagged read is closed by deploying the tagged resource, an ambiguous one by
narrowing a run's selection, and a non-pipeline one is not an attribution gap
at all — what a person may read is a brokering decision.

A read is never attributed to a plausible neighbour. A run that materialized
two assets explains neither.

## Two numbers that are easy to fake

**`coverage` is absent, not 1.0, when nothing was read.** A window the audit
saw nothing in must not report full coverage — that is the shape of every
vacuous pass worth going back and fixing. The field is `None` and the
materialization metadata omits it entirely.

**Rows that could not be read are counted.** A malformed history row is
reported in `row_failures` rather than dropped, because silently dropping
rows under-reports the gap.

## Where to point it next

Run it against an export first; the account that can read query history is
often not the account Dagster holds, and measurement should not wait on a
grant. Once the numbers are worth watching, move to the resource topology and
schedule the asset. Once the tagged warehouse resource is deployed,
`untagged_relations` should trend to empty — that transition is the whole
argument for the tag.

## Compat floor

Verified on `dagster==1.10.19` as well as current: the example loads and
materializes identically on both. The component file stays inside `type:` +
`attributes:` and every attribute is a plain dict or scalar, which is what
keeps it loadable there — 1.10.19 rejects an unknown top-level key in a
component file outright and fails the entire code-location load, not just
that block. `dag_tools_tests/test_read_audit_component.py` pins both halves
and reports it rather than hiding it if some future field is refused anyway.
