# Documents to tables

A runnable example of `DocumentTablesComponent`. It turns two bundles of XML documents in an
invented "parcel depot" format into five Lance tables plus `_rejects`. It needs no object
store and no credentials. Everything in the format is made up, and it is the only document
format that appears anywhere in dag-tools. A real deployment writes its own mapping and keeps
it private.

The component, the mapping grammar and the concurrency findings are documented in
[`docs/DOCUMENT_TABLES.md`](../../docs/DOCUMENT_TABLES.md).

## Run it

```bash
uv pip install -e ".[documents]"
cd examples/document_tables/dagster_home && dagster dev -w workspace.yaml
```

You can turn on the `document_tables_example_discovery` sensor, which adds one partition per
bundle and requests both runs. Or you can add the partitions `2026-03_depot-a` and
`2026-04_depot-b` by hand and materialize them. The tables land in
`examples/document_tables/output/`, which is gitignored.

## What is in the bundles

| Bundle | Layout |
|---|---|
| `2026-03_depot-a` | Two manifests, one malformed document, a `readme.txt`, and the photos flat in `manifest_m1/` |
| `2026-04_depot-b` | The same documents, with the photos in numbered subfolders `manifest_m1/01/` and `manifest_m1/02/`, and `S2.png` in both |

Each bundle is built so that every behaviour worth seeing happens once:

- **`broken.xml` does not parse.** It becomes one row in `_rejects`, and the rest of the bundle
  is ingested.
- **One parcel weighs `heavy`.** Its `weight` is null, and `coercion_failures` counts it.
- **One photo is named but not shipped.** Its row is kept with `image_resolution = missing`.
- **In `2026-04_depot-b`, `S2.png` is found twice.** The row is kept with
  `image_resolution = ambiguous` and stores no bytes, because picking one would be a guess.
- **`readme.txt` and the photos match no `document.match` glob.** They are members of the
  bundle, not documents.

## What a run produces

Per bundle:

| Table | Rows |
|---|---|
| `loads` | 2 |
| `parcels` | 3 |
| `scans` | 4 |
| `flags` | 3 |
| `photos` | 3 |
| `_rejects` | 1 |

Materializing a partition again leaves every count where it was. The writer deletes the
bundle's rows before appending them.

The checks pass under this example's thresholds, and only because the example sets them:

| Check | `2026-03_depot-a` | `2026-04_depot-b` | Threshold here | Default |
|---|---|---|---|---|
| `reject_rate` | 1/3 | 1/3 | 0.34 | 0.0 |
| `file_resolution_rate` (photos) | 2/3 | 1/3 | 0.3 | 1.0 |
| `unknown_paths` | 0 | 0 | 0 | 0 |
| `non_empty` | every table | every table | | |

Under the defaults, `reject_rate` and `file_resolution_rate` would WARN, which is what they are
for. Every check WARNs rather than blocking.

`document_tables_example_maintenance` compacts every table and removes versions older than
seven days.

## Files

- `dagster_home/components/document_tables/component.yaml`: the component, with the S3 form
  of `source` and `destination` in comments.
- `dagster_home/components/document_tables/mapping.yaml`: the mapping. A test holds it equal
  to the `MAPPING` in `dag_tools_tests/documents_synthetic.py`, and holds every bundle file
  byte-equal to what that module generates, so the example and the test suite describe one
  format. `.gitattributes` keeps a checkout from rewriting the bundles' line endings.
