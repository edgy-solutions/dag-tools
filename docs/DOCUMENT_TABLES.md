# Documents to tables (`DocumentTablesComponent`)

Bundles of structured documents go in. One Lance table per mapped entity comes out.

A **bundle** is a zip or a directory tree, local or on S3, holding documents (XML or JSON) and
the files they refer to, such as images. A **mapping** is a YAML file the deployment writes. It
says which documents to read, which rows each table takes from them, and how each column is
computed and typed. The component reads the mapping, adds one Dagster partition per bundle and
writes each bundle's rows to Lance.

**Nothing in dag-tools knows any document format.** The engine knows the mapping's grammar and
nothing else. Every element, attribute, table and column name lives in the deployment's
mapping, which the deployment keeps to itself. The one format that appears in this repository
(code, tests, fixtures and this page) is the synthetic "parcel depot" format of
`examples/document_tables`, and everything in it is invented.

- [Quick start](#quick-start)
- [The component](#the-component)
- [The mapping](#the-mapping) (and [`vars` are per document](#the-mapping),
  [JSON documents](#json-documents), [rows chains](#rows-chains-reaching-another-level),
  [scoped indexes](#scoped-indexes), [member tables](#member-tables),
  [several tables, one dataset](#several-tables-one-dataset-into),
  [parser limits](#parser-limits))
- [What a run produces](#what-a-run-produces)
- [Profiling bundles before writing a mapping](#profiling-bundles-before-writing-a-mapping)
- [Concurrency and Lance: what was measured](#concurrency-and-lance-what-was-measured)
- [What is not verified](#what-is-not-verified)

## Quick start

```bash
uv pip install -e ".[documents]"      # lxml, pyarrow, pylance, Pillow, boto3, tzdata on Windows
cd examples/document_tables/dagster_home
dagster dev -w workspace.yaml
```

The example holds two bundles under `examples/document_tables/bundles/`. Turn on the
`document_tables_example_discovery` sensor, or add the partitions by hand and materialize
them. The tables land in `examples/document_tables/output/<table>.lance`, which is gitignored.

## The component

```yaml
# components/document_tables/component.yaml
type: dag_tools.DocumentTablesComponent

attributes:
  name: my_documents                 # names the partitions, jobs, sensor and asset prefix
  source:
    uri: "{{ env('DOCUMENT_BUNDLES_URI') }}"   # a local directory or s3://bucket/prefix
    endpoint_url: "{{ env('S3_ENDPOINT_URL') }}"  # optional; MinIO and friends
    bundle_depth: 1                  # 0: the root is one bundle; 1: each top-level dir is one
  mapping: mapping.yaml              # a path relative to this directory, or the mapping inline
  destination:
    uri: "{{ env('DOCUMENT_TABLES_URI') }}"
    storage_options:                 # handed to Lance as-is; secrets as env references
      aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
      aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
    flush_rows: 50000                # rows buffered per dataset before a write
  asset_key_prefix: [documents]      # default: [<name>]
  checks:
    max_reject_rate: 0.0
    min_file_resolution_rate: 1.0
    unknown_paths: 0
    allow_empty: []                  # datasets allowed to come out with no rows
  discovery:
    interval_seconds: 300
    max_new_partitions_per_tick: 25
    default_status: STOPPED
  maintenance:
    older_than_days: 7
    retain_versions: null
    cron: null                       # a schedule for the maintenance job, also STOPPED
  write_concurrency: 1
  redact: false
  group_name: my_documents
```

Only `name`, `source`, `mapping` and `destination.uri` are required. Every block is validated
when the code location loads: a misspelled key, a mapping that does not hold together, or a
source URI with an unknown scheme fails the load and names every problem at once. Nothing is
read from a bundle or the destination until a run.

Credentials never go in the file. S3 sources take them from the standard AWS environment. A
Lance destination takes them through `storage_options`, written as `{{ env('X') }}` references.
Use the call form: the attribute form `{{ env.X }}` does not resolve on Dagster 1.10.19.

### What it defines

| Definition | Name | Notes |
|---|---|---|
| Multi-asset | `<name>_tables` | One asset per dataset (a mapping table, or tables sharing an `into`), plus `_rejects`. Dynamically partitioned, one partition per bundle. |
| Asset job | `<name>_ingest` | Selects exactly those assets. |
| Sensor | `<name>_discovery` | Adds a partition per new bundle and requests its run. STOPPED by default. |
| Job | `<name>_maintenance` | Compacts every table, maintains its `source_bundle` index and removes old versions. |
| Schedule | `<name>_maintenance_schedule` | Only when `maintenance.cron` is set. STOPPED by default. |
| Asset checks | `reject_rate`, `unknown_paths` on `_rejects`; `file_resolution_rate` on each table with a file column; `non_empty` on each table | All WARN. They never block a run. |

### Partitions and bundles

A bundle's partition key is its key with `/` spelled `__`. The discovery sensor lists the
source, adds the bundles it has not seen, oldest first, at most `max_new_partitions_per_tick`
per tick, and requests one run each. The run carries the bundle's own key in the
`dagtools/document_bundle` tag. Two bundles whose keys encode to the same partition key fail
the tick and are both named, rather than one silently standing in for the other. A key Dagster
cannot hold as a partition key is skipped with a warning.

A bundle that already has a partition is not requested again. To re-ingest a bundle whose
contents changed, re-materialize its partition. That is safe, because a run is idempotent (see
below).

## The mapping

Real mappings are the deployment's, so this page explains the grammar using the synthetic
example. The full example is `examples/document_tables/dagster_home/components/document_tables/mapping.yaml`.

```yaml
version: 1

document:
  format: xml
  match: ["*.xml", "*/*.xml"]   # path globs on the member path; see "Which members are documents"
  max_bytes: 1000000            # larger documents are rejected, not parsed
  huge_tree: false              # lift the XML parser's built-in limits; see "Parser limits"
  strip_namespaces: false
  nulls: []                     # strings meaning absent; see "Column options"

lineage:
  publish_period:               # every row carries publish_period
    from: bundle_name           # or bundle_key
    regex: '(?P<period>\d{4}-\d{2})'

expected_paths: [/Manifest, /Manifest/@id, ...]
on_unknown_path: warn           # ignore | warn | fail

indexes:
  reviews:
    rows: /Manifest/Review
    keys: "@flagRef"            # the entry is filed under every key in this node-set
    values:
      decision: Decision/@code
  photo_refs:
    rows: /Manifest/Extras/Photos/photo
    scope: "@parcel"            # a scan id is unique only within its parcel
    keys: ref/@scan
    values:
      path: "@path"

tables:
  parcels:
    rows: /Manifest/Parcel      # one row per node
    vars:
      manifest: string(/Manifest/@id)   # ONCE PER DOCUMENT, from the root -- not per row
    row_id: "{manifest_id}/{parcel_id}"
    columns:
      manifest_id: $manifest
      parcel_id: "@id"          # a bare string is an expression, typed string
      weight:
        type: float64
        expr: "@weight"
```

Expressions are XPath 1.0 (through lxml) for `format: xml` and JMESPath for `format: json`
(see [JSON documents](#json-documents)), evaluated relative to the row node. Common shapes
(`@attr`, `child/@attr`, `child`; a dotted chain of JSON keys) take a fast path that skips
XPath or JMESPath; a test holds each fast path equal to the full engine.

> **`vars` are evaluated once per document, not once per row.**
>
> A table's `vars` are computed when its document is opened, before any row is read, against
> the **document root**, in the order they are declared (a var may use an earlier one). Every
> row of that table in that document then sees the same value. Two consequences:
>
> - **A relative var reads the root, not the row.** `vars: {p: "@id"}` is the root element's
>   `id`, on every row. It is not the row's `id`, and it does not change from row to row.
> - **A per-row value is a column, not a var.** Write it as the column's own expression
>   (`parcel_id: "@id"`), or as a `lookup` whose `keys` or `scope` are relative to the row.
>
> A var that yields nothing becomes the empty string. Use vars for values that really are
> per document, such as a header field that every row should carry.

### JSON documents

```yaml
document:
  format: json                  # expressions are JMESPath; needs the documents extra (jmespath)
  match: ["**/*.json"]
  max_bytes: 1000000
expected_paths: [$, $.Depot, $.Depot."Dock door", $.parcels, $.parcels[], $.parcels[].id]
tables:
  parcels:
    rows: parcels[]
    columns:
      parcel_id: id
      door: {type: int32, from: root, expr: 'Depot."Dock door"'}
```

- **Every XML guarantee holds.** Expressions are compiled once, a dotted chain of keys takes a
  fast path, coercion never raises, a document is committed whole or rejected, and the
  structure is checked against `expected_paths`.
- **Keys that are not identifiers** are quoted the JMESPath way: `Depot."Dock door"`,
  `"Count(late)"`. That works in expressions, in the fast path, in structural paths and in
  `expected_paths`.
- **Structural paths** are written `$`, `$.key`, `$.key.sub`, `$.list[]`, `$.list[].leaf`, and a
  list inside a list is `$.grid[][]`. The profiler writes `paths/<family>.txt` in this form, so
  its output pastes straight into `expected_paths`.
- **Native values.** A JSON `3` or `3.0` fills an `int32`; `3.5` does not and is a counted
  coercion failure. `true` fills a `bool`. A string goes through the same text parsing as an
  XML attribute (`"4"` fills an `int32`). An object or a list where one value was expected is a
  counted coercion failure.
- **`a[][]` flattens** one level per `[]`, as JMESPath does.
- **Nothing to read is not a reject.** A zero-byte file, `{}`, `[]` and a bare scalar such as
  `3` give no rows and no `_rejects` row. Only text that is not JSON is rejected, and the
  message names a line and column, never the text. `NaN` and `Infinity` are not JSON and are
  rejected; a number too large to be finite (`1e999`) is kept as its text and refused by a
  `float64` column exactly as the XML route refuses it.
- **A UTF-8 BOM is accepted.**
- **No `vars`.** JMESPath has no variables. A JSON mapping reaches another level with `from:`
  (below). `huge_tree` and `strip_namespaces` apply to XML only and fail the load under
  `format: json`.

### Rows chains: reaching another level

A plain-string `rows` keeps its meaning. A list of levels names each step instead:

```yaml
tables:
  scans:
    rows:
      - {as: parcel, each: "parcels[]"}   # evaluated from the root
      - {as: scan, each: "scans[]"}       # evaluated from every parcel
    columns:
      parcel_id: {from: parcel, expr: id} # read at the parcel this scan belongs to
      scan_id: id                         # the row node: the last level
      manifest_id: {from: root, expr: id}
      scan_pos: {index_of: scan}          # 0-based within this parcel's scans[]
```

- The first level's `each` is evaluated from the root, each later one from every node of the
  level before. The row node is the last level.
- `from: <level>` evaluates the column at that level's node. `root` is always available, also
  on a plain-string `rows`. It applies to `expr`, `first_of`, a lookup's `keys` and `scope`, a
  `list<struct>`'s `rows` and a file column's `expr`. It is how a JSON mapping does what an XML
  mapping does with `../@id`, and it works for XML too.
- `index_of: <level>` is the 0-based position of that level's node within its own `each`
  result, so it restarts under every parent. It is an `int64`.
- An index takes the chain form too, and its `from:` applies to its `keys` and `scope`.
- These fail the load: a level that does not exist; a `from:` other than `root` on a plain-string
  `rows`; two levels with the same `as:`; `index_of` on a plain-string `rows`; `from:` or
  `index_of` on a `list<struct>` field (they apply to the column).

### Which members are documents

`document.match` is a list of globs over each member's path inside the bundle (`/`-separated,
relative to the bundle root). A member matching none of them is not a document; it can still be
the target of a file column.

- `*` and `?` match within one path segment and never cross `/`. `[abc]` and `[!abc]` match one
  character and never match `/`.
- `**`, written as a whole segment, matches zero or more whole segments: `**/*.xml` matches
  `a.xml`, `d/a.xml` and `d/e/a.xml`.
- Matching is case-sensitive and anchored to the whole path.
- A glob that is empty, starts with `/`, contains a backslash, uses `**` inside a segment
  (`a**`), or leaves a `[` unclosed fails the load.

Before this release `*` crossed `/` (Python's `fnmatch`), so `*.xml` also matched
`d/e/a.xml`. A mapping that relied on that now needs `**/*.xml`. The example's
`["*.xml", "*/*.xml"]` is unchanged and selects the same documents from its bundles, whose
documents all sit within one directory of the bundle root.

### Parser limits

libxml2, which lxml uses, refuses some documents by default even when they are well formed:

| Input | Default parser | `huge_tree: true` |
|---|---|---|
| Nesting deeper than 256 elements | refused | parsed |
| An attribute value over 10 MB | refused | parsed |
| A name over 50,000 characters | refused | parsed |
| A CDATA section over 10 MB | refused | parsed |
| A 120 MB text node | parsed | parsed |
| A 31 MB depot manifest, 765,031 elements | parsed, 0.72 s | parsed, 0.95 s |

Measured with lxml 6.1.3 / libxml2 2.11.9. The bulk depot manifest the tests generate
(`large_manifest(30_000_000)`: 30,000,201 bytes, 587,159 elements) parses under the default
parser. **Size alone does not trip a limit**; depth and very long single values do.

A document refused for a limit is rejected with a message that says so and names
`document.huge_tree: true`, so it is not mistaken for malformed XML. The engine tells the two
apart by re-parsing a refused document with the limits lifted (and with entities, DTDs and the
network still off) and discarding that tree; documents that parse cost nothing extra.

`huge_tree: true` lifts the limits for every document the mapping reads. It does not lift
`max_bytes`, which is enforced before parsing either way. The profiler always parses with the
limits lifted, so it reports on documents the engine would refuse.

### Column types

| Type | Read as |
|---|---|
| `string` (default) | The text, as written. An empty string is null unless `empty_as_null: false`. |
| `int32`, `int64` | Exact integer text (`^[+-]?\d+$`), never through a float, so a value past 2**53 is not rounded. `int32` is range-checked. |
| `float64` | Decimal-number text only. `nan`, `inf`, `1_000` and any non-finite result fail. |
| `bool` | `true` / `false` / `1` / `0` in any case. Anything else goes through a `map`. |
| `timestamp` | ISO-8601 with any number of fractional digits (truncated to microseconds) and `Z` or an offset, or a strptime `format`. Stored as UTC microseconds. A value naming no offset is read in the column's `timezone`. With no `timezone` it is a failure, because a naive value names no instant. |
| `list<T>` | Every node in the expression's node-set, each read as `T`. |
| `list<struct>` | `rows:` selects nodes, and `fields:` are columns relative to each node. |
| `file` | The expression is a reference to another member of the bundle. See [File columns](#file-columns). |

**Coercion never raises.** A value that is present but cannot be read as its type is stored as
null and counted per column (`coercion_failures`). Absence is not a failure: a null in is a
null out.

### Column options

- `first_of: [expr, expr, ...]`: the first expression that yields a value wins.
- `when_empty: <value>`: what an absent value becomes.
- `empty_as_null: true`: whether an empty string counts as absent.
- `nulls: ["-", "N/A", ...]`: strings that mean absent. A value equal to an entry **after
  trimming** is null, before `split`, `extract`, `map` and coercion, so it takes `when_empty` or
  `map.null` and is not a coercion failure, an `extract` miss or an unmapped value. The match is
  on the whole value and is case-sensitive: `NIL` does not match `nil` or `NILx`. A JSON number
  or bool is compared by its text (`-1` matches `nulls: ["-1"]`).
  - `document.nulls` is every column's default. A column's own `nulls` **replaces** it, and
    `nulls: []` turns it off for that column. A `list<struct>` column's `nulls` is its fields'
    default.
  - Under `split` the parts are compared one by one: an absent part is dropped, or kept as null
    under `keep_empty`. In `first_of` and `prefer` an absent value is skipped. A `file`
    reference that is a `nulls` entry names no file and is not counted missing.
  - A blank entry fails the load (an empty value is already absent under `empty_as_null`), and
    so does `nulls` on an `index_of` column, which is a position.
- `const: <value>`: the same value on every row. It is read as the column's `type` when the
  mapping loads, so `{type: int32, const: "7"}` stores 7, `list<T>` takes a YAML list, and a
  YAML date is read as its ISO text. A value that is not a value of the type fails the load,
  naming the column and never the value. A `timestamp` const takes `format` and `timezone`, and
  one naming no offset needs `timezone`. `const` reads no source, so it refuses `expr`,
  `first_of`, `lookup`, `from`, `split`, `extract`, `map`, `nulls`, `when_empty` and the rest;
  a `file` column has no constant.
- `map: {values: {...}, default: ..., null: ...}`: translate through a table, comparing keys
  as strings. A value the table does not name takes `default` when one is written (even
  `default: null`). Otherwise it is null, and it is counted (`unmapped_values`) so an
  unfamiliar code is visible.
- `split: "<sep>"`: cut one value on a literal separator. Each part is trimmed. Empty parts
  are dropped unless `keep_empty: true`, and an empty or null value gives `[]`. The column is
  a `list<T>`, and each part goes through `extract`, `map` and coercion on its own, so a part
  that cannot be read is counted like any other value. With `prefer`, the parts are the list
  `prefer` chooses from, and the column is the element type. A kept empty part is null when
  `empty_as_null` is on (the default).
- `extract: "<regex>"`: keep part of the value. The result is the named group `value` if the
  pattern has one, else group 1, else the whole match. It runs before `map` and coercion, so
  `extract: '^ZX-(?P<value>[0-9]+)$'` with `type: int32` stores the number. A value the pattern
  does not match is null and counted in `unmapped_values`, like an unmapped `map` value:
  `when_empty` and `map.null` do not stand in for it, because the source did have a value. A
  pattern that does not compile fails the load.
  **The pattern is trusted deployer configuration.** It runs through Python's `re` on every
  value of the column, and nothing guards against a pattern that backtracks badly: a pattern
  such as `^(a+)+$` can take seconds or longer on one long value, and the run waits for it.
  Write anchored patterns without nested repetition, and test them on the longest values the
  profile shows.
- `from: <level>`, `index_of: <level>`: see [Rows chains](#rows-chains-reaching-another-level).
- `format`, `timezone`: timestamp columns only. `format` is a strptime pattern, for values
  that are not ISO-8601. `timezone` is an IANA name read through `zoneinfo`, and it is required
  for any value that names no offset: without it such a value is a counted coercion failure,
  because a naive time names no instant. A value that does name an offset keeps its own.

  ```yaml
  scanned_at:
    type: timestamp
    expr: "@scanned"            # e.g. 01/05/2026 08:30 -- a day-first local time, no offset
    format: "%d/%m/%Y %H:%M"
    timezone: Asia/Tokyo        # stored as 2026-04-30T23:30:00Z
  ```
- `lookup: {index, keys, scope, field, mode, order, distinct, sort}`: look values up in an
  index built from the same document. `mode` is `exists` (bool), `count` (int64), `first` /
  `last` (the field's type) or `list` (a list of it). An index may look up an index declared
  above it, never one below.
  - `order: document` (the default) returns matched entries in document order. `order: keys`
    returns them in the order of the looked-up keys, each key's entries in document order; an
    entry matched by two keys keeps its first place. It is refused for `exists` and `count`.
  - `scope`: see [Scoped indexes](#scoped-indexes).
- `prefer: [value, ...]`: pick one element of a list by precedence. The first `prefer` entry
  present in the list wins, compared as strings. The list comes from a lookup in `mode: list`
  or from an expression that yields a node-set; declare the element type (`string`, not
  `list<string>`).
  - `map` is applied to each element first, and an element the map does not cover is not a
    candidate.
  - An empty list gives `when_empty` (not mapped) or null.
  - A non-empty list holding no preferred value gives null and is counted in `unmapped_values`.
  - It cannot be combined with `first_of`: `first_of` chooses among expressions, `prefer` among
    the values of one list.

### Scoped indexes

An index key is often unique only inside something larger: a scan id that restarts at `S1` in
every parcel, say. Unscoped, a lookup by that key matches entries from every parcel in the
document, and the row reads another parcel's data without any error.

An index declares `scope`, an expression evaluated relative to each **index row**. Entries are
filed under (scope, key). A lookup into a scoped index must then give its own `scope`,
evaluated relative to the **table row**, and matches only entries whose scope string is equal:

```yaml
indexes:
  photo_refs:
    rows: /Manifest/Extras/Photos/photo
    scope: "@parcel"          # the photo's parcel
    keys: ref/@scan
    values: {path: "@path"}

tables:
  scans:
    rows: /Manifest/Parcel/Scan
    columns:
      has_photo:
        lookup:
          index: photo_refs
          scope: ../@id       # the scan's parcel
          keys: "@id"
          mode: exists
```

A lookup into a scoped index without `scope` fails the load, and so does a lookup giving
`scope` to an unscoped index. A scope that comes out null or empty matches nothing. It is
counted: per table and column in `null_scopes`, and for index rows in `index_null_scopes` on
`_rejects`.

### Member tables

A table can describe the bundle's members instead of reading documents:

```yaml
tables:
  overviews:
    members: ["overview_*.png", "*/overview_*.png"]   # same glob rules as document.match
    columns:
      name: {member: name}
      folder: {member: dir}
      bytes: {type: int64, member: size}
      side: {member: stem, extract: '^overview_(?P<value>[a-z]+)'}
      image: {type: file, member: self, probe: image}
```

- One row per matching member, in path order, once per bundle. `doc_id` is derived from the
  bundle key and the member path, and `source_file` is the member path.
- `member:` is one of `path`, `name` (the file name), `stem` (the name without its extension),
  `ext` (lower-case with the dot, or empty when there is none), `dir` (the folder, empty at the
  bundle root), `size` (bytes) and `modified`. `member: self` on a `file` column stores the
  member itself; `probe: image` works as on any file column.
- `extract`, `map`, `split` and coercion apply as on any column.
- **A root member's `dir` is null**, because an empty string is null under `empty_as_null`.
- **A zip member's `modified` names no time zone.** Typed `timestamp` it is a counted coercion
  failure unless the column declares `timezone`.
- A member matched by `document.match` may also match a member table, and it is then both read
  as a document and described as a member.
- Member tables are written, counted, checked and made idempotent exactly like other tables.
  A member whose row fails writes a `_rejects` row naming the member.
- `member:` on a table built from `rows`, and `vars` on a member table, fail the load.

#### Members no document refers to (`only_unreferenced`)

```yaml
  loose_photos:
    members: ["**/*.png"]
    only_unreferenced: true     # keep only members no file column resolved to
    columns:
      path: {member: path}
```

- A member is **referenced** when a `file` column of any table resolved to it, in any document
  of the bundle. The decision is made after every document has been read, so a reference from
  a document that sorts after the member still counts.
- **A rejected document references nothing.** Its rows are dropped from every table, so the
  members it resolved to stay unreferenced.
- A reference that did not resolve to exactly one member (missing or ambiguous) references
  nothing either: an ambiguous pair of candidates is kept, both of them.
- The run metadata counts `members_kept` and `members_skipped`. On a dataset fed by several
  tables both are json, keyed by table.
- `only_unreferenced` on a table built from `rows` fails the load.

### Several tables, one dataset (`into`)

Two tables that describe the same kind of row from different places in a document can write
one Lance dataset:

```yaml
tables:
  flag_events:
    rows: /Manifest/Parcel/Flag
    into: events
    columns:
      event_id: "@id"
      kind: {const: flag}
  scan_events:
    rows: /Manifest/Parcel/Scan
    into: events
    columns:
      kind: {const: scan}       # column order is not part of the schema
      event_id: "@id"
```

- **One asset per dataset.** `events` is the asset; `flag_events` and `scan_events` are not.
  The asset's description names the tables that feed it.
- **The columns must be identical.** Every table sharing an `into` declares the same column
  names with the same types, and either every one of them declares a `row_id` or none does.
  Otherwise the load fails at `tables.<name>.into`, naming the columns only one side has and
  each column whose type differs.
- **`into` may name a table**, and then joins that table's own dataset. Naming a table that
  itself writes elsewhere, `_rejects`, or a name that is not an identifier fails the load.
- **Writes, counts and checks are per dataset.** The bundle's rows of every feeding table are
  deleted together and appended together. `coercion_failures` and the other per-column counts
  are kept under the dataset. The checks attach to the dataset, and `checks.allow_empty`
  names the dataset, never a table that feeds it.
- **`rows_by_table`** in the dataset's metadata counts the rows each feeding table wrote. A
  dataset fed by one table does not carry it.
- **A `row_id` must be unique across every table sharing the dataset.** Nothing checks this:
  two tables whose templates can produce the same id write rows a reader cannot tell apart. Put
  something that differs per table into the template, such as a `const` column naming the
  table (`row_id: "{kind}/{event_id}"`).
- A member table and a document table may share an `into`.

### System columns

Every table carries `doc_id`, `source_bundle`, `source_file`, `publish_period` and
`ingested_at`, plus `row_id` when the table declares a template. A mapping may not declare
these names.

- `doc_id` is a digest of the bundle key and the member path, so it is stable across runs and
  machines.
- A `row_id` template may name any of the table's columns, `doc_id` and `row_number`.

### File columns

```yaml
image:
  type: file
  expr: "@path"
  resolve:                      # tried in order; the first that finds exactly one member wins
    - document_dir              # relative to the referring document's directory
    - bundle_root               # relative to the bundle root
    - basename_under: "{document_stem}"   # by file name, anywhere under that directory
    - basename_in: document_dir           # by file name, in exactly that one folder
  probe: image                  # adds _format, _width, _height (needs Pillow)
  store: blob                   # or reference: keep the member path, no bytes
```

A file column emits itself (the bytes, as `large_binary` with `lance-encoding:blob=true`, or
the member path under `store: reference`) and also `_ref`, `_member`, `_resolution`, `_bytes`
and `_sha256`.

`basename_in: document_dir | bundle_root` takes the reference's file name and looks for it
in exactly one folder: the referring document's directory, or the bundle root. It never
recurses, so it cannot be ambiguous. It is for references carrying a folder prefix the bundle
does not have (`outbound/p1.png` beside a document that sits next to `p1.png`).
`basename_under` is unchanged.

A reference that is not text (a JSON number or object where a path was expected) is counted as
a coercion failure on `<column>_ref` and resolves to nothing.

`_resolution` holds the strategy that found the member (`document_dir`, `bundle_root`,
`basename_under`, `basename_in`, or `self` in a member table), `missing` or `ambiguous`. A name found
more than once is ambiguous: the row is kept, and no bytes are stored, because picking one
would be a guess. The `file_resolution_rate` check is resolved / (resolved + missing +
ambiguous).

Read blob columns with Lance's `take_blobs`.

### Validation

A mapping is checked twice when the code location loads:

- **Shape.** Every block forbids unknown keys, so a misspelling names its full path.
- **Meaning.** The second pass checks for:
  - an undefined `$var`;
  - a lookup into an index that is not declared earlier;
  - a lookup `mode` that disagrees with the declared type;
  - a reserved column name;
  - `on_unknown_path` with no `expected_paths`;
  - a file option on a non-file column;
  - an unknown time zone;
  - a lookup into a scoped index with no `scope`, or a `scope` on a lookup into an unscoped
    one;
  - an index `scope` that references a `$var` (vars belong to a table, and an index has none);
  - `order` on an `exists` or `count` lookup;
  - `prefer` that is empty, lists a non-scalar value, is combined with `first_of`, sits on a
    `list<...>` type, or reads a source that returns one value (a single-value lookup mode, or
    an expression that is a scalar on its face, such as `string(...)` or `count(...)`);
  - a `document.match` or `members` glob that is empty, starts with `/`, contains a
    backslash, has `**` inside a segment, or leaves a `[` unclosed;
  - in a rows chain: an unknown level in `from:` or `index_of`, a non-root `from:` on a
    plain-string `rows`, or a duplicate `as:`;
  - `keep_empty` without `split`, or an `extract` that does not compile;
  - `member:` outside a member table, or a table with both `rows` and `members` (or neither);
  - `vars` in a JSON mapping or a member table, and `huge_tree` / `strip_namespaces` under
    `format: json`.
  - a blank `nulls` entry, or `nulls` on an `index_of` column;
  - a `const` that is not a value of its column's type, a `timestamp` const naming no offset
    with no `timezone`, a `const` combined with anything that reads a source value, or a
    `const` on a `file` column;
  - an `into` that is `_rejects`, is not an identifier, or names a table that writes elsewhere;
    tables sharing an `into` with different columns, types or `row_id`;
  - `only_unreferenced` on a table that is not a member table;
  - `checks.allow_empty` naming something that is not a dataset (a table feeding a shared
    `into` is named by its dataset).

Every problem is reported at once.

## What a run produces

One run reads one bundle.

**Documents.** The run walks the bundle's documents in path order. Each document is parsed with
external entity and DTD resolution off, after reading at most `max_bytes + 1` bytes.

**Rejects.** A document is committed whole or not at all. If it is unparseable, too large or
refused for a [parser limit](#parser-limits), an expression fails, or `on_unknown_path: fail`
trips, that document's rows are dropped from *every* table and it gets one row in `_rejects`.
The rest of the bundle carries on.

**Writes.** Every table is written idempotently. The writer deletes the bundle's existing rows
(`source_bundle = <key>`) and then appends the new ones in chunks of `flush_rows`. Running a
bundle twice leaves the counts where one run left them. The schema comes from the mapping, so
a bundle that yields no rows for a table still creates that table with its full schema.

**The `source_bundle` index.** That delete would scan the whole table on every write, so the
maintenance job keeps a BTREE scalar index on `source_bundle`. It creates the index the first
time and folds newly appended fragments into it after that. Rows appended since the last
maintenance run are not in the index until the next one. Each table's maintenance
result reports `source_bundle_index` as `created`, `updated`, `unsupported` (the installed
pylance has no scalar-index API) or `failed: <error type>`. A failure is logged as a warning
and never fails maintenance, because the index only makes the delete faster. Measured with
pylance 14.0.3: after maintenance the delete filter is answered by the index
(`ScalarIndexQuery` in the plan).

**Listing the source.** Opening a bundle lists only that bundle's own prefix, never the whole
source. Measured against moto:

| Operation | S3 calls |
|---|---|
| Open one directory-tree bundle | 2 `ListObjectsV2`, whether the source holds 2 bundles or 30 |
| Discover bundles at `bundle_depth: 1` | 1 `ListObjectsV2` with `Delimiter="/"` |
| Locate a zip bundle | 1 `HeadObject`, no list call |

Locally, discovery at `bundle_depth: 2` reads only the directories above the bundles. The whole
source is listed only at `bundle_depth: 0`, where the root is the one bundle.

**Materialization metadata, per dataset:**

- `dagster/row_count`, `dagster/column_schema`, `dagster/uri`
- `documents_parsed`, `documents_rejected`, `rows_deleted`, `duration_seconds`
- `coercion_failures` and `unmapped_values`, per column
- `null_scopes`, per column: lookups whose scope came out null or empty
- `file_resolution`, per column and outcome
- `unknown_paths`, `documents_with_unknown_paths` and a sample of the unknown paths
- on `_rejects` only: `index_null_scopes`, per index, the index rows whose scope came out null
  or empty and were therefore filed under no key
- on a dataset fed by several tables (`into`): `rows_by_table`
- on a member table with `only_unreferenced`: `members_kept` and `members_skipped`

`dagster/column_schema` and `dagster/uri` are what the existing `DatahubLineageComponent`
sensor reads, so lineage needs no extra wiring.

**Logging.** No value read from a document is ever logged at INFO or above. Logs name bundles,
members, counts and column names. `redact: true` additionally replaces the error text in
`_rejects` (other than the engine's own messages) with the error type and a digest, for
sources whose parser errors quote document content.

**Drift.** Every structural path a document carries (`/Root/Child/@attr`) is compared with
`expected_paths`. Paths the mapping does not expect are counted, sampled into the metadata and
checked by `unknown_paths`. Under `on_unknown_path: fail` such a document is rejected.

**Path spelling.** The comparison is of text, so `expected_paths` must spell each path exactly
as the engine does, and the profiler's `paths/<family>.txt` is written by the engine's own
function. For JSON, a key that is a JMESPath identifier is written bare (`$.Depot.door`) and any
other key is written as a JSON string (`$.Depot."Dock door"`, `$."2026"`), which is also how an
expression quotes it. A list written by hand in another spelling (an identifier quoted, or an
awkward key left bare) matches nothing, and every document then reports its paths as unknown.
When every unknown path of a document would match after unquoting, the run says so once per
bundle in a warning, and under `on_unknown_path: fail` the reject message adds that
`expected_paths` appear to use a different key spelling. Neither names a path, because a path
is built from the document's own keys. The fix is to regenerate the list with the profiler.

## Profiling bundles before writing a mapping

```bash
dagtools documents profile --source s3://bucket/prefix --bundle-depth 1 --out ./profile \
  --sample 50 --redact --anon-bundles --scrub '<regex>'
```

The profile is written to `--out`:

- `summary.md`
- CSV and parquet tables of members, paths, documents, references, images and bundles
- `shapes.json`
- `paths/<family>.txt`: one file per document family, listing its structural paths in the
  `expected_paths` format, ready to paste into a mapping

The profiler reports on the data as it actually is, so it is a tool for **inspecting** bundles,
not for running in a pipeline.

- **Resumable.** Every finished bundle is checkpointed under `<out>/checkpoint`, and running
  again resumes. `--fresh` starts over. `--report-only` rebuilds the report without reading a
  bundle.
- **Interruptible.** The first Ctrl-C stops at the next member boundary and writes the report.
- **Bounded.** `--max-bundle-bytes`, `--max-docs` and `--max-images` bound the work.
- **Example values.** The report files carry example values, which is the point of a profile.
  `--redact` replaces them with hashes. `--anon-bundles` anonymises bundle names. `--scrub`
  removes whatever a regex matches from the report text. Use all three before a profile leaves
  the machine it was made on.

## Concurrency and Lance: what was measured

Each table is one Lance dataset. A run deletes its bundle's rows and then appends, in several
commits, because flushes are chunked. These are the cases that matter, measured against Lance
14.0.3 (`pylance`) on a local filesystem:

| Case | Result |
|---|---|
| 6 processes × 15 concurrent appends to one dataset | 900 of 900 rows. Lance retries a conflicting manifest commit. |
| 6 processes, each deleting its own bundle and then appending it, concurrently | Exactly the expected rows per bundle. Lance logs benign WARN lines while it retries. |
| **2 writers on the same bundle**, interleaved (delete A, delete B, append A, append B) | **The bundle lands twice**: every table holds double its expected rows. |

Delete-then-append is not atomic, and a bundle spans several commits. So two runs of the
**same** partition writing at the same time duplicate it. This happens whenever a sensor tick
and a manual re-materialization overlap, or a retry starts before the first attempt dies.

The component therefore serialises writes. Every write (ingest and maintenance) runs in a
Dagster **pool** named from a digest of the destination URI (`dagtools_lance_<digest>`). The
URI itself is not used, because pool names show in the UI. The component sets the pool's limit
to `write_concurrency`.

The default is **1**. Raise it only for a destination whose store has been shown to commit
concurrent writers safely, and only if two runs of one bundle can never overlap. The pool
limit applies to runs that go through the instance's concurrency machinery. A deployment
whose run coordinator ignores pools gets no serialisation from it.

## What is not verified

- **S3 as a Lance destination.** Every measurement above is on a local filesystem. Concurrent
  Lance commits on S3 need either a commit lock (a DynamoDB-style external manifest store) or
  an object store with conditional writes. Until a deployment has measured its own store, keep
  `write_concurrency: 1`.
- **S3 as a bundle source** is tested against moto, not a real bucket.
- **S3 listing costs** are counted against moto, not a real bucket.
- **20,000 rows/s.** The bundled benchmark (`test_throughput_on_a_bulk_manifest`, 120,001
  rows from one 20,000-entry manifest, no Lance write) measures about 13,000-17,000 rows/s on
  a Windows desktop under Python 3.12 and about 14,000-15,000 under 3.10. Scoped indexes,
  `prefer` and the parser-limit check did not move it outside that range. About two thirds of that
  time is spent evaluating values at roughly 4 microseconds each, which is close to the limit
  of pure Python for this design. Rows chains, `split`, `extract` and member tables left the
  XML figure where it was: interleaved against the previous release on one machine, both read
  about 11,000-16,000 rows/s on 3.12 and 3.10. The same manifest as JSON
  (`test_json_throughput_on_a_bulk_manifest`) reads about 10,000-11,000 rows/s on 3.12 and
  8,500-9,700 on 3.10, slower than XML. Run them yourself with
  `DAGTOOLS_BENCH=1 pytest dag_tools_tests/test_documents_engine.py dag_tools_tests/test_documents_json.py -k throughput -s`;
  under `DAGTOOLS_BENCH=1` both tests fail below 20,000.
