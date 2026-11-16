# mfg_trace — the curated slice, end to end

Three curated tables (`test_result`, `test_measurement`, `test_catalog`)
derived from a test-repository source, with their contract checks.

The point of this example is the **shape**, not the vocabulary. Nothing in
dag-tools names a database, a schema, a table or a column from your source,
and that rule is what the two-stage split below exists to keep true while
the library still ships real SQL.

```
your source  --(dlt)-->  raw tables  --(stage 1)-->  conf_* --(stage 2)--> curated
                                        your YAML              our dbt package
                                        names your columns     names none of them
```

| | lives in | names columns from below | this example |
|---|---|---|---|
| ingest | `systemlink/component.yaml` | no (fields arrive as the server sends them) | shipped |
| stage 1 — conform | your deployment's `contract_mapping` YAML | **yes, and only here** | see *Stage 1*, below |
| stage 2 — derive | `dag_tools/dbt_project_packages/mfg_trace_curated` | no | shipped, `curated/` |

## The contracts

`contracts/mfg_trace.yaml` holds every document, governed together:

- `mfg_trace.conf_test_repo_result` and
  `mfg_trace.conf_test_repo_step_parameter` — the **conformed** vocabulary.
  This is the seam: stage 1 produces exactly these columns, stage 2 reads
  exactly these columns, and neither half has to know anything about the
  other.
- `mfg_trace.test_result`, `mfg_trace.test_measurement`,
  `mfg_trace.test_catalog` — the curated documents the package produces.
  These are dag-tools' *published interface*, which is why the package's SQL
  is allowed to name them.

Every column carries `produced_since`, so a reader can be written against a
column before its producer exists and can tell which release started
answering.

## Stage 2 — the shipped dbt package

`curated/` is a deployment's own dbt project, reduced to the two files that
matter here.

Install the package (`curated/packages.yml`). It ships inside the library,
so ask the library where it is rather than guessing at a path:

```bash
python -c "from dag_tools.systemlink import sql; print(sql.PACKAGE_DIR)"
dbt deps
```

Then set the four variables (`curated/dbt_project.yml`). The package
declares a default for **none** of them, so one left unset fails
`dbt parse` rather than resolving to something:

| variable | what it is |
|---|---|
| `mfg_trace_conf_result_relation` | where the conformed result table is |
| `mfg_trace_conf_step_parameter_relation` | where the conformed step-parameter table is |
| `mfg_trace_decimal_type` | your warehouse's exact numeric type, for the tolerance arithmetic |
| `mfg_trace_comparators` | your comparator table (see below) |

```bash
dbt build --select package:mfg_trace_curated
```

### What the models derive

- `attempt_seq` numbers a unit's attempts in order.
- **`is_latest` is terminal-only.** A non-terminal attempt is never the
  latest result, whatever its `attempt_seq`. Read as
  `attempt_seq = max(attempt_seq)` instead, an in-flight run becomes the
  latest result, and a reader filtering to one row per unit is served a row
  whose result is not in yet — which reads as a correct answer. A unit with
  no terminal attempt at all has no latest row.
- `is_first` is the first attempt.
- `test_catalog` groups over `test_measurement`, not over the raw tables, so
  the catalog counts what was actually published.

### The comparator table

`mfg_trace_comparators` is **yours**. The library ships no comparison codes,
for the same reason it ships no column names: what a comparison is called is
a decision about your own data, and a code compiled into the library would
turn it into a library release. `curated/dbt_project.yml` carries a sample
table, and the sample is exercised by the library's own tests — rows in, SQL
out, both routes compared — so it is advice that has been run.

A row is `{text, code, bound}`: the operator as your source writes it, the
code that lands in `comparison_type`, and which of the two limit columns it
bounds (`low` or `high` — the library's own words, named after the columns
the curated contract declares).

Matching is on the **whole** operator, never a prefix. A table carrying only
`<` does not cover `<=`: reading it as `<` would publish a bound the source
never stated — right number, right side, wrong strictness.

An operator your table does not carry is **kept as text and counted, never
dropped and never read as a value**. It publishes no limit and no
`comparison_type`; `limits_text` still holds what was received, so the
inference is always auditable against it; and the package's
`uncovered_comparator` test reports it at **warning** severity. That
severity is deliberate — an unfamiliar operator is a missing row in your
table, not a defect in the data, and a build that refuses to publish over it
puts the pressure on dropping the row rather than on adding the row. It is
also **not** a `parse_failed`: that column says a number was expected and
not found. The two go to different people.

### Tolerance arithmetic

A `±` tolerance is computed in `mfg_trace_decimal_type`, not in binary
floats. `1.005 ± 0.005` through doubles gives `0.9999999999999999` and
`1.0099999999999998` — both wrong, both display as the right answer, so a
limit computed that way rejects a reading that passed and nothing downstream
looks odd. The published `low_limit_num` / `high_limit_num` are doubles, as
the contract declares: the exact type is the arithmetic, not the storage.

## Stage 1 — conform (`conform/`)

One `contract_mapping` component instance per raw table, mapping the
SystemLink kind's landing tables onto the two `conf_*` contracts:
`conform/components/conf_test_repo_result/` and
`conform/components/conf_test_repo_step_parameter/`.

This is the only place anything from below is named, and that is the whole
design: the mapping is a file an operator edits when a source renames a
field, not a library release. Nothing in `dag_tools` names any of these
columns.

It needed three transforms this library did not have, and each one exists
because the source disagrees with the contract about exactly one thing:

| conformed column | what it needed | what it uses |
|---|---|---|
| `part_number`, `serial_number` | a normalizer. The contract says "the deployment's normalizer decides how"; `trim`/`upper`/`lower` are single-step and cannot be composed, and a real normalizer is a rule, not a case fold. | `normalize` with `rules: identifier`, declared in `conform/normalizers.yaml`. The raw value is kept beside it as a plain copy, so a normalizer change stays auditable — nothing generates a `*_raw` column, because what to keep verbatim is the mapping author's decision. |
| `is_terminal` | which of your status values are terminal — a membership test over `status_type`. | `in_set`. The match is exact, so a value not listed is not terminal. The library carries no such set: `filters.terminal_statuses` in `systemlink/component.yaml` has no default and must be stated, and the values listed here are that same sample. The two decide different things on the same vocabulary — one when the kind fetches steps, one what the conformed column says — so a server whose vocabulary differs needs **both** edited, and they must agree. |
| `measurement_text`, `units`, `limits_text` | `''` where absent and null where empty. | `coalesce` with a declared `default: ""` for the first, `nullif` for the other two. |

`loaded_at` uses `run_timestamp`: one instant for a whole load, not a
per-row clock reading, so rows of one load are distinguishable by what they
are rather than by when they were written. `extracted_at` and `source_system`
read the kind's own stamps rather than restating constants the table already
carries.

`emit.dry_run: true` on both instances, so the example renders the SQL and
writes nothing. Each instance sets an explicit `asset_name`: the default is
`"contract_mapping"`, so two instances leaving it unset would mint one asset
key and the load would fail naming neither.

**Two limits, stated rather than papered over.** The flattened spelling of
the nested status column (`status__status_type`) is dlt's own decision and is
**unverified against a real run** — confirm it against your server before
trusting the mapping. And six of the result columns (a part number, a serial
number, a start time, a duration, a system id, a host name) are fields this
repo has never observed; each is marked `# declared` in the mapping, against
the `# observed` ones. `contract_mapping` has no normalization tolerance, so
each column block names exactly one spelling.

## The BW-shaped fixture (`bw/`)

A second `DltPipelineComponent` instance, over a synthetic sqlite stand-in
for a SAP-BW-shaped source: a master-data-shaped table and an as-built-shaped
table. `python bw/seed.py` builds it (no warehouse, no credentials, no binary
committed); `seed.apply_second_load()` is what changed in the source before
the next extraction. `bw/component.yaml` expresses the active-version filter as
`row_filters` and keeps history on the as-built table with `history: scd2`
(no cursor: the two are refused together). Every name in it is this example's
own; dag-tools names none of them, and
`dag_tools_tests/test_mfg_trace_fixture.py` enforces that in both directions.

Each plant sits on its own key, beside a control key that trips nothing:

| plant | what it proves at the raw layer |
|---|---|
| active and inactive versions of one key | the extraction filter drops the inactive row and keeps the active one |
| an open-ended validity with the `99991231` sentinel | raw carries the sentinel as written, not a null |
| a blank-key row | the extraction filter does **not** touch it; raw stays a faithful copy |
| a child absent from the second load | scd2 keeps its open version (the key is a `merge_key`, so a key missing from a load is not retired) |
| a child replaced at the same position | scd2 closes the old version and opens a new one |

**Deferred, by name.** Two of these plants have their second half in the
curated layer, which is PR D's and does not exist yet: dropping the blank-key
row is a curated `where`, and turning the sentinel end date into a null is a
`nullif`. Both are planted here and asserted to reach raw unchanged; their
behaviour is proven in `contract_mapping`'s own suites, not against this data.

## Hazards

- **A SQL cast rounds where the row route refuses.** The casts in a mapping
  are your SQL, and this library owns no dialect. If silent rounding is
  unacceptable for a column, the cast is where the check goes.
- **`dbt build --empty` makes dbt tests vacuous.** They pass on empty
  relations, so contract-test parity under `--empty` is not evidence of data
  correctness.
- **The uncovered-comparator count is a warning.** It is meant to be read.
  A deployment that never looks at warnings should promote it in its own
  project rather than let the number go unread.
