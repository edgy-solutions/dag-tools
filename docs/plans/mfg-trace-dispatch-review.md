# The dag-tools lane in `dispatch-mfg-trace.md`, checked against the spec

**Settled by the architect (2026-11-11).**
- The template's keys stand, and the dispatch now defers to the
  template's grains. `ingest_status` stays one row per run; there is no
  split.
- The projections in item 3 were **already in the template**
  (`by_child`, `by_descendant`). This review missed them; they are now
  being built.
- The as-of reader in item 4 is approved; it goes in PR D.
- The fixture in items 1 and 2 is approved at the proposed volume. dag-tools
  proposes its own inputs, and Lane 1 ratifies them in ADR review.
- An open observed interval has null precision: approved.

The rest of this file is the review as written, kept for the record.

The dispatch is from the architect, dated 2026-11-11. It lives at
`invincible-agent/dispatch-mfg-trace.md`, and its §2 sets out the dag-tools lane. This file checks
that lane against the approved spec (`mfg-trace.md`) and the contract template
(`examples/mfg_trace/contracts/mfg_trace.yaml`).

## Already covered by the approved spec

| Dispatch, dag-tools lane | Where |
|---|---|
| Materialise through `DltPipelineComponent`, with the source kind chosen by `source_config.type` | spec §4.1 (PR A) |
| The template is our published interface, and iagent pins it | spec §6.2 (Q1) |
| `all_terminal` steps | spec §4.1 (Q2) |
| Validity on `as_built_edge`, with its origin, from the first load on | spec §4.2 (`interval_origin`, `valid_to_precision_s`) |
| Source ids `pdm`, `mes`, `test_repo` above the adapter | spec §4.1 and the template header |
| `timestamp`, `date` and `array<string>`, in the same change as iagent's reader | spec D10, §12 PR B; order approved |
| `_current` views, ordering of `test_measurement` by part, then parameter, then time | spec §6.1; contract `ORDER BY` |
| Catalog publishing under the physical URN, with lineage | spec §9 (`datahub_lineage` and the dbt publisher) |

## New for dag-tools

1. **The sandbox fixture.** It's synthetic and at realistic volume, and it includes one removal
   and replacement plus one unit inside a parent the sample viewer can't see. The spec has no
   fixture deliverable yet.
   - It belongs in a new PR D, built on the `demo_ev_refusal` pattern (§4k-1): gated on demo mode,
     written through the accepted fixture's IO manager configuration, and with the cases derived
     from a manifest.
   - "Realistic volume" needs a number. My proposal is the scale of one product line over 90 days:
     about 2k units, about 10k results and about 2M measurements, generated deterministically from
     a seed.
2. **One refusal asset per refusal kind:** `wrong_type`, `duplicate_key`, `not_determined_by` and a
   missing declared column. Each asset carries one defect, with a clean control, following the
   §4k-1 discipline. This needs the Lane 1 inputs below.
3. **A fast path for containment and genealogy (our gap).** `containment` and the upward half of
   `unitGenealogy` look up by **descendant**. Both `as_built_closure` and `as_built_edge` are
   ordered by the parent or ancestor, so those lookups scan the table. With a 500 ms budget for
   containment of 10 units, that won't hold at volume.
   - Fix: a ClickHouse projection ordered by `(scope, descendant_pn, descendant_serial)` on the
     closure, and one ordered by `(child_pn, child_serial)` on the edge.
   - This is a template and spec change, which is ours to make. It changes the access path, not
     the contract's columns.
4. **Provenance `as_of` for ClickHouse reads.** The broker already mints ClickHouse tickets
   (`_MINTERS["clickhouse"]`), but `AS_OF_READERS` ships empty. So every mfg-trace answer would
   carry `as_of_unavailable`, while the dispatch requires an `as_of`.
   - Proposal: a ClickHouse as-of reader that runs an as-of statement the deployment declares
     (`warehouse_source`'s stance: an operator's statement, sent verbatim, with no column named in
     code), returned exactly as the source wrote it.
   - This is gateway and broker work, which this seat owns. It needs the architect's go-ahead,
     because §4o says no reader ships until one can be tested. The `DAGTOOLS_E2E_CLICKHOUSE` job
     makes this one testable.

## Sent to Lane 1, waiting for answers

Packet: `invincible-agent/sessions/2026-11-11-packet-to-lane-01-from-dag-tools-owner-mfg-trace-dispatch-keys-and-fixture-inputs.md`.

- **`test_measurement` key.** The dispatch's `result_id, step_path, parameter` repeats on looped
  steps. Proposal: the template key `result_id, step_id, parameter_index`.
- **`test_catalog` key.** It lacks `units`, which the dispatch's own rule makes part of a
  parameter's identity, and `program_name`.
- **`ingest_status` key.** The dispatch has one row per source, while the template has a run log.
  Proposal: I split the run log out as `ingest_run`, and `ingest_status` keeps one row per source.
- **`as_built_closure` key.** It needs `scope`.
- **Fixture inputs:**
  - the part-to-program patterns (proposed: `PN-1000-*` and `PN-1100-*` map to `SAMPLE-PROGRAM-A`,
    `PN-9000-*` to `SAMPLE-PROGRAM-B`);
  - the `determined_by` declarations;
  - the target dataset for each refusal kind (default: `test_measurement`).

## Timing

The dispatch starts after the 0.4.85 cut (Thursday 11-12, 23:00Z), and nothing in it rides 0.4.85.
dag-tools isn't part of that cut, and the architect has already approved "spike, then PR A". So PR A
can proceed, and the template changes in item 3 and the `ingest_status` split wait for Lane 1's
answer.

## Not ours

Classification per column (`operator_ref` as personal data), the `determined_by` declarations, the
gates, the verbs, caching and the UI. The fixture keeps `operator_ref` synthetic (`OP-0001`).
