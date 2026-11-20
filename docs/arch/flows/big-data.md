# Big-data access (FL-BIG)

This flow follows a large read through the mesh data client: a consumer asks for a dataset by URN, receives a ticket, and reads the bytes. It asks where the data moves, where it is held in memory, and what bounds the read. Two reference elements frame it:

- the data exchange management service: registered ways to deliver data to a consumer ([DoD record, row S7](../alignment/dod-dmra.md));
- the analytical data plane, where data is turned into insight (NATO Appendix 14, ¶10; [NATO record, row L4](../alignment/nato-dcra.md)).

The access decision itself is in [need to know](need-to-know.md) and is not repeated here.

**This record describes; it proposes nothing.** The remedies for its gaps are in [the roadmap note](../ROADMAP.md), each marked `proposed`. Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **The gateway returns a ticket, not data.** The client posts to the gateway's authorize route and gets back a source type, a physical location and a credential. It then reads from the store directly. No dataset bytes pass through the gateway or the broker. `built`
2. **How a read is held in memory depends on the backend.**

   | Backend | How the client reads | Held in client memory before the caller sees the frame | Where narrowing applies |
   |---|---|---|---|
   | Object store: Parquet, Delta | a lazy scan | nothing until the caller collects | on the lazy plan, so column selection and row filters compose before any read |
   | Object store: Iceberg | the catalog loads the table, then a lazy scan | nothing until the caller collects | as above |
   | Postgres | `SELECT *` of the whole table, read eagerly | the whole table | in the database, through the minted role; the client does not narrow |
   | ClickHouse | `SELECT *` of the whole table, read eagerly into Arrow | the whole table | in the database, through the minted role, and again on the frame after the transfer |

   `built`. Whether the object-store scans push the selection and filter down into the files is the query engine's behaviour; no run was recorded, so it is not stated here as measured.
3. **Nothing bounds a read from a SQL backend.** The two SQL paths send no column list, no predicate, no limit and no page size. The client, the gateway and the broker contain no pagination, chunking, streaming or columnar transport (a search of the three packages for each finds none). Since no deployed policy produces narrowing ([need to know, finding 2](need-to-know.md)), the minted role reaches the whole table, and so does the read. `built` (the unbounded read); `not built` (a bounded one)
4. **Both timeouts in the client are on control-plane calls.** The token fetch from the identity provider and the gateway's authorize call each wait at most ten seconds. Neither data read sets a timeout. `built`
5. **Consumers bound memory themselves, and the bound comes too late for a SQL backend.** The data-analyst agent in invincible-agent keeps the frame lazy, counts its rows and columns, and refuses before the query when rows × columns exceeds a configured cell limit. The refusal names the limit and tells the agent not to retry. A second limit, in bytes, applies to the result. Both are pinned by tests. For an object-store dataset the count reads metadata only. For Postgres or ClickHouse, the client has already transferred the whole table inside `get_dataframe` before the agent's gate runs, so the gate can refuse the query but not the transfer. All the read paths return the same lazy frame type, so a consumer cannot tell from the frame whether narrowing it narrows the read or only its view of a transfer that has already happened. `built` (the gate); the ordering is read from code and is not tested.
6. **Dagster assets read as the service.** `CortexPolarsIOManager` builds the client from its machine credentials with no person named, so every read through it is the service's read. When the upstream published no URN, it derives one on the `dagster` platform from the asset key, which is not the physical dataset URN the catalog registers (dag-tools AGENTS.md §4l). `built`
7. **The SDK leaves the read to the caller.** The SDK resolves a URN to a data reference and does not read it. Its documented pattern is the dag-tools client's `get_dataframe`. It does not call `get_dataframe_with_ticket`, so an SDK caller following that pattern gets no receipt. Supplied by the SDK's session at its pin; not read here.
8. **The receipt has a version field, and nothing fills it.** Every read can return a receipt carrying `as_of`. The client copies the field from the ticket and never fills it itself. The broker is the side meant to fill it, in one of two ways: a version reader registered for the backend type reads it when the ticket is authorized, or the broker uses a version the producer advertised when it loaded. The table of version readers ships empty. So `as_of` is absent unless a producer advertises one, and a reader cannot say which version of a large dataset it read. invincible-agent #375 now cites this receipt's as-of and ticket id in a finance agent's answer. When the receipt has no as-of, the answer claims none, and tests pin that. The field has a consumer but still no producer. `built`
9. **Writing streams; reading does not.** The Arrow IO manager accepts a streaming record-batch reader as output and writes it without draining it first. No equivalent exists on the read side. `built`
10. **No dag-tools test reads a real backend through the client.** The client's tests replace the scan ("the read itself is not under test"), and no test reaches the Postgres or ClickHouse read call. Every row in this record is therefore `built` and none is `measured`.

## The flow, step by step

Each actor and target cell holds one id. An `RL-` id names a role, and an `XE-` id a party with no record of its own; both are defined in [the README](../README.md#ids-and-connections).

| # | Actor | Action | Target | What passes | Decision or check | Evidence | Status |
|---|---|---|---|---|---|---|---|
| 1 | XE-CONSUMER | asks for a dataset by URN | CO-CLIENT | the URN, directly or through `CortexPolarsIOManager` | — | dag-tools `dag_tools/cortex_data/client.py:161-171`; `dag_tools/io_managers/cortex_io_manager.py:117-153` | `built` |
| 2 | CO-CLIENT | posts to the authorize route | CO-GATEWAY | the URN and the caller's token | a ten-second timeout | `client.py:188-205` | `built` |
| 3 | CO-GATEWAY | decides, then asks the producer's broker for a ticket, as in [FL-NTK](need-to-know.md), steps 3 to 9 | CO-BROKER | the dataset | the broker is found through a route each broker registers, which expires after five minutes | `dag_tools/central_gateway/main.py:184-202`, `:374`, `:476` | `built` |
| 4 | CO-BROKER | mints a credential | XE-DATASET-BACKEND | — | an expiring object-store credential scoped by prefix, or an expiring database role | `dag_tools/domain_broker/main.py:272-279`, `:298-307`; `dag_tools/domain_broker/sql_minting.py:296-340` | `built` |
| 5 | CO-CLIENT | reads the ticket | CO-CLIENT | the source type, location and credential | — | `client.py:228-235` | `built` |
| 6 | CO-CLIENT | reads from the store directly, by backend (finding 2) | XE-DATASET-BACKEND | the minted credential; the data back | any other source type is refused | `client.py:260-264` (Parquet, Delta), `:266-297` (Iceberg), `:299-325` (Postgres), `:327-353` (ClickHouse), `:355-356` (refusal) | `built` |
| 7 | CO-CLIENT | applies column and row narrowing to the frame | CO-CLIENT | — | skipped for Postgres | `client.py:358-366` | `built` |
| 8 | XE-CONSUMER | collects the frame, or bounds it first (finding 5) | XE-CONSUMER | the frame | — | invincible-agent `agent_fleet/data_analyst/main.py:19`, `:45`, `:519`, `:555-583`, `:598`, `:653-680` | `built` |

## Gaps

- **Bounded reads from SQL backends:** not built. The read is the whole table, held in memory (findings 2, 3). Remedy: [roadmap row 4](../ROADMAP.md). Status: `built`.
- **A consumer's size gate cannot prevent the transfer from a SQL backend:** the transfer has already happened inside the client (finding 5). Roadmap row 4 would close this too. Status: `built`.
- **No timeout on the data read** (finding 4). Remedy: roadmap row 4. Status: `built`.
- **Version of the data read:** not built for every backend (finding 8). The field exists and the client passes it through. The broker fills it only when a producer advertises a version. The side expected to fill it is the domain broker's version readers. **Owner: dag-tools.** The consumer waiting on it is the finance agent's answer provenance (invincible-agent #375). Remedy: [roadmap row 5](../ROADMAP.md). Status: `built`.
- **No streaming or columnar transport on the read side:** not built (findings 3, 9). Roadmap row 4 proposes batched reads; a columnar transport is not proposed. Status: `built`.
- **The IO manager reads as the service and can derive a `dagster`-platform URN** (finding 6). The second conflicts with the physical-URN rule; it is a fallback only. Status: `built`.
- **No test reads a real backend through the client** (finding 10). Status: `built`.

## Not established from code

- Whether the object-store scans push column selection and row filters into the files, and how much they read. That is the query engine's behaviour; no run was recorded.
- The largest dataset any deployment reads through the client, and the memory its consumers are given. Both are facts of deployments, not of code.
- Whether any consumer other than the data-analyst agent bounds its reads. The invincible-agent finance tests also use the client and were not read for this.
- The SDK lines were supplied by the SDK's session and not read here.

## Sources

| Claim | Source | Decision | Evidence | Status |
|---|---|---|---|---|
| The gateway returns a ticket; bytes go from store to client | dag-tools `dag_tools/central_gateway/main.py:374`; `dag_tools/cortex_data/client.py:201-205`, `:228-231`, `:260-353` | not stated | `test_authorize_endpoint_shape` | `built` |
| Object-store reads are lazy scans | dag-tools `client.py:260-264`, `:295-297` | not stated | read stubbed: `dag_tools_tests/test_ticket_receipt.py:153-156` | `built` |
| SQL reads are an eager `SELECT *` of the whole table | dag-tools `client.py:318`, `:323`, `:352-353` | not stated | no test reaches either call | `built` |
| Postgres skips client narrowing; ClickHouse narrows after the transfer | dag-tools `client.py:325`, `:358-366` | not stated | `test_the_frame_is_still_narrowed_when_the_receipt_is_asked_for` | `built` |
| No pagination, chunking, streaming, limit or columnar transport | dag-tools `dag_tools/cortex_data/`, `dag_tools/central_gateway/`, `dag_tools/domain_broker/` searched | not stated | — | `not built` |
| Ten-second timeouts on the token fetch and the authorize call only; none on a data read | dag-tools `client.py:156`, `:205`, `:323`, `:352` | not stated | — | `built` |
| Brokers register routes that expire after five minutes | dag-tools `main.py:184-202` | not stated | — | `built` |
| SQL minters push narrowing into the database | dag-tools `dag_tools/domain_broker/main.py:298-307` | cited in code: invincible-agent ADR-0044 (not read) | as in [need to know](need-to-know.md) | `built` |
| Data-analyst cell gate before the query; byte gate on the result | invincible-agent `agent_fleet/data_analyst/main.py:19`, `:45`, `:555-583`, `:653-680` | not stated | invincible-agent `tests/test_da_source_size_gate.py`: `test_the_source_table_is_NOT_collected_before_the_query`, `test_a_size_gate_runs_BEFORE_the_query`, `test_the_gate_measures_CELLS_not_rows`, `test_the_refusal_NAMES_the_limit_and_forbids_retry`, `test_a_precheck_that_cannot_run_is_LOUD_not_a_silent_bypass` | `built` |
| The gate runs after the SQL transfer | dag-tools `client.py:318-325`, `:352-353`; invincible-agent `agent_fleet/data_analyst/main.py:519`, `:555` | not stated | not tested | `built` (read from code) |
| The IO manager reads as the service; `dagster`-platform URN fallback | dag-tools `dag_tools/io_managers/cortex_io_manager.py:135-153` | dag-tools AGENTS.md §4l (physical URN) | `test_cortex_still_reads` (read stubbed) | `built` |
| The SDK leaves the read to the caller and takes no receipt | SDK `interfaces.py:1013`, `:1076`, `:1085`; `core.py:503` | none | supplied by the SDK session | `built` (supplied) |
| No version reader ships | dag-tools `dag_tools/domain_broker/ticket_identity.py:110` | dag-tools AGENTS.md §4o | `test_no_as_of_reader_ships_by_default` | `built` |
| The broker fills `as_of` from a reader, else from a version the producer advertised; the client only copies it | dag-tools `ticket_identity.py:47`, `:149-194`, `:216`; `dag_tools/cortex_data/client.py:23-32`, `:45-56` | dag-tools AGENTS.md §4o | `test_a_version_the_producer_advertises_is_carried_and_labelled`, `test_an_absent_as_of_is_not_the_issue_time` | `built` |
| An answer cites the receipt's as-of only when it has one | invincible-agent `agent_fleet/finance_agent/asset_inputs.py:146-152`, at `136793d` (#375, merged after this record's pin) | cited in code: invincible-agent ADR-0065 (not read) | invincible-agent `tests/finance/test_provenance_arrives_whole.py`: `test_AN_ASSET_ANSWER_CARRIES_ITS_AS_OF_AND_TICKET_ID`, `test_A_RECEIPT_MISSING_ONE_HALF_CLAIMS_ONLY_THE_OTHER` | `built` |
| Writing accepts a streaming record-batch reader | dag-tools `dag_tools/io_managers/arrow.py:208-210` | not stated | `test_arrow_writes_streaming_record_batch_reader`, `test_arrow_schema_does_not_drain_a_streaming_reader` | `built` |
| Remedies | [the roadmap note](../ROADMAP.md), rows 4 and 5 | none | — | `proposed` |
