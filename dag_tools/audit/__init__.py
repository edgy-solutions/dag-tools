"""Read-only audits of what the pipelines actually touch.

Step 1 of the classification build (ADR-0064): before any object is labelled,
measure which assets read which objects, and how many objects nothing
explains. The report is the scope number the rest of the build is sized
against.

Three modules, split along the lines that make them testable:

  * `tag` -- the attribution contract carried on the query itself. Written by
    the warehouse resource (step 2), read back out of history here. Both
    halves import this one file so they cannot drift apart.
  * `reads` -- the pure roll-up. No warehouse, no Dagster, no IO; this is
    where the judgment about what counts as explained lives.
  * `history` -- the operator's history relation. Builds the read and maps its
    rows; it never executes anything.

Nothing in this package names a database, schema, table, column or level. The
operator's object names arrive as data (arguments, rows) and leave as data
(the report); none is written down here.

This package imports nothing from `dag_tools.qual` and nothing from Dagster,
so the audit runs on a laptop with neither installed.
"""
from .reads import (
    AMBIGUOUS,
    BY_ASSET_KEY,
    BY_CORRELATION,
    BY_RUN_ID,
    NOT_A_PIPELINE,
    UNATTRIBUTED,
    ReadEvent,
    ReadsReport,
    RunWindow,
    build_reads_report,
)
from .tag import (
    MAX_TAG_CHARS,
    ON_BEHALF_OF,
    PIPELINE,
    QueryAttribution,
    TagTooLong,
    decode,
    encode,
    is_pipeline,
    on_behalf_of_principal,
    parse_principal,
    pipeline_principal,
)

__all__ = [
    "AMBIGUOUS",
    "BY_ASSET_KEY",
    "BY_CORRELATION",
    "BY_RUN_ID",
    "MAX_TAG_CHARS",
    "NOT_A_PIPELINE",
    "ON_BEHALF_OF",
    "PIPELINE",
    "QueryAttribution",
    "ReadEvent",
    "ReadsReport",
    "RunWindow",
    "TagTooLong",
    "UNATTRIBUTED",
    "build_reads_report",
    "decode",
    "encode",
    "is_pipeline",
    "on_behalf_of_principal",
    "parse_principal",
    "pipeline_principal",
]
