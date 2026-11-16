"""Row-count asset check for cursorless (``replace``) tables.

Attached ONLY to tables whose ``table_config`` entry declares no ``cursor``.
An incremental table legitimately loads zero rows (nothing moved since the
watermark), so a check there would fail a correct run and be switched off
within a week; a ``replace`` table that loads zero rows has just emptied its
destination.

A source table with NO ``table_config`` entry at all is cursorless too, and
is deliberately left uncovered -- the one part of this scoping that is not
self-evident, so it is written down rather than left to be "fixed". Covering
it would attach new asset checks to every pipeline that upgrades without
having asked for any, changing the shape of a deployment's asset graph as a
side effect of a library bump. The check therefore follows the operator's own
declaration: a table someone wrote a ``table_config`` entry for is a table
someone decided about. Widening it is a deployment-visible change and belongs
in a release that says so, not in this module.

Reads the latest materialization's metadata and opens no connection, so it
runs where no warehouse driver is installed. dagster-dlt's
``fetch_row_count`` is deliberately not used: it issues a ``select count(*)``
through the destination.

dagster-dlt writes ``rows_loaded`` only ``if rows_loaded:``, so a zero-row
load carries NO such key -- absence and zero are indistinguishable. Absence
therefore FAILS (it is the condition the check exists to catch), and the
message says no row count was recorded, never "loaded 0 rows": the metadata
does not carry that number and claiming it sends an operator to debug the
source when the table may simply never have been normalized.
"""
from typing import Any, Dict, List

from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

CHECK_NAME = "rows_loaded_recorded"


def evaluate(materialization_event: Any) -> AssetCheckResult:
    if materialization_event is None:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            description="this asset has no materialization to read a row count from",
        )
    materialization = materialization_event.asset_materialization
    metadata = dict(getattr(materialization, "metadata", None) or {})
    if "rows_loaded" not in metadata:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.WARN,
            description=(
                "no row count was recorded for the latest materialization of "
                "a table that is fully replaced on every run"
            ),
        )
    value = getattr(metadata["rows_loaded"], "value", metadata["rows_loaded"])
    return AssetCheckResult(
        passed=True, metadata={"rows_loaded": value},
    )


def build_row_count_checks(
    pipeline: str, tables: List[str], asset_keys: List[AssetKey]
) -> List[Any]:
    """One non-blocking check per cursorless table, matched by key tail."""
    by_tail: Dict[str, List[AssetKey]] = {}
    for key in asset_keys:
        by_tail.setdefault(key.path[-1], []).append(key)

    checks = []
    for table in tables:
        matches = by_tail.get(table, [])
        if len(matches) != 1:
            raise ValueError(
                f"pipeline '{pipeline}': table '{table}' needs a row-count "
                f"check but {len(matches)} asset keys end in it: {matches}"
            )
        key = matches[0]

        def _make(target: AssetKey, label: str):
            @asset_check(
                asset=target, name=CHECK_NAME, blocking=False,
                description=f"a replace-loaded table ({label}) recorded a row count",
            )
            def _check(context) -> AssetCheckResult:
                return evaluate(context.instance.get_latest_materialization_event(target))

            return _check

        checks.append(_make(key, table))
    return checks
