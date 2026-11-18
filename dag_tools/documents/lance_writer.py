"""Writing a bundle's tables to Lance, one dataset per table.

Each table lives at ``<destination>/<table>.lance``. A bundle is written
**idempotently**: :meth:`LanceWriter.begin_bundle` deletes every row whose
``source_bundle`` is the bundle's key -- in every table, ``_rejects``
included -- and the run then appends. Re-running a bundle therefore leaves
row counts unchanged, and a run that died half-way is repaired by running it
again.

The schema comes from the mapping, never from the data, so a bundle that
produced no row for a table still creates that table with its exact schema.
A dataset whose stored schema disagrees with the mapping fails the write
naming the columns added, removed and retyped: appending under a different
schema would either fail deep inside Lance or, worse, succeed.

Binary attachments are ``large_binary`` columns carrying the field metadata
``lance-encoding:blob=true``; Lance stores them as its blob extension type
and a reader fetches them with ``dataset.take_blobs(column, indices=...)``.

Concurrency (measured on pylance 14.0.3, local filesystem; see
``docs/DOCUMENT_TABLES.md``): concurrent appends from separate writers to one
dataset all commit -- Lance retries the manifest commit -- but two writers
running the **same** bundle each delete and then each append, so the bundle
lands twice. The component serialises writes per destination with a Dagster
pool whose limit is ``write_concurrency`` (default 1). Object stores are not
verified here: S3 needs a commit lock or a store with conditional writes, and
that is a deployment's to establish before it raises the limit.

Pure: no Dagster import. ``storage_options`` come from configuration and the
environment only and are passed to Lance as-is; nothing here logs them.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional

log = logging.getLogger(__name__)

BLOB_KEY = b"lance-encoding:blob"


class SchemaMismatch(ValueError):
    """The stored dataset's schema is not the schema the mapping declares."""


def _lance() -> Any:
    try:
        import lance
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "writing Lance datasets needs pylance; install the `documents` extra: "
            "pip install 'edgy-dag-tools[documents]'"
        ) from e
    return lance


def _pa() -> Any:
    import pyarrow as pa

    return pa


def _is_blob_field(f: Any) -> bool:
    return bool(f.metadata) and f.metadata.get(BLOB_KEY) == b"true"


def _stored_type_name(f: Any) -> str:
    t = f.type
    name = getattr(t, "extension_name", None)
    if name and str(name).startswith("lance.blob"):
        return "blob"
    return str(t)


def _expected_type_name(f: Any) -> str:
    return "blob" if _is_blob_field(f) else str(f.type)


def schema_differences(expected: Any, stored: Any) -> List[str]:
    """Human-readable differences between the mapping's schema and a dataset's."""
    exp = {f.name: _expected_type_name(f) for f in expected}
    got = {f.name: _stored_type_name(f) for f in stored}
    out: List[str] = []
    added = [n for n in exp if n not in got]
    removed = [n for n in got if n not in exp]
    retyped = [f"{n} ({got[n]} -> {exp[n]})" for n in exp if n in got and exp[n] != got[n]]
    if added:
        out.append("columns the mapping adds: " + ", ".join(added))
    if removed:
        out.append("columns the mapping no longer has: " + ", ".join(removed))
    if retyped:
        out.append("columns whose type changed: " + ", ".join(retyped))
    return out


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


BUNDLE_COLUMN = "source_bundle"


def _indexed_columns(ds: Any) -> Optional[List[List[str]]]:
    """The column lists of a dataset's indices, or ``None`` if this pylance cannot say.

    ``describe_indices`` is the current API (pylance 14 measured);
    ``list_indices`` is the older one, kept so the floor of the ``pylance``
    pin still answers rather than being reported as unsupported.
    """
    if hasattr(ds, "describe_indices"):
        return [list(getattr(i, "field_names", None) or []) for i in ds.describe_indices()]
    if hasattr(ds, "list_indices"):
        return [list(i.get("fields") or []) for i in ds.list_indices()]
    return None


def ensure_bundle_index(ds: Any, table: str) -> str:
    """Make the per-bundle delete an index lookup rather than a scan.

    Every bundle write starts with ``delete where source_bundle = <key>``, so
    without an index each write scans the whole table. Creates a BTREE scalar
    index on ``source_bundle`` when the dataset has none; when one exists,
    folds fragments appended since into it (an append leaves its rows
    unindexed until then -- measured: one unindexed row after an append,
    zero after ``optimize_indices``). Returns what it did, for the run log:
    ``created``, ``updated``, ``unsupported`` (this pylance has no scalar
    index API) or ``failed: <type>``. A failure is logged and reported, never
    raised: the index is an optimisation, and maintenance must still
    compact and clean up without it.
    """
    if not hasattr(ds, "create_scalar_index"):
        log.info("lance: %s: this pylance has no scalar index API; source_bundle index skipped", table)
        return "unsupported"
    try:
        indexed = _indexed_columns(ds)
        if indexed is None:
            log.info("lance: %s: this pylance cannot list indices; source_bundle index skipped", table)
            return "unsupported"
        if any(cols == [BUNDLE_COLUMN] for cols in indexed):
            ds.optimize.optimize_indices()
            return "updated"
        ds.create_scalar_index(BUNDLE_COLUMN, index_type="BTREE")
        return "created"
    except Exception as e:  # noqa: BLE001 - an optimisation may not fail maintenance
        log.warning("lance: %s: source_bundle index not maintained (%s)", table, type(e).__name__)
        return f"failed: {type(e).__name__}"


def table_uri(destination: str, table: str) -> str:
    base = destination.removeprefix("file://")
    return f"{base.rstrip('/')}/{table}.lance"


@dataclass
class WriteResult:
    deleted: Dict[str, int] = field(default_factory=dict)
    appended: Dict[str, int] = field(default_factory=dict)
    created: List[str] = field(default_factory=list)
    versions: Dict[str, int] = field(default_factory=dict)


class LanceWriter:
    """One destination; one dataset per table."""

    def __init__(self, destination: str, storage_options: Optional[Mapping[str, str]] = None):
        if not destination:
            raise ValueError("a Lance destination URI is required")
        self.destination = destination
        self.storage_options = dict(storage_options) if storage_options else None
        self._lance = _lance()
        self._schemas: Dict[str, Any] = {}
        self._bundle: Optional[str] = None
        self.result = WriteResult()

    # ---------------------------------------------------------------- paths
    def uri(self, table: str) -> str:
        return table_uri(self.destination, table)

    def _open(self, table: str) -> Optional[Any]:
        try:
            return self._lance.dataset(self.uri(table), storage_options=self.storage_options)
        except (FileNotFoundError, ValueError, OSError) as e:
            # pylance raises ValueError("Dataset at path ... was not found") for a
            # missing dataset; anything else is re-raised by the probe below.
            if "not found" in str(e).lower() or isinstance(e, FileNotFoundError):
                return None
            raise

    # ---------------------------------------------------------------- bundle
    def begin_bundle(self, bundle_key: str, schemas: Mapping[str, Any]) -> WriteResult:
        """Create missing tables, check schemas, delete the bundle's old rows."""
        self._schemas = dict(schemas)
        self._bundle = bundle_key
        self.result = WriteResult()
        problems: List[str] = []
        opened: Dict[str, Any] = {}
        for table, schema in self._schemas.items():
            ds = self._open(table)
            if ds is None:
                continue
            diff = schema_differences(schema, ds.schema)
            if diff:
                problems.append(f"{table}: " + "; ".join(diff))
            opened[table] = ds
        if problems:
            raise SchemaMismatch(
                "the destination's tables do not match the mapping -- write to a new "
                "destination or migrate the tables: " + " | ".join(problems)
            )
        predicate = f"source_bundle = {_sql_literal(bundle_key)}"
        for table, schema in self._schemas.items():
            ds = opened.get(table)
            if ds is None:
                ds = self._lance.write_dataset(
                    schema.empty_table(), self.uri(table), schema=schema, mode="create",
                    storage_options=self.storage_options,
                )
                self.result.created.append(table)
                self.result.deleted[table] = 0
            else:
                r = ds.delete(predicate)
                self.result.deleted[table] = int((r or {}).get("num_deleted_rows", 0)) if isinstance(r, dict) else 0
            self.result.appended[table] = 0
        log.info(
            "lance: bundle %s prepared; deleted %s; created %s",
            bundle_key, self.result.deleted, self.result.created,
        )
        return self.result

    def append(self, table: str, batch: Any) -> None:
        """Append one batch; the schema is the mapping's, never the batch's."""
        if self._bundle is None:
            raise RuntimeError("append() before begin_bundle()")
        schema = self._schemas[table]
        if batch.num_rows == 0:
            return
        pa = _pa()
        data = pa.Table.from_batches([batch], schema=schema)
        self._lance.write_dataset(
            data, self.uri(table), schema=schema, mode="append",
            storage_options=self.storage_options,
        )
        self.result.appended[table] = self.result.appended.get(table, 0) + batch.num_rows

    def sink(self, table: str, batch: Any) -> None:
        """The engine's ``sink`` signature."""
        self.append(table, batch)

    def finish(self) -> WriteResult:
        for table in self._schemas:
            ds = self._open(table)
            if ds is not None:
                self.result.versions[table] = int(ds.version)
        self._bundle = None
        return self.result

    # ---------------------------------------------------------------- reads
    def count_rows(self, table: str, bundle_key: Optional[str] = None) -> int:
        ds = self._open(table)
        if ds is None:
            return 0
        if bundle_key is None:
            return int(ds.count_rows())
        return int(ds.count_rows(filter=f"source_bundle = {_sql_literal(bundle_key)}"))

    # ---------------------------------------------------------------- maintenance
    def maintain(
        self,
        tables: Iterable[str],
        *,
        older_than: timedelta = timedelta(days=7),
        retain_versions: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Compact fragments, index ``source_bundle``, then remove old versions.

        Missing tables are skipped. The index step is :func:`ensure_bundle_index`.
        """
        out: Dict[str, Dict[str, Any]] = {}
        for table in tables:
            ds = self._open(table)
            if ds is None:
                out[table] = {"skipped": "absent"}
                continue
            c = ds.optimize.compact_files()
            ds = self._open(table)
            indexed = ensure_bundle_index(ds, table)
            ds = self._open(table)
            kwargs: Dict[str, Any] = {"older_than": older_than}
            if retain_versions is not None:
                kwargs["retain_versions"] = retain_versions
            cl = ds.cleanup_old_versions(**kwargs)
            out[table] = {
                "fragments_removed": int(getattr(c, "fragments_removed", 0) or 0),
                "fragments_added": int(getattr(c, "fragments_added", 0) or 0),
                "bytes_removed": int(getattr(cl, "bytes_removed", 0) or 0),
                "old_versions_removed": int(getattr(cl, "old_versions", 0) or 0),
                "version": int(ds.version),
                "source_bundle_index": indexed,
            }
            log.info("lance: maintained %s: %s", table, out[table])
        return out


__all__ = [
    "LanceWriter",
    "SchemaMismatch",
    "WriteResult",
    "ensure_bundle_index",
    "schema_differences",
    "table_uri",
]
