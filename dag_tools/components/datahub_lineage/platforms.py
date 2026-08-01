"""Translating a producer's idea of its platform into DataHub's.

Two vocabularies meet here and they are not the same one.

A producing IO manager knows what it wrote in *its own* terms — the
``source_type`` it already advertises to the mesh, which encodes both
the storage and the table format: ``s3_parquet``, ``s3_delta``,
``s3_iceberg``, ``postgres``. DataHub names platforms differently:
Delta and Iceberg tables on S3 are ``delta-lake`` and ``iceberg``, NOT
``s3``, because DataHub is classifying the *technology*, not the bucket
they happen to sit in.

So a mapping is unavoidable, and this module is where it lives — inside
the catalog integration, not in the IO managers. A producer should not
have to know what DataHub calls things; if the catalog is swapped or its
naming changes, only this file moves.

This replaces inferring the platform from asset-key prefixes. That
worked, but it required every asset key to be spelled in a way the
inference recognised, against a hardcoded set of platform names, so
adding a backend meant editing a list that lived nowhere near the
backend. Naming is now the producer's job — it is the only party that
actually knows.

Unrecognised names pass through unchanged rather than being forced to
``unknown``: DataHub has ~100 platforms and this table only covers what
dag-tools produces, so a name we do not recognise is far more likely to
be a valid platform we simply have not listed than a mistake. Hardcoding
a closed set is the fragility this is meant to remove.
"""
from typing import Dict, Mapping, Optional

# What dag-tools' own IO managers advertise, keyed by the source_type
# they return from physical_coordinates -- the same string, so the
# catalog and the mesh can never disagree about what an asset is.
SOURCE_TYPE_PLATFORMS: Dict[str, str] = {
    # Plain parquet in a bucket: the platform really is the object store.
    "s3_parquet": "s3",
    # Table formats are their own platform in DataHub. Mapping these to
    # "s3" would lose the distinction that makes them worth cataloguing.
    "s3_delta": "delta-lake",
    "s3_iceberg": "iceberg",
    # Databases already agree with DataHub's naming.
    "postgres": "postgres",
    "clickhouse": "clickhouse",
}

# Names that reach us from outside dag-tools' IO managers. dlt writes
# ``destination_name`` into its asset config, using its own short names
# -- "abs" for Azure Blob Storage, which DataHub calls "adlsGen2". Left
# unmapped these create a platform entity that does not exist.
PRODUCER_ALIASES: Dict[str, str] = {
    "abs": "adlsGen2",
    "adls": "adlsGen2",
    "filesystem": "file",
    "local": "file",
}

# Platforms whose dataset names are laid out as a path rather than a
# dotted identifier. This has to stay in step with PRODUCER_ALIASES: the
# name format is chosen by platform, so mapping "abs" to "adlsGen2"
# without listing the target here would silently change the NAME of
# every Azure dataset -- a different name is a different entity, so the
# catalog would grow a duplicate rather than update the original.
FILESYSTEM_PLATFORMS = [
    "s3",
    "gcs",
    "hdfs",
    "adlsGen2",
    "adlsGen1",
    "file",
    # Pre-mapping names, still accepted so a producer that has not been
    # updated keeps the layout it has always had.
    "abs",
    "filesystem",
]

UNKNOWN_PLATFORM = "unknown"


def resolve_platform(
    declared: Optional[str], overrides: Optional[Mapping[str, str]] = None
) -> str:
    """DataHub's name for whatever a producer called its platform.

    Resolution order, most specific first:

      1. ``overrides`` — per-deployment configuration, so a new backend
         or a renamed platform can be handled without a code change.
      2. :data:`SOURCE_TYPE_PLATFORMS` — dag-tools' own IO managers.
      3. :data:`PRODUCER_ALIASES` — other producers' vocabularies.
      4. The name unchanged, on the assumption it is already one of
         DataHub's.

    Returns ``"unknown"`` only for a genuinely absent name. ``unknown``
    is itself a real platform entity in DataHub, so this is a visible
    "nobody said", not a silent failure.
    """
    if not declared:
        return UNKNOWN_PLATFORM
    name = str(declared).strip()
    if not name:
        return UNKNOWN_PLATFORM

    for table in (overrides or {}, SOURCE_TYPE_PLATFORMS, PRODUCER_ALIASES):
        if name in table:
            return table[name]
    return name
