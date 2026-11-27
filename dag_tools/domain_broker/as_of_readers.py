"""The as-of readers this build ships -- invincible-agent #529 item 2.

Two of them: one file-backed (``s3_parquet``) and one table-backed
(``postgres``). Each is here because it was **measured against a real store of
that kind** before it was written, which is the gate this work was accepted
under: no reader ships until it can be exercised against a real table of that
kind. ``scripts/verify_as_of_readers.py`` is that exercise, and it runs the
functions below rather than a copy of them.

WHAT A READER MAY AND MAY NOT DO, restated here because both halves bit:

* **It may not fail the read.** A reader that raises degrades to an absent
  as-of (``reader_failed``); it never refuses a ticket, because an as-of is
  metadata about the read while a credential **is** the read. That contract is
  enforced by ``ticket_identity.resolve_as_of``, not by these functions, so
  each one is free to raise and each one is still bounded on purpose:
* **It may not HANG the read either**, which is the half the contract does not
  cover. A reader runs inside the authorize call, once per request, so an
  unreachable store with no bound would park every authorize for that backend
  until the client gave up -- a refusal wearing a timeout's clothes. Every call
  below carries a measured bound; see ``_BOUND_*``.
* **It holds no credential of its own, and that is a property worth keeping.**
  ``stamp`` runs *after* resolution, so a reader is handed the ticket with the
  minted, scoped, expiring credential already on it. It therefore cannot read
  anything the consumer could not -- an as-of reader can never become a
  privilege escalation. The flip side is the rule both readers below obey: a
  ticket carrying no usable credential is **declined**, never read with the
  broker's own ambient identity. Falling back to the environment would read as
  working and would quietly be the escalation.

WHAT THESE TWO VALUES ARE, and the one sentence a consumer needs: **they are
an identity, not an order.** Equal means the same data; unequal says only that
something changed. A listing digest has no order at all, and a ``pg_lsn``
compared as text is wrong in a way that looks right (measured: ``'0/9' < '0/10'``
is true as a ``pg_lsn`` and false as text). Compare an as-of for equality, or
compare it in the type the server wrote it in -- never as a string ordering.

NO CLICKHOUSE READER, and it is a decision rather than an omission. Two
independent measurements, either of which alone is sufficient:

1. **The minted user cannot read ``system.parts`` at all** -- measured, code
   497, "Not enough privileges". Since a reader holds exactly the consumer's
   credential by design, there is no ClickHouse as-of readable without either
   widening the scoped grant or letting the reader use the broker's admin
   identity, which breaks the no-escalation property above. That is a
   governance decision, not a gap to paper over here.
2. **``max_block_number`` provably misses a mutation** -- measured: a mutation
   moved ``data_version`` 1 -> 3 and left ``max_block_number`` at 2 while the
   row's value genuinely changed. An as-of that stays equal across a real
   change is the failure that reads as correct, which is strictly worse than
   ``as_of: null``. The alternatives are no better: ``max(data_version)`` is not
   monotonic (it fell 2 -> 1 after a merge) and ``max(modification_time)`` is a
   timestamp, refused by the same rule as an object's ``LastModified``.

NO DELTA OR ICEBERG READER at this pin: those wait, as ruled.
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """One bound, read ONCE at import.

    Never per call: a malformed bound consulted inside an authorize call is a
    config error that can fail a read, which is the one thing this module does
    not allow. A malformed value here falls back to the default and says so,
    because refusing to import would take the read door down over a typo in a
    knob that only makes a reading faster or slower.
    """
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "%s=%r is not an integer; using the default of %d.", name, raw, default
        )
        return default
    if value <= 0:
        logger.warning(
            "%s=%d is not positive; using the default of %d.", name, value, default
        )
        return default
    return value


# MEASURED, not assumed. `connect_timeout=1` bounded an unroutable host at
# 1.01s and `connect_timeout=2` at 2.00s, each with a positive control against
# a live server so the timeout is about the host and not about the bound.
# (libpq honours a sub-2s value; a floor was predicted and is not there.)
_BOUND_CONNECT_SECONDS = _env_int("BROKER_AS_OF_CONNECT_TIMEOUT_SEC", 2)

# MEASURED: `options="-c statement_timeout=1500"` cut `pg_sleep(10)` off at
# 1.50s, with the as-of statement itself running well inside the same bound.
_BOUND_STATEMENT_MS = _env_int("BROKER_AS_OF_STATEMENT_TIMEOUT_MS", 1500)

# A listing is one request per 1000 objects, so this bound is a count of round
# trips inside an authorize call, not a size limit on the data. Past it the
# reader DECLINES rather than paging on: a prefix with a hundred thousand
# objects would turn one authorize into a hundred requests, and slow is the
# failure mode this module is least willing to buy an as-of with.
_BOUND_S3_MAX_OBJECTS = _env_int("BROKER_AS_OF_S3_MAX_OBJECTS", 1000)

_S3_PAGE_SIZE = 1000

# The value's own label. `listing-sha256:` rather than bare hex, and the reason
# is not decoration: this reader COMPUTES its value, so unlike a version a
# source wrote, its representation is the reader's to choose. A labelled token
# cannot be parsed as a number by a careless consumer -- the 2^53 hazard that
# silently corrupts a nineteen-digit snapshot id -- and it tells a consumer at a
# glance that the thing is an identity rather than something to sort.
LISTING_DIGEST_PREFIX = "listing-sha256:"


# ── file-backed: s3_parquet ────────────────────────────────────────────────

def _s3_parts(physical_uri: str) -> Optional[Tuple[str, str]]:
    """``s3://bucket/prefix`` -> ``(bucket, prefix)``, or None."""
    if not physical_uri.startswith("s3://"):
        return None
    remainder = physical_uri[len("s3://"):]
    bucket, _, prefix = remainder.partition("/")
    if not bucket:
        return None
    return bucket, prefix


def read_s3_parquet_as_of(ticket: Mapping[str, Any]) -> Optional[str]:
    """A CONTENT IDENTITY for a parquet dataset: the digest of its listing.

    sha256 over the sorted ``(key, ETag, size)`` of every object under the
    prefix. Measured properties, each of which is why it is this and not
    something simpler:

    * **It is genuinely read at authorize time.** Nothing is cached and nothing
      is advertised; the digest describes the bytes the consumer is about to
      read.
    * **It is exact.** Equal digest if and only if the same bytes will be read.
      An ETag is the store's own content hash of each object, so this is a hash
      of hashes rather than of names.
    * **It carries no date**, which is the point. An object's ``LastModified``
      was refused by name because the ticket has no honest label for it -- there
      are exactly two ``as_of_source`` values and a write time is neither, so it
      would travel as a version while describing a file operation. A digest
      cannot be misread that way: measured, an identical rewrite of the same
      bytes leaves the digest unchanged, which is the *proof* that no clock
      reading is in it.
    * **It needs only ``s3:ListBucket`` on the prefix**, which both s3 paths
      already grant, prefix-conditioned. No policy widening.
    * **Its one over-report is in the safe direction.** A repack into different
      part sizes changes the digest without changing the data, so the reader can
      say "different" about data that is the same. It can never say "same"
      about data that differs, which is the direction that would be a lie.

    Declines (returns ``None``) rather than guessing: a prefix with no objects,
    a non-s3 ``physical_uri``, a ticket with no usable credential, and a prefix
    wider than the object bound. A declined read still lets a producer's
    advertised version through, labelled as advertised.
    """
    parts = _s3_parts(str(ticket.get("physical_uri") or ""))
    if parts is None:
        return None
    bucket, prefix = parts

    credentials = ticket.get("credentials") or {}
    key_id = credentials.get("aws_access_key_id")
    secret = credentials.get("aws_secret_access_key")
    if not key_id or not secret:
        # NEVER fall through to the broker's ambient identity. boto3 would
        # happily find one in the environment and the reader would work, while
        # reading with a credential the consumer does not hold -- the one thing
        # the after-resolution placement of the stamp guarantees cannot happen.
        logger.debug(
            "as-of: ticket for s3://%s carries no minted key pair; declining "
            "rather than reading with the broker's own identity.", bucket,
        )
        return None

    import boto3  # noqa: PLC0415 - lazy: a driver import must not block start
    from botocore.config import Config  # noqa: PLC0415

    client = boto3.client(
        "s3",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        aws_session_token=credentials.get("aws_session_token") or None,
        endpoint_url=credentials.get("aws_endpoint_url") or None,
        region_name=credentials.get("aws_region") or None,
        config=Config(
            connect_timeout=_BOUND_CONNECT_SECONDS,
            read_timeout=_BOUND_CONNECT_SECONDS,
            # One attempt. A retry inside an authorize call multiplies the
            # bound by the attempt count, and an as-of is not worth waiting
            # twice for.
            retries={"max_attempts": 1},
        ),
    )

    rows = []
    token: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {
            "Bucket": bucket, "Prefix": prefix, "MaxKeys": _S3_PAGE_SIZE,
        }
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for obj in page.get("Contents") or ():
            rows.append((obj["Key"], str(obj.get("ETag") or ""), int(obj.get("Size") or 0)))
        if len(rows) > _BOUND_S3_MAX_OBJECTS:
            logger.warning(
                "as-of: s3://%s/%s holds more than %d objects; declining the "
                "as-of rather than paging through it inside an authorize call.",
                bucket, prefix, _BOUND_S3_MAX_OBJECTS,
            )
            return None
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
        if not token:
            break

    if not rows:
        # An answer, not a failure: there is nothing under this prefix, so
        # there is no content to identify.
        return None

    digest = hashlib.sha256()
    for key, etag, size in sorted(rows):
        # A separator that cannot appear in any of the three fields, so two
        # different listings cannot render to one string.
        digest.update(f"{key}\x00{etag}\x00{size}\n".encode("utf-8"))
    return LISTING_DIGEST_PREFIX + digest.hexdigest()


# ── table-backed: postgres ─────────────────────────────────────────────────

def _pg_host_port(physical_uri: str) -> Optional[Tuple[str, Optional[int]]]:
    """``postgres://host:port/schema/table`` -> ``(host, port)``, or None."""
    if "://" not in physical_uri:
        return None
    _, _, remainder = physical_uri.partition("://")
    host_port, _, rest = remainder.partition("/")
    if not host_port or not rest:
        return None
    host, _, port = host_port.partition(":")
    if not host:
        return None
    try:
        return host, int(port) if port else None
    except ValueError:
        return None


def read_postgres_as_of(ticket: Mapping[str, Any]) -> Optional[str]:
    """The cluster's current WAL position, as the server wrote it.

    ``pg_current_wal_lsn()``, carried verbatim -- ``0/197EC60``, not reformatted
    and not parsed. Measured under a **real minted role** rather than as admin,
    which is the only measurement worth anything here:

    * **Callable by the plain minted non-superuser role**, which was the open
      question and the answer was better than predicted. No grant to widen.
    * **Exact, and already text the server chose.** It travels under the
      module's standing rule that a version is carried as the source wrote it.
    * **Quiet means unchanged** -- measured: two reads with no write between
      them return the identical value.
    * **It carries no date**, so nothing can read it as freshness.
    * **It never wraps.** ``pg_snapshot_xmax(pg_current_snapshot())`` was the
      obvious alternative and is derived from 32-bit transaction ids, so it
      wraps; a version that comes round again is not a version.
    * **``txid_current()`` is excluded because it ASSIGNS an xid** -- measured,
      it moved the snapshot's xmax. A reading that changes the thing it reads is
      not a reading, and this one consumes a finite resource.

    **Its one honest limit, measured and stated rather than hidden: it is
    cluster-scoped, not table-scoped.** A commit to an unrelated table advances
    it, so an unequal LSN does not mean this table changed. It errs in the safe
    direction -- equal LSN implies the read sees identical data, unequal says
    nothing -- and postgres exposes no per-table version to this role: a
    resettable counter, a vacuum horizon, and a function unavailable without
    ``track_commit_timestamp`` were the three candidates, and none is a version.

    Two further limits, deliberately not handled rather than guessed at. On a
    standby this function raises (recovery is in progress), which degrades to
    ``reader_failed``; ``pg_last_wal_replay_lsn()`` is arguably the right answer
    there and has not been measured, so it is not shipped. And the value is an
    identity, not an order: compared as text it sorts wrongly.

    Declines rather than reading with anything of the broker's own: a ticket
    with no minted username and password is the unminted path, and there is no
    credential on it to read with.
    """
    parts = _pg_host_port(str(ticket.get("physical_uri") or ""))
    if parts is None:
        return None
    host, port = parts

    credentials = ticket.get("credentials") or {}
    user = credentials.get("username")
    password = credentials.get("password")
    database = credentials.get("database")
    if not user or not password or not database:
        logger.debug(
            "as-of: ticket for %s carries no minted role; declining rather "
            "than connecting as the broker.", host,
        )
        return None

    import psycopg2  # noqa: PLC0415 - lazy: the broker extra may be absent

    kwargs: Dict[str, Any] = {
        "host": host,
        "dbname": database,
        "user": user,
        "password": password,
        # BOTH bounds, because they stop different things: the first bounds an
        # unreachable host, the second a server that accepts the connection and
        # then does not answer. Either alone leaves the other case unbounded.
        "connect_timeout": _BOUND_CONNECT_SECONDS,
        "options": "-c statement_timeout=%d" % _BOUND_STATEMENT_MS,
    }
    if port is not None:
        kwargs["port"] = port

    conn = psycopg2.connect(**kwargs)
    try:
        with conn.cursor() as cur:
            # ::text so the driver hands back what the server printed, rather
            # than any adaptation of a pg_lsn.
            cur.execute("SELECT pg_current_wal_lsn()::text")
            row = cur.fetchone()
    finally:
        # Closed on every path, including the bound firing. A reader that
        # leaked a connection per authorize would exhaust the server's slots
        # and take the read door down with it -- failing the read by another
        # road.
        conn.close()
    if not row:
        return None
    return row[0]


# ── the one table ──────────────────────────────────────────────────────────

# source_type -> reader. This dict is what populates
# ``ticket_identity.AVAILABLE_AS_OF_READERS``, which is read by three things
# (the chart's constant at render, the startup check at start, ``/health`` at
# runtime). There is ONE table, and every other answer is derived from it: a
# second list anywhere disagrees silently while each copy looks correct.
SHIPPED_AS_OF_READERS: Dict[str, Callable[[Mapping[str, Any]], Optional[str]]] = {
    "s3_parquet": read_s3_parquet_as_of,
    "postgres": read_postgres_as_of,
}
