#!/usr/bin/env python
"""Do the two as-of readers actually read, against a real store of each kind?

invincible-agent #529 item 2 ships an as-of reader for a file-backed asset and
one for a table-backed asset, under the ruling that **no reader ships until it
can be exercised against a real table of that kind.** This is that exercise.
``dag_tools_tests/test_as_of_readers.py`` is the other half and says so in its
own docstring; neither half is a substitute for the other.

What the unit tests cannot do, which is why this file exists:

  * They cannot prove libpq honours the bound. A kwarg captured off a driver
    double proves the reader asked. It says nothing about whether asking works,
    and an ``options`` string the server rejects fails every connection.
  * They cannot prove the plain, non-superuser credential a consumer receives
    is allowed to read the version at all. That is the whole no-escalation
    property and it is a property of the *server's* grants, not of our code.
    ClickHouse is the measured counter-example: its minted user cannot read
    ``system.parts`` (code 497), which is why no ClickHouse reader ships.
  * They cannot prove a reader declines rather than falling back on the ambient
    identity, because in a test there is no ambient identity to fall back on.
    Here there is a real one, deliberately left working in the environment, and
    the reader has to refuse it anyway. That is the most important line in this
    file: it is the one arm where a working reader and a privilege escalation
    are indistinguishable from the outside.

Every refusal is paired with a POSITIVE CONTROL. Without one, "declined" is
indistinguishable from a broken reader, a wrong URI or a typo -- the same
observation for opposite reasons.

    python scripts/verify_as_of_readers.py postgres --host 127.0.0.1 --port 15432
    python scripts/verify_as_of_readers.py s3 --bucket probe-lake \\
        --endpoint http://127.0.0.1:9000 --prefix probe/as_of

Nothing is left behind: the postgres run creates a scratch table and a scratch
role and drops both; the s3 run writes under a prefix of its own and deletes
what it wrote. No credential is read from anywhere but the environment and the
flags, and none is printed. CI in this repo names no secret and none is added
for this, which is why this is a script behind an operator's hand rather than a
test behind an env gate that could never run anywhere.

---------------------------------------------------------------------------
PRE-REGISTERED PREDICTIONS. Written before the first run against a deployed
server, so the result is able to disagree. A prediction recorded afterwards
cannot surprise anyone.

  P1. The reader returns exactly the text the server prints for
      pg_current_wal_lsn() -- the same string, not merely the same position.
      High. The standing rule is that a version is carried as the source wrote
      it, and this is where "as written" is checked against a server instead of
      against our own double. Bracketed by two admin reads, because the LSN can
      advance between two statements and that is P2's business, not P1's.

  P2. Two reads with nothing written between them are EQUAL. Medium. The LSN is
      cluster-scoped, so autovacuum or another database on the same cluster can
      advance it with nothing of ours having changed. If this comes back
      unequal the reader is not wrong -- it over-reports change, which is the
      safe direction -- but the prediction is, and the honest reading is that
      equality means identical data while inequality means only that something,
      somewhere on the cluster, moved.

  P3. A committed write MOVES it. High. Without this the reader could return a
      constant and P1 and P2 would both still pass.

  P4. The server orders the two LSNs 0/9 and 0/10 the OPPOSITE way to how text
      orders them. High -- measured locally, re-measured here against the
      deployed version, because "they are an identity, not an order" rests on
      it and a rule resting on an unverified measurement is a rule nobody
      should follow.

  P5. A plain, non-superuser role can call pg_current_wal_lsn(). Medium-high.
      It is not superuser-only, but a hardened cluster can REVOKE EXECUTE from
      PUBLIC -- and if this deployment has, then this reader cannot ship for it
      and that is the finding, not a bug to work around.

  P6. With a WORKING ambient libpq chain in the environment (PGHOST, PGUSER and
      PGPASSWORD all set and valid), a ticket carrying no credential is still
      declined. High, and the one that must not be wrong.

  P7. An unroutable host is given up on within about a second of the configured
      connect bound, rather than hanging. High -- and WRONG AS FIRST WRITTEN,
      recorded here rather than quietly corrected. It predicted the reader
      would answer ``None``; against a real unroutable host the reader RAISES
      ``psycopg2.OperationalError`` and the first run of this script died on
      it. The reader is right and the prediction was wrong. The rule is that a
      reading may not fail the read, and the place that holds it is
      ``ticket_identity.resolve_as_of``, which catches every exception and
      issues the ticket with no as-of -- deliberately NOT each reader, because
      a reader that swallowed its own driver errors would also swallow the one
      case that must stay visible, a reader whose credential is wrong. So P7
      is now measured where the property actually lives: through
      ``resolve_as_of``, asserting both the bound AND that the read was not
      failed. That is a better measurement than the one I set out to make.

  P8. The statement bound the module ships is accepted by libpq and actually
      fires. Medium-high. This one is measured through the module's own option
      string rather than through the reader's own statement, because
      pg_current_wal_lsn() cannot be made slow -- so it proves the bound is
      real and does NOT prove the reader's own statement is subject to it.
      Stated rather than papered over.

  P9. The s3 digest is UNCHANGED by rewriting identical bytes. High. This is
      the proof that no object modification time is in the value -- a write
      time was refused by name, because the ticket has exactly two
      as_of_source values and a write time is neither of them.

  P10. The digest CHANGES when an object is added. High, and the partner P9
      needs: without it, a reader returning a constant satisfies P9.

  P11. A credential the broker actually MINTED can list the prefix, so no
      policy has to be widened for this reader. Medium-high. The credential is
      supplied by the operator from a real /authorize response rather than
      minted here, so this measures the thing a consumer holds and not a
      reimplementation of it. If it fails, the finding is that the reader
      needs s3:ListBucket granted, and it does not ship until it is.

      PARTLY SETTLED WITHOUT A BROKER, and the part that is settled is the
      part that could break silently. Both minting paths grant s3:ListBucket
      on the bucket under a StringLike condition on s3:prefix, and the
      condition is built from the SAME physical_uri the reader parses -- so
      whether the reader's request satisfies it is decidable, and it is
      decided in CI by
      test_the_readers_listing_is_inside_the_policy_the_broker_mints over
      every URI shape the two parsers read differently. What is left for a
      real /authorize is the STS round trip itself: that the assume-role call
      returns, that the session policy is applied as written, and that the
      store enforces it the way the document reads. A local S3 cannot stand in
      for that, which is why this stays INFO here rather than being marked
      passed on the strength of the CI half.

  P12. With a WORKING ambient AWS chain in the environment, a ticket carrying
      no credential is still declined. High. The same property as P6, against
      the more eager of the two credential chains.
---------------------------------------------------------------------------
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dag_tools.domain_broker import as_of_readers as readers  # noqa: E402
from dag_tools.domain_broker import ticket_identity as ti  # noqa: E402

TABLE = "as_of_probe"

_FAILURES: list = []
_UNTESTED: list = []


def _p(label, ok, detail=""):
    mark = {True: "PASS", False: "FAIL", None: "INFO"}[ok]
    print("  [%s] %s%s" % (mark, label, (" -- %s" % detail) if detail else ""))
    if ok is False:
        _FAILURES.append(label)
    elif ok is None:
        _UNTESTED.append(label)


def _ticket(uri, **credentials):
    return {"physical_uri": uri, "credentials": credentials}


# -- postgres: the table-backed reader -------------------------------------

def verify_postgres(args) -> int:
    import psycopg2

    admin_user = os.getenv("PG_USER", "postgres")
    admin_password = os.getenv("PG_PASSWORD", "")
    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.database,
        user=admin_user, password=admin_password,
    )
    conn.autocommit = True
    role = "as_of_probe_%s" % uuid.uuid4().hex[:8]
    role_password = uuid.uuid4().hex
    uri = "postgres://%s:%d/public/%s" % (args.host, args.port, TABLE)

    def admin_lsn():
        with conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            return cur.fetchone()[0]

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS %s" % TABLE)
        cur.execute("CREATE TABLE %s (id int)" % TABLE)
        cur.execute("INSERT INTO %s VALUES (1)" % TABLE)
        cur.execute("CREATE ROLE %s LOGIN PASSWORD %%s" % role, (role_password,))
        cur.execute("GRANT SELECT ON %s TO %s" % (TABLE, role))

    consumer = _ticket(
        uri, username=role, password=role_password, database=args.database,
    )

    print("\n1. the reader reads, with the credential a CONSUMER would hold")
    print("   P5: a plain non-superuser role, which is the no-escalation property")
    before = admin_lsn()
    value = readers.read_postgres_as_of(consumer)
    after = admin_lsn()
    _p("P5 a plain non-superuser role may read the version", value is not None,
       "DECLINED -- this cluster has revoked it, so no reader ships here"
       if value is None else "read as %s" % role)

    if value is None:
        _p("P1 carried exactly as the server wrote it", None, "P5 did not read")
    else:
        _p("P1 carried exactly as the server wrote it", value in (before, after),
           "reader=%s, bracketed by %s and %s" % (value, before, after))

        print("\n2. what equality and inequality are each allowed to mean")
        quiet = readers.read_postgres_as_of(consumer)
        _p("P2 quiet means unchanged", quiet == value,
           "equal" if quiet == value else
           "MOVED with nothing of ours written (%s -> %s); the reader "
           "over-reports change, which is the safe direction, but equality is "
           "the only half that means anything here" % (value, quiet))

        with conn.cursor() as cur:
            cur.execute("INSERT INTO %s VALUES (2)" % TABLE)
        moved = readers.read_postgres_as_of(consumer)
        _p("P3 a committed write moves it", moved != quiet,
           "%s -> %s" % (quiet, moved))

        print("\n3. P4: the value is an IDENTITY, not an order")
        with conn.cursor() as cur:
            cur.execute("SELECT '0/9'::pg_lsn < '0/10'::pg_lsn")
            as_lsn = cur.fetchone()[0]
        as_text = "0/9" < "0/10"
        _p("P4 text ordering disagrees with the server's", as_lsn != as_text,
           "as pg_lsn: %s, as text: %s -- compare for equality, or in the "
           "server's own type" % (as_lsn, as_text))

    print("\n4. P6: the ambient identity is NOT used when the ticket carries none")
    print("   the environment is left WORKING on purpose; refusing it is the point")
    saved = {k: os.environ.get(k) for k in
             ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE")}
    os.environ.update({
        "PGHOST": str(args.host), "PGPORT": str(args.port),
        "PGUSER": admin_user, "PGPASSWORD": admin_password,
        "PGDATABASE": args.database,
    })
    try:
        ambient = psycopg2.connect("")
        ambient.close()
    except Exception as exc:
        _p("P6 a ticket with no credential is declined", None,
           "the ambient chain is not usable here (%s), so a decline would "
           "prove nothing -- untested, not passed" % type(exc).__name__)
    else:
        _p("control: the ambient chain IS usable", True,
           "so a decline below is a refusal, not a failure")
        declined = readers.read_postgres_as_of(_ticket(uri))
        _p("P6 a ticket with no credential is declined", declined is None,
           "READ ANYWAY as %s -- this is the escalation" % admin_user
           if declined is not None else "declined")
    finally:
        for key, was in saved.items():
            if was is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = was

    print("\n5. P7/P8: the bounds are real, not merely requested")
    print("   through resolve_as_of, because the reader RAISES, and that seam")
    print("   is what turns a raise into an absent as-of, not a failed read")
    ti.register_as_of_reader("postgres", readers.read_postgres_as_of)
    started = time.monotonic()
    as_of, source, unavailable = ti.resolve_as_of("postgres", _ticket(
        "postgres://192.0.2.1:5432/public/%s" % TABLE,
        username=role, password=role_password, database=args.database,
    ))
    elapsed = time.monotonic() - started
    bound = readers._BOUND_CONNECT_SECONDS
    _p("P7 an unroutable host is given up on near the bound",
       elapsed < bound + 2.0,
       "%.2fs against a connect bound of %ds" % (elapsed, bound))
    _p("P7 the failed reading does not fail the read",
       (as_of, source, unavailable) == (None, None, ti.ABSENT_READER_FAILED),
       "as_of=%r source=%r unavailable=%r" % (as_of, source, unavailable))

    options = "-c statement_timeout=%d" % readers._BOUND_STATEMENT_MS
    slept = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.database,
        user=role, password=role_password,
        connect_timeout=readers._BOUND_CONNECT_SECONDS, options=options,
    )
    started = time.monotonic()
    try:
        with slept.cursor() as cur:
            cur.execute("SELECT pg_sleep(30)")
        fired = False
    except Exception:
        fired = True
    cut_off = time.monotonic() - started
    slept.close()
    _p("P8 the shipped statement bound is accepted and fires", fired,
       "cut off at %.2fs against %dms; measured through the module's own "
       "option string, NOT through the reader's statement, which cannot be "
       "made slow" % (cut_off, readers._BOUND_STATEMENT_MS))

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS %s" % TABLE)
        cur.execute("DROP ROLE IF EXISTS %s" % role)
    conn.close()
    print("\ncleaned up.")
    return 1 if _FAILURES else 0


# -- s3: the file-backed reader --------------------------------------------

def verify_s3(args) -> int:
    import boto3

    session = boto3.session.Session()
    admin = session.client("s3", endpoint_url=args.endpoint)
    prefix = "%s/%s" % (args.prefix.rstrip("/"), uuid.uuid4().hex[:8])
    uri = "s3://%s/%s" % (args.bucket, prefix)

    def put(key, body):
        admin.put_object(Bucket=args.bucket, Key="%s/%s" % (prefix, key), Body=body)

    frozen = session.get_credentials().get_frozen_credentials()
    consumer = _ticket(
        uri,
        aws_access_key_id=frozen.access_key,
        aws_secret_access_key=frozen.secret_key,
        aws_session_token=frozen.token,
        aws_endpoint_url=args.endpoint,
        aws_region=session.region_name,
    )

    put("part-0.parquet", b"one")

    print("\n1. the reader reads a real prefix")
    value = readers.read_s3_parquet_as_of(consumer)
    _p("a real listing is identified",
       value is not None and value.startswith(readers.LISTING_DIGEST_PREFIX),
       "declined" if value is None else value)

    if value is None:
        _p("P9 an identical rewrite leaves it unchanged", None, "nothing was read")
        _p("P10 an added object changes it", None, "nothing was read")
    else:
        print("\n2. P9/P10: what the digest is and is not made of")
        put("part-0.parquet", b"one")          # identical bytes, new write time
        rewritten = readers.read_s3_parquet_as_of(consumer)
        _p("P9 an identical rewrite leaves it unchanged", rewritten == value,
           "unchanged -- no write time is in the value" if rewritten == value
           else "MOVED; an object modification time has got into the digest")

        put("part-1.parquet", b"two")
        grown = readers.read_s3_parquet_as_of(consumer)
        _p("P10 an added object changes it", grown != rewritten,
           "the control P9 needs: without it a constant would satisfy P9")

    print("\n3. P11: a credential the broker MINTED can list the prefix")
    minted_key = args.minted_key or os.getenv("DAGTOOLS_E2E_MINTED_KEY")
    minted_secret = args.minted_secret or os.getenv("DAGTOOLS_E2E_MINTED_SECRET")
    minted_token = args.minted_token or os.getenv("DAGTOOLS_E2E_MINTED_TOKEN")
    if not (minted_key and minted_secret):
        _p("P11 the minted credential lists the prefix", None,
           "no minted credential supplied (--minted-key/--minted-secret, from "
           "a real /authorize response for this prefix) -- untested, not "
           "passed. The grant and its prefix condition are settled in CI; "
           "what is untested here is the STS round trip and the store's own "
           "enforcement of the session policy")
    else:
        with_minted = readers.read_s3_parquet_as_of(_ticket(
            uri,
            aws_access_key_id=minted_key,
            aws_secret_access_key=minted_secret,
            aws_session_token=minted_token,
            aws_endpoint_url=args.endpoint,
            aws_region=session.region_name,
        ))
        _p("P11 the minted credential lists the prefix", with_minted is not None,
           "no policy widening needed" if with_minted is not None
           else "DECLINED -- the reader needs s3:ListBucket on the minted "
                "policy before it ships")

    print("\n4. P12: the ambient chain is NOT used when the ticket carries none")
    try:
        admin.list_objects_v2(Bucket=args.bucket, Prefix=prefix, MaxKeys=1)
    except Exception as exc:
        _p("P12 a ticket with no credential is declined", None,
           "the ambient chain is not usable here (%s), so a decline would "
           "prove nothing -- untested, not passed" % type(exc).__name__)
    else:
        _p("control: the ambient chain IS usable", True,
           "so a decline below is a refusal, not a failure")
        declined = readers.read_s3_parquet_as_of(
            _ticket(uri, aws_endpoint_url=args.endpoint)
        )
        _p("P12 a ticket with no credential is declined", declined is None,
           "READ ANYWAY with the broker's own identity -- this is the escalation"
           if declined is not None else "declined")

    listed = admin.list_objects_v2(
        Bucket=args.bucket, Prefix=prefix).get("Contents", [])
    for obj in listed:
        admin.delete_object(Bucket=args.bucket, Key=obj["Key"])
    print("\ncleaned up (%d objects)." % len(listed))
    return 1 if _FAILURES else 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Exercise the shipped as-of readers against a real store.")
    ap.add_argument("kind", choices=["postgres", "s3"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--database", default="postgres")
    ap.add_argument("--endpoint", default=None,
                    help="s3 endpoint url; omit for AWS")
    ap.add_argument("--bucket", default=None)
    ap.add_argument("--prefix", default="as-of-probe")
    ap.add_argument("--minted-key", default=None,
                    help="access key id from a real /authorize response (P11)")
    ap.add_argument("--minted-secret", default=None)
    ap.add_argument("--minted-token", default=None)
    args = ap.parse_args()

    if args.kind == "s3" and not args.bucket:
        ap.error("--bucket is required for the s3 reader")

    status = verify_postgres(args) if args.kind == "postgres" else verify_s3(args)

    if _FAILURES:
        print("\n%d prediction(s) did not hold:" % len(_FAILURES))
        for label in _FAILURES:
            print("  - %s" % label)
    else:
        print("\nevery prediction that could be tested held.")
    if _UNTESTED:
        print("%d untested (reported as INFO, never counted as a pass):"
              % len(_UNTESTED))
        for label in _UNTESTED:
            print("  - %s" % label)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
