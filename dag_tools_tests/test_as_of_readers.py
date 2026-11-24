"""The two shipped as-of readers -- invincible-agent #529 item 2.

What this module is and is NOT. It proves the readers' *semantics* and their
wiring: the digest's measured properties, the bounds reaching the driver, the
decline paths, and the one rule both readers share. It proves **nothing** about
whether a credential works against a real store, and it is deliberately not
dressed up as if it did -- that half is ``scripts/verify_as_of_readers.py``,
run against real engines behind an env gate, because this repo's CI names no
secret. A reader whose only evidence is that it parses is the half-measure the
acceptance gate exists to refuse, and a unit test that quietly claimed to be
the exercise would be exactly that half-measure with a passing mark on it.

The s3 half runs against moto, which is a real listing of real objects through
the real botocore stack -- so the digest's properties are measured here rather
than asserted about an imagined response. The postgres half cannot be: there is
no in-process postgres, so its connection is taken through an injected driver
double, which is honest about what it covers (every kwarg the reader hands the
driver, and that the connection is closed on every path) and about what it
cannot (that libpq honours the bound -- measured in the verify script instead).
"""

import hashlib
import logging
import subprocess
import sys
import types
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from dag_tools.domain_broker import as_of_readers as readers

ROOT = Path(__file__).resolve().parents[1]

BUCKET = "as-of-probe"


class _Reached(Exception):
    """Raised by a driver double to say the reader got that far."""


def _ticket(uri, **credentials):
    return {"physical_uri": uri, "credentials": credentials}


def _s3_ticket(prefix="data", **overrides):
    credentials = {
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
        "aws_region": "us-east-1",
    }
    credentials.update(overrides)
    return _ticket("s3://%s/%s" % (BUCKET, prefix), **credentials)


@pytest.fixture
def s3():
    """A real bucket through the real botocore stack, in process."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


# --------------------------------------------------------------------------
# The digest's measured properties
# --------------------------------------------------------------------------

def test_a_listing_is_identified_and_the_value_says_what_it_is(s3):
    """The label is part of the value, and that is not decoration.

    This reader COMPUTES its value, so unlike a version a source wrote, the
    representation is the reader's to choose -- and a labelled token cannot be
    parsed as a number by a careless consumer, which is the hazard that
    silently corrupts a nineteen-digit snapshot id into a plausible wrong one.
    """
    s3.put_object(Bucket=BUCKET, Key="data/part-0.parquet", Body=b"one")

    value = readers.read_s3_parquet_as_of(_s3_ticket())

    assert value is not None
    assert value.startswith(readers.LISTING_DIGEST_PREFIX)
    assert len(value) == len(readers.LISTING_DIGEST_PREFIX) + 64
    with pytest.raises(ValueError):
        float(value)


def test_an_identical_rewrite_leaves_the_digest_unchanged(s3):
    """The PROOF that no clock reading is in it.

    An object's ``LastModified`` was refused by name because the ticket has no
    honest label for a write time -- there are exactly two ``as_of_source``
    values and a write time is neither. This is what shows the refusal was
    carried out rather than merely intended: the same bytes written again move
    every modification time under the prefix and the as-of does not budge.
    """
    s3.put_object(Bucket=BUCKET, Key="data/part-0.parquet", Body=b"one")
    before = readers.read_s3_parquet_as_of(_s3_ticket())

    s3.put_object(Bucket=BUCKET, Key="data/part-0.parquet", Body=b"one")
    after = readers.read_s3_parquet_as_of(_s3_ticket())

    assert after == before


def test_changed_bytes_at_the_same_key_change_the_digest(s3):
    """An ETag is the store's content hash, so this is a hash of hashes.

    The key did not move and the object count did not change: a digest over
    names alone would report the same value for data that differs, which is the
    one direction an as-of must never be wrong in.
    """
    s3.put_object(Bucket=BUCKET, Key="data/part-0.parquet", Body=b"one")
    before = readers.read_s3_parquet_as_of(_s3_ticket())

    s3.put_object(Bucket=BUCKET, Key="data/part-0.parquet", Body=b"two")

    assert readers.read_s3_parquet_as_of(_s3_ticket()) != before


def test_an_appended_object_changes_the_digest(s3):
    s3.put_object(Bucket=BUCKET, Key="data/part-0.parquet", Body=b"one")
    before = readers.read_s3_parquet_as_of(_s3_ticket())

    s3.put_object(Bucket=BUCKET, Key="data/part-1.parquet", Body=b"two")

    assert readers.read_s3_parquet_as_of(_s3_ticket()) != before


def test_the_digest_is_the_same_whichever_page_size_it_arrived_in(s3, monkeypatch):
    """Paging is transport, not content.

    A digest that depended on how many round trips the listing took would
    change when a prefix crossed a page boundary -- an as-of moving because the
    dataset grew past a thousand objects, with no data having changed.
    """
    for index in range(7):
        s3.put_object(Bucket=BUCKET, Key="data/part-%d.parquet" % index, Body=b"x")

    one_page = readers.read_s3_parquet_as_of(_s3_ticket())

    monkeypatch.setattr(readers, "_S3_PAGE_SIZE", 2)
    many_pages = readers.read_s3_parquet_as_of(_s3_ticket())

    assert many_pages == one_page


def test_the_order_objects_came_back_in_does_not_reach_the_digest(s3):
    """Sorted before hashing, so a store's listing order is not in the value."""
    for key in ("data/b", "data/a", "data/c"):
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")
    value = readers.read_s3_parquet_as_of(_s3_ticket())

    rows = sorted(
        (obj["Key"], str(obj["ETag"]), int(obj["Size"]))
        for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix="data")["Contents"]
    )
    expected = hashlib.sha256()
    for key, etag, size in rows:
        expected.update(f"{key}\x00{etag}\x00{size}\n".encode("utf-8"))

    assert value == readers.LISTING_DIGEST_PREFIX + expected.hexdigest()


def test_a_sibling_prefix_does_not_reach_the_digest(s3):
    """The digest identifies the dataset the ticket names, not the bucket.

    Without this, a write to any other dataset in the same bucket would move
    this one's as-of, and a consumer comparing two receipts would be told its
    data changed because somebody else's did.
    """
    s3.put_object(Bucket=BUCKET, Key="data/part-0.parquet", Body=b"one")
    before = readers.read_s3_parquet_as_of(_s3_ticket())

    s3.put_object(Bucket=BUCKET, Key="other/part-0.parquet", Body=b"two")

    assert readers.read_s3_parquet_as_of(_s3_ticket()) == before


# --------------------------------------------------------------------------
# Where the s3 reader declines
# --------------------------------------------------------------------------

def test_an_empty_prefix_is_an_answer_of_none_not_a_failure(s3):
    """There is nothing under the prefix, so there is no content to identify.

    Returning a digest of the empty listing would be a stable, confident value
    for a dataset that is not there -- and two different absent datasets would
    carry the identical as-of.
    """
    assert readers.read_s3_parquet_as_of(_s3_ticket()) is None


def test_a_prefix_wider_than_the_bound_declines_rather_than_paging_on(
    s3, monkeypatch, caplog
):
    """Slow is the failure mode this module is least willing to buy an as-of with.

    A reader runs inside the authorize call, so paging through a prefix with a
    hundred thousand objects would turn one authorize into a hundred requests.
    The decline is logged at WARNING, because an operator who sees no as-of for
    one dataset needs to be able to find out why without reading this file.
    """
    monkeypatch.setattr(readers, "_BOUND_S3_MAX_OBJECTS", 2)
    monkeypatch.setattr(readers, "_S3_PAGE_SIZE", 2)
    for index in range(5):
        s3.put_object(Bucket=BUCKET, Key="data/part-%d.parquet" % index, Body=b"x")

    with caplog.at_level(logging.WARNING, logger=readers.logger.name):
        assert readers.read_s3_parquet_as_of(_s3_ticket()) is None

    assert "declining" in caplog.text


def test_control_the_same_prefix_under_the_bound_is_read(s3, monkeypatch):
    """Without this, the test above would pass on a reader that declines always."""
    monkeypatch.setattr(readers, "_BOUND_S3_MAX_OBJECTS", 10)
    monkeypatch.setattr(readers, "_S3_PAGE_SIZE", 2)
    for index in range(5):
        s3.put_object(Bucket=BUCKET, Key="data/part-%d.parquet" % index, Body=b"x")

    assert readers.read_s3_parquet_as_of(_s3_ticket()) is not None


@pytest.mark.parametrize(
    "uri",
    [
        "",
        "s3://",
        "postgres://host:5432/schema/table",
        "/mnt/share/data",
        "https://example.invalid/bucket/prefix",
    ],
)
def test_a_uri_that_is_not_an_s3_prefix_is_declined_rather_than_guessed(uri):
    assert readers.read_s3_parquet_as_of(_ticket(uri, aws_access_key_id="a",
                                                 aws_secret_access_key="b")) is None


def test_a_ticket_with_no_physical_uri_at_all_is_declined():
    assert readers.read_s3_parquet_as_of({}) is None


# --------------------------------------------------------------------------
# The rule both readers obey: decline, never read as the broker
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "reader, ticket",
    [
        (readers.read_s3_parquet_as_of, _s3_ticket(aws_access_key_id="",
                                                   aws_secret_access_key="")),
        (readers.read_s3_parquet_as_of, _ticket("s3://%s/data" % BUCKET)),
        (readers.read_postgres_as_of, _ticket("postgres://host:5432/s/t")),
        (readers.read_postgres_as_of, _ticket("postgres://host:5432/s/t",
                                              username="u", password="p")),
        (readers.read_postgres_as_of, _ticket("postgres://host:5432/s/t",
                                              token="legacy-placeholder")),
    ],
)
def test_a_ticket_with_no_usable_credential_is_declined(reader, ticket, monkeypatch):
    """The flip side of the no-escalation property, and the half that can go wrong.

    The stamp runs after resolution, so a reader is handed the minted, scoped,
    expiring credential and therefore cannot read anything the consumer could
    not. That holds only while a reader *declines* when there is nothing on the
    ticket: both drivers would happily find an ambient identity -- boto3 in the
    environment, libpq in ``PG*`` and ``.pgpass`` -- and the reader would appear
    to work while reading with a credential the consumer does not hold.

    So the assertion is not merely that the answer is ``None``: it is that no
    driver is reached at all. Both imports are replaced with something that
    raises, which is what makes "did not read" distinguishable from "read and
    got nothing".
    """
    for name in ("boto3", "psycopg2"):
        exploding = types.ModuleType(name)

        def _never(*args, **kwargs):
            raise AssertionError(
                "the reader reached a driver with no credential on the ticket"
            )

        exploding.client = _never
        exploding.connect = _never
        monkeypatch.setitem(sys.modules, name, exploding)

    assert reader(ticket) is None


@pytest.mark.parametrize(
    "reader, ticket",
    [
        (readers.read_s3_parquet_as_of, _s3_ticket()),
        (
            readers.read_postgres_as_of,
            _ticket(
                "postgres://host:5432/s/t",
                username="u",
                password="p",
                database="d",
            ),
        ),
    ],
)
def test_control_a_ticket_carrying_a_credential_does_reach_its_driver(
    reader, ticket, monkeypatch
):
    """Without this, the test above passes on a reader that declines everything.

    The doubles raise, so the measurement is which side of the decline the call
    stopped on -- and a reader that never reaches a driver at all would satisfy
    the refusal test while reading nothing, ever. Measured both ways: with a
    credential present each reader reaches its driver, without one neither does.
    """
    for name, attribute in (("boto3", "client"), ("psycopg2", "connect")):
        module = types.ModuleType(name)

        def _reached(*args, **kwargs):
            raise _Reached()

        setattr(module, attribute, _reached)
        monkeypatch.setitem(sys.modules, name, module)

    with pytest.raises(_Reached):
        reader(ticket)


# --------------------------------------------------------------------------
# postgres: the parse, the bounds, and the value
# --------------------------------------------------------------------------

class _Cursor:
    def __init__(self, rows, raises=None):
        self._rows = rows
        self._raises = raises
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        if self._raises is not None:
            raise self._raises

    def fetchone(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Connection:
    def __init__(self, rows, raises=None):
        self.cursor_obj = _Cursor(rows, raises)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def _driver(monkeypatch, rows=("0/197EC60",), raises=None):
    """An injected psycopg2, because there is no in-process postgres.

    It covers exactly what it can: every kwarg the reader hands the driver, the
    statement it issues, and that the connection is closed on every path. It
    covers nothing about whether libpq honours the bound -- that was measured
    against a real server (1.01s against an unroutable host, 1.50s on a
    ``pg_sleep(10)``) and is re-measured by the verify script, not here.
    """
    captured = {}
    connection = _Connection(rows, raises)

    def connect(**kwargs):
        captured.update(kwargs)
        return connection

    module = types.ModuleType("psycopg2")
    module.connect = connect
    monkeypatch.setitem(sys.modules, "psycopg2", module)
    return captured, connection


def test_the_value_is_carried_exactly_as_the_server_wrote_it(monkeypatch):
    """The module's standing rule, and the reason it is a rule.

    Reformatting makes two equal watermarks look different, which turns an
    identity into a coin flip. ``0/197EC60`` is what the server printed and it
    is what the receipt carries -- not zero-padded, not split, not upper-cased
    by us because it happens to already be.
    """
    _driver(monkeypatch, rows=("0/197EC60",))

    assert readers.read_postgres_as_of(
        _ticket("postgres://db:5432/s/t", username="u", password="p", database="d")
    ) == "0/197EC60"


def test_the_statement_asks_for_text_so_the_driver_cannot_adapt_it(monkeypatch):
    """``::text`` is what makes the sentence above true at the driver seam."""
    _, connection = _driver(monkeypatch)

    readers.read_postgres_as_of(
        _ticket("postgres://db:5432/s/t", username="u", password="p", database="d")
    )

    assert connection.cursor_obj.statements == ["SELECT pg_current_wal_lsn()::text"]


def test_both_bounds_reach_the_driver_because_they_stop_different_things(monkeypatch):
    """Either alone leaves the other case unbounded.

    ``connect_timeout`` bounds a host that never answers; ``statement_timeout``
    bounds a server that accepts the connection and then does not answer. A
    reader runs inside the authorize call, so an unbounded one parks every
    authorize for that backend until the client gives up -- a refusal wearing a
    timeout's clothes, which is the half of the contract that
    ``resolve_as_of``'s exception handling does not cover.
    """
    captured, _ = _driver(monkeypatch)

    readers.read_postgres_as_of(
        _ticket("postgres://db:5432/s/t", username="u", password="p", database="d")
    )

    assert captured["connect_timeout"] == readers._BOUND_CONNECT_SECONDS
    assert captured["options"] == (
        "-c statement_timeout=%d" % readers._BOUND_STATEMENT_MS
    )


def test_the_connection_is_closed_even_when_the_statement_raises(monkeypatch):
    """A leak per authorize would take the read door down by another road.

    Exhausting the server's connection slots fails every read, including the
    ones whose as-of nobody asked for -- an instrument breaking the thing it
    measures. The raise still propagates, because degrading to an absent as-of
    is ``resolve_as_of``'s job and swallowing it here would hide a reader that
    has stopped working.
    """
    _, connection = _driver(monkeypatch, raises=RuntimeError("canceling statement"))

    with pytest.raises(RuntimeError):
        readers.read_postgres_as_of(
            _ticket("postgres://db:5432/s/t", username="u", password="p", database="d")
        )

    assert connection.closed is True


def test_an_empty_result_is_declined_rather_than_reported_as_a_version(monkeypatch):
    _driver(monkeypatch, rows=None)

    assert readers.read_postgres_as_of(
        _ticket("postgres://db:5432/s/t", username="u", password="p", database="d")
    ) is None


def test_a_uri_with_no_port_connects_on_the_drivers_own_default(monkeypatch):
    """A port this library invented would be a name from below with a number."""
    captured, _ = _driver(monkeypatch)

    readers.read_postgres_as_of(
        _ticket("postgres://db/s/t", username="u", password="p", database="d")
    )

    assert captured["host"] == "db"
    assert "port" not in captured


@pytest.mark.parametrize(
    "uri",
    [
        "",
        "db:5432/s/t",                      # no scheme
        "postgres://db:5432",               # names no relation
        "postgres:///s/t",                  # names no host
        "postgres://db:not-a-port/s/t",
    ],
)
def test_a_uri_this_reader_cannot_read_is_declined_rather_than_guessed(uri, monkeypatch):
    module = types.ModuleType("psycopg2")

    def _never(**kwargs):
        raise AssertionError("connected on a uri the reader could not parse")

    module.connect = _never
    monkeypatch.setitem(sys.modules, "psycopg2", module)

    assert readers.read_postgres_as_of(
        _ticket(uri, username="u", password="p", database="d")
    ) is None


# --------------------------------------------------------------------------
# The bounds themselves
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value, expected",
    [(None, 5), ("", 5), ("9", 9), (" 9 ", 9), ("nine", 5), ("0", 5), ("-1", 5)],
)
def test_a_malformed_bound_falls_back_rather_than_refusing_to_import(
    value, expected, monkeypatch
):
    """A typo in a knob must not take the read door down.

    This one only makes a reading faster or slower, so refusing to import over
    it would trade a degraded as-of for no reads at all. A zero is folded in
    with the malformed cases deliberately: ``connect_timeout=0`` means *no
    timeout* to libpq, so reading it as written would turn the bound off while
    looking like a very tight one.
    """
    if value is None:
        monkeypatch.delenv("BROKER_AS_OF_PROBE", raising=False)
    else:
        monkeypatch.setenv("BROKER_AS_OF_PROBE", value)

    assert readers._env_int("BROKER_AS_OF_PROBE", 5) == expected


def test_a_bound_is_read_once_at_import_not_inside_an_authorize_call(monkeypatch):
    """Where it is read is the property, not what it says.

    A bound consulted per call makes a config edit able to change the behaviour
    of a running pod between two authorizes, and puts a parse of an operator's
    string inside the one code path that may not fail a read.
    """
    monkeypatch.setenv("BROKER_AS_OF_CONNECT_TIMEOUT_SEC", "99")
    captured, _ = _driver(monkeypatch)

    readers.read_postgres_as_of(
        _ticket("postgres://db:5432/s/t", username="u", password="p", database="d")
    )

    assert captured["connect_timeout"] == readers._BOUND_CONNECT_SECONDS != 99


# --------------------------------------------------------------------------
# What the module does not ship, and what importing it does not cost
# --------------------------------------------------------------------------

def test_the_table_ships_exactly_the_two_measured_readers():
    assert sorted(readers.SHIPPED_AS_OF_READERS) == ["postgres", "s3_parquet"]


@pytest.mark.parametrize("absent", ["clickhouse", "s3_delta", "s3_iceberg"])
def test_a_source_type_that_was_not_measured_ships_no_reader(absent):
    """Absent is a decision here, and for ClickHouse a measured one.

    Two independent measurements, either sufficient on its own: the minted user
    cannot read ``system.parts`` (code 497), so there is no ClickHouse as-of
    readable without breaking the no-escalation property; and
    ``max_block_number`` provably missed a mutation, which is an as-of that
    stays equal across a real change -- the failure that reads as correct, and
    strictly worse than ``as_of: null``. Delta and Iceberg wait, as ruled.
    """
    assert absent not in readers.SHIPPED_AS_OF_READERS


def test_importing_the_module_imports_no_driver():
    """The broker imports this at module scope, and ``psycopg2`` is in an EXTRA.

    ``psycopg2-binary`` lives in the ``broker`` extra, not the core
    dependencies, so a module-scope driver import here would make the broker
    unimportable on an install without that extra -- the trap that cost the
    tagged warehouse nine failures on a floor venv, in a suite that was green
    on a machine where the dependency happened to be present. Asserted in a
    subprocess because this test session has already imported both drivers for
    other reasons, so nothing in process can see it.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys\n"
            "import dag_tools.domain_broker.as_of_readers as m\n"
            "assert m.SHIPPED_AS_OF_READERS\n"
            "print('psycopg2' in sys.modules, 'boto3' in sys.modules)\n",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "False False"


# -- the reader's request against the credential the broker actually mints ----
#
# The one thing the live script CANNOT measure without a sandbox broker (P11:
# "a credential the broker MINTED can list the prefix") is decidable here, and
# it is decidable because both sides derive from ONE string -- the ticket's own
# physical_uri. The broker scopes the policy with ``_s3_scope_from_uri`` and
# the reader builds its request with ``_s3_parts``; if either is edited the
# reader starts getting AccessDenied from a credential that looks correct, and
# nothing in either module would fail. So the coupling is asserted rather than
# described, over every URI shape the two functions read differently.


def _string_like(pattern: str, value: str) -> bool:
    """IAM ``StringLike`` over the one wildcard this policy uses.

    Deliberately not a general matcher: the condition the broker writes uses
    ``*`` and nothing else, so a fnmatch (which also reads ``?`` and ``[]``)
    would accept patterns this test has no business accepting.
    """
    head, star, tail = pattern.partition("*")
    if not star:
        return value == pattern
    return value.startswith(head) and value.endswith(tail) and (
        len(value) >= len(head) + len(tail)
    )


@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket/warehouse/table",
        "s3://bucket/warehouse/table/",
        "s3://bucket/warehouse/table/part=1",
        "s3://bucket/one",
        "s3://bucket/",
        "s3://bucket",
    ],
)
def test_the_readers_listing_is_inside_the_policy_the_broker_mints(uri):
    """The reader needs no policy widening -- asserted over every URI shape.

    ``as_of_readers`` says in its own docstring that the reader "needs only
    s3:ListBucket on the prefix, which both s3 paths already grant,
    prefix-conditioned". That sentence is a claim about a DIFFERENT module, so
    here it is measured: the prefix the reader asks for is checked against the
    ``s3:prefix`` condition the minting path writes, for the same URI.

    The shapes matter because the two functions disagree on purpose. The scope
    strips the slashes (``prefix.strip("/")``) and the reader does not, so
    ``s3://b/t/`` asks for ``t/`` against a condition built from ``t`` -- which
    the ``t/*`` arm covers, and which is why that arm is not redundant.
    """
    from dag_tools.domain_broker import main as broker

    scope = broker._s3_scope_from_uri(uri)
    assert scope is not None

    prefix = scope["prefix"]
    listable = [f"{prefix}/*", prefix] if prefix else ["*"]

    requested = readers._s3_parts(uri)
    assert requested is not None
    _, requested_prefix = requested

    assert any(_string_like(p, requested_prefix) for p in listable), (
        "the reader would ask for Prefix=%r, which the minted condition %r "
        "does not allow -- a credential that looks correct and 403s"
        % (requested_prefix, listable)
    )


def test_control_a_prefix_outside_the_scope_is_not_allowed_by_that_condition():
    """Without this, the test above would pass on a matcher that allows anything.

    The arm it rules out is the one that matters: a ticket advertising a prefix
    SHALLOWER than the one the credential was scoped to. That is not a reader
    defect, but it is the shape in which the assertion above would be worthless
    if ``_string_like`` were too generous.
    """
    listable = ["warehouse/table/*", "warehouse/table"]

    assert not any(_string_like(p, "warehouse") for p in listable)
    assert not any(_string_like(p, "warehouse/other") for p in listable)
    assert any(_string_like(p, "warehouse/table/part=1") for p in listable)
