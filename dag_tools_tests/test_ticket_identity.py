"""A ticket says which read it is, when it was issued, and over what version.

WHY THESE TESTS EXIST. Two things a routing ticket could not previously do.

It could not be referred to. The gateway logged its decision and the broker
logged its minting, and nothing joined either to the frame a consumer actually
held, so "which read was this?" had no answer -- the answer an audit, a
complaint and a bill all begin with.

And a frame could not be pinned to a version. Two agents reading a dataset
being rewritten underneath them legitimately disagree, and neither could say
which version it read.

THE ONE RULE, and most of what is asserted below: ``as_of`` is the data's
version, ``issued_at`` is the time we looked. An absent ``as_of`` is reported
as absent and counted; it is NEVER filled in with the issue time. That
substitution is the tempting one -- it makes every ticket look complete -- and
it would make the field worthless, because a reader could no longer tell a
version from a clock reading and would have to assume the worst of every
ticket, including the ones carrying a real version.

The broker half asserts on the ticket a CONSUMER receives, not on what the
identity module returns in isolation, for the reason ADR-0044's tests state:
the defect that matters is a resolve path returning in front of the stamp.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

from dag_tools.domain_broker import main as broker
from dag_tools.domain_broker import ticket_identity as ti


# ── doubles ────────────────────────────────────────────────────────────────

class _Advertising:
    """A producer that publishes its own version alongside its coordinates."""

    def __init__(self, extra: Dict[str, Any] | None = None, source_type="s3_parquet"):
        self._extra = extra or {}
        self._source_type = source_type

    def physical_coordinates(self, asset_key_path):
        return {
            "source_type": self._source_type,
            "physical_uri": "s3://lake/warehouse/thing/",
            **self._extra,
        }


class _Record:
    def __init__(self, source_type="s3_parquet"):
        self.asset_key = ["lake", "warehouse", "thing"]
        self.io_manager_key = "io_manager"
        self.io_manager_family = source_type
        self.io_manager_class = "dag_tools.io_managers.arrow.ConfigurableArrowIOManager"
        self.tags = {}
        self.urn = URN


class _FakeSTS:
    def assume_role(self, **kwargs):
        return {
            "Credentials": {
                "AccessKeyId": "MINTED_KEY",
                "SecretAccessKey": "MINTED_SECRET",
                "SessionToken": "MINTED_TOKEN",
            }
        }


URN = "urn:li:dataset:(urn:li:dataPlatform:s3,lake,PROD)"


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    broker.LOCAL_ASSETS.clear()
    broker.ECHOED_CREDENTIALS_DROPPED.clear()
    broker.UNPROTECTED_SOURCE_TYPES.clear()
    broker.NON_FQDN_HOSTS.clear()
    broker.AS_OF_ABSENT.clear()
    ti.AS_OF_READERS.clear()
    monkeypatch.setattr(broker.boto3, "client", lambda *a, **k: _FakeSTS())
    yield
    ti.AS_OF_READERS.clear()


def _register(io_manager, source_type="s3_parquet"):
    broker.LOCAL_ASSETS[URN] = broker._build_asset_info_from_record(
        _Record(source_type), io_manager=io_manager
    )


def _resolve():
    return asyncio.run(broker.resolve_asset(broker.ResolveRequest(urn=URN)))


# ── every path that returns a ticket is identified ─────────────────────────

def test_a_minted_ticket_carries_an_identity():
    _register(_Advertising())
    ticket = _resolve()

    assert ticket["ticket_id"]
    assert ticket["issued_at"]
    assert ticket["credentials"]["aws_access_key_id"] == "MINTED_KEY"


def test_the_unprotected_path_is_identified_TOO():
    """A backend with no minter returns early -- and is the one most worth citing.

    A read the broker could not protect is exactly the read someone will ask
    about later. Stamping only the minted path would leave those anonymous.
    """
    _register(_Advertising(source_type="snowflake"), source_type="snowflake")
    ticket = _resolve()

    assert ticket["ticket_id"], (
        "the no-minter path returns before the minting block; it returned "
        "around the stamp as well"
    )
    assert ticket["mode"] == "producer-credential-unprotected"


def test_the_legacy_coordinate_path_is_identified_too():
    """The route for an IO manager that advertises nothing at all."""
    broker.LOCAL_ASSETS[URN] = {"io_manager_type": "postgres", "db_host": "db.example"}
    ticket = _resolve()

    assert ticket["ticket_id"]
    assert ticket["source_type"] == "postgres"


def test_two_resolves_of_one_urn_get_different_ticket_ids():
    """THE LOAD-TIME TRAP, the same one ADR-0044 closed for credentials.

    ``physical_coordinates()`` runs ONCE at broker startup and its result lives
    in ``LOCAL_ASSETS`` for the process lifetime. An identity minted there
    would hand every caller of every asset one ticket id and an issue time from
    whenever the pod booted -- a log that cannot distinguish two reads, which
    is worse than no log because it reads as though it can.
    """
    _register(_Advertising())

    first, second = _resolve(), _resolve()

    assert first["ticket_id"] != second["ticket_id"], (
        "both reads got the same ticket id: the identity is being minted once "
        "and cached, not minted per request"
    )


def test_the_identity_does_not_reach_the_cached_coordinates():
    """Nothing from a request may settle into the process-lifetime cache."""
    _register(_Advertising())
    _resolve()

    cached = broker.LOCAL_ASSETS[URN]["_routing_ticket"]
    assert "ticket_id" not in cached
    assert "issued_at" not in cached


# ── the one rule: a version is not a clock reading ─────────────────────────

def test_an_absent_as_of_is_not_the_issue_time():
    """The whole point. Filling it in is what would make the field worthless."""
    _register(_Advertising())
    ticket = _resolve()

    assert ticket["as_of"] is None, (
        "as_of was populated for a producer that advertises no version -- "
        "almost certainly from the issue time, which is a clock reading and "
        "not a version of anything"
    )
    assert ticket["as_of"] != ticket["issued_at"]


def test_an_absent_as_of_carries_a_reason():
    _register(_Advertising())
    ticket = _resolve()

    assert ticket["as_of_unavailable"] == ti.ABSENT_NO_READER


def test_an_absent_as_of_is_counted_per_source_type():
    """Reported as a NUMBER, not a log line -- same posture as the ADR-0044 gauges.

    A backend still in this dict after a cycle is one that needs a row in
    AS_OF_READERS, and this is how anyone would know that without reading logs.
    """
    _register(_Advertising())
    _resolve()
    _resolve()

    assert broker.AS_OF_ABSENT == {"s3_parquet": 2}


def test_the_absence_is_on_health_where_an_operator_can_see_it(monkeypatch):
    monkeypatch.setattr(broker, "DEFINITIONS_LOADED", True)
    monkeypatch.setattr(broker, "DEFINITIONS_ERROR", None)
    _register(_Advertising())
    _resolve()

    body = asyncio.run(broker.health())
    assert body["ticket_identity"]["as_of_absent_by_source_type"] == {"s3_parquet": 1}


def test_a_present_as_of_is_not_counted_as_absent():
    _register(_Advertising({"as_of": "2026-11-07T00:00:00Z"}))
    _resolve()

    assert broker.AS_OF_ABSENT == {}


@pytest.mark.parametrize("key", ti.ADVERTISED_AS_OF_KEYS)
def test_a_version_the_producer_advertises_is_carried_and_labelled(key):
    _register(_Advertising({key: "v41"}))
    ticket = _resolve()

    assert ticket["as_of"] == "v41"
    assert ticket["as_of_source"] == ti.SOURCE_ADVERTISED_AT_LOAD, (
        "an advertised version describes the dataset as it was when THIS "
        "BROKER BOOTED; presenting it as a fresh read is the load-time trap "
        "again, in the one field a consumer would trust most"
    )


def test_a_version_read_at_authorize_beats_one_advertised_at_load():
    ti.register_as_of_reader("s3_parquet", lambda t: "99")
    _register(_Advertising({"as_of": "stale-from-boot"}))
    ticket = _resolve()

    assert ticket["as_of"] == "99"
    assert ticket["as_of_source"] == ti.SOURCE_READ_AT_AUTHORIZE


def test_the_producers_own_key_survives_the_stamp():
    """A consumer reading the advertised key keeps working."""
    ti.register_as_of_reader("s3_parquet", lambda t: "99")
    _register(_Advertising({"as_of": "stale-from-boot"}))
    stamped = ti.stamp({"source_type": "s3_parquet", "snapshot_id": "stale"})

    assert stamped["snapshot_id"] == "stale"
    assert stamped["as_of"] == "99"


def test_as_of_is_always_a_key_even_when_unknown():
    """ONE SHAPE from every backend.

    A key that appears only sometimes gets read with ``.get()`` and is then
    indistinguishable from one that was merely misspelled.
    """
    _register(_Advertising(source_type="snowflake"), source_type="snowflake")
    ticket = _resolve()

    assert "as_of" in ticket and "as_of_source" in ticket


def test_issued_at_is_utc_and_parses():
    moment = datetime(2026, 11, 8, 12, 30, tzinfo=timezone.utc)
    identity = ti.mint("s3_parquet", {}, now=moment)

    parsed = datetime.fromisoformat(identity.issued_at)
    assert parsed.utcoffset() == timezone.utc.utcoffset(None)
    assert parsed == moment


def test_a_naive_timestamp_is_read_as_utc_not_left_bare():
    """A naive timestamp in an audit field is one in an unknown zone."""
    identity = ti.mint("s3_parquet", {}, now=datetime(2026, 11, 8, 12, 30))

    assert datetime.fromisoformat(identity.issued_at).tzinfo is not None


# ── a reading must not be able to fail the read ────────────────────────────

def test_a_failing_reader_does_not_fail_the_ticket(caplog):
    """An as-of is metadata about the read; a credential IS the read.

    Only one of the two is worth refusing a consumer over.
    """
    def boom(ticket):
        raise RuntimeError("catalog unreachable")

    ti.register_as_of_reader("s3_parquet", boom)
    _register(_Advertising())

    with caplog.at_level(logging.WARNING):
        ticket = _resolve()

    assert ticket["credentials"]["aws_access_key_id"] == "MINTED_KEY"
    assert ticket["as_of"] is None
    assert ticket["as_of_unavailable"] == ti.ABSENT_READER_FAILED
    assert any("as-of reader" in r.getMessage() for r in caplog.records)


def test_a_reader_that_declines_is_distinguished_from_having_no_reader():
    """"This dataset has no version yet" is an answer. "Nobody asked" is not."""
    ti.register_as_of_reader("s3_parquet", lambda t: None)
    _register(_Advertising())
    ticket = _resolve()

    assert ticket["as_of_unavailable"] == ti.ABSENT_READER_DECLINED


def test_a_declining_reader_still_lets_an_advertised_version_through():
    ti.register_as_of_reader("s3_parquet", lambda t: None)
    _register(_Advertising({"version": "v7"}))
    ticket = _resolve()

    assert ticket["as_of"] == "v7"
    assert ticket["as_of_source"] == ti.SOURCE_ADVERTISED_AT_LOAD


# ── what can honestly be carried as a version ──────────────────────────────

def test_an_integer_table_version_is_carried_as_text():
    """A delta table version IS an integer and ``str`` of one is faithful."""
    ti.register_as_of_reader("s3_parquet", lambda t: 41)
    _register(_Advertising())

    assert _resolve()["as_of"] == "41"


def test_a_structured_value_is_refused_rather_than_stringified(caplog):
    """A dict's repr in a ticket field is not a version.

    Two consumers comparing them would be comparing Python syntax.
    """
    ti.register_as_of_reader("s3_parquet", lambda t: {"version": 41})
    _register(_Advertising())

    with caplog.at_level(logging.WARNING):
        ticket = _resolve()

    assert ticket["as_of"] is None
    assert ticket["as_of_unavailable"] == ti.ABSENT_UNUSABLE_VALUE
    assert "{" not in json.dumps(ticket["as_of"])


def test_a_boolean_is_not_a_version():
    """``bool`` is an ``int`` in Python; a flag carried as "True" is noise."""
    ti.register_as_of_reader("s3_parquet", lambda t: True)
    _register(_Advertising())

    assert _resolve()["as_of"] is None


def test_an_empty_string_is_absent_not_a_version():
    ti.register_as_of_reader("s3_parquet", lambda t: "")
    _register(_Advertising())

    assert _resolve()["as_of"] is None


def test_a_version_is_carried_exactly_as_the_source_wrote_it():
    """The rule ``warehouse_source.observe.read_as_of`` already follows.

    Reformatting makes two equal watermarks look different.
    """
    raw = "2026-11-07 00:00:00.000 +0000"
    ti.register_as_of_reader("s3_parquet", lambda t: raw)
    _register(_Advertising())

    assert _resolve()["as_of"] == raw


# ── no reader ships for any backend yet, and that is reported ──────────────

def test_no_as_of_reader_ships_by_default():
    """A registry shipped empty, deliberately, and visible in the numbers.

    Reading a delta or iceberg version means real IO against a real store,
    which nothing here can exercise -- and a reader that cannot be tested is
    the half-measure this repo refuses. The mechanism plus a counted absence is
    the honest v1; until a backend gets a row, ``as_of`` comes only from what a
    producer advertised, labelled as such.
    """
    import importlib

    fresh = importlib.reload(ti)
    try:
        assert fresh.AS_OF_READERS == {}
    finally:
        importlib.reload(ti)
