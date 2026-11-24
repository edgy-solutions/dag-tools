"""Which as-of readers a deployment may ask for -- invincible-agent #529 items 1 and 3.

One table is read by three things: the chart's constant at render time, the
startup check in ``domain_broker.main`` at start, and ``/health`` at runtime. The
rule this module pins is that the three cannot disagree, because there is only
one table and every other answer is derived from it.

Two properties here are not visible to an ordinary in-process test and are
asserted in a subprocess for the same reason
``test_resolving_a_connection_does_not_import_dlt`` is: the refusal has to happen
while the module is still importing. This broker's ``lifespan`` creates a
background task and yields immediately -- so a refusal raised anywhere later is
swallowed into the task object while the pod is already Ready and serving, which
is the failure the chart's render check exists to prevent, reproduced one layer
down. Only a process that fails to start proves otherwise.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

from dag_tools.domain_broker import main as broker
from dag_tools.domain_broker import ticket_identity as ti

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _reset():
    """The registry is process-global; leave it exactly as it was found."""
    before = dict(ti.AS_OF_READERS)
    ti.AS_OF_READERS.clear()
    yield
    ti.AS_OF_READERS.clear()
    ti.AS_OF_READERS.update(before)


def _reader(value):
    return lambda ticket: value


# --------------------------------------------------------------------------
# The selection itself
# --------------------------------------------------------------------------

def test_an_unset_value_selects_nothing_and_registers_nothing():
    """The setting ships inert, which is today's behaviour exactly.

    A deployment that has not heard of this variable must behave as it did
    before the variable existed, or the mechanism is a breaking change wearing
    an opt-in's clothes.
    """
    assert ti.select_as_of_readers(None) == ()
    assert ti.register_selected_as_of_readers(None) == ()
    assert ti.AS_OF_READERS == {}


@pytest.mark.parametrize("value", ["", "   ", ",", " , ,,"])
def test_an_empty_value_names_no_source_type_so_it_is_dropped_not_refused(value):
    """A trailing or doubled comma names nothing; it is not a typo to refuse.

    Refusing it would fail a deployment's start over a punctuation mark in a
    value that asks for nothing at all.
    """
    assert ti.select_as_of_readers(value) == ()
    assert ti.register_selected_as_of_readers(value) == ()
    assert ti.AS_OF_READERS == {}


def test_a_name_this_build_does_not_ship_is_refused():
    with pytest.raises(ti.UnknownAsOfReader):
        ti.select_as_of_readers("s3_delta")


def test_the_refusal_is_a_value_error_so_existing_handlers_still_catch_it():
    """The courtesy ``MissingCredentials`` already extends."""
    assert issubclass(ti.UnknownAsOfReader, ValueError)
    with pytest.raises(ValueError):
        ti.select_as_of_readers("s3_delta")


def test_the_refusal_names_the_type_and_the_available_set(monkeypatch):
    """Both halves, because a refusal naming only one sends someone to the code.

    This is the first of the two conditions the #529 addendum accepted item 1
    under: the fix is in the message and nobody has to go and find the table.
    """
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "s3_delta", _reader("2"))

    with pytest.raises(ti.UnknownAsOfReader) as caught:
        ti.select_as_of_readers("s3_iceberg")

    message = str(caught.value)
    assert "s3_iceberg" in message          # the type that was refused
    assert "s3_delta" in message            # what is available instead
    assert ti.AS_OF_READERS_ENV in message  # which variable to edit


def test_an_empty_available_set_says_so_rather_than_offering_nothing():
    """At this pin the build ships no reader, so the offer has no names in it.

    An offer rendered as an empty list reads as a formatting defect; the
    sentence says why it is empty and what has to happen first, which is the
    sequencing consequence a first operator will otherwise file as a bug.
    """
    assert ti.available_as_of_readers() == ()

    with pytest.raises(ti.UnknownAsOfReader) as caught:
        ti.select_as_of_readers("s3_delta")

    message = str(caught.value)
    assert "ships no as-of reader yet" in message
    assert "repins" in message


def test_every_unknown_name_is_named_at_once(monkeypatch):
    """House rule: whoever is fixing the value wants the list, not the first one."""
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "s3_delta", _reader("2"))

    with pytest.raises(ti.UnknownAsOfReader) as caught:
        ti.select_as_of_readers("s3_iceberg,s3_delta,postgres")

    message = str(caught.value)
    assert "s3_iceberg" in message
    assert "postgres" in message


def test_a_refused_selection_registers_nothing(monkeypatch):
    """Refuse before registering, so a half-applied selection cannot exist.

    One bad name among good ones would otherwise leave a deployment holding
    some of what it asked for while being told it got none of it.
    """
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "s3_delta", _reader("2"))

    with pytest.raises(ti.UnknownAsOfReader):
        ti.register_selected_as_of_readers("s3_delta,s3_iceberg")

    assert ti.AS_OF_READERS == {}


def test_a_shipped_name_is_registered_and_is_the_reader_a_ticket_gets(monkeypatch):
    """The selection is wired to the thing that resolves, not merely recorded."""
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "s3_delta", _reader(2))

    assert ti.register_selected_as_of_readers("s3_delta") == ("s3_delta",)
    assert ti.registered_as_of_readers() == ("s3_delta",)

    as_of, source, unavailable = ti.resolve_as_of("s3_delta", {})
    assert (as_of, source, unavailable) == ("2", ti.SOURCE_READ_AT_AUTHORIZE, None)


def test_whitespace_and_a_repeat_do_not_change_what_is_selected(monkeypatch):
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "s3_delta", _reader("2"))
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "postgres", _reader("7"))

    assert ti.select_as_of_readers(" s3_delta , postgres,s3_delta ") == (
        "postgres", "s3_delta",
    )


# --------------------------------------------------------------------------
# The two sets answer two different questions
# --------------------------------------------------------------------------

def test_the_available_set_is_a_property_of_the_package_not_of_a_process():
    """This is what makes the chart's render check possible: there is no pod.

    It is answered by importing the pinned package, so a template can compare a
    deployment's list against it before anything runs.
    """
    assert ti.available_as_of_readers() == tuple(sorted(ti.AVAILABLE_AS_OF_READERS))


def test_the_registered_set_is_read_at_call_time_not_snapshotted():
    """``register_as_of_reader`` mutates the registry, and modules register late.

    A set captured at import would be invisible to anything that registered
    afterwards, and would report an empty registry on a pod that has readers.
    """
    assert ti.registered_as_of_readers() == ()

    ti.register_as_of_reader("s3_delta", _reader("2"))

    assert ti.registered_as_of_readers() == ("s3_delta",)


def test_the_two_sets_can_disagree_which_is_why_there_are_two(monkeypatch):
    """A pod that registered nothing while the build ships a reader is the gap.

    That is the state item 3 exists to expose: the render check passes and the
    pod reports no as-of forever, with nothing looking wrong anywhere.
    """
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "s3_delta", _reader("2"))

    assert ti.available_as_of_readers() == ("s3_delta",)
    assert ti.registered_as_of_readers() == ()


# --------------------------------------------------------------------------
# Item 3: the runtime exposure
# --------------------------------------------------------------------------

def _loaded(monkeypatch):
    """Reach the ``ok`` branch of ``/health``.

    A fresh process has not loaded its definitions, and the two earlier
    branches answer a different question (is this pod still importing) in a
    deliberately smaller body.
    """
    monkeypatch.setattr(broker, "DEFINITIONS_LOADED", True)
    monkeypatch.setattr(broker, "DEFINITIONS_ERROR", None)


def _health():
    return asyncio.run(broker.health())


def test_health_reports_both_sets(monkeypatch):
    _loaded(monkeypatch)
    monkeypatch.setitem(ti.AVAILABLE_AS_OF_READERS, "s3_delta", _reader("2"))
    ti.register_as_of_reader("postgres", _reader("7"))

    body = _health()["ticket_identity"]

    assert body["as_of_readers_available"] == ["s3_delta"]
    assert body["as_of_readers_registered"] == ["postgres"]


def test_health_reports_no_unknown_set_because_there_can_be_none(monkeypatch):
    """Deliberately absent, and this test is what keeps it absent.

    An unknown name refuses at import, so at runtime no such state exists. A
    key reporting one would always be empty, which reads as reassurance rather
    than as nothing to say -- and it belonged to a paragraph that was withdrawn.
    """
    _loaded(monkeypatch)

    assert "as_of_readers_unknown" not in _health()["ticket_identity"]


def test_health_still_answers_when_this_build_ships_no_reader(monkeypatch):
    """The empty case is published rather than omitted.

    An operator who finds no key cannot tell "this pin ships none" from "this
    version does not report it".
    """
    _loaded(monkeypatch)

    body = _health()["ticket_identity"]

    assert body["as_of_readers_available"] == []
    assert body["as_of_readers_registered"] == []


# --------------------------------------------------------------------------
# Item 1: where a refusal in this process actually refuses
# --------------------------------------------------------------------------

def _import_broker(value):
    env = dict(os.environ)
    if value is None:
        env.pop(ti.AS_OF_READERS_ENV, None)
    else:
        env[ti.AS_OF_READERS_ENV] = value
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "import dag_tools.domain_broker.main; print('imported')",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        env=env,
    )


def test_an_unshipped_name_fails_the_import_itself():
    """Asserted on the exit status of a real process, which is the whole point.

    A raise inside ``lifespan``'s background task leaves the pod Ready and
    serving; only a module that never finishes importing refuses. So the
    property under test is "this process does not start", and nothing short of
    starting one can observe it.
    """
    result = _import_broker("s3_iceberg")

    assert result.returncode != 0, result.stdout + result.stderr
    assert "imported" not in result.stdout
    assert "UnknownAsOfReader" in result.stderr
    assert "s3_iceberg" in result.stderr


def test_control_the_same_import_with_no_selection_succeeds():
    """Without this, the test above would pass on an import broken for any reason."""
    result = _import_broker(None)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "imported" in result.stdout


def test_control_an_empty_selection_imports_too():
    """The inert case, proven where it matters: a deployment that sets it empty."""
    result = _import_broker("")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "imported" in result.stdout
