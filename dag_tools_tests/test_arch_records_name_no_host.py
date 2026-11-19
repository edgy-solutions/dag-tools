"""The architecture records name no host.

A list of forbidden terms catches only the names someone thought to list, and
a host name can also read as something else (a settings key, a link), so the
records are scanned by shape: a dotted name ending in a public or internal
suffix, an IPv4 address, a scheme followed by a host, or a bare host word. The
whole tree is scanned, not only what a change adds, so a name already pushed
is found the next time CI runs.
"""

import re
from pathlib import Path

import pytest

ARCH = Path(__file__).resolve().parent.parent / "docs" / "arch"

_SUFFIX = (
    "com|org|net|io|dev|app|cloud|ai|co|us|uk|de|eu|biz|info|mil|gov|edu|int|"
    "local|internal|lan|corp|home|intranet|svc|localdomain|test|example|invalid"
)
PATTERNS = (
    ("dotted name", re.compile(
        r"(?<![\w.-])(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+(?:" + _SUFFIX + r")(?![\w-])", re.I)),
    ("ipv4 address", re.compile(r"(?<![\w.])\d{1,3}(?:\.\d{1,3}){3}(?!\w|\.\d)")),
    ("scheme with a host", re.compile(r"\b[a-z][a-z0-9+.-]*://[a-z0-9\[]", re.I)),
    # the loopback name, spelled as a pattern
    ("bare host word", re.compile(r"\blocal[ ]?host\b", re.I)),
)


def host_shapes(text):
    """Every (line number, pattern name, match) in ``text``."""
    found = []
    for n, line in enumerate(text.split("\n"), 1):
        for name, rx in PATTERNS:
            found.extend((n, name, m.group(0)) for m in rx.finditer(line))
    return found


def _records():
    return sorted(ARCH.rglob("*.md"))


def test_the_records_exist_so_the_scan_is_not_vacuous():
    assert _records(), f"no records under {ARCH}"


@pytest.mark.parametrize(
    "planted",
    [
        "the `org.example` setting",
        "a link [kit](https://kit.example/a)",
        "an address 192.0.2.1.",
        "a service at db.internal",
        "reached on " + "local" + "host",
    ],
)
def test_control_the_scan_finds_a_planted_host(planted):
    assert host_shapes(planted), planted


@pytest.mark.parametrize(
    "clean",
    [
        "`dag_tools/resources/grist.py:61-125`",
        "the `host` in the `grist` block",
        "`source.uri` must be `s3://`, `file://` or a plain path",
        "version 1.10.19 and tag `v0.1.0`",
    ],
)
def test_control_the_scan_passes_text_with_no_host(clean):
    assert not host_shapes(clean), clean


def test_no_record_names_a_host():
    hits = [
        f"{f.relative_to(ARCH).as_posix()}:{n}: {name}: {tok}"
        for f in _records()
        for n, name, tok in host_shapes(f.read_text(encoding="utf-8"))
    ]
    assert not hits, "\n".join(hits)
