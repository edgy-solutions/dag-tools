#!/usr/bin/env python3
"""The fleet half of CI's image record: collect the refs, or verify a record.

WHY A RECORD AT ALL. A deployment pins an image by digest, and a
qualification's preflight compares what a code location is running against the
digests recorded for the commit it is pinned to. Until something writes those
digests down, that comparison has nothing to match and every location reads as
unverified. The digest is also the one fact only CI knows: it is minted by the
push, and nothing downstream can recover it for a commit whose tags have since
moved.

WHY THIS FILE EXISTS ALONGSIDE THE COMMAND. The record document itself is
written by ``dagtools registry write-image-record``, which is the one parser
and the one writer. What that command cannot know is the FLEET: it is handed
refs, so a service that produced nothing contributes no ref and is invisible to
it. Each matrix job, in turn, knows its own service and nothing else, so no
build job can say what the whole fleet is. The fleet is therefore declared once
in the workflow (``SERVICES``) and passed in here, which is the whole point --
a record that lists only the services that ran is a claim that what it lists is
everything.

WHY IT REFUSES INSTEAD OF NAMING A GAP IN THE DOCUMENT. The record has no field
for a service that produced no digest, and a record is never rewritten, so a
partial one could never be corrected. Nothing is published unless every
declared service produced a usable ref. That makes "the file is absent" and
"the file is short a service" one state rather than two readings of one, and it
is the guarantee the reader side is specced against.

WHY PYTHON HERE AND SHELL IN THE JOB. This is the part with a rule in it, and a
rule in a YAML heredoc can only ever be string-matched, never run. The git
plumbing that commits the record is in the job body because a Python rewrite of
it would be no more testable than the shell is.

TWO SUBCOMMANDS, ONE FLEET RULE:

``collect``
    Reads one fragment per declared service and prints one ref per line, for
    the job to splat into ``--image`` arguments. Refuses, naming every problem
    at once, if a declared service has no fragment, an empty one, or one that
    does not name that service's own image at a digest.

``verify``
    Reads a record document and the resolved digests of a release, and refuses
    unless the record names exactly the fleet and every digest agrees. This is
    what makes "retag, not rebuild means a release's digests equal its
    commit's" a checkable claim rather than a sentence: the release asset is
    the commit's own record bytes, attached only once each release name has
    been resolved against the registry and found to agree with it.

The ref check here is an EXPECTATION, not a parser: it asks whether service X's
fragment names X's image at a digest. The grammar's authority is
``dag_tools.qual.image_record``, which the writing command calls, and which
refuses a ref with no digest or one repository at two digests. Two questions,
two checks, deliberately.

Fragments are flat files named after their service, holding one line:

    ghcr.io/<owner>/dag-tools/<service>@sha256:<64 hex>

A blank line is a service that ran and produced no digest. An absent file is a
service that never ran. Both refuse; they are distinguished because they send
an operator to different places.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

_DIGEST = r"sha256:[0-9a-f]{64}"


def expected_ref(registry: str, service: str) -> re.Pattern[str]:
    """What service X's fragment must say, and nothing else.

    One expectation, used by both subcommands, so a fragment that landed under
    the wrong service name is caught wherever it is read rather than only on
    the side that happened to check.
    """
    return re.compile(r"^" + re.escape(f"{registry}/{service}@") + _DIGEST + r"$")


def read_fragment(
    service: str, registry: str, fragments: pathlib.Path
) -> tuple[str | None, str | None]:
    """``(digest, problem)`` for one service's fragment -- never both.

    Absent and empty are separated because they send an operator to different
    places: absent means the job never ran, empty means it ran and pushed
    nothing.
    """
    path = fragments / service
    if not path.is_file():
        return None, (
            f"{service}: no fragment at {path}. Its job did not run, or did "
            "not reach the step that records its ref."
        )
    line = path.read_text(encoding="utf-8").strip()
    if not line:
        return None, (
            f"{service}: its fragment is empty, so no digest was reported. The "
            "image was not pushed, or the step failed after starting."
        )
    if not expected_ref(registry, service).fullmatch(line):
        return None, (
            f"{service}: its fragment reads {line!r}, which is not "
            f"{registry}/{service}@sha256:<64 hex>. A fragment names its own "
            "service's image at a digest, or it is not used."
        )
    return line.rsplit("@", 1)[1], None


def parse_fleet(raw: str) -> list[str]:
    """The declared services, in the order the workflow declares them.

    A fleet of zero is refused rather than treated as "nothing to record": it
    would make every record vacuously complete.
    """
    services = raw.split()
    if not services:
        raise SystemExit(
            "::error::no services were declared. The fleet is the workflow's "
            "SERVICES and an empty one would make every record vacuously "
            "complete, so there is nothing safe to do with it."
        )
    duplicates = sorted({s for s in services if services.count(s) > 1})
    if duplicates:
        raise SystemExit(
            "::error::the declared fleet names "
            + ", ".join(duplicates)
            + " more than once. One service is one image is one entry."
        )
    return services


def collect(services: list[str], registry: str, fragments: pathlib.Path) -> list[str]:
    """One ref per declared service, or every problem at once.

    ``registry`` is the repository prefix the build pushed under, so a fragment
    that landed under the wrong service name, or that names some other image
    entirely, is caught here rather than recorded.
    """
    refs: list[str] = []
    problems: list[str] = []
    for service in services:
        digest, problem = read_fragment(service, registry, fragments)
        if problem is not None:
            problems.append(problem)
            continue
        refs.append(f"{registry}/{service}@{digest}")

    if problems:
        raise SystemExit(
            "::error::no image record was written, because "
            f"{len(problems)} of {len(services)} declared service(s) produced "
            "no usable ref. A record is never rewritten, so a partial one "
            "could never be corrected and none is published at all:\n"
            + "\n".join(f"  - {problem}" for problem in problems)
        )
    return refs


def verify(
    services: list[str],
    registry: str,
    record_path: pathlib.Path,
    fragments: pathlib.Path,
) -> None:
    """Refuse unless the record names exactly the fleet at the resolved digests.

    The record is the commit's, written by the master build. The fragments are
    the release's, each holding what that service's release names were resolved
    to against the registry. They agree by construction -- a release renames a
    commit's build rather than rebuilding it -- so a disagreement means that
    construction broke, and the release fails rather than the record changing.
    """
    try:
        document = json.loads(record_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(
            f"::error::there is no record at {record_path} to verify. The "
            "commit's master build writes it; a release does not write one of "
            "its own."
        )
    except json.JSONDecodeError as exc:
        raise SystemExit(f"::error::{record_path} is not readable as JSON: {exc}")

    recorded: dict[str, str] = {}
    problems: list[str] = []
    for entry in document.get("images") or []:
        repository = str(entry.get("repository", ""))
        recorded[repository.rsplit("/", 1)[-1]] = str(entry.get("digest", ""))

    extra = sorted(set(recorded) - set(services))
    if extra:
        problems.append(
            "the record names " + ", ".join(extra) + ", which the declared "
            "fleet does not."
        )

    for service in services:
        in_record = recorded.get(service)
        if not in_record:
            problems.append(f"{service}: the record names no digest for it.")
            continue
        resolved, problem = read_fragment(service, registry, fragments)
        if problem is not None:
            problems.append(problem)
            continue
        if resolved != in_record:
            problems.append(
                f"{service}: the release resolves to {resolved}, but the "
                f"commit's record says {in_record}. A release renames a "
                "commit's build, so these cannot differ unless something "
                "rebuilt or re-pointed it."
            )

    if problems:
        raise SystemExit(
            "::error::the release does not match the commit's image record, so "
            "nothing is attached and the record is NOT rewritten:\n"
            + "\n".join(f"  - {problem}" for problem in problems)
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--services", required=True, help="the declared fleet")
    common.add_argument(
        "--registry", required=True, help="the repository prefix images push under"
    )
    common.add_argument(
        "--fragments", required=True, type=pathlib.Path, help="one file per service"
    )

    sub.add_parser("collect", parents=[common])
    verify_parser = sub.add_parser("verify", parents=[common])
    verify_parser.add_argument(
        "--record", required=True, type=pathlib.Path, help="the record to verify"
    )

    args = parser.parse_args(argv)
    services = parse_fleet(args.services)

    if args.command == "collect":
        for ref in collect(services, args.registry.rstrip("/"), args.fragments):
            print(ref)
        return 0

    verify(services, args.registry.rstrip("/"), args.record, args.fragments)
    print(f"{args.record} agrees with every resolved release name")
    return 0


if __name__ == "__main__":
    sys.exit(main())
