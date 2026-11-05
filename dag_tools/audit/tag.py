"""The attribution contract: who read what, carried on the query itself.

A warehouse's query history records *an identity* and *a statement*. It does
not record which asset in which run issued the statement, so the mapping from
a read back to the pipeline that caused it has to be carried by the query. One
JSON object, set as the query's tag, is that carrier:

    {"asset_key": ..., "principal": ..., "run_id": ..., "schema_version": 1}

Both halves of the audit depend on this module and on nothing else, which is
why it is its own file: the resource that *writes* the tag (step 2) and the
report that *reads* it back out of history (step 1) must agree exactly, and a
disagreement would show up as reads no asset explains -- the very number the
report exists to produce. A silent disagreement is therefore the worst
available outcome, so `encode` refuses rather than emits anything a decoder
could misread.

Two grammar decisions worth stating:

  * **A principal is `<kind>:<value>`, split on the FIRST colon**, so a value
    containing a colon survives. `pipeline:<name>` is a service identity
    acting for itself; `user:<authz_id>` is the gateway's *verified*
    `on_behalf_of` -- the authz_id the token proved, never an email and never
    a client-supplied header. An unrecognised or absent kind parses as kind
    `None`, and callers must treat that as "not a pipeline": history is full
    of tags set by things that are not this system, and a reader that guesses
    would attribute a stranger's query to an asset.
  * **Nothing here names a database, schema, table or column.** The tag
    carries Dagster identifiers and a principal; the objects a query touched
    come from the history row, not from us.

This module holds no classification and no level. It answers "who and why",
not "how sensitive" -- those are registry rows (step 3).
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, ConfigDict


SCHEMA_VERSION = 1
"""Monotonic version of the tag payload. Additive-only, same discipline as
`dag_tools.inventory.schema`: a reader of a newer tag must still work, so
every field added later needs a default and unknown fields are ignored.

History:
  1 — principal, asset_key, run_id.
"""

PIPELINE = "pipeline"
"""Kind for a service identity acting as itself."""

ON_BEHALF_OF = "user"
"""Kind for a read the gateway attributed to a verified `on_behalf_of`."""

KINDS = (PIPELINE, ON_BEHALF_OF)

MAX_TAG_CHARS = 2000
"""Warehouses cap the query tag. The cap is enforced here rather than left to
the driver because a truncated tag does not decode -- it degrades into a read
that no asset explains, which is indistinguishable from an asset that was
never attributed at all. Refusing is loud; truncating is a lie."""


class TagTooLong(ValueError):
    """Raised instead of emitting a tag the warehouse would truncate."""


class QueryAttribution(BaseModel):
    """What one tagged query says about itself."""

    model_config = ConfigDict(extra="ignore")

    principal: str
    asset_key: Optional[str] = None
    run_id: Optional[str] = None
    schema_version: int = SCHEMA_VERSION


def pipeline_principal(name: str) -> str:
    """`pipeline:<name>` -- a service identity reading for itself."""
    cleaned = (name or "").strip()
    if not cleaned:
        raise ValueError("a pipeline principal needs a name")
    return f"{PIPELINE}:{cleaned}"


def on_behalf_of_principal(authz_id: str) -> str:
    """`user:<authz_id>` -- a read the gateway attributed to a verified user.

    The caller is responsible for having verified it. This function cannot
    tell a proven authz_id from a header someone typed, and the difference is
    the whole value of the field, so it is named for what it means and the
    verification stays at the gateway (see `central_gateway.identity.decide`).
    """
    cleaned = (authz_id or "").strip()
    if not cleaned:
        raise ValueError("an on-behalf-of principal needs an authz_id")
    return f"{ON_BEHALF_OF}:{cleaned}"


def parse_principal(value: Optional[str]) -> Tuple[Optional[str], str]:
    """Split a principal into `(kind, value)`; kind is None if unrecognised.

    Deny by default: an unknown prefix is NOT a pipeline, so a tag written by
    something outside this system can never be attributed to an asset.
    """
    raw = (value or "").strip()
    if ":" not in raw:
        return None, raw
    kind, _, rest = raw.partition(":")
    if kind not in KINDS or not rest:
        return None, raw
    return kind, rest


def is_pipeline(value: Optional[str]) -> bool:
    return parse_principal(value)[0] == PIPELINE


def encode(attribution: QueryAttribution) -> str:
    """Render the tag. Deterministic, compact, and refused if over the cap.

    Sorted keys and no whitespace: two runs of the same asset produce the same
    bytes, so a diff of history is a diff of behaviour rather than of
    serialisation.
    """
    payload: Dict[str, Any] = {
        "principal": attribution.principal,
        "schema_version": attribution.schema_version,
    }
    if attribution.asset_key is not None:
        payload["asset_key"] = attribution.asset_key
    if attribution.run_id is not None:
        payload["run_id"] = attribution.run_id
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(rendered) > MAX_TAG_CHARS:
        raise TagTooLong(
            f"the query tag would be {len(rendered)} characters, over the "
            f"{MAX_TAG_CHARS} the warehouse keeps. A truncated tag does not "
            f"decode, so the read would look unattributed. Shorten the asset "
            f"key or the principal."
        )
    return rendered


def decode(raw: Optional[str]) -> Optional[QueryAttribution]:
    """Read a tag back, returning None for anything that is not one of ours.

    Never raises. Query history carries tags set by every other tool that
    touches the warehouse, and the report iterates over all of them.
    """
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    principal = parsed.get("principal")
    if not isinstance(principal, str) or not principal.strip():
        return None
    try:
        return QueryAttribution.model_validate(parsed)
    except Exception:
        return None
