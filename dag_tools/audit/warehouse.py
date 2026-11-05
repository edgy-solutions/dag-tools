"""How the tag travels with the query, decided as data and never executed here.

Step 2 of the classification build (ADR-0064). `tag.py` decides *what* a
tagged query says about itself; this module decides *how* that text reaches
the warehouse, and it produces a plan rather than running one.

**The transport is configuration, not code.** Warehouses disagree about where
a query tag lives: some keep a session parameter the next statement inherits,
some record only the statement text, and plenty of operators expose history
through a view of their own making. So the placement arrives as a YAML value,
and the session form arrives as the operator's own statement with `:tag` bound
into it. Hard-coding one dialect would make this library wrong everywhere else
and would put a vendor's grammar in a shared repo; see `AGENTS.md` §1.

**A plan, not an execution.** `plan_tagged_query` returns the ordered steps and
their bound parameters. Same split as `history.build_history_statement` and
`otel_api_sync._staging_url`, for the same two reasons: the decision worth
pinning is the one this module makes, and a test that opened a connection would
die on a driver CI does not install. The resource executes the plan; the plan
is what the tests read.

**An untagged query is refused, not sent.** `plan_tagged_query` raises when it
has no tag text, and the plan says for itself whether a tag is on it
(`carries_tag`), which the resource checks again at the point of execution. A
query that reached the warehouse untagged would come back out of history as a
read no asset explains -- indistinguishable from a pipeline nobody has
attributed yet, which is exactly the number step 1's report exists to measure.
Refusing is loud; sending is a quiet lie about the scope.

**Nothing here names a database, schema, table, column or level.** The
statement, its parameters and the tag text all arrive as arguments.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, model_validator


SESSION = "session"
"""The tag is set on the connection, by the operator's own statement, and the
query that follows inherits it. Requires that both run on one connection."""

COMMENT = "comment"
"""The tag rides in a leading comment on the statement itself. Per-statement,
no session state, and the only form that survives a pool that may hand the
next statement to a different session."""

PLACEMENTS = (SESSION, COMMENT)

TAG_PARAMETER = "tag"
"""The bound parameter the operator's session statement names as `:tag`. The
tag is a value and is always bound -- it carries an asset key, which is the
one part of this an attacker could influence, so it never reaches SQL text."""

TAG_STEP = "tag"
QUERY_STEP = "query"

COMMENT_OPEN = "/*"
COMMENT_CLOSE = "*/"


class UntaggedQuery(RuntimeError):
    """Raised instead of sending a query that carries no attribution."""


class UnsafeTag(ValueError):
    """Raised instead of placing tag text that would not survive placement."""


class TagTransport(BaseModel):
    """Where the tag goes. Both fields are validated at load, not at query time.

    An operator's typo here would otherwise cost a whole window of history:
    every query would run, none would be attributed, and the report would call
    it a gap. So the shape is checked when the code location loads.
    """

    model_config = ConfigDict(extra="forbid")

    placement: str
    statement: Optional[str] = None
    """For `session` placement only: the operator's own statement, naming the
    tag as `:tag`. No default, because a default would be one warehouse's
    dialect written into a library that serves several."""

    @model_validator(mode="after")
    def _check(self) -> "TagTransport":
        if self.placement not in PLACEMENTS:
            raise ValueError(
                f"tag placement must be one of {', '.join(PLACEMENTS)}; "
                f"got {self.placement!r}"
            )
        if self.placement == SESSION:
            text = (self.statement or "").strip()
            if not text:
                raise ValueError(
                    "session placement needs tag.statement -- the statement "
                    "your warehouse uses to set a query tag, with the tag "
                    "named as :tag. This library does not know your dialect "
                    "and will not guess one"
                )
            if f":{TAG_PARAMETER}" not in text:
                raise ValueError(
                    "tag.statement must bind the tag as :tag; without it the "
                    "statement would run and every query after it would be "
                    f"attributed to nothing: {self.statement!r}"
                )
            if ";" in text.rstrip(";"):
                raise ValueError(
                    "tag.statement must be one statement -- a batch would run "
                    f"text this library never inspected: {self.statement!r}"
                )
        elif self.statement is not None:
            raise ValueError(
                "tag.statement applies to session placement only; with "
                "comment placement it would be silently ignored, so it is "
                "refused instead"
            )
        return self


class PlanStep(BaseModel):
    """One statement and its bound parameters, in the order it must run."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    sql: str
    parameters: Dict[str, Any] = {}


class QueryPlan(BaseModel):
    """The steps one tagged query becomes, and the tag they carry."""

    model_config = ConfigDict(extra="forbid")

    tag: str
    placement: str
    steps: List[PlanStep]

    @property
    def prelude(self) -> List[PlanStep]:
        """Everything that must run, on the same connection, before the read."""
        return [step for step in self.steps if step.kind == TAG_STEP]

    @property
    def query(self) -> PlanStep:
        for step in self.steps:
            if step.kind == QUERY_STEP:
                return step
        raise UntaggedQuery("a plan with no query step cannot be executed")

    @property
    def carries_tag(self) -> bool:
        """Whether the tag is really on this plan -- computed, not assumed.

        Checked again by the resource immediately before execution. The two
        checks look redundant until a later edit grows a path that builds a
        plan some other way; then this is the one that holds.
        """
        if not self.tag.strip():
            return False
        if self.placement == COMMENT:
            return self.tag in self.query.sql
        return any(
            step.parameters.get(TAG_PARAMETER) == self.tag for step in self.prelude
        )


def comment_tagged(statement: str, tag: str) -> str:
    """Prefix the statement with the tag in a comment, or refuse the tag.

    A tag containing a comment terminator would end the comment early and the
    remainder would be parsed as SQL. The tag is JSON, and JSON escapes
    neither `*` nor `/`, so an object name or asset key carrying `*/` would
    reach the statement text intact. Refusing is the only safe answer:
    escaping would change the bytes the report later decodes, and truncating
    would produce a tag that does not decode at all.
    """
    if COMMENT_CLOSE in tag or COMMENT_OPEN in tag:
        raise UnsafeTag(
            "the query tag contains a comment marker and cannot ride in a "
            "comment: it would close the comment and the remainder would be "
            f"read as SQL. Tag: {tag!r}"
        )
    return f"{COMMENT_OPEN} {tag} {COMMENT_CLOSE} {statement}"


def plan_tagged_query(
    transport: TagTransport,
    tag: str,
    statement: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> QueryPlan:
    """Turn one read into the ordered steps that carry its attribution.

    The tag step comes first and the query second, which is the whole of the
    session form's correctness: a tag set after the read describes the next
    query, not this one.
    """
    text = (tag or "").strip()
    if not text:
        raise UntaggedQuery(
            "refusing to plan a query with no attribution. A read that "
            "reaches the warehouse untagged comes back out of history as a "
            "read no asset explains, which is indistinguishable from an "
            "unattributed pipeline -- the number this build exists to close"
        )

    body = (statement or "").strip()
    if not body:
        raise ValueError("a query plan needs a statement")

    values = dict(parameters or {})
    steps: List[PlanStep] = []

    if transport.placement == SESSION:
        steps.append(
            PlanStep(
                kind=TAG_STEP,
                sql=str(transport.statement),
                parameters={TAG_PARAMETER: text},
            )
        )
        steps.append(PlanStep(kind=QUERY_STEP, sql=body, parameters=values))
    else:
        steps.append(
            PlanStep(kind=QUERY_STEP, sql=comment_tagged(body, text), parameters=values)
        )

    plan = QueryPlan(tag=text, placement=transport.placement, steps=steps)
    if not plan.carries_tag:
        raise UntaggedQuery(
            "the plan came out without its tag on it; refusing to execute it"
        )
    return plan


def plan_steps(plan: QueryPlan) -> List[Tuple[str, Dict[str, Any]]]:
    """The plan as `(sql, parameters)` pairs, in order. For callers and tests."""
    return [(step.sql, dict(step.parameters)) for step in plan.steps]
