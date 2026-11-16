"""The normalizer document, and what its ops do to a value.

A normalizer is an ordered list of string-to-string ops, named by an id. A
mapping's `normalize` transform holds only that id (`rules:`); the document
holds the ops. That split is the point: every source's conform mapping
references the SAME id, which makes "identical across sources" a property of
the configuration rather than of discipline.

Pure: dicts in, models out, no IO. The SQL rendering lives in `emit`, which
owns the literal quoting; the row rendering is `apply_ops` here.

**Two regex spellings are the operator's.** `strip`, `replace`,
`lstrip_zeros` and `split_revision` need a regex replace and a positional
regex extract, and every warehouse spells them differently. They arrive as
the mapping's `regex_templates`, as `cast_templates` do; this module names no
dialect. `trim`, `upper` and `lower` render as the standalone transforms of
the same names already do, so the common normalizer needs no configuration.

**`lstrip_zeros` is a regex replace on purpose, never `ltrim(x, '0')`.** The
second argument of `ltrim` is a character SET in some dialects and a
SUBSTRING in others, and the two differ on `"0010"`. One template means one
thing. That near-equivalence is exactly what a later edit "simplifies" into
a wrong answer on some warehouse. `"000"` becomes `""`, not `"0"`.

**The original values go to a `*_raw` column, and that column is the mapping
author's:** a second contract column with `transform: copy` over the same
source. Nothing here generates one -- an invented column name would be a name
from below, so do not build the generator.

**A group that did not participate is NULL on both routes.** A dialect's
positional extract answers `''` or NULL depending on the dialect, so the SQL
wraps it in `nullif(..., '')` and the row route maps an empty result to None.
The stated cost: a genuinely captured empty string reads as no match.

**RE2.** A pattern valid in Python and not in RE2 renders SQL that fails at
the warehouse for a reason that has nothing to do with the data, and the
library cannot know the dialect, so `check_re2` refuses those constructs
ALWAYS. Detection is textual and conservative: an escaped construct such as
a backslash-escaped `(?=` is refused too. A false refusal is a message; a
false accept is a broken model.
"""
import re
import string
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

#: op -> the fields it takes (all required). No other field is accepted.
OP_FIELDS: Dict[str, tuple] = {
    "trim": (),
    "upper": (),
    "lower": (),
    "strip": ("pattern",),
    "replace": ("pattern", "replacement"),
    "lstrip_zeros": (),
    "split_revision": ("pattern",),
}

#: Ops that need the operator's `regex_replace` / `regex_extract` template.
REGEX_TEMPLATE_FOR: Dict[str, str] = {
    "strip": "regex_replace",
    "replace": "regex_replace",
    "lstrip_zeros": "regex_replace",
    "split_revision": "regex_extract",
}

#: Placeholders each template must use, and the only ones it may.
TEMPLATE_PLACEHOLDERS: Dict[str, frozenset] = {
    "regex_replace": frozenset({"expr", "pattern", "replacement"}),
    "regex_extract": frozenset({"expr", "pattern", "group"}),
}

LSTRIP_ZEROS_PATTERN = "^0+"

# (needle, name) pairs checked textually, in order.
_RE2_TEXTUAL = (
    ("(?=", "lookahead"),
    ("(?!", "lookahead"),
    ("(?<=", "lookbehind"),
    ("(?<!", "lookbehind"),
    ("(?P=", "named backreference"),
    ("(?(", "conditional"),
    ("(?>", "atomic group"),
)
_BACKREF = re.compile(r"\\[1-9]")
_POSSESSIVE = re.compile(r"(?:[*+?]|\})\+")


def check_re2(pattern: str) -> None:
    """Refuse a pattern Python cannot compile, then one RE2 lacks.

    Raises `ValueError`. One function with its own tests so the next
    construct has somewhere to go.
    """
    try:
        re.compile(pattern)
    except re.error as error:
        raise ValueError(f"pattern {pattern!r} does not compile: {error}") from error
    for needle, name in _RE2_TEXTUAL:
        if needle in pattern:
            raise ValueError(
                f"pattern {pattern!r} uses a {name} ({needle}), which RE2 does "
                "not support; a warehouse on RE2 would refuse the rendered "
                "SQL for a reason that has nothing to do with the data"
            )
    if _BACKREF.search(pattern):
        raise ValueError(
            f"pattern {pattern!r} uses a backreference, which RE2 does not "
            "support"
        )
    if _POSSESSIVE.search(pattern):
        raise ValueError(
            f"pattern {pattern!r} uses a possessive quantifier, which RE2 "
            "does not support"
        )


def check_template(key: str, template: str) -> None:
    """A `regex_templates` entry names only its own placeholders, all of them."""
    if key not in TEMPLATE_PLACEHOLDERS:
        raise ValueError(
            f"unknown regex template {key!r}; the templates are "
            f"{', '.join(sorted(TEMPLATE_PLACEHOLDERS))}"
        )
    try:
        names = {
            field
            for _, field, _, _ in string.Formatter().parse(template)
            if field is not None
        }
    except ValueError as error:
        raise ValueError(f"regex template {key!r} is malformed: {error}") from error
    allowed = TEMPLATE_PLACEHOLDERS[key]
    if names != allowed:
        raise ValueError(
            f"regex template {key!r} must use exactly "
            f"{', '.join('{' + n + '}' for n in sorted(allowed))}, got "
            f"{', '.join(sorted(names)) or 'none'}"
        )


class NormalizerOp(BaseModel):
    """One op in a normalizer."""

    model_config = ConfigDict(extra="forbid")

    op: str
    pattern: Optional[str] = None
    replacement: Optional[str] = None

    @model_validator(mode="after")
    def _shape(self) -> "NormalizerOp":
        if self.op not in OP_FIELDS:
            raise ValueError(
                f"unknown normalizer op {self.op!r}; the ops are "
                f"{', '.join(OP_FIELDS)}"
            )
        wanted = OP_FIELDS[self.op]
        for field in ("pattern", "replacement"):
            present = getattr(self, field) is not None
            if present and field not in wanted:
                raise ValueError(f"op {self.op!r} takes no `{field}`")
            if not present and field in wanted:
                raise ValueError(f"op {self.op!r} requires `{field}`")
        if self.pattern is not None:
            check_re2(self.pattern)
        if self.replacement is not None and (
            "\\" in self.replacement or "$" in self.replacement
        ):
            # Dialects read `\1` / `$1` in a replacement as a group reference
            # and disagree on which; the row route takes the replacement as
            # literal text, so refuse what could split the two routes.
            raise ValueError(
                f"op 'replace' replacement {self.replacement!r} carries a "
                "backslash or dollar sign, which dialects read as a group "
                "reference and the row route reads as text"
            )
        return self

    @property
    def regex_template(self) -> Optional[str]:
        return REGEX_TEMPLATE_FOR.get(self.op)

    @property
    def idempotent(self) -> bool:
        """Is feeding this op's own output back in a valid inverse?

        The proof builds an input that yields the expected output, so what
        matters is idempotence on the op's own output. `replace` is checked
        structurally (the replacement cannot match the pattern); `strip` is
        taken as idempotent, which is not provable for every pattern -- the
        proof's own comparison catches a pattern where it fails.
        """
        if self.op == "split_revision":
            return False
        if self.op == "replace":
            return re.search(self.pattern, self.replacement) is None
        return True


class Normalizers(BaseModel):
    """The whole document: `normalizers:` maps an id to an ordered op list."""

    model_config = ConfigDict(extra="forbid")

    normalizers: Dict[str, List[NormalizerOp]]

    @field_validator("normalizers")
    @classmethod
    def _each(
        cls, value: Dict[str, List[NormalizerOp]]
    ) -> Dict[str, List[NormalizerOp]]:
        for ident, ops in value.items():
            if not isinstance(ident, str) or not ident.strip():
                raise ValueError(f"a normalizer id must be non-empty text, got {ident!r}")
            if not ops:
                raise ValueError(f"normalizer {ident!r} has no ops")
            for position, op in enumerate(ops):
                if op.op == "split_revision" and position != len(ops) - 1:
                    raise ValueError(
                        f"normalizer {ident!r}: split_revision must be the last "
                        "op, because it selects a part and an op after it would "
                        "operate on that part while reading as the whole value"
                    )
        return value

    @property
    def ids(self) -> List[str]:
        return sorted(self.normalizers)

    def get(self, ident: str) -> Optional[List[NormalizerOp]]:
        return self.normalizers.get(ident)


def load_normalizers(document: Dict[str, Any]) -> Normalizers:
    """Validate a parsed normalizer document. Dict in, model out, no IO."""
    if not isinstance(document, dict):
        raise TypeError(
            f"a normalizer document is a mapping, got {type(document).__name__}"
        )
    return Normalizers.model_validate(document)


class _StrictLoader(yaml.SafeLoader):
    """Refuses a repeated key: PyYAML otherwise keeps the last silently."""


def _construct_unique(loader: _StrictLoader, node: yaml.MappingNode, deep: bool = False):
    seen = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise ValueError(f"duplicate key {key!r} (line {key_node.start_mark.line + 1})")
        seen.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique
)


def load_normalizers_text(text: str) -> Normalizers:
    """Parse YAML/JSON text and validate it, refusing a duplicate normalizer id.

    A parsed dict cannot carry a duplicate -- the parser has already kept the
    last one -- so duplicates are caught here, at the text.
    """
    try:
        document = yaml.load(text, Loader=_StrictLoader)  # safe subclass
    except yaml.YAMLError as error:
        raise ValueError(f"the normalizer document does not parse: {error}") from error
    return load_normalizers(document)


def part_index(pattern: str, part: str) -> int:
    """The positional index of a named group, from Python's own `groupindex`."""
    groups = re.compile(pattern).groupindex
    if part not in groups:
        raise ValueError(
            f"part {part!r} is not a group the pattern {pattern!r} declares; "
            f"it declares {sorted(groups) or 'none'}"
        )
    return groups[part]


def apply_ops(ops: List[NormalizerOp], part: Optional[str], value: Any) -> Any:
    """The row route: ops left to right, each on the previous result.

    Every op is a no-op on a non-`str`, so a null passes through, agreeing
    with SQL's null-propagating functions.
    """
    for op in ops:
        if not isinstance(value, str):
            return value
        if op.op == "trim":
            value = value.strip()
        elif op.op == "upper":
            value = value.upper()
        elif op.op == "lower":
            value = value.lower()
        elif op.op == "strip":
            value = re.sub(op.pattern, "", value)
        elif op.op == "replace":
            replacement = op.replacement
            value = re.sub(op.pattern, lambda _m: replacement, value)
        elif op.op == "lstrip_zeros":
            value = re.sub(LSTRIP_ZEROS_PATTERN, "", value)
        elif op.op == "split_revision":
            # `search`, not `match`: the SQL route's extract is a search in
            # every dialect that has one, and the routes must agree.
            found = re.search(op.pattern, value)
            captured = found.group(part) if found else None
            value = captured if captured else None  # '' is no match
        else:  # pragma: no cover - the model refuses anything else
            raise ValueError(f"no row rendering for normalizer op {op.op!r}")
    return value
