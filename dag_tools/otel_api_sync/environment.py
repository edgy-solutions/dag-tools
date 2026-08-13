"""Structural template rendering with native JSON types.

Two properties are load-bearing here and both come from the same class:

**Sandboxed.** Mapping YAML is configuration, but it is executable
configuration. ``SandboxedEnvironment`` blocks attribute escapes
(``__class__``, ``__init__``, …) so a mapping file cannot reach out of
the template namespace.

**Native-typed.** A template that is exactly one expression returns the
Python value, not its ``str()``. This is not cosmetic: API payloads carry
numbers (``resultNumeric``), booleans (``deleteMissingEntries``) and
arrays (``entityIdentifiers``), while every value read out of an
OpenTelemetry ``Map(String, String)`` attribute column is a string. A
plain ``SandboxedEnvironment`` would silently stringify all of them and
the resulting JSON would be wrong in a way that only the target API
notices.

``SandboxedEnvironment`` defines neither ``code_generator_class`` nor
``concat``, so combining the two classes resolves cleanly: the MRO picks
up ``NativeCodeGenerator``/``native_concat`` from ``NativeEnvironment``
while ``sandboxed = True`` and the sandbox's method overrides still
apply. (Verified against Jinja2 3.1.x — if a future release moves either
attribute onto the sandbox class, ``test_render_is_native_and_sandboxed``
fails loudly.)
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence
from urllib.parse import quote, urlencode

from jinja2 import StrictUndefined, Undefined
from jinja2.nativetypes import NativeEnvironment
from jinja2.sandbox import SandboxedEnvironment

from dag_tools.otel_api_sync.functions import DEFAULT_ATTRIBUTE_COLUMNS, build_functions
# Re-exported so callers keep one import site; the definition lives in
# plan.py because the Restate handler needs it without pulling in Jinja.
from dag_tools.otel_api_sync.plan import set_path  # noqa: F401

# Keys that give a mapping node list semantics instead of object semantics.
FOR_EACH_KEY = "_for_each"
AS_KEY = "_as"
TEMPLATE_KEY = "_template"


class NativeSandboxedEnvironment(SandboxedEnvironment, NativeEnvironment):
    """Sandboxed evaluation that preserves native Python types."""


def build_environment(
    attribute_columns: Sequence[str] = DEFAULT_ATTRIBUTE_COLUMNS,
    strict: bool = False,
) -> NativeSandboxedEnvironment:
    """Create the rendering environment for one pipeline.

    ``strict=True`` swaps in ``StrictUndefined`` so a typo in a mapping
    expression raises at render time in Dagster rather than shipping a
    null to the API.
    """
    env = NativeSandboxedEnvironment(
        undefined=StrictUndefined if strict else Undefined,
        keep_trailing_newline=False,
    )
    env.globals.update(build_functions(attribute_columns))
    return env


class RawPath(str):
    """A path substitution that opts out of percent-encoding.

    Use via the ``raw()`` template function when a value is *meant* to
    span multiple path segments (``{{ raw(prefix) }}/items``). Anything
    else is encoded, because the common case is a single segment.
    """


def _encode_path_value(value: Any) -> str:
    """Percent-encode one substituted value for use in a path segment.

    Applied by Jinja's ``finalize`` hook, which sees only the output of
    ``{{ }}`` expressions — the literal ``/`` separators an author writes
    in the template are untouched, while an identifier containing a
    space, slash or quote is encoded instead of silently producing a
    broken URL. Real-world identifiers are frequently sentences, so this
    is the difference between working and 404-ing on the first live run.
    """
    if value is None:
        return ""
    if isinstance(value, RawPath):
        return str(value)
    if isinstance(value, Undefined):
        return ""
    return quote(str(value), safe="")


def build_path_environment(
    attribute_columns: Sequence[str] = DEFAULT_ATTRIBUTE_COLUMNS,
    strict: bool = False,
) -> SandboxedEnvironment:
    """Environment for rendering URL paths.

    Deliberately *not* native-typed: a path is always a string, and
    native mode would bypass ``finalize`` for single-expression
    templates — exactly the ``path: "{{ item }}"`` case that most needs
    encoding.
    """
    env = SandboxedEnvironment(
        undefined=StrictUndefined if strict else Undefined,
        finalize=_encode_path_value,
    )
    env.globals.update(build_functions(attribute_columns))
    env.globals["raw"] = RawPath
    return env


def render_query(query: Mapping[str, Any], context: Mapping[str, Any], env) -> str:
    """Render a query mapping into an encoded query string.

    Encoded separately from the path because the escaping rules differ;
    list values repeat the key (``?tag=a&tag=b``).
    """
    if not query:
        return ""
    pairs = []
    for key, template in query.items():
        value = render_structure(template, context, env)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            pairs.extend((key, str(v)) for v in value if v is not None)
        elif isinstance(value, bool):
            pairs.append((key, "true" if value else "false"))
        else:
            pairs.append((key, str(value)))
    return urlencode(pairs)


def render_value(node: Any, context: Mapping[str, Any], env: NativeSandboxedEnvironment) -> Any:
    """Render one leaf. Non-strings and template-free strings pass through."""
    if not isinstance(node, str) or "{{" not in node:
        return node
    value = env.from_string(node).render(**context)
    # Native mode leaves Undefined objects intact; JSON cannot carry them.
    return None if isinstance(value, Undefined) else value


def render_structure(node: Any, context: Mapping[str, Any], env: NativeSandboxedEnvironment) -> Any:
    """Render a YAML payload structure, preserving its shape.

    Nesting is preserved verbatim; only leaf strings are treated as
    templates. A mapping carrying ``_for_each`` becomes a list instead:

    .. code-block:: yaml

        items:
          _for_each: "{{ item_names }}"
          _as: name
          _template: {itemName: "{{ name }}"}

    That indirection exists because arrays of objects cannot be built by
    string interpolation without hand-assembling JSON — which is exactly
    where generic mappers usually start emitting malformed payloads.
    """
    if isinstance(node, dict):
        if FOR_EACH_KEY in node:
            sequence = render_value(node[FOR_EACH_KEY], context, env)
            if sequence is None:
                return []
            if isinstance(sequence, dict):
                sequence = list(sequence.items())
            alias = node.get(AS_KEY, "item")
            template = node.get(TEMPLATE_KEY)
            if template is None:
                raise ValueError(f"'{FOR_EACH_KEY}' node is missing '{TEMPLATE_KEY}'")
            rendered = []
            for index, element in enumerate(sequence):
                scope: Dict[str, Any] = dict(context)
                scope[alias] = element
                scope["loop_index"] = index
                rendered.append(render_structure(template, scope, env))
            return rendered
        return {key: render_structure(value, context, env) for key, value in node.items()}

    if isinstance(node, (list, tuple)):
        return [render_structure(value, context, env) for value in node]

    return render_value(node, context, env)
