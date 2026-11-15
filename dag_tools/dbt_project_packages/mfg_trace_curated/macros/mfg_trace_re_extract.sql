{#
  Extract one capture group of `pattern` from `expression`.

  Hand-written -- the one piece of this package that is not generated,
  because it is the one piece that is dialect-specific. Every dialect
  decision in the slice is funnelled through here so the generated models
  carry none: they call this macro and nothing else.

  Both arguments arrive as strings holding SQL and a regex respectively.
  They have to: Jinja evaluates what is inside `{{ ... }}` as an
  *expression*, so a bare SQL fragment there is read as a call to a Jinja
  function of that name and the parse fails. The generator quotes both.

  `group_index` is 1-based and counts the pattern's groups left to right.
  The generator strips the named-group syntax `(?P<name>` down to `(`
  before the pattern gets here, because POSIX engines reject the named
  form; stripping renumbers nothing, so the index still answers.

  TWO adapters are implemented. A deployment on any other warehouse
  supplies its own `<adapter>__mfg_trace_re_extract` in its own project --
  which is the whole reason this is a dispatched macro. Do not widen the
  `default__` body to a vendor's spelling: a default that is wrong for an
  adapter fails at the warehouse for a reason that has nothing to do with
  the data, and one that silently returns the whole match rather than a
  group publishes the wrong number with no error at all.
#}

{% macro mfg_trace_re_extract(expression, pattern, group_index) -%}
{{ return(adapter.dispatch('mfg_trace_re_extract', 'mfg_trace_curated')(expression, pattern, group_index)) }}
{%- endmacro %}


{% macro default__mfg_trace_re_extract(expression, pattern, group_index) -%}
regexp_extract({{ expression }}, '{{ pattern | replace("'", "''") }}', {{ group_index }})
{%- endmacro %}


{#
  Postgres has no three-argument extract: `regexp_match` returns the groups
  as an array, which is then subscripted. The subscript is 1-based, so it is
  the same index the other adapter takes.
#}
{% macro postgres__mfg_trace_re_extract(expression, pattern, group_index) -%}
(regexp_match({{ expression }}, '{{ pattern | replace("'", "''") }}'))[{{ group_index }}]
{%- endmacro %}
