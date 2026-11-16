"""The grammar table's regressions. One per row of plan 6.5, plus the traps.

Every assertion here is about a failure that is silent in production: a
number that came out rounded, a unit that overwrote the source's own, a
status word counted as a parse failure, a non-finite value reaching a
column.
"""

import math

import pytest

from dag_tools.systemlink.measurements import (
    BOUND_HIGH,
    BOUND_LOW,
    NO_COMPARATORS,
    RULES,
    Comparator,
    find_comparator,
    parse_limits,
    parse_row,
    parse_value,
)

#: A sample table, standing in for the one the synthetic example ships. The
#: codes are this *test's* invention, not the library's -- which is the
#: point: a code the library carried would be vocabulary a release decides.
SAMPLE = (
    Comparator(text="<", code="UPPER", bound=BOUND_HIGH),
    Comparator(text=">", code="LOWER", bound=BOUND_LOW),
)


# --- the table's value rows ------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("4.95", 4.95),
        ("-1e-3", -0.001),
        (" 12 ", 12.0),
        ("+0.5", 0.5),
        (".25", 0.25),
    ],
)
def test_a_plain_number_parses(text, expected):
    assert parse_value(text).num == pytest.approx(expected)


@pytest.mark.parametrize(
    "text, number, units",
    [
        ("4.95 V", 4.95, "V"),
        ("120mA", 120.0, "mA"),
        ("3.3 kOhm", 3.3, "kOhm"),
        ("50 %", 50.0, "%"),
    ],
)
def test_a_unit_suffix_is_captured_beside_the_number(text, number, units):
    parsed = parse_value(text)
    assert parsed.num == pytest.approx(number)
    assert parsed.units == units


def test_an_exponent_is_never_mistaken_for_a_unit():
    """`-1e-3` is one number, not `-1` with a unit of `e-3`."""
    parsed = parse_value("-1e-3")
    assert parsed.num == pytest.approx(-0.001)
    assert parsed.units is None


# --- the table's limit rows -----------------------------------------------


def test_a_less_than_limit_is_a_high_limit_with_an_inferred_comparator():
    parsed = parse_limits("< 50", SAMPLE)
    assert parsed.high == 50.0
    assert parsed.low is None
    assert parsed.comparison_type == "UPPER"


def test_a_greater_than_limit_is_a_low_limit_with_an_inferred_comparator():
    parsed = parse_limits("> 4.2", SAMPLE)
    assert parsed.low == pytest.approx(4.2)
    assert parsed.high is None
    assert parsed.comparison_type == "LOWER"


@pytest.mark.parametrize("text", ["1.20 ± 0.02", "1.20 +/- 0.02"])
def test_a_tolerance_becomes_a_low_and_high_pair(text):
    parsed = parse_limits(text)
    assert parsed.low == 1.18
    assert parsed.high == 1.22


def test_a_tolerance_is_computed_in_decimal_not_binary_float():
    """The whole point of the decimal route.

    In binary float this centre and tolerance give 0.9999999999999999 and
    1.0099999999999998. Both read as 1.0 and 1.01 in every report and are
    neither. An exact comparison is the only assertion that can tell the
    two routes apart -- the plan table's own 1.20/0.02 case cannot, because
    those happen to round to the same doubles.
    """
    centre, tolerance = 1.005, 0.005
    assert centre - tolerance != 1.0, "the trap this test exists for has moved"
    assert centre + tolerance != 1.01, "the trap this test exists for has moved"

    parsed = parse_limits("1.005 ± 0.005")
    assert parsed.low == 1.0
    assert parsed.high == 1.01


def test_a_tolerance_wins_over_the_bare_number_rule():
    """Rule order is load-bearing.

    If `number` matched first, a tolerance text would silently become its
    centre with no limits at all -- a row that looks parsed and bounds
    nothing.
    """
    assert parse_limits("1.20 ± 0.02").rule_id == "tolerance"


@pytest.mark.parametrize("text", ["< 50", "<50", "> 4.2", "1.20 ± 0.02"])
def test_a_bound_is_never_published_as_a_measurement(text):
    """A reading of "less than 50" is not a reading of 50.

    This is the defect worth a test: the bound parses to a perfectly good
    number, so a value route that accepted it would publish a measurement
    nobody took, and every downstream average would include it.
    """
    assert parse_value(text).num is None
    assert parse_row(text).measurement_num is None


def test_a_bound_as_a_measurement_text_still_offers_its_unit():
    """No number, but the unit it carries is still a usable fallback."""
    assert parse_row("< 50 mV").units == "mV"
    assert parse_row("< 50 mV").measurement_num is None


def test_a_bare_number_as_a_limits_text_bounds_nothing():
    parsed = parse_limits("50")
    assert parsed.low is None and parsed.high is None
    assert not parsed.is_numeric


# --- non-numeric and non-finite ------------------------------------------


@pytest.mark.parametrize("text", ["PASS", "", "   ", "NaN", "inf", "-inf", None])
def test_a_non_numeric_text_yields_no_number(text):
    assert parse_value(text).num is None


@pytest.mark.parametrize("text", ["1e999", "-1e999"])
def test_a_text_that_overflows_to_infinity_is_absent_not_infinite(text):
    """`float("1e999")` is `inf`. A column must never receive it."""
    assert math.isinf(float(text)), "the overflow this test exists for has moved"
    assert parse_value(text).num is None


def test_a_tolerance_that_overflows_is_absent_rather_than_infinite():
    parsed = parse_limits("1e308 ± 1e308")
    assert parsed.high is None or math.isfinite(parsed.high)


# --- parse_failed --------------------------------------------------------


def test_a_non_numeric_measurement_with_a_numeric_limit_is_a_parse_failure():
    """A numeric limit is what says a number was expected here."""
    row = parse_row("PASS", limits_text="< 50", comparators=SAMPLE)
    assert row.measurement_num is None
    assert row.parse_failed is True


def test_a_non_numeric_measurement_with_no_numeric_limit_is_not_a_failure():
    """Otherwise every status-valued row in the warehouse reads as broken."""
    assert parse_row("PASS", limits_text=None).parse_failed is False
    assert parse_row("PASS", limits_text="").parse_failed is False
    assert parse_row("PASS", limits_text="PASS").parse_failed is False


def test_a_numeric_measurement_is_never_a_parse_failure():
    assert (
        parse_row("4.95", limits_text="< 50", comparators=SAMPLE).parse_failed
        is False
    )


# --- precedence: what the source supplied wins ---------------------------


def test_the_supplied_units_column_wins_over_a_parsed_suffix():
    """The suffix is a fallback. Backwards, it relabels every row where the
    two disagree."""
    assert parse_row("4.95 V", units="mV").units == "mV"


def test_a_parsed_suffix_fills_units_only_when_the_column_is_empty():
    assert parse_row("4.95 V", units=None).units == "V"
    assert parse_row("4.95 V", units="").units == "V"
    assert parse_row("4.95 V", units="   ").units == "V"


def test_a_supplied_comparison_type_wins_over_the_inferred_one():
    row = parse_row(
        "4.95",
        limits_text="< 50",
        comparison_type="SUPPLIED",
        comparators=SAMPLE,
    )
    assert row.comparison_type == "SUPPLIED"


def test_the_comparator_is_inferred_only_when_none_was_supplied():
    inferred = parse_row("4.95", limits_text="< 50", comparators=SAMPLE)
    assert inferred.comparison_type == "UPPER"
    assert parse_row("4.95", limits_text="50", comparators=SAMPLE).comparison_type is None


# --- the table itself ----------------------------------------------------


def test_every_rule_pattern_compiles_and_is_anchored():
    for rule in RULES:
        assert rule.pattern.startswith("^") and rule.pattern.endswith("$")
        rule.regex  # compiles or raises


def test_exactly_one_rule_may_produce_a_measurement():
    """If a second rule ever sets `reads_value`, a bound becomes a reading
    again -- so the count is pinned, not just the flag."""
    value_rules = [rule.id for rule in RULES if rule.reads_value]
    assert value_rules == ["number"]


def test_every_rule_names_the_groups_its_kind_consumes():
    """The SQL generator reads these group names, so a renamed group that
    the Python route still happens to work with would break the other
    route silently."""
    required = {
        "tolerance": {"center", "tol", "unit"},
        "comparator": {"op", "value", "unit"},
        "number": {"value", "unit"},
    }
    for rule in RULES:
        assert rule.kind in required, rule.kind
        assert required[rule.kind] <= set(rule.regex.groupindex)


def test_every_comparator_maps_to_exactly_one_side():
    """A comparator that set neither bound, or both, would publish a limit
    nobody can check against."""
    for row in SAMPLE:
        parsed = parse_limits(row.text + " 7", SAMPLE)
        assert (parsed.low is None) != (parsed.high is None), row.text
        assert parsed.comparison_type == row.code


# --- the comparator table belongs to the deployment ----------------------


def test_the_library_ships_no_comparator_rows():
    """The rule, made true at runtime rather than asserted in a comment.

    A code compiled in here would turn a vocabulary decision into a library
    release -- the same reasoning that keeps classification levels and
    warehouse object names out of this repo.
    """
    assert NO_COMPARATORS == ()
    assert find_comparator("<", NO_COMPARATORS) is None
    assert parse_limits("< 50").comparison_type is None


def test_an_uncovered_bound_is_kept_as_text_and_counted_never_dropped():
    """The architect's ruling, in three assertions.

    No limit is published (nothing says which side an unknown operator
    bounds), no value is read from it, and the row is identifiable -- so a
    table missing a row shows up as a number someone can act on instead of
    data quietly going missing.
    """
    parsed = parse_limits("<= 50", SAMPLE)
    assert parsed.low is None and parsed.high is None
    assert parsed.comparison_type is None
    assert parsed.uncovered_comparator == "<="

    row = parse_row("4.95", limits_text="<= 50", comparators=SAMPLE)
    assert row.uncovered_comparator == "<="
    assert row.low_limit_num is None and row.high_limit_num is None
    assert row.measurement_num == pytest.approx(4.95)


def test_an_uncovered_bound_is_not_read_as_the_operator_it_starts_with():
    """`<=` is not `<`.

    A prefix match would publish a bound the source never stated, and it
    would read as a correct answer: the number is right, the side is right,
    and only the strictness is wrong.
    """
    parsed = parse_limits("<= 50", SAMPLE)
    assert parsed.comparison_type is None
    assert parsed.high is None


def test_an_uncovered_bound_is_not_a_parse_failure():
    """It is its own counted category.

    `parse_failed` says a number was expected and not found. An uncovered
    operator means the *limit* was not understood, which is a gap in the
    deployment's table, not a malformed reading -- and conflating the two
    sends the operator to the wrong place.
    """
    row = parse_row("PASS", limits_text="<= 50", comparators=SAMPLE)
    assert row.parse_failed is False
    assert row.uncovered_comparator == "<="


def test_the_count_comes_from_the_rule_not_from_the_published_columns():
    """Found by probing, and it decides where the counter lives.

    The curated contract declares no column for an uncovered operator, and
    the tempting shortcut -- count any row whose limits text bounds nothing
    and names no comparison -- cannot tell an uncovered operator from a
    limits text that was never a bound at all. Those two go to different
    people: one is a missing row in the deployment's table, the other is a
    step whose limits are a status word, which is nothing to fix. So the SQL
    route counts by re-reading the text with the same rule and the same
    table, exactly as the Python route does.
    """
    uncovered = parse_row("4.95", limits_text="<= 50", comparators=SAMPLE)
    never_a_bound = parse_row("4.95", limits_text="PASS", comparators=SAMPLE)

    for row in (uncovered, never_a_bound):
        assert row.low_limit_num is None
        assert row.high_limit_num is None
        assert row.comparison_type is None

    assert uncovered.uncovered_comparator == "<="
    assert never_a_bound.uncovered_comparator is None


def test_a_bound_with_no_number_is_not_reported_as_an_uncovered_operator():
    """`<` on its own matches no rule, so there is no operator to report.

    It bounds nothing either way; what it is not is evidence that the
    deployment's table is missing a row.
    """
    for text in ("<", "<=", ">"):
        parsed = parse_limits(text, SAMPLE)
        assert parsed.rule_id is None
        assert parsed.uncovered_comparator is None
        assert parsed.low is None and parsed.high is None


def test_a_comparator_row_must_bound_a_side_the_contract_declares():
    with pytest.raises(ValueError):
        Comparator(text="<", code="UPPER", bound="middle")
    assert {BOUND_LOW, BOUND_HIGH} == {"low", "high"}


def test_a_comparator_row_needs_both_a_text_and_a_code():
    with pytest.raises(ValueError):
        Comparator(text="", code="UPPER", bound=BOUND_HIGH)
    with pytest.raises(ValueError):
        Comparator(text="<", code="  ", bound=BOUND_HIGH)
