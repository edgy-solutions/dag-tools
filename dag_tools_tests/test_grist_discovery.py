"""Tests for Grist friendly-naming + table discovery."""
from dag_tools.components.grist_ingest.discovery import (
    discover_tables,
    friendly_table_name,
    normalize_identifier,
)


# ---------------------------------------------------------------------------
# normalize_identifier
# ---------------------------------------------------------------------------


def test_normalize_lowercases_and_replaces_nonalnum():
    assert normalize_identifier("Sales Ops! 2026") == "sales_ops_2026"


def test_normalize_collapses_and_strips_underscores():
    assert normalize_identifier("  a--b__c  ") == "a_b_c"


def test_normalize_prefixes_leading_digit():
    assert normalize_identifier("123 table").startswith("t_")


def test_normalize_empty_becomes_unnamed():
    assert normalize_identifier("") == "unnamed"
    assert normalize_identifier("!!!") == "unnamed"


def test_normalize_truncates_to_63():
    out = normalize_identifier("x" * 200)
    assert len(out) <= 63


# ---------------------------------------------------------------------------
# friendly_table_name
# ---------------------------------------------------------------------------


def test_friendly_name_combines_parts():
    assert friendly_table_name("Sales Ops", "Quarterly Budget", "Line_Items") == (
        "sales_ops__quarterly_budget__line_items"
    )


def test_friendly_name_can_drop_workspace():
    assert friendly_table_name("WS", "Doc", "T", include_workspace=False) == "doc__t"


def test_friendly_name_is_valid_pg_identifier_length():
    out = friendly_table_name("w" * 40, "d" * 40, "t" * 40)
    assert len(out) <= 63


# ---------------------------------------------------------------------------
# discover_tables (fake client)
# ---------------------------------------------------------------------------


class _FakeClient:
    def __init__(self, docs, tables_by_doc):
        self._docs = docs
        self._tables = tables_by_doc

    def list_docs(self, since=None):
        return [
            d for d in self._docs
            if not since or d.get("updatedAt", "") > since
        ]

    def list_tables(self, doc_id):
        return self._tables.get(doc_id, [])


def test_discover_flattens_docs_and_tables():
    client = _FakeClient(
        docs=[
            {"id": "d1", "name": "Budget", "workspace": "Finance", "updatedAt": "2026-01-01"},
            {"id": "d2", "name": "Roster", "workspace": "HR", "updatedAt": "2026-01-02"},
        ],
        tables_by_doc={
            "d1": [{"id": "Lines"}, {"id": "Summary"}],
            "d2": [{"id": "People"}],
        },
    )
    out = discover_tables(client)
    names = sorted(t.friendly_name for t in out)
    assert names == [
        "finance__budget__lines",
        "finance__budget__summary",
        "hr__roster__people",
    ]
    # doc/table ids ride along for run config; keys stay friendly.
    byname = {t.friendly_name: t for t in out}
    assert byname["hr__roster__people"].doc_id == "d2"
    assert byname["hr__roster__people"].table_id == "People"


def test_discover_respects_since():
    client = _FakeClient(
        docs=[
            {"id": "d1", "name": "A", "workspace": "W", "updatedAt": "2026-01-01"},
            {"id": "d2", "name": "B", "workspace": "W", "updatedAt": "2026-01-05"},
        ],
        tables_by_doc={"d1": [{"id": "T"}], "d2": [{"id": "T"}]},
    )
    out = discover_tables(client, since="2026-01-03")
    assert [t.doc_id for t in out] == ["d2"]


def test_discover_disambiguates_friendly_name_collisions():
    # Two different docs that normalize to the same workspace+doc, same
    # table id -> would collide; discovery must keep them distinct.
    client = _FakeClient(
        docs=[
            {"id": "d1", "name": "Report!", "workspace": "Ops", "updatedAt": "2026-01-01"},
            {"id": "d2", "name": "Report?", "workspace": "Ops", "updatedAt": "2026-01-02"},
        ],
        tables_by_doc={"d1": [{"id": "Data"}], "d2": [{"id": "Data"}]},
    )
    out = discover_tables(client)
    names = [t.friendly_name for t in out]
    assert len(names) == len(set(names)), f"names collided: {names}"


def test_discover_uses_prefetched_docs_without_refetch():
    class _NoListDocs(_FakeClient):
        def list_docs(self, since=None):
            raise AssertionError("should not be called when docs passed in")

    client = _NoListDocs(docs=[], tables_by_doc={"d1": [{"id": "T"}]})
    docs = [{"id": "d1", "name": "Doc", "workspace": "W", "updatedAt": "2026-01-01"}]
    out = discover_tables(client, docs=docs)
    assert [t.friendly_name for t in out] == ["w__doc__t"]
