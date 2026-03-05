"""
Tests for VizRecommender and NarrativeGenerator
==================================================
Covers chart-type recommendations and narrative generation.
"""
from __future__ import annotations

import io, csv, os, textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_schema_dir(tmp_path: Path) -> Path:
    """Create a minimal schema dir with one dataset."""
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    (schema_dir / "test_data.yaml").write_text(textwrap.dedent("""\
        dataset: test_data
        display_name: Test Data
        description: Test dataset for viz/narrative
        silver_path: silver/test/data.csv
        grain: one row per event
        columns:
          - name: UsageDate
            type: date
            description: Date of the event
            semantic_tag: date
            queryable: true
          - name: AppHost
            type: string
            description: Application host
            semantic_tag: category
            queryable: true
          - name: Department
            type: string
            description: Department
            semantic_tag: category
            queryable: true
          - name: EventCount
            type: integer
            description: Number of events
            semantic_tag: count
            queryable: true
          - name: TokensTotal
            type: integer
            description: Total tokens
            semantic_tag: tokens
            queryable: true
          - name: UserId
            type: string
            description: User email
            semantic_tag: identifier
            queryable: true
    """))
    return schema_dir


def _make_plan(
    dataset="test_data",
    filters=None,
    group_by=None,
    aggregations=None,
    sort_by=None,
    limit=1000,
    columns=None,
):
    from shared.query.query_generator import QueryPlan
    return QueryPlan(
        dataset=dataset,
        silver_path=f"silver/test/{dataset}.csv",
        filters=filters or [],
        group_by=group_by or [],
        aggregations=aggregations or [],
        sort_by=sort_by or [],
        limit=limit,
        columns=columns,
    )


# ─── fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def schema_dir(tmp_path):
    return _make_schema_dir(tmp_path)


@pytest.fixture()
def catalog(schema_dir, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from shared.query.schema_catalog import SchemaCatalog
    return SchemaCatalog(schemas_dir=schema_dir)


@pytest.fixture()
def recommender(catalog):
    from shared.query.viz_recommender import VizRecommender
    return VizRecommender(catalog)


@pytest.fixture()
def narrator(catalog):
    from shared.query.narrative import NarrativeGenerator
    return NarrativeGenerator(catalog)


# ═════════════════════════════════════════════════════════════════════════════
# VizRecommender tests
# ═════════════════════════════════════════════════════════════════════════════

class TestVizRecommender:

    # ── KPI ───────────────────────────────────────────────────────────

    def test_kpi_single_row(self, recommender):
        """Single-row aggregation → KPI chart."""
        plan = _make_plan(aggregations=[{"function": "sum", "column": "TokensTotal", "alias": "total"}])
        result = {"row_count": 1, "columns": ["total"], "data": [{"total": 42000}]}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "kpi"

    def test_kpi_multiple_measures(self, recommender):
        """Single row with multiple measures → still KPI."""
        plan = _make_plan(aggregations=[
            {"function": "sum", "column": "TokensTotal", "alias": "tokens"},
            {"function": "count", "alias": "events"},
        ])
        result = {"row_count": 1, "columns": ["tokens", "events"],
                  "data": [{"tokens": 5000, "events": 100}]}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "kpi"

    # ── Line chart (temporal grouping) ────────────────────────────────

    def test_line_temporal_group(self, recommender):
        """Group by temporal column → line chart."""
        plan = _make_plan(
            group_by=["UsageDate"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 7, "columns": ["UsageDate", "events"],
                  "data": [{"UsageDate": f"2026-03-0{i}", "events": i * 10} for i in range(1, 8)]}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "line"
        assert rec.x_axis == "UsageDate"

    def test_line_with_series(self, recommender):
        """Temporal group + secondary group → line with series."""
        plan = _make_plan(
            group_by=["UsageDate", "AppHost"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 14, "columns": ["UsageDate", "AppHost", "events"],
                  "data": []}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "line"
        assert rec.series == "AppHost"

    # ── Bar chart ─────────────────────────────────────────────────────

    def test_bar_high_cardinality(self, recommender):
        """Category group with many values → bar."""
        plan = _make_plan(
            group_by=["UserId"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 15, "columns": ["UserId", "events"],
                  "data": [{"UserId": f"user{i}@ex.com", "events": i} for i in range(15)]}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "bar"

    # ── Stacked bar ───────────────────────────────────────────────────

    def test_stacked_bar_two_groups(self, recommender):
        """Two non-temporal categorical groups → stacked bar."""
        plan = _make_plan(
            group_by=["AppHost", "Department"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 9, "columns": ["AppHost", "Department", "events"],
                  "data": []}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "stacked_bar"
        assert rec.series == "Department"

    # ── Donut / pie ───────────────────────────────────────────────────

    def test_donut_low_cardinality(self, recommender):
        """Category group ≤ 8 with single value → donut."""
        plan = _make_plan(
            group_by=["AppHost"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 4, "columns": ["AppHost", "events"],
                  "data": [{"AppHost": h, "events": c} for h, c in
                           [("Word", 30), ("Teams", 50), ("Excel", 15), ("PPT", 5)]]}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "donut"

    # ── Table fallback ────────────────────────────────────────────────

    def test_table_fallback_no_grouping(self, recommender):
        """No grouping, no temporal, no measure → table."""
        plan = _make_plan(columns=["UserId", "AppHost"])
        result = {"row_count": 10, "columns": ["UserId", "AppHost"],
                  "data": [{"UserId": "a", "AppHost": "Word"}] * 10}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "table"

    def test_table_empty_result(self, recommender):
        """Empty result → table."""
        plan = _make_plan()
        result = {"row_count": 0, "columns": [], "data": []}
        rec = recommender.recommend(plan, result)
        assert rec.chart_type == "table"

    # ── recommend_multiple ────────────────────────────────────────────

    def test_multiple_includes_table(self, recommender):
        """Multiple recommendations always include a table alternative."""
        plan = _make_plan(
            group_by=["AppHost"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 4, "columns": ["AppHost", "events"], "data": []}
        recs = recommender.recommend_multiple(plan, result)
        types = [r.chart_type for r in recs]
        assert "table" in types
        assert recs[0].chart_type != "table"  # primary is not table

    def test_multiple_line_includes_area(self, recommender):
        """Line chart alternatives include area."""
        plan = _make_plan(
            group_by=["UsageDate"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 7, "columns": ["UsageDate", "events"], "data": []}
        recs = recommender.recommend_multiple(plan, result)
        types = [r.chart_type for r in recs]
        assert "area" in types

    def test_multiple_donut_includes_bar(self, recommender):
        """Donut alternatives include bar."""
        plan = _make_plan(
            group_by=["AppHost"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 3, "columns": ["AppHost", "events"],
                  "data": [{"AppHost": "Word", "events": 10}]}
        recs = recommender.recommend_multiple(plan, result)
        types = [r.chart_type for r in recs]
        assert "bar" in types

    # ── VizRecommendation.to_dict ─────────────────────────────────────

    def test_recommendation_to_dict(self, recommender):
        plan = _make_plan()
        result = {"row_count": 0, "columns": [], "data": []}
        rec = recommender.recommend(plan, result)
        d = rec.to_dict()
        assert isinstance(d, dict)
        assert "chart_type" in d
        assert "title" in d
        assert "confidence" in d

    # ── Column classification without schema ──────────────────────────

    def test_classify_columns_no_schema(self, recommender):
        """Fallback heuristic classification when dataset is unknown."""
        meta = recommender._classify_columns("unknown_dataset",
                                             ["EventDate", "TokenCount", "Foo"])
        assert "EventDate" in meta["temporal"]
        assert "TokenCount" in meta["measure"]
        assert "Foo" in meta["other"]

    # ── Confidence ordering ──────────────────────────────────────────

    def test_kpi_high_confidence(self, recommender):
        plan = _make_plan(aggregations=[{"function": "count", "alias": "n"}])
        result = {"row_count": 1, "columns": ["n"], "data": [{"n": 42}]}
        rec = recommender.recommend(plan, result)
        assert rec.confidence >= 0.9

    def test_table_low_confidence(self, recommender):
        plan = _make_plan()
        result = {"row_count": 0, "columns": [], "data": []}
        rec = recommender.recommend(plan, result)
        assert rec.confidence <= 0.5


# ═════════════════════════════════════════════════════════════════════════════
# NarrativeGenerator tests
# ═════════════════════════════════════════════════════════════════════════════

class TestNarrativeGenerator:

    def test_empty_result(self, narrator):
        plan = _make_plan(filters=[{"column": "AppHost", "op": "=", "value": "Word"}])
        result = {"row_count": 0, "total_rows": 0, "data": [], "columns": []}
        nar = narrator.generate(plan, result)
        assert "No data" in nar["summary"]
        assert isinstance(nar["insights"], list)
        assert isinstance(nar["methodology"], str)

    def test_kpi_summary(self, narrator):
        """Single-row aggregation gets KPI-style summary."""
        plan = _make_plan(aggregations=[
            {"function": "sum", "column": "TokensTotal", "alias": "total_tokens"},
        ])
        result = {"row_count": 1, "total_rows": 1, "columns": ["total_tokens"],
                  "data": [{"total_tokens": 1500000}]}
        nar = narrator.generate(plan, result)
        assert "total_tokens" in nar["summary"]
        assert "1.5M" in nar["summary"]

    def test_grouped_summary(self, narrator):
        plan = _make_plan(
            group_by=["AppHost"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 3, "total_rows": 3, "columns": ["AppHost", "events"],
                  "data": [{"AppHost": "Word", "events": 100},
                           {"AppHost": "Teams", "events": 50},
                           {"AppHost": "Excel", "events": 25}]}
        nar = narrator.generate(plan, result)
        assert "3" in nar["summary"]
        assert "grouped" in nar["summary"].lower()

    def test_grouped_insights_top_bottom(self, narrator):
        """Top and bottom performers are called out."""
        plan = _make_plan(
            group_by=["AppHost"],
            aggregations=[{"function": "count", "alias": "events"}],
        )
        result = {"row_count": 3, "total_rows": 3, "columns": ["AppHost", "events"],
                  "data": [{"AppHost": "Word", "events": 100},
                           {"AppHost": "Teams", "events": 50},
                           {"AppHost": "Excel", "events": 10}]}
        nar = narrator.generate(plan, result)
        assert any("Highest" in i for i in nar["insights"])
        assert any("Lowest" in i for i in nar["insights"])

    def test_concentration_insight(self, narrator):
        """When top group > 50%, concentration is flagged."""
        plan = _make_plan(
            group_by=["AppHost"],
            aggregations=[{"function": "sum", "column": "TokensTotal", "alias": "tokens"}],
        )
        result = {"row_count": 3, "total_rows": 3, "columns": ["AppHost", "tokens"],
                  "data": [{"AppHost": "Teams", "tokens": 800},
                           {"AppHost": "Word", "tokens": 100},
                           {"AppHost": "Excel", "tokens": 50}]}
        nar = narrator.generate(plan, result)
        assert any("accounts for" in i for i in nar["insights"])

    def test_truncation_warning(self, narrator):
        plan = _make_plan(limit=2)
        result = {"row_count": 2, "total_rows": 100, "truncated": True,
                  "columns": ["UserId"], "data": [{"UserId": "a"}, {"UserId": "b"}]}
        nar = narrator.generate(plan, result)
        assert any("limited" in i.lower() for i in nar["insights"])

    def test_methodology_includes_filters(self, narrator):
        plan = _make_plan(
            filters=[{"column": "AppHost", "op": "=", "value": "Word"}],
            group_by=["Department"],
            aggregations=[{"function": "count", "alias": "n"}],
            sort_by=[{"column": "n", "order": "desc"}],
        )
        result = {"row_count": 2, "total_rows": 2, "elapsed_ms": 45.2,
                  "columns": ["Department", "n"],
                  "data": [{"Department": "Eng", "n": 50}, {"Department": "Sales", "n": 30}]}
        nar = narrator.generate(plan, result)
        assert "Filters applied" in nar["methodology"]
        assert "Grouped by" in nar["methodology"]
        assert "Aggregations" in nar["methodology"]
        assert "Sorted by" in nar["methodology"]
        assert "45ms" in nar["methodology"]

    def test_flat_numeric_insights(self, narrator):
        """Non-grouped numeric data produces range insights."""
        plan = _make_plan()
        result = {"row_count": 5, "total_rows": 5,
                  "columns": ["EventCount", "TokensTotal"],
                  "data": [{"EventCount": 10, "TokensTotal": 100},
                           {"EventCount": 20, "TokensTotal": 200},
                           {"EventCount": 30, "TokensTotal": 300},
                           {"EventCount": 40, "TokensTotal": 400},
                           {"EventCount": 50, "TokensTotal": 500}]}
        nar = narrator.generate(plan, result)
        assert any("avg" in i.lower() and "range" in i.lower() for i in nar["insights"])

    def test_generate_returns_expected_keys(self, narrator):
        plan = _make_plan()
        result = {"row_count": 1, "total_rows": 1, "columns": ["UserId"],
                  "data": [{"UserId": "alice"}]}
        nar = narrator.generate(plan, result)
        assert "summary" in nar
        assert "insights" in nar
        assert "methodology" in nar

    def test_measure_total_insight(self, narrator):
        """Sum aggregation across groups produces total insight."""
        plan = _make_plan(
            group_by=["Department"],
            aggregations=[{"function": "sum", "column": "TokensTotal", "alias": "tokens"}],
        )
        result = {"row_count": 3, "total_rows": 3, "columns": ["Department", "tokens"],
                  "data": [{"Department": "Eng", "tokens": 5000},
                           {"Department": "Sales", "tokens": 3000},
                           {"Department": "HR", "tokens": 1000}]}
        nar = narrator.generate(plan, result)
        assert any("Total" in i and "tokens" in i for i in nar["insights"])


# ─── Number formatting ───────────────────────────────────────────────────────

class TestFmtNum:
    def test_millions(self):
        from shared.query.narrative import _fmt_num
        assert _fmt_num(1_500_000) == "1.5M"

    def test_thousands(self):
        from shared.query.narrative import _fmt_num
        assert _fmt_num(2_500) == "2.5K"

    def test_small_int(self):
        from shared.query.narrative import _fmt_num
        assert _fmt_num(42) == "42"

    def test_none(self):
        from shared.query.narrative import _fmt_num
        assert _fmt_num(None) == "N/A"

    def test_nan(self):
        from shared.query.narrative import _fmt_num
        assert _fmt_num(float("nan")) == "N/A"
