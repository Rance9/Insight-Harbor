"""
Insight Harbor — Visualization Recommender
=============================================
Analyses a :class:`QueryPlan` and its result set and recommends the most
appropriate chart type plus configuration (axes, series, colours, labels).

The recommender uses a rule-based engine that inspects:
  • column semantic tags from the schema catalog
  • the presence of ``group_by`` / ``aggregations`` in the plan
  • column data types and cardinality in the result data
  • temporal columns (date / datetime)

Supported chart types
---------------------
- ``kpi``        — single aggregate value / scorecard
- ``bar``        — categorical comparison
- ``stacked_bar``— categorical comparison with sub-categories
- ``line``       — time-series trends
- ``area``       — time-series (cumulative / stacked)
- ``pie``        — proportion (≤ 8 categories)
- ``donut``      — proportion variant
- ``heatmap``    — two categorical dimensions + measure
- ``table``      — fallback / raw data
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from shared.query.query_generator import QueryPlan
from shared.query.schema_catalog import SchemaCatalog

logger = logging.getLogger("ih.query.viz")

# Upper bound for pie / donut — beyond this, recommend bar instead
_PIE_MAX_CATEGORIES = 8

# Semantic tags that indicate temporal data
_TEMPORAL_TAGS = frozenset({"date", "datetime", "timestamp", "time_series"})

# Semantic tags that indicate a category / dimension
_CATEGORY_TAGS = frozenset({"category", "dimension", "identifier", "app", "department"})

# Semantic tags that indicate a measure / metric
_MEASURE_TAGS = frozenset({"measure", "metric", "count", "duration", "tokens", "score"})


@dataclass
class VizRecommendation:
    """Recommended visualisation for a query result."""

    chart_type: str
    title: str
    x_axis: str | None = None
    y_axis: str | None = None
    series: str | None = None        # colour / grouping dimension
    value_columns: list[str] = field(default_factory=list)
    description: str = ""
    confidence: float = 1.0          # 0–1 heuristic confidence score
    options: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class VizRecommender:
    """Rule-based chart recommender.

    Parameters
    ----------
    catalog : SchemaCatalog
        Schema catalog used to look up semantic tags for columns.
    """

    def __init__(self, catalog: SchemaCatalog) -> None:
        self._catalog = catalog

    # ── Public API ────────────────────────────────────────────────────────

    def recommend(
        self,
        plan: QueryPlan,
        result: dict[str, Any],
    ) -> VizRecommendation:
        """Return the best chart recommendation for *plan* + *result*.

        Parameters
        ----------
        plan : QueryPlan
            The executed query plan (provides group_by, aggregations, etc.)
        result : dict
            The executor result dict (provides data, row_count, columns).
        """
        row_count = result.get("row_count", 0)
        columns = result.get("columns", [])
        data = result.get("data", [])
        dataset = plan.dataset

        # Gather semantic metadata about columns
        col_meta = self._classify_columns(dataset, columns)
        temporal_cols = col_meta["temporal"]
        category_cols = col_meta["category"]
        measure_cols = col_meta["measure"]
        other_cols = col_meta["other"]

        # Aggregation aliases are always measures even if not in the schema
        if plan.aggregations:
            agg_aliases = {a.get("alias", a["function"]) for a in plan.aggregations}
            for alias in agg_aliases:
                if alias in columns and alias not in measure_cols:
                    measure_cols.append(alias)

        # ── Decision tree ─────────────────────────────────────────────

        # 1. KPI — single row, single/few numeric columns
        if row_count == 1 and measure_cols:
            return VizRecommendation(
                chart_type="kpi",
                title=self._make_title("KPI", plan),
                value_columns=measure_cols,
                description="Single-value scorecard",
                confidence=1.0,
            )

        # 2. Grouped aggregation with temporal x-axis → line chart
        if plan.group_by and temporal_cols:
            time_col = temporal_cols[0]
            if time_col in plan.group_by:
                value_cols = measure_cols or [c for c in columns if c not in plan.group_by]
                non_time_groups = [g for g in plan.group_by if g != time_col]
                series_col = non_time_groups[0] if non_time_groups else None
                return VizRecommendation(
                    chart_type="line",
                    title=self._make_title("Trend", plan),
                    x_axis=time_col,
                    y_axis=value_cols[0] if value_cols else None,
                    series=series_col,
                    value_columns=value_cols,
                    description=f"Time-series trend over {time_col}",
                    confidence=0.9,
                    options={"show_markers": row_count < 50},
                )

        # 3. Grouped aggregation (non-temporal) → bar or pie
        if plan.group_by and category_cols:
            group_col = plan.group_by[0]
            value_cols = measure_cols or [c for c in columns if c not in plan.group_by]
            cardinality = row_count  # each row is a group in aggregated result

            # Sub-category grouping → stacked bar
            if len(plan.group_by) >= 2:
                return VizRecommendation(
                    chart_type="stacked_bar",
                    title=self._make_title("Breakdown", plan),
                    x_axis=plan.group_by[0],
                    y_axis=value_cols[0] if value_cols else None,
                    series=plan.group_by[1],
                    value_columns=value_cols,
                    description=f"{plan.group_by[0]} by {plan.group_by[1]}",
                    confidence=0.85,
                )

            # Low cardinality → pie / donut
            if cardinality <= _PIE_MAX_CATEGORIES and len(value_cols) == 1:
                return VizRecommendation(
                    chart_type="donut",
                    title=self._make_title("Distribution", plan),
                    x_axis=group_col,
                    y_axis=value_cols[0],
                    value_columns=value_cols,
                    description=f"Proportion by {group_col}",
                    confidence=0.8,
                )

            # Higher cardinality → bar
            return VizRecommendation(
                chart_type="bar",
                title=self._make_title("Comparison", plan),
                x_axis=group_col,
                y_axis=value_cols[0] if value_cols else None,
                value_columns=value_cols,
                description=f"Comparison across {group_col}",
                confidence=0.85,
            )

        # 4. Grouped aggregation (no semantic tags matched) — still bar
        if plan.group_by:
            group_col = plan.group_by[0]
            value_cols = [c for c in columns if c not in plan.group_by]
            return VizRecommendation(
                chart_type="bar",
                title=self._make_title("Results", plan),
                x_axis=group_col,
                y_axis=value_cols[0] if value_cols else None,
                value_columns=value_cols,
                description=f"Grouped by {group_col}",
                confidence=0.6,
            )

        # 5. No grouping, temporal column present → line
        if temporal_cols and measure_cols and row_count > 1:
            return VizRecommendation(
                chart_type="line",
                title=self._make_title("Trend", plan),
                x_axis=temporal_cols[0],
                y_axis=measure_cols[0],
                value_columns=measure_cols,
                description=f"Time-series of {measure_cols[0]}",
                confidence=0.7,
            )

        # 6. Fallback → table
        return VizRecommendation(
            chart_type="table",
            title=self._make_title("Data", plan),
            value_columns=columns,
            description="Tabular data view",
            confidence=0.5,
        )

    def recommend_multiple(
        self,
        plan: QueryPlan,
        result: dict[str, Any],
        max_alternatives: int = 3,
    ) -> list[VizRecommendation]:
        """Return primary + alternative chart recommendations.

        The first element is always the primary (best) recommendation.
        """
        primary = self.recommend(plan, result)
        alternatives: list[VizRecommendation] = [primary]

        # Always offer a table alternative
        if primary.chart_type != "table":
            columns = result.get("columns", [])
            alternatives.append(
                VizRecommendation(
                    chart_type="table",
                    title=self._make_title("Data", plan),
                    value_columns=columns,
                    description="Tabular data view",
                    confidence=0.5,
                )
            )

        # For pies/donuts, also suggest bar
        if primary.chart_type in ("pie", "donut"):
            alternatives.append(
                VizRecommendation(
                    chart_type="bar",
                    title=self._make_title("Comparison", plan),
                    x_axis=primary.x_axis,
                    y_axis=primary.y_axis,
                    value_columns=primary.value_columns,
                    description="Bar chart alternative",
                    confidence=0.7,
                )
            )

        # For line charts, also suggest area
        if primary.chart_type == "line":
            alternatives.append(
                VizRecommendation(
                    chart_type="area",
                    title=primary.title.replace("Trend", "Area Trend"),
                    x_axis=primary.x_axis,
                    y_axis=primary.y_axis,
                    series=primary.series,
                    value_columns=primary.value_columns,
                    description="Stacked area alternative",
                    confidence=primary.confidence - 0.1,
                )
            )

        return alternatives[:max_alternatives + 1]

    # ── Internal helpers ──────────────────────────────────────────────────

    def _classify_columns(
        self,
        dataset: str,
        columns: list[str],
    ) -> dict[str, list[str]]:
        """Classify columns by semantic category using the schema catalog."""
        temporal: list[str] = []
        category: list[str] = []
        measure: list[str] = []
        other: list[str] = []

        try:
            schema = self._catalog.get_schema(dataset)
            if schema is None:
                raise KeyError(dataset)
        except KeyError:
            # No schema — fall back to name-based heuristics
            for col in columns:
                cl = col.lower()
                if any(t in cl for t in ("date", "time", "created", "loaded")):
                    temporal.append(col)
                elif any(t in cl for t in ("count", "total", "sum", "avg", "duration", "tokens")):
                    measure.append(col)
                else:
                    other.append(col)
            return {"temporal": temporal, "category": category,
                    "measure": measure, "other": other}

        col_index = {c["name"]: c for c in schema.get("columns", [])}

        for col in columns:
            meta = col_index.get(col, {})
            tag = meta.get("semantic_tag", "").lower()
            col_type = meta.get("type", "").lower()

            if tag in _TEMPORAL_TAGS or col_type in ("date", "datetime"):
                temporal.append(col)
            elif tag in _CATEGORY_TAGS:
                category.append(col)
            elif tag in _MEASURE_TAGS:
                measure.append(col)
            else:
                # Fall back to name heuristics
                cl = col.lower()
                if any(t in cl for t in ("date", "time", "created")):
                    temporal.append(col)
                elif any(t in cl for t in ("count", "total", "sum", "avg", "duration", "tokens")):
                    measure.append(col)
                else:
                    other.append(col)

        return {"temporal": temporal, "category": category,
                "measure": measure, "other": other}

    @staticmethod
    def _make_title(prefix: str, plan: QueryPlan) -> str:
        """Build a human-readable chart title."""
        display = plan.dataset.replace("_", " ").title()
        return f"{prefix}: {display}"
