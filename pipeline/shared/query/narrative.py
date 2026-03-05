"""
Insight Harbor — AI Narrative Generator
==========================================
Produces human-readable natural-language summaries of query results.

Two modes:
  1. **Rule-based** (default / offline) — deterministic template engine that
     describes key statistics, trends, and rankings found in the result data.
  2. **LLM-powered** (optional) — sends the result + schema context to an
     Azure OpenAI deployment for a richer narrative.  This path is only
     activated when ``IH_OPENAI_ENDPOINT`` and ``IH_OPENAI_KEY`` are set.

The rule-based engine is always available and requires no external services,
making it suitable for the PoC.  The LLM path is a future enhancement.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from shared.query.query_generator import QueryPlan
from shared.query.schema_catalog import SchemaCatalog

logger = logging.getLogger("ih.query.narrative")

# Number formatting helpers
_LARGE_NUM_THRESHOLD = 1_000_000
_MEDIUM_NUM_THRESHOLD = 1_000


def _fmt_num(val: Any) -> str:
    """Format a number for human display."""
    if val is None:
        return "N/A"
    try:
        n = float(val)
    except (TypeError, ValueError):
        return str(val)

    if n != n:  # NaN
        return "N/A"

    if abs(n) >= _LARGE_NUM_THRESHOLD:
        return f"{n / 1_000_000:,.1f}M"
    if abs(n) >= _MEDIUM_NUM_THRESHOLD:
        return f"{n / 1_000:,.1f}K"
    if n == int(n):
        return f"{int(n):,}"
    return f"{n:,.2f}"


class NarrativeGenerator:
    """Rule-based narrative generator for query results.

    Parameters
    ----------
    catalog : SchemaCatalog
        Used to look up column descriptions and display names.
    """

    def __init__(self, catalog: SchemaCatalog) -> None:
        self._catalog = catalog

    def generate(
        self,
        plan: QueryPlan,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """Generate a narrative summary of the query result.

        Returns
        -------
        dict with keys:
            - ``summary`` — 1-3 sentence overview
            - ``insights`` — list of individual insight strings
            - ``methodology`` — description of how the data was queried
        """
        row_count = result.get("row_count", 0)
        total_rows = result.get("total_rows", row_count)
        data = result.get("data", [])
        columns = result.get("columns", [])
        dataset_display = self._display_name(plan.dataset)

        insights: list[str] = []

        # Build the summary sentence
        summary = self._build_summary(plan, result, dataset_display)

        # Generate insights based on the query shape
        if plan.group_by and data:
            insights.extend(self._grouped_insights(plan, data, columns))

        if not plan.group_by and data:
            insights.extend(self._flat_insights(plan, data, columns))

        # Measure-specific insights
        insights.extend(self._measure_insights(plan, data, columns))

        # Truncation warning
        if result.get("truncated"):
            insights.append(
                f"Results were limited to {row_count:,} rows out of "
                f"{total_rows:,} total matching rows."
            )

        methodology = self._build_methodology(plan, result, dataset_display)

        return {
            "summary": summary,
            "insights": insights,
            "methodology": methodology,
        }

    # ── Summary builders ──────────────────────────────────────────────────

    def _build_summary(
        self,
        plan: QueryPlan,
        result: dict[str, Any],
        dataset_display: str,
    ) -> str:
        row_count = result.get("row_count", 0)
        data = result.get("data", [])

        if row_count == 0:
            return f"No data was found in {dataset_display} matching the specified criteria."

        # KPI-style: single row aggregation
        if row_count == 1 and plan.aggregations:
            parts = []
            row = data[0]
            for agg in plan.aggregations:
                alias = agg.get("alias", agg["function"])
                val = row.get(alias)
                col_desc = agg.get("column", "records")
                parts.append(f"{alias}: {_fmt_num(val)}")
            agg_summary = ", ".join(parts)
            return f"{dataset_display} shows {agg_summary}."

        # Grouped data
        if plan.group_by:
            group_desc = " and ".join(plan.group_by)
            return (
                f"Analysis of {dataset_display} grouped by {group_desc} "
                f"returned {row_count:,} groups."
            )

        # Flat data
        filter_desc = ""
        if plan.filters:
            filter_desc = " with applied filters"
        return (
            f"Query returned {row_count:,} records from "
            f"{dataset_display}{filter_desc}."
        )

    def _build_methodology(
        self,
        plan: QueryPlan,
        result: dict[str, Any],
        dataset_display: str,
    ) -> str:
        parts = [f"Data source: {dataset_display} (Silver layer)."]

        if plan.filters:
            filter_descs = []
            for f in plan.filters:
                filter_descs.append(f"{f['column']} {f['op']} {f.get('value', '')}")
            parts.append(f"Filters applied: {'; '.join(filter_descs)}.")

        if plan.group_by:
            parts.append(f"Grouped by: {', '.join(plan.group_by)}.")

        if plan.aggregations:
            agg_descs = []
            for a in plan.aggregations:
                col = a.get("column", "*")
                agg_descs.append(f"{a['function']}({col})")
            parts.append(f"Aggregations: {', '.join(agg_descs)}.")

        if plan.sort_by:
            sort_descs = [f"{s['column']} {s.get('order', 'asc')}" for s in plan.sort_by]
            parts.append(f"Sorted by: {', '.join(sort_descs)}.")

        elapsed = result.get("elapsed_ms", 0)
        parts.append(f"Executed in {elapsed:.0f}ms.")

        return " ".join(parts)

    # ── Insight generators ─────────────────────────────────────────────

    def _grouped_insights(
        self,
        plan: QueryPlan,
        data: list[dict],
        columns: list[str],
    ) -> list[str]:
        """Generate insights for grouped/aggregated results."""
        insights: list[str] = []
        if not data:
            return insights

        group_col = plan.group_by[0] if plan.group_by else None

        # Find the primary value column (first aggregation or first non-group column)
        value_col = None
        if plan.aggregations:
            value_col = plan.aggregations[0].get("alias", plan.aggregations[0]["function"])
        else:
            for c in columns:
                if c not in (plan.group_by or []):
                    value_col = c
                    break

        if not value_col or not group_col:
            return insights

        # Top and bottom performers
        try:
            sorted_data = sorted(data, key=lambda r: float(r.get(value_col, 0) or 0), reverse=True)
        except (TypeError, ValueError):
            return insights

        if sorted_data:
            top = sorted_data[0]
            insights.append(
                f"Highest {value_col}: {top.get(group_col)} "
                f"with {_fmt_num(top.get(value_col))}."
            )

        if len(sorted_data) > 1:
            bottom = sorted_data[-1]
            insights.append(
                f"Lowest {value_col}: {bottom.get(group_col)} "
                f"with {_fmt_num(bottom.get(value_col))}."
            )

        # Concentration — does top group represent > 50% of total?
        try:
            total = sum(float(r.get(value_col, 0) or 0) for r in sorted_data)
            if total > 0:
                top_val = float(sorted_data[0].get(value_col, 0) or 0)
                pct = (top_val / total) * 100
                if pct > 50:
                    insights.append(
                        f"{sorted_data[0].get(group_col)} accounts for "
                        f"{pct:.0f}% of total {value_col}."
                    )
        except (TypeError, ValueError):
            pass

        return insights

    def _flat_insights(
        self,
        plan: QueryPlan,
        data: list[dict],
        columns: list[str],
    ) -> list[str]:
        """Generate insights for non-grouped (flat) results."""
        insights: list[str] = []
        if not data:
            return insights

        # Find numeric columns and compute basic stats
        for col in columns[:5]:  # Limit to first 5 columns
            numeric_vals = []
            for row in data:
                try:
                    numeric_vals.append(float(row.get(col, 0) or 0))
                except (TypeError, ValueError):
                    break
            else:
                if numeric_vals and len(numeric_vals) > 1:
                    avg = sum(numeric_vals) / len(numeric_vals)
                    mn = min(numeric_vals)
                    mx = max(numeric_vals)
                    if mx > mn:
                        insights.append(
                            f"{col}: avg {_fmt_num(avg)}, "
                            f"range {_fmt_num(mn)}–{_fmt_num(mx)}."
                        )

        return insights

    def _measure_insights(
        self,
        plan: QueryPlan,
        data: list[dict],
        columns: list[str],
    ) -> list[str]:
        """Generate measure-specific insights (e.g. token totals)."""
        insights: list[str] = []
        if not data or not plan.aggregations:
            return insights

        for agg in plan.aggregations:
            alias = agg.get("alias", agg["function"])
            func = agg["function"]
            col = agg.get("column", "")

            if func in ("sum", "count") and len(data) > 1:
                try:
                    total = sum(float(r.get(alias, 0) or 0) for r in data)
                    avg = total / len(data)
                    insights.append(
                        f"Total {alias} across all groups: {_fmt_num(total)} "
                        f"(avg per group: {_fmt_num(avg)})."
                    )
                except (TypeError, ValueError):
                    pass

        return insights

    # ── Utilities ─────────────────────────────────────────────────────────

    def _display_name(self, dataset: str) -> str:
        """Get a human-readable name for the dataset."""
        try:
            return self._catalog.get_display_name(dataset)
        except (KeyError, AttributeError):
            return dataset.replace("_", " ").title()
