"""
Insight Harbor — Query Generator
===================================
Translates structured query DSL (from the API or an LLM) into
pandas operations that can be executed by the QueryExecutor.

The DSL is a JSON dict with keys:
    - ``dataset``   (str)  — required, e.g. "copilot_usage"
    - ``filters``   (list) — optional, each: {"column", "op", "value"}
    - ``group_by``  (list) — optional, column names
    - ``aggregations`` (list) — optional, each: {"function", "column", "alias"}
    - ``sort_by``   (list) — optional, each: {"column", "order"}  (order = "asc"|"desc")
    - ``limit``     (int)  — optional, default 1000
    - ``columns``   (list) — optional, subset of columns to return

Supported filter ops: ``=``, ``!=``, ``>``, ``>=``, ``<``, ``<=``,
``contains``, ``startswith``, ``in``, ``not_in``, ``is_null``, ``is_not_null``

Supported aggregation functions: ``count``, ``sum``, ``avg`` / ``mean``,
``min``, ``max``, ``nunique``
"""

from __future__ import annotations

import logging
from typing import Any

from shared.query.schema_catalog import SchemaCatalog

logger = logging.getLogger("ih.query.generator")

# Allowed filter operators (prevents arbitrary code execution)
ALLOWED_OPS = frozenset(
    {"=", "!=", ">", ">=", "<", "<=",
     "contains", "startswith", "in", "not_in",
     "is_null", "is_not_null"}
)

ALLOWED_AGG_FUNCS = frozenset(
    {"count", "sum", "avg", "mean", "min", "max", "nunique"}
)


class QueryValidationError(Exception):
    """Raised when the query DSL fails validation."""


class QueryPlan:
    """Validated, executable representation of a query.

    Attributes
    ----------
    dataset : str
    silver_path : str
    filters : list[dict]
    group_by : list[str]
    aggregations : list[dict]
    sort_by : list[dict]
    limit : int
    columns : list[str] | None
    """

    def __init__(
        self,
        *,
        dataset: str,
        silver_path: str,
        filters: list[dict],
        group_by: list[str],
        aggregations: list[dict],
        sort_by: list[dict],
        limit: int,
        columns: list[str] | None,
    ) -> None:
        self.dataset = dataset
        self.silver_path = silver_path
        self.filters = filters
        self.group_by = group_by
        self.aggregations = aggregations
        self.sort_by = sort_by
        self.limit = limit
        self.columns = columns

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "silver_path": self.silver_path,
            "filters": self.filters,
            "group_by": self.group_by,
            "aggregations": self.aggregations,
            "sort_by": self.sort_by,
            "limit": self.limit,
            "columns": self.columns,
        }


class QueryGenerator:
    """Validates a query DSL dict and produces a :class:`QueryPlan`."""

    def __init__(self, catalog: SchemaCatalog) -> None:
        self._catalog = catalog

    def generate(self, query_dsl: dict[str, Any]) -> QueryPlan:
        """Validate *query_dsl* and return a ``QueryPlan``.

        Raises ``QueryValidationError`` on any problems.
        """
        errors: list[str] = []

        # ── Dataset ──────────────────────────────────────────────────────
        dataset = query_dsl.get("dataset", "")
        if not dataset:
            raise QueryValidationError("'dataset' is required")

        schema = self._catalog.get_schema(dataset)
        if schema is None:
            avail = ", ".join(self._catalog.list_datasets())
            raise QueryValidationError(
                f"Unknown dataset '{dataset}'. Available: {avail}"
            )

        valid_columns = set(self._catalog.get_column_names(dataset))
        silver_path = self._catalog.get_silver_path(dataset)

        # ── Filters ──────────────────────────────────────────────────────
        filters: list[dict] = []
        for i, f in enumerate(query_dsl.get("filters", [])):
            col = f.get("column", "")
            op = f.get("op", "=")
            val = f.get("value")

            if col not in valid_columns:
                errors.append(f"filters[{i}]: unknown column '{col}'")
                continue
            if op not in ALLOWED_OPS:
                errors.append(f"filters[{i}]: unsupported op '{op}'")
                continue

            filters.append({"column": col, "op": op, "value": val})

        # ── Group By ─────────────────────────────────────────────────────
        group_by: list[str] = []
        for col in query_dsl.get("group_by", []):
            if col not in valid_columns:
                errors.append(f"group_by: unknown column '{col}'")
            else:
                group_by.append(col)

        # ── Aggregations ─────────────────────────────────────────────────
        aggregations: list[dict] = []
        for i, agg in enumerate(query_dsl.get("aggregations", [])):
            func = agg.get("function", "count")
            col = agg.get("column", "")
            alias = agg.get("alias", "")

            if func not in ALLOWED_AGG_FUNCS:
                errors.append(f"aggregations[{i}]: unsupported function '{func}'")
                continue
            if func != "count" and col and col not in valid_columns:
                errors.append(f"aggregations[{i}]: unknown column '{col}'")
                continue

            if not alias:
                alias = f"{func}_{col}" if col else func

            aggregations.append({"function": func, "column": col, "alias": alias})

        # ── Sort By ──────────────────────────────────────────────────────
        sort_by: list[dict] = []
        for s in query_dsl.get("sort_by", []):
            col = s.get("column", "")
            order = s.get("order", "asc").lower()
            # Allow sorting by aggregation aliases too
            if col not in valid_columns and col not in {a["alias"] for a in aggregations}:
                errors.append(f"sort_by: unknown column '{col}'")
            else:
                sort_by.append({"column": col, "order": order})

        # ── Limit ────────────────────────────────────────────────────────
        limit = min(int(query_dsl.get("limit", 1000)), 10_000)

        # ── Column subset ────────────────────────────────────────────────
        columns: list[str] | None = None
        raw_cols = query_dsl.get("columns")
        if raw_cols:
            columns = []
            for col in raw_cols:
                if col not in valid_columns:
                    errors.append(f"columns: unknown column '{col}'")
                else:
                    columns.append(col)

        # ── Raise on validation errors ───────────────────────────────────
        if errors:
            raise QueryValidationError("; ".join(errors))

        return QueryPlan(
            dataset=dataset,
            silver_path=silver_path,
            filters=filters,
            group_by=group_by,
            aggregations=aggregations,
            sort_by=sort_by,
            limit=limit,
            columns=columns,
        )
