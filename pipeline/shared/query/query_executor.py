"""
Insight Harbor — Query Executor
==================================
Executes a :class:`QueryPlan` against Silver CSV data stored in ADLS Gen2,
using pandas for filtering, grouping, aggregation, and sorting.

The executor downloads the Silver CSV into a pandas DataFrame, applies
the plan, and returns a JSON-serialisable result dict.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from shared.query.query_generator import QueryPlan

logger = logging.getLogger("ih.query.executor")


class QueryExecutor:
    """Run a :class:`QueryPlan` against ADLS Silver data.

    Parameters
    ----------
    adls_client : object
        Anything with a ``download_text(blob_path) -> str`` method.
        In production this is ``shared.adls_client.ADLSClient``.
    """

    def __init__(self, adls_client: Any) -> None:
        self._adls = adls_client

    def execute(self, plan: QueryPlan) -> dict[str, Any]:
        """Execute *plan* and return a result dict.

        Returns
        -------
        dict with keys:
            - ``dataset`` — dataset name
            - ``row_count`` — rows in result
            - ``columns`` — list of column names
            - ``data`` — list of row dicts
            - ``truncated`` — whether the result was limited
            - ``executed_at`` — ISO timestamp
            - ``plan`` — the QueryPlan as dict (for debugability)
        """
        started = datetime.now(timezone.utc)

        # 1. Load CSV into DataFrame
        df = self._load_dataframe(plan.silver_path)

        # 2. Apply filters
        df = self._apply_filters(df, plan.filters)

        # 3. Apply grouping + aggregation (or column subset)
        if plan.group_by:
            df = self._apply_group_by(df, plan.group_by, plan.aggregations)
        elif plan.columns:
            # Select column subset
            valid = [c for c in plan.columns if c in df.columns]
            if valid:
                df = df[valid]

        # 4. Apply sorting
        df = self._apply_sort(df, plan.sort_by)

        # 5. Apply limit
        total_rows = len(df)
        truncated = total_rows > plan.limit
        df = df.head(plan.limit)

        # 6. Build result
        elapsed_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000

        # Replace NaN/NaT with None for JSON serialisation
        df = df.where(pd.notnull(df), None)

        return {
            "dataset": plan.dataset,
            "row_count": len(df),
            "total_rows": total_rows,
            "columns": list(df.columns),
            "data": df.to_dict(orient="records"),
            "truncated": truncated,
            "executed_at": started.isoformat(),
            "elapsed_ms": round(elapsed_ms, 1),
            "plan": plan.to_dict(),
        }

    # ── Internal helpers ─────────────────────────────────────────────────

    def _load_dataframe(self, silver_path: str) -> pd.DataFrame:
        """Download CSV from ADLS and return a DataFrame."""
        try:
            csv_text = self._adls.download_text(silver_path)
            df = pd.read_csv(io.StringIO(csv_text), low_memory=False)
            logger.info("Loaded %s: %d rows × %d cols", silver_path, len(df), len(df.columns))
            return df
        except Exception as exc:
            logger.error("Failed to load %s: %s", silver_path, exc)
            return pd.DataFrame()

    @staticmethod
    def _apply_filters(df: pd.DataFrame, filters: list[dict]) -> pd.DataFrame:
        """Apply filter predicates sequentially (AND logic)."""
        for f in filters:
            col = f["column"]
            op = f["op"]
            val = f.get("value")

            if col not in df.columns:
                continue

            series = df[col]

            # Coerce comparison value to match column dtype for equality ops
            cmp_val = val
            if series.dtype == "bool" and isinstance(val, str):
                cmp_val = val.lower() in ("true", "1", "yes")

            if op == "=":
                df = df[series == cmp_val]
            elif op == "!=":
                df = df[series != cmp_val]
            elif op == ">":
                df = df[pd.to_numeric(series, errors="coerce") > float(val)]
            elif op == ">=":
                df = df[pd.to_numeric(series, errors="coerce") >= float(val)]
            elif op == "<":
                df = df[pd.to_numeric(series, errors="coerce") < float(val)]
            elif op == "<=":
                df = df[pd.to_numeric(series, errors="coerce") <= float(val)]
            elif op == "contains":
                df = df[series.astype(str).str.contains(str(val), case=False, na=False)]
            elif op == "startswith":
                df = df[series.astype(str).str.startswith(str(val), na=False)]
            elif op == "in":
                if isinstance(val, list):
                    df = df[series.isin(val)]
            elif op == "not_in":
                if isinstance(val, list):
                    df = df[~series.isin(val)]
            elif op == "is_null":
                df = df[series.isna()]
            elif op == "is_not_null":
                df = df[series.notna()]

        return df

    @staticmethod
    def _apply_group_by(
        df: pd.DataFrame,
        group_by: list[str],
        aggregations: list[dict],
    ) -> pd.DataFrame:
        """Group and aggregate the DataFrame."""
        valid_group = [c for c in group_by if c in df.columns]
        if not valid_group:
            return df

        grouped = df.groupby(valid_group, dropna=False)

        if not aggregations:
            # Default: count
            result = grouped.size().reset_index(name="count")
            return result

        agg_dict: dict[str, list] = {}
        renames: dict[str, str] = {}

        for agg in aggregations:
            func = agg["function"]
            col = agg.get("column", "")
            alias = agg.get("alias", func)

            if func == "count":
                # Use first available column for count
                target_col = col if col and col in df.columns else df.columns[0]
                agg_dict.setdefault(target_col, []).append("count")
                renames[(target_col, "count")] = alias
            elif col and col in df.columns:
                pandas_func = "mean" if func == "avg" else func
                agg_dict.setdefault(col, []).append(pandas_func)
                renames[(col, pandas_func)] = alias

        if not agg_dict:
            return grouped.size().reset_index(name="count")

        result = grouped.agg(agg_dict)

        # Flatten MultiIndex columns and apply aliases
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = [
                renames.get((c1, c2), f"{c2}_{c1}")
                for c1, c2 in result.columns
            ]
        result = result.reset_index()

        return result

    @staticmethod
    def _apply_sort(df: pd.DataFrame, sort_by: list[dict]) -> pd.DataFrame:
        """Sort the DataFrame."""
        if not sort_by:
            return df

        cols = []
        ascending = []
        for s in sort_by:
            col = s["column"]
            if col in df.columns:
                cols.append(col)
                ascending.append(s.get("order", "asc") == "asc")

        if cols:
            df = df.sort_values(cols, ascending=ascending)

        return df
