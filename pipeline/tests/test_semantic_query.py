"""
Insight Harbor — Semantic Query Engine Tests
=============================================
Tests for SchemaCatalog, QueryGenerator, QueryPlan, and QueryExecutor.
"""

from __future__ import annotations

import csv
import io
import os
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
import yaml

# Ensure pipeline/shared is importable
ROOT = Path(__file__).resolve().parents[1]          # pipeline/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def tmp_schema_dir(tmp_path: Path) -> Path:
    """Create a minimal schema directory with two YAML files."""
    schemas_dir = tmp_path / "schemas"
    schemas_dir.mkdir()

    copilot_schema = {
        "dataset": "copilot_usage",
        "display_name": "Copilot Usage",
        "connector": "purview_audit",
        "silver_path": "silver/copilot-usage/silver_copilot_usage.csv",
        "description": "Copilot interaction events from Purview audit logs.",
        "grain": "one row per Copilot message turn",
        "refresh": "daily",
        "columns": [
            {"name": "RecordId", "type": "string", "description": "Unique record ID.", "semantic": "id"},
            {"name": "UserId", "type": "string", "description": "User UPN.", "semantic": "user_id", "queryable": True},
            {"name": "CopilotEventData_AppHost", "type": "string", "description": "Copilot host app.",
             "semantic": "category", "queryable": True,
             "common_values": ["Word", "Teams", "Excel"]},
            {"name": "TokensTotal", "type": "integer", "description": "Total tokens.", "semantic": "measure", "queryable": True},
            {"name": "DurationMs", "type": "integer", "description": "Duration in ms.", "semantic": "measure", "queryable": True},
            {"name": "UsageDate", "type": "date", "description": "Usage date.", "semantic": "date", "queryable": True},
            {"name": "Department", "type": "string", "description": "User department.", "semantic": "category", "queryable": True},
            {"name": "IsAgent", "type": "string", "description": "Is agent flag.", "semantic": "flag", "queryable": True},
            {"name": "_LoadedAtUtc", "type": "datetime", "description": "Load timestamp.", "semantic": "metadata"},
        ],
        "aliases": {
            "copilot events": "dataset = 'copilot_usage'",
            "which apps": "group by CopilotEventData_AppHost with count",
            "token usage": "sum of TokensTotal",
        },
    }

    entra_schema = {
        "dataset": "entra_users",
        "display_name": "Entra ID Users",
        "connector": "entra",
        "silver_path": "silver/entra-users/silver_entra_users.csv",
        "description": "User directory snapshot.",
        "grain": "one row per user",
        "columns": [
            {"name": "UserPrincipalName", "type": "string", "description": "UPN.", "semantic": "user_id", "queryable": True},
            {"name": "DisplayName", "type": "string", "description": "Display name.", "semantic": "label", "queryable": True},
            {"name": "Department", "type": "string", "description": "Dept.", "semantic": "category", "queryable": True},
            {"name": "HasCopilotLicense", "type": "string", "description": "Copilot license.", "semantic": "flag", "queryable": True},
            {"name": "Country", "type": "string", "description": "Country.", "semantic": "category", "queryable": True},
        ],
        "aliases": {
            "copilot licensed": "filter HasCopilotLicense = 'True'",
        },
    }

    (schemas_dir / "copilot_usage.yaml").write_text(yaml.dump(copilot_schema), encoding="utf-8")
    (schemas_dir / "entra_users.yaml").write_text(yaml.dump(entra_schema), encoding="utf-8")

    return schemas_dir


@pytest.fixture()
def catalog(tmp_schema_dir: Path):
    from shared.query.schema_catalog import SchemaCatalog
    return SchemaCatalog(tmp_schema_dir)


@pytest.fixture()
def generator(catalog):
    from shared.query.query_generator import QueryGenerator
    return QueryGenerator(catalog)


@pytest.fixture()
def sample_csv() -> str:
    """Build a small sample CSV for the copilot_usage dataset."""
    rows = [
        {"RecordId": "r1", "UserId": "alice@example.com", "CopilotEventData_AppHost": "Word",
         "TokensTotal": "150", "DurationMs": "1200", "UsageDate": "2026-03-01",
         "Department": "Engineering", "IsAgent": "false", "_LoadedAtUtc": "2026-03-01T10:00:00Z"},
        {"RecordId": "r2", "UserId": "bob@example.com", "CopilotEventData_AppHost": "Teams",
         "TokensTotal": "200", "DurationMs": "800", "UsageDate": "2026-03-01",
         "Department": "Sales", "IsAgent": "false", "_LoadedAtUtc": "2026-03-01T10:00:00Z"},
        {"RecordId": "r3", "UserId": "alice@example.com", "CopilotEventData_AppHost": "Word",
         "TokensTotal": "100", "DurationMs": "600", "UsageDate": "2026-03-02",
         "Department": "Engineering", "IsAgent": "true", "_LoadedAtUtc": "2026-03-02T10:00:00Z"},
        {"RecordId": "r4", "UserId": "carol@example.com", "CopilotEventData_AppHost": "Excel",
         "TokensTotal": "300", "DurationMs": "2000", "UsageDate": "2026-03-02",
         "Department": "Engineering", "IsAgent": "false", "_LoadedAtUtc": "2026-03-02T10:00:00Z"},
        {"RecordId": "r5", "UserId": "bob@example.com", "CopilotEventData_AppHost": "Teams",
         "TokensTotal": "175", "DurationMs": "900", "UsageDate": "2026-03-03",
         "Department": "Sales", "IsAgent": "false", "_LoadedAtUtc": "2026-03-03T10:00:00Z"},
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@pytest.fixture()
def mock_adls(sample_csv: str):
    """Mock ADLS client that returns the sample CSV."""
    client = MagicMock()
    client.download_text.return_value = sample_csv
    return client


# ═══════════════════════════════════════════════════════════════════════════════
# SchemaCatalog Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestSchemaCatalog:
    def test_list_datasets(self, catalog):
        datasets = catalog.list_datasets()
        assert datasets == ["copilot_usage", "entra_users"]

    def test_get_schema_returns_dict(self, catalog):
        schema = catalog.get_schema("copilot_usage")
        assert schema is not None
        assert schema["dataset"] == "copilot_usage"
        assert schema["display_name"] == "Copilot Usage"

    def test_get_schema_unknown_returns_none(self, catalog):
        assert catalog.get_schema("nonexistent") is None

    def test_get_columns(self, catalog):
        cols = catalog.get_columns("copilot_usage")
        assert len(cols) == 9
        assert cols[0]["name"] == "RecordId"

    def test_get_column_names(self, catalog):
        names = catalog.get_column_names("copilot_usage")
        assert "RecordId" in names
        assert "TokensTotal" in names
        assert len(names) == 9

    def test_get_queryable_columns(self, catalog):
        qcols = catalog.get_queryable_columns("copilot_usage")
        names = [c["name"] for c in qcols]
        assert "UserId" in names
        assert "RecordId" not in names  # not marked queryable
        assert "_LoadedAtUtc" not in names

    def test_get_column_single(self, catalog):
        col = catalog.get_column("copilot_usage", "TokensTotal")
        assert col is not None
        assert col["type"] == "integer"
        assert col["semantic"] == "measure"

    def test_get_column_unknown(self, catalog):
        assert catalog.get_column("copilot_usage", "Nope") is None

    def test_get_silver_path(self, catalog):
        assert catalog.get_silver_path("copilot_usage") == "silver/copilot-usage/silver_copilot_usage.csv"

    def test_get_display_name(self, catalog):
        assert catalog.get_display_name("entra_users") == "Entra ID Users"

    def test_get_description(self, catalog):
        desc = catalog.get_description("copilot_usage")
        assert "Copilot interaction events" in desc

    def test_resolve_alias(self, catalog):
        hint = catalog.resolve_alias("which apps")
        assert hint is not None
        assert "CopilotEventData_AppHost" in hint

    def test_resolve_alias_unknown(self, catalog):
        assert catalog.resolve_alias("unknown phrase") is None

    def test_resolve_alias_case_insensitive(self, catalog):
        hint = catalog.resolve_alias("COPILOT EVENTS")
        assert hint is not None

    def test_get_all_aliases(self, catalog):
        aliases = catalog.get_all_aliases()
        assert len(aliases) >= 4  # 3 from copilot + 1 from entra

    def test_build_context_prompt_single(self, catalog):
        prompt = catalog.build_context_prompt("copilot_usage")
        assert "Copilot Usage" in prompt
        assert "Queryable columns:" in prompt
        assert "TokensTotal" in prompt

    def test_build_context_prompt_all(self, catalog):
        prompt = catalog.build_context_prompt()
        assert "Copilot Usage" in prompt
        assert "Entra ID Users" in prompt

    def test_to_summary(self, catalog):
        summary = catalog.to_summary()
        assert len(summary) == 2
        ds_names = {s["dataset"] for s in summary}
        assert ds_names == {"copilot_usage", "entra_users"}
        assert summary[0]["column_count"] > 0
        assert summary[0]["queryable_column_count"] > 0

    def test_reload(self, catalog, tmp_schema_dir):
        # Add a new schema file
        new_schema = {
            "dataset": "test_dataset",
            "display_name": "Test",
            "columns": [{"name": "Col1", "type": "string", "description": "test"}],
        }
        (tmp_schema_dir / "test.yaml").write_text(yaml.dump(new_schema), encoding="utf-8")

        catalog.reload()
        assert "test_dataset" in catalog.list_datasets()

    def test_empty_dir(self, tmp_path):
        from shared.query.schema_catalog import SchemaCatalog
        empty = tmp_path / "empty_schemas"
        empty.mkdir()
        cat = SchemaCatalog(empty)
        assert cat.list_datasets() == []

    def test_missing_dir(self, tmp_path):
        from shared.query.schema_catalog import SchemaCatalog
        cat = SchemaCatalog(tmp_path / "does_not_exist")
        assert cat.list_datasets() == []


# ═══════════════════════════════════════════════════════════════════════════════
# QueryGenerator Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestQueryGenerator:
    def test_minimal_query(self, generator):
        plan = generator.generate({"dataset": "copilot_usage"})
        assert plan.dataset == "copilot_usage"
        assert plan.silver_path == "silver/copilot-usage/silver_copilot_usage.csv"
        assert plan.filters == []
        assert plan.group_by == []
        assert plan.limit == 1000

    def test_missing_dataset_raises(self, generator):
        from shared.query.query_generator import QueryValidationError
        with pytest.raises(QueryValidationError, match="dataset.*required"):
            generator.generate({})

    def test_unknown_dataset_raises(self, generator):
        from shared.query.query_generator import QueryValidationError
        with pytest.raises(QueryValidationError, match="Unknown dataset"):
            generator.generate({"dataset": "nope"})

    def test_filters_valid(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "filters": [
                {"column": "Department", "op": "=", "value": "Engineering"},
                {"column": "TokensTotal", "op": ">", "value": 100},
            ],
        })
        assert len(plan.filters) == 2
        assert plan.filters[0]["column"] == "Department"
        assert plan.filters[1]["op"] == ">"

    def test_filters_unknown_column_raises(self, generator):
        from shared.query.query_generator import QueryValidationError
        with pytest.raises(QueryValidationError, match="unknown column"):
            generator.generate({
                "dataset": "copilot_usage",
                "filters": [{"column": "FakeColumn", "op": "=", "value": "x"}],
            })

    def test_filters_invalid_op_raises(self, generator):
        from shared.query.query_generator import QueryValidationError
        with pytest.raises(QueryValidationError, match="unsupported op"):
            generator.generate({
                "dataset": "copilot_usage",
                "filters": [{"column": "Department", "op": "LIKE", "value": "%eng%"}],
            })

    def test_group_by(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "group_by": ["Department", "CopilotEventData_AppHost"],
        })
        assert plan.group_by == ["Department", "CopilotEventData_AppHost"]

    def test_group_by_unknown_column(self, generator):
        from shared.query.query_generator import QueryValidationError
        with pytest.raises(QueryValidationError, match="unknown column"):
            generator.generate({
                "dataset": "copilot_usage",
                "group_by": ["Nonexistent"],
            })

    def test_aggregations(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "group_by": ["Department"],
            "aggregations": [
                {"function": "sum", "column": "TokensTotal", "alias": "total_tokens"},
                {"function": "avg", "column": "DurationMs"},
            ],
        })
        assert len(plan.aggregations) == 2
        assert plan.aggregations[0]["alias"] == "total_tokens"
        assert plan.aggregations[1]["alias"] == "avg_DurationMs"

    def test_aggregation_invalid_func(self, generator):
        from shared.query.query_generator import QueryValidationError
        with pytest.raises(QueryValidationError, match="unsupported function"):
            generator.generate({
                "dataset": "copilot_usage",
                "aggregations": [{"function": "median", "column": "TokensTotal"}],
            })

    def test_sort_by(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "sort_by": [{"column": "TokensTotal", "order": "desc"}],
        })
        assert plan.sort_by == [{"column": "TokensTotal", "order": "desc"}]

    def test_limit_cap(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "limit": 50000,
        })
        assert plan.limit == 10000  # capped

    def test_columns_subset(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "columns": ["RecordId", "UserId", "TokensTotal"],
        })
        assert plan.columns == ["RecordId", "UserId", "TokensTotal"]

    def test_columns_unknown(self, generator):
        from shared.query.query_generator import QueryValidationError
        with pytest.raises(QueryValidationError, match="unknown column"):
            generator.generate({
                "dataset": "copilot_usage",
                "columns": ["RecordId", "FakeCol"],
            })

    def test_plan_to_dict(self, generator):
        plan = generator.generate({"dataset": "copilot_usage"})
        d = plan.to_dict()
        assert d["dataset"] == "copilot_usage"
        assert "silver_path" in d

    def test_contains_op(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "filters": [{"column": "UserId", "op": "contains", "value": "alice"}],
        })
        assert plan.filters[0]["op"] == "contains"

    def test_in_op(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "filters": [{"column": "CopilotEventData_AppHost", "op": "in", "value": ["Word", "Teams"]}],
        })
        assert plan.filters[0]["op"] == "in"

    def test_sort_by_aggregation_alias(self, generator):
        plan = generator.generate({
            "dataset": "copilot_usage",
            "group_by": ["Department"],
            "aggregations": [{"function": "count", "alias": "total"}],
            "sort_by": [{"column": "total", "order": "desc"}],
        })
        assert plan.sort_by[0]["column"] == "total"


# ═══════════════════════════════════════════════════════════════════════════════
# QueryExecutor Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestQueryExecutor:
    def _make_plan(self, generator, **overrides):
        dsl = {"dataset": "copilot_usage", **overrides}
        return generator.generate(dsl)

    def test_basic_execute(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator)
        result = executor.execute(plan)

        assert result["dataset"] == "copilot_usage"
        assert result["row_count"] == 5
        assert result["truncated"] is False
        assert len(result["data"]) == 5
        assert "RecordId" in result["columns"]

    def test_filter_equals(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, filters=[
            {"column": "Department", "op": "=", "value": "Engineering"},
        ])
        result = executor.execute(plan)
        assert result["row_count"] == 3
        for row in result["data"]:
            assert row["Department"] == "Engineering"

    def test_filter_not_equals(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, filters=[
            {"column": "Department", "op": "!=", "value": "Sales"},
        ])
        result = executor.execute(plan)
        assert result["row_count"] == 3

    def test_filter_greater_than(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, filters=[
            {"column": "TokensTotal", "op": ">", "value": 150},
        ])
        result = executor.execute(plan)
        # r2=200, r4=300, r5=175 → 3 rows
        assert result["row_count"] == 3

    def test_filter_contains(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, filters=[
            {"column": "UserId", "op": "contains", "value": "alice"},
        ])
        result = executor.execute(plan)
        assert result["row_count"] == 2

    def test_filter_in(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, filters=[
            {"column": "CopilotEventData_AppHost", "op": "in", "value": ["Word", "Excel"]},
        ])
        result = executor.execute(plan)
        assert result["row_count"] == 3  # r1=Word, r3=Word, r4=Excel

    def test_group_by_default_count(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, group_by=["Department"])
        result = executor.execute(plan)
        assert result["row_count"] == 2  # Engineering, Sales
        dept_counts = {row["Department"]: row["count"] for row in result["data"]}
        assert dept_counts["Engineering"] == 3
        assert dept_counts["Sales"] == 2

    def test_group_by_with_sum(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator,
            group_by=["CopilotEventData_AppHost"],
            aggregations=[{"function": "sum", "column": "TokensTotal", "alias": "total_tokens"}],
        )
        result = executor.execute(plan)
        apps = {row["CopilotEventData_AppHost"]: row["total_tokens"] for row in result["data"]}
        assert apps["Word"] == 250   # 150 + 100
        assert apps["Teams"] == 375  # 200 + 175
        assert apps["Excel"] == 300

    def test_group_by_with_avg(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator,
            group_by=["Department"],
            aggregations=[{"function": "avg", "column": "DurationMs", "alias": "avg_duration"}],
        )
        result = executor.execute(plan)
        eng_row = next(r for r in result["data"] if r["Department"] == "Engineering")
        # Engineering: (1200 + 600 + 2000) / 3 ≈ 1266.67
        assert abs(eng_row["avg_duration"] - 1266.67) < 1.0

    def test_sort_ascending(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator,
            sort_by=[{"column": "TokensTotal", "order": "asc"}],
        )
        result = executor.execute(plan)
        tokens = [row["TokensTotal"] for row in result["data"]]
        assert tokens == sorted(tokens)

    def test_sort_descending(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator,
            sort_by=[{"column": "TokensTotal", "order": "desc"}],
        )
        result = executor.execute(plan)
        tokens = [row["TokensTotal"] for row in result["data"]]
        assert tokens == sorted(tokens, reverse=True)

    def test_limit(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, limit=2)
        result = executor.execute(plan)
        assert result["row_count"] == 2
        assert result["truncated"] is True
        assert result["total_rows"] == 5

    def test_column_subset(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator, columns=["RecordId", "UserId"])
        result = executor.execute(plan)
        assert set(result["columns"]) == {"RecordId", "UserId"}
        assert result["row_count"] == 5

    def test_filter_plus_group_plus_sort(self, generator, mock_adls):
        """Integration: filter → group → aggregate → sort → limit."""
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator,
            filters=[{"column": "IsAgent", "op": "=", "value": "false"}],
            group_by=["CopilotEventData_AppHost"],
            aggregations=[
                {"function": "count", "alias": "events"},
                {"function": "sum", "column": "TokensTotal", "alias": "tokens"},
            ],
            sort_by=[{"column": "tokens", "order": "desc"}],
            limit=10,
        )
        result = executor.execute(plan)
        # After filtering: r1(Word), r2(Teams), r4(Excel), r5(Teams)
        assert result["row_count"] == 3  # Word(1), Teams(2), Excel(1)

    def test_execute_empty_csv(self, generator):
        from shared.query.query_executor import QueryExecutor
        mock = MagicMock()
        mock.download_text.return_value = ""
        executor = QueryExecutor(mock)
        plan = generator.generate({"dataset": "copilot_usage"})
        result = executor.execute(plan)
        assert result["row_count"] == 0

    def test_result_has_plan(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator)
        result = executor.execute(plan)
        assert result["plan"]["dataset"] == "copilot_usage"

    def test_result_has_timing(self, generator, mock_adls):
        from shared.query.query_executor import QueryExecutor
        executor = QueryExecutor(mock_adls)
        plan = self._make_plan(generator)
        result = executor.execute(plan)
        assert "executed_at" in result
        assert "elapsed_ms" in result
        assert isinstance(result["elapsed_ms"], float)


# ═══════════════════════════════════════════════════════════════════════════════
# Schema YAML Validation — ensure the real schema files parse correctly
# ═══════════════════════════════════════════════════════════════════════════════

class TestRealSchemas:
    """Load the actual YAML schema files shipped with the codebase."""

    @pytest.fixture()
    def real_catalog(self):
        from shared.query.schema_catalog import SchemaCatalog
        return SchemaCatalog()  # default schemas dir

    def test_all_datasets_load(self, real_catalog):
        datasets = real_catalog.list_datasets()
        assert len(datasets) == 4
        assert set(datasets) == {"copilot_usage", "entra_users", "m365_usage", "graph_activity"}

    def test_each_has_columns(self, real_catalog):
        for ds in real_catalog.list_datasets():
            cols = real_catalog.get_columns(ds)
            assert len(cols) > 0, f"{ds} has no columns"

    def test_each_has_silver_path(self, real_catalog):
        for ds in real_catalog.list_datasets():
            path = real_catalog.get_silver_path(ds)
            assert path.startswith("silver/"), f"{ds} silver_path doesn't start with 'silver/'"

    def test_copilot_usage_column_count(self, real_catalog):
        """Copilot usage schema should match the full explosion header + computed + enrichment."""
        cols = real_catalog.get_column_names("copilot_usage")
        # Not exact (schemas may subset), but should have the key columns
        assert "RecordId" in cols
        assert "TokensTotal" in cols
        assert "Department" in cols
        assert "UsageDate" in cols

    def test_queryable_columns_have_description(self, real_catalog):
        for ds in real_catalog.list_datasets():
            for col in real_catalog.get_queryable_columns(ds):
                assert col.get("description"), f"{ds}.{col['name']} is queryable but has no description"
