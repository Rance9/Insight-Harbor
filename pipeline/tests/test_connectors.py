"""
Insight Harbor — Connector Framework Unit Tests
=================================================
Tests for BaseConnector, ConnectorRegistry, PurviewAuditConnector,
and EntraConnector.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

# ═══════════════════════════════════════════════════════════════════════════════
# Helper: Minimal concrete connector for testing BaseConnector ABC
# ═══════════════════════════════════════════════════════════════════════════════


class _StubConnector:
    """Deferred import to avoid module-level config issues."""

    @staticmethod
    def create():
        from shared.connectors.base import BaseConnector, ConnectorPhase

        class StubConnector(BaseConnector):
            @property
            def name(self):
                return "stub"

            @property
            def display_name(self):
                return "Stub Connector"

            @property
            def source_type(self):
                return "test"

            def validate_config(self):
                return []

            def get_required_permissions(self):
                return ["TestPermission.Read"]

            def get_bronze_prefix(self):
                return "bronze/stub"

            def get_silver_prefix(self):
                return "silver/stub"

            def get_orchestration_phases(self):
                return [
                    ConnectorPhase(name="ingest", activity_name="stub_ingest"),
                ]

            def ingest(self, input_data):
                return {"status": "completed", "records": 42}

            def transform_to_silver(self, input_data):
                return {"output_blob_path": "silver/stub/out.csv", "new_records": 42}

        return StubConnector()


class _FailingConnector:
    """A connector whose config validation always fails."""

    @staticmethod
    def create():
        from shared.connectors.base import BaseConnector, ConnectorPhase

        class FailingConnector(BaseConnector):
            @property
            def name(self):
                return "failing"

            @property
            def display_name(self):
                return "Failing Connector"

            @property
            def source_type(self):
                return "test"

            def validate_config(self):
                return ["MISSING_VAR is not set", "ANOTHER_VAR is not set"]

            def get_required_permissions(self):
                return []

            def get_bronze_prefix(self):
                return "bronze/failing"

            def get_silver_prefix(self):
                return "silver/failing"

            def get_orchestration_phases(self):
                return []

            def ingest(self, input_data):
                raise NotImplementedError

            def transform_to_silver(self, input_data):
                raise NotImplementedError

        return FailingConnector()


# ═══════════════════════════════════════════════════════════════════════════════
# BaseConnector Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestBaseConnector:
    """Test the abstract base class via StubConnector."""

    def test_stub_connector_identity(self):
        stub = _StubConnector.create()
        assert stub.name == "stub"
        assert stub.display_name == "Stub Connector"
        assert stub.source_type == "test"
        assert stub.version == "1.0.0"

    def test_capability_flags_default_false(self):
        stub = _StubConnector.create()
        assert stub.supports_partitioning is False
        assert stub.supports_polling is False
        assert stub.supports_subdivision is False
        assert stub.supports_explosion is False

    def test_validate_config_returns_empty_list(self):
        stub = _StubConnector.create()
        assert stub.validate_config() == []

    def test_get_status_includes_all_fields(self):
        stub = _StubConnector.create()
        status = stub.get_status()
        assert status["name"] == "stub"
        assert status["display_name"] == "Stub Connector"
        assert status["configured"] is True
        assert status["validation_errors"] == []
        assert status["capabilities"]["partitioning"] is False
        assert status["permissions"] == ["TestPermission.Read"]
        assert status["adls_paths"]["bronze"] == "bronze/stub"
        assert status["adls_paths"]["silver"] == "silver/stub"

    def test_get_status_failing_connector(self):
        failing = _FailingConnector.create()
        status = failing.get_status()
        assert status["configured"] is False
        assert len(status["validation_errors"]) == 2

    def test_plan_default_returns_single_item(self):
        stub = _StubConnector.create()
        items = stub.plan({"start": "2026-01-01", "end": "2026-01-02"})
        assert len(items) == 1
        assert items[0]["start"] == "2026-01-01"

    def test_explode_default_passthrough(self):
        stub = _StubConnector.create()
        result = stub.explode({"bronze_blob_path": "test.jsonl"})
        assert result == {"bronze_blob_path": "test.jsonl"}

    def test_cleanup_default_returns_cleaned(self):
        stub = _StubConnector.create()
        result = stub.cleanup({"query_id": "q1"})
        assert result == {"cleaned": True}

    def test_ingest_returns_result(self):
        stub = _StubConnector.create()
        result = stub.ingest({"partition_id": 1})
        assert result["status"] == "completed"
        assert result["records"] == 42

    def test_transform_to_silver_returns_result(self):
        stub = _StubConnector.create()
        result = stub.transform_to_silver({"paths": []})
        assert result["new_records"] == 42

    def test_schema_defaults_empty(self):
        stub = _StubConnector.create()
        assert stub.get_bronze_schema() == []
        assert stub.get_silver_schema() == []

    def test_repr(self):
        stub = _StubConnector.create()
        assert "StubConnector" in repr(stub)
        assert "stub" in repr(stub)

    def test_orchestration_phases(self):
        stub = _StubConnector.create()
        phases = stub.get_orchestration_phases()
        assert len(phases) == 1
        assert phases[0].name == "ingest"
        assert phases[0].activity_name == "stub_ingest"
        assert phases[0].fan_out is False


# ═══════════════════════════════════════════════════════════════════════════════
# ConnectorPhase Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestConnectorPhase:
    """Test ConnectorPhase dataclass."""

    def test_defaults(self):
        from shared.connectors.base import ConnectorPhase

        phase = ConnectorPhase(name="test", activity_name="test_activity")
        assert phase.fan_out is False
        assert phase.parallel_group is None
        assert phase.retry_max_attempts == 3
        assert phase.retry_interval_ms == 5_000
        assert phase.optional is False

    def test_custom_values(self):
        from shared.connectors.base import ConnectorPhase

        phase = ConnectorPhase(
            name="ingest",
            activity_name="do_ingest",
            fan_out=True,
            parallel_group="batch_1",
            retry_max_attempts=5,
            retry_interval_ms=10_000,
            optional=True,
        )
        assert phase.fan_out is True
        assert phase.parallel_group == "batch_1"
        assert phase.retry_max_attempts == 5
        assert phase.retry_interval_ms == 10_000
        assert phase.optional is True


# ═══════════════════════════════════════════════════════════════════════════════
# ConnectorRegistry Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestConnectorRegistry:
    """Test the singleton ConnectorRegistry."""

    def setup_method(self):
        """Reset registry singleton before each test."""
        from shared.connectors.registry import ConnectorRegistry
        ConnectorRegistry.reset()

    def teardown_method(self):
        """Reset after each test."""
        from shared.connectors.registry import ConnectorRegistry
        ConnectorRegistry.reset()

    def test_singleton_pattern(self):
        from shared.connectors.registry import ConnectorRegistry
        r1 = ConnectorRegistry.instance()
        r2 = ConnectorRegistry.instance()
        assert r1 is r2

    def test_reset_clears_singleton(self):
        from shared.connectors.registry import ConnectorRegistry
        r1 = ConnectorRegistry.instance()
        ConnectorRegistry.reset()
        r2 = ConnectorRegistry.instance()
        assert r1 is not r2

    def test_auto_discover_registers_purview_and_entra(self):
        from shared.connectors.registry import ConnectorRegistry
        registry = ConnectorRegistry.instance()
        assert "purview_audit" in registry.names
        assert "entra" in registry.names
        assert len(registry) >= 2

    def test_register_custom_connector(self):
        from shared.connectors.registry import ConnectorRegistry
        registry = ConnectorRegistry.instance()
        stub = _StubConnector.create()
        registry.register(stub)
        assert "stub" in registry.names
        assert registry.get("stub") is stub

    def test_unregister(self):
        from shared.connectors.registry import ConnectorRegistry
        registry = ConnectorRegistry.instance()
        stub = _StubConnector.create()
        registry.register(stub)
        assert registry.unregister("stub") is True
        assert registry.get("stub") is None
        assert registry.unregister("nonexistent") is False

    def test_get_returns_none_for_unknown(self):
        from shared.connectors.registry import ConnectorRegistry
        registry = ConnectorRegistry.instance()
        assert registry.get("nonexistent") is None

    def test_get_enabled_all_when_no_filter(self, monkeypatch):
        """When IH_ENABLED_CONNECTORS is empty, all connectors are enabled."""
        monkeypatch.setenv("IH_ENABLED_CONNECTORS", "")
        from shared.connectors.registry import ConnectorRegistry
        ConnectorRegistry.reset()
        registry = ConnectorRegistry.instance()
        enabled = registry.get_enabled()
        assert len(enabled) >= 2

    def test_get_enabled_filters_by_config(self, monkeypatch):
        """When IH_ENABLED_CONNECTORS is set, only listed connectors are enabled."""
        monkeypatch.setenv("IH_ENABLED_CONNECTORS", "purview_audit")
        # Need to re-create config to pick up the env change
        from shared.connectors.registry import ConnectorRegistry
        # Patch config.ENABLED_CONNECTORS directly since PipelineConfig
        # reads env at class-definition time
        from shared import config as config_mod
        monkeypatch.setattr(config_mod.config, "ENABLED_CONNECTORS", ["purview_audit"])
        ConnectorRegistry.reset()
        registry = ConnectorRegistry.instance()
        enabled = registry.get_enabled()
        enabled_names = [c.name for c in enabled]
        assert "purview_audit" in enabled_names
        assert "entra" not in enabled_names

    def test_list_all_returns_status_dicts(self):
        from shared.connectors.registry import ConnectorRegistry
        registry = ConnectorRegistry.instance()
        statuses = registry.list_all()
        assert isinstance(statuses, list)
        assert all(isinstance(s, dict) for s in statuses)
        assert all("name" in s for s in statuses)

    def test_validate_all(self):
        from shared.connectors.registry import ConnectorRegistry
        registry = ConnectorRegistry.instance()
        validation = registry.validate_all()
        assert isinstance(validation, dict)
        # With test env vars set, should have no errors
        assert "purview_audit" in validation
        assert "entra" in validation

    def test_repr(self):
        from shared.connectors.registry import ConnectorRegistry
        registry = ConnectorRegistry.instance()
        r = repr(registry)
        assert "ConnectorRegistry" in r
        assert "purview_audit" in r


# ═══════════════════════════════════════════════════════════════════════════════
# PurviewAuditConnector Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestPurviewAuditConnector:
    """Test the Purview Audit Log connector."""

    def test_identity(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        assert c.name == "purview_audit"
        assert c.display_name == "Purview Audit Log"
        assert c.source_type == "graph_api"

    def test_capability_flags(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        assert c.supports_partitioning is True
        assert c.supports_polling is True
        assert c.supports_subdivision is True
        assert c.supports_explosion is True

    def test_config_validation_passes_with_env(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        errors = c.validate_config()
        assert errors == []

    def test_config_validation_fails_without_env(self, monkeypatch):
        monkeypatch.setenv("IH_TENANT_ID", "")
        monkeypatch.setenv("IH_CLIENT_ID", "")
        monkeypatch.setenv("IH_CLIENT_SECRET", "")
        monkeypatch.setenv("IH_ADLS_ACCOUNT_NAME", "")
        # Need to reload config
        from shared import config as config_mod
        monkeypatch.setattr(config_mod.config, "TENANT_ID", "")
        monkeypatch.setattr(config_mod.config, "CLIENT_ID", "")
        monkeypatch.setattr(config_mod.config, "CLIENT_SECRET", "")
        monkeypatch.setattr(config_mod.config, "ADLS_ACCOUNT_NAME", "")
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        errors = c.validate_config()
        assert len(errors) == 4

    def test_required_permissions(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        perms = c.get_required_permissions()
        assert "AuditLogsQuery.Read.All" in perms

    def test_adls_paths(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        assert c.get_bronze_prefix() == "bronze/purview"
        assert c.get_silver_prefix() == "silver/copilot-usage"

    def test_orchestration_phases(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        phases = c.get_orchestration_phases()
        assert len(phases) == 4
        phase_names = [p.name for p in phases]
        assert phase_names == ["plan", "ingest", "explode", "transform"]

    def test_bronze_schema_matches_exploded_header(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        from shared.explosion import PURVIEW_EXPLODED_HEADER
        c = PurviewAuditConnector()
        schema = c.get_bronze_schema()
        assert len(schema) == len(PURVIEW_EXPLODED_HEADER)
        assert "RecordId" in schema
        assert "CreationTime" in schema

    def test_silver_schema_includes_computed_cols(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        schema = c.get_silver_schema()
        assert "UsageDate" in schema
        assert "IsAgent" in schema
        assert "Department" in schema  # Entra enrichment

    def test_get_status(self):
        from shared.connectors.purview_audit import PurviewAuditConnector
        c = PurviewAuditConnector()
        status = c.get_status()
        assert status["name"] == "purview_audit"
        assert status["configured"] is True
        assert status["capabilities"]["partitioning"] is True
        assert status["capabilities"]["explosion"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# EntraConnector Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestEntraConnector:
    """Test the Entra ID Users connector."""

    def test_identity(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        assert c.name == "entra"
        assert c.display_name == "Entra ID Users"
        assert c.source_type == "graph_api"

    def test_capability_flags(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        assert c.supports_partitioning is False
        assert c.supports_polling is False
        assert c.supports_subdivision is False
        assert c.supports_explosion is False

    def test_config_validation_passes(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        errors = c.validate_config()
        assert errors == []

    def test_required_permissions(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        perms = c.get_required_permissions()
        assert "User.Read.All" in perms
        assert "Directory.Read.All" in perms

    def test_adls_paths(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        assert c.get_bronze_prefix() == "bronze/entra"
        assert c.get_silver_prefix() == "silver/entra-users"

    def test_orchestration_phases(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        phases = c.get_orchestration_phases()
        assert len(phases) == 1
        assert phases[0].activity_name == "pull_entra"

    def test_silver_schema_has_30_columns(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        schema = c.get_silver_schema()
        assert len(schema) == 30
        assert "UserPrincipalName" in schema
        assert "HasCopilotLicense" in schema

    def test_get_status(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        status = c.get_status()
        assert status["name"] == "entra"
        assert status["configured"] is True
        assert status["capabilities"]["partitioning"] is False

    def test_transform_to_silver_passthrough(self):
        from shared.connectors.entra import EntraConnector
        c = EntraConnector()
        result = c.transform_to_silver({
            "silver_blob_path": "silver/entra-users/out.csv",
            "users_count": 100,
        })
        assert result["new_records"] == 100
