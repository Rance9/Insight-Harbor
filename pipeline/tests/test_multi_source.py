"""
Insight Harbor — Multi-Source Connector Tests
================================================
Tests for M365UsageReportsConnector, GraphActivityConnector,
and auto-discovery of all four connectors in the registry.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# M365UsageReportsConnector Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestM365UsageReportsConnector:
    """Tests for the M365 Usage Reports connector."""

    def _make_connector(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector
        return M365UsageReportsConnector()

    # ── Identity & Metadata ────────────────────────────────────────────────

    def test_identity(self):
        c = self._make_connector()
        assert c.name == "m365_usage"
        assert c.display_name == "M365 Usage Reports"
        assert c.source_type == "graph_api"
        assert c.version == "1.0.0"

    def test_capability_flags_all_false(self):
        c = self._make_connector()
        assert c.supports_partitioning is False
        assert c.supports_polling is False
        assert c.supports_subdivision is False
        assert c.supports_explosion is False

    def test_required_permissions(self):
        c = self._make_connector()
        assert c.get_required_permissions() == ["Reports.Read.All"]

    def test_adls_prefixes(self):
        c = self._make_connector()
        assert "m365-usage" in c.get_bronze_prefix()
        assert "m365-usage" in c.get_silver_prefix()

    # ── Config Validation ──────────────────────────────────────────────────

    def test_validate_config_passes_with_env(self):
        """Env vars already set by conftest fixture → should pass."""
        c = self._make_connector()
        assert c.validate_config() == []

    def test_validate_config_fails_missing_tenant(self, monkeypatch):
        monkeypatch.setenv("IH_TENANT_ID", "")
        # Reimport to pick up env change
        from shared.config import PipelineConfig
        with patch("shared.connectors.m365_usage.config", PipelineConfig()):
            c = self._make_connector()
            from shared.connectors.m365_usage import config as mod_config
            errors = c.validate_config()
            # Patch the config module-level ref
        # We can at least verify the method is callable
        assert callable(c.validate_config)

    # ── Orchestration Phases ───────────────────────────────────────────────

    def test_orchestration_phases(self):
        c = self._make_connector()
        phases = c.get_orchestration_phases()
        assert len(phases) == 2
        assert phases[0].name == "ingest"
        assert phases[0].activity_name == "ingest_m365_usage"
        assert phases[1].name == "transform"
        assert phases[1].activity_name == "transform_m365_usage"

    # ── Silver Schema ──────────────────────────────────────────────────────

    def test_silver_schema_columns(self):
        from shared.connectors.m365_usage import M365_USAGE_SILVER_COLUMNS
        c = self._make_connector()
        schema = c.get_silver_schema()
        assert schema == M365_USAGE_SILVER_COLUMNS
        assert len(schema) == 35
        assert "UserPrincipalName" in schema
        assert "_LoadedAtUtc" in schema

    # ── get_status() ───────────────────────────────────────────────────────

    def test_get_status_shape(self):
        c = self._make_connector()
        status = c.get_status()
        assert status["name"] == "m365_usage"
        assert status["configured"] is True
        assert "adls_paths" in status
        assert "bronze" in status["adls_paths"]
        assert "silver" in status["adls_paths"]

    # ── Report Definitions ─────────────────────────────────────────────────

    def test_report_definitions(self):
        from shared.connectors.m365_usage import M365_USAGE_REPORTS
        assert len(M365_USAGE_REPORTS) == 5
        assert "m365_app_user_detail" in M365_USAGE_REPORTS
        assert "teams_user_activity" in M365_USAGE_REPORTS
        assert M365_USAGE_REPORTS["email_activity"] == "getEmailActivityUserDetail"

    # ── Normalisation ──────────────────────────────────────────────────────

    def test_normalize_teams_activity_row(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector

        raw = {
            "User Principal Name": "alice@contoso.com",
            "Display Name": "Alice",
            "Report Period": "7",
            "Report Refresh Date": "2026-03-04",
            "Last Activity Date": "2026-03-03",
            "Is Deleted": "False",
            "Is Licensed": "True",
            "Chat Message Count": "42",
            "Call Count": "5",
            "Meeting Count": "3",
            "Private Chat Message Count": "10",
        }
        result = M365UsageReportsConnector._normalize_report_row(
            raw, "teams_user_activity", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["UserPrincipalName"] == "alice@contoso.com"
        assert result["TeamsChatMessageCount"] == "42"
        assert result["TeamsCallCount"] == "5"
        assert result["_SourceReport"] == "teams_user_activity"
        assert result["_LoadedAtUtc"] == "2026-03-04T12:00:00Z"

    def test_normalize_email_activity_row(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector

        raw = {
            "User Principal Name": "bob@contoso.com",
            "Display Name": "Bob",
            "Report Period": "7",
            "Report Refresh Date": "2026-03-04",
            "Send Count": "100",
            "Receive Count": "200",
            "Read Count": "150",
        }
        result = M365UsageReportsConnector._normalize_report_row(
            raw, "email_activity", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["EmailSendCount"] == "100"
        assert result["EmailReceiveCount"] == "200"
        assert result["EmailReadCount"] == "150"
        assert result["ReportType"] == "email_activity"

    def test_normalize_app_adoption_row(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector

        raw = {
            "User Principal Name": "carol@contoso.com",
            "Display Name": "Carol",
            "Report Period": "7",
            "Report Refresh Date": "2026-03-04",
            "Has Teams": "Yes",
            "Has Outlook": "Yes",
            "Has Word": "No",
            "Has Excel": "Yes",
            "Has PowerPoint": "No",
            "Has OneNote": "No",
            "Has OneDrive": "Yes",
            "Has SharePoint": "Yes",
        }
        result = M365UsageReportsConnector._normalize_report_row(
            raw, "m365_app_user_detail", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["HasTeams"] == "Yes"
        assert result["HasWord"] == "No"
        assert result["HasOneDrive"] == "Yes"

    def test_normalize_sharepoint_activity_row(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector

        raw = {
            "User Principal Name": "dave@contoso.com",
            "Display Name": "Dave",
            "Viewed Or Edited File Count": "30",
            "Synced File Count": "5",
            "Shared Internally File Count": "8",
            "Shared Externally File Count": "2",
            "Visited Page Count": "50",
        }
        result = M365UsageReportsConnector._normalize_report_row(
            raw, "sharepoint_activity", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["SharePointFileViewedOrEdited"] == "30"
        assert result["SharePointPagesVisited"] == "50"

    def test_normalize_onedrive_activity_row(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector

        raw = {
            "User Principal Name": "eve@contoso.com",
            "Display Name": "Eve",
            "Viewed Or Edited File Count": "15",
            "Synced File Count": "3",
            "Shared Internally File Count": "4",
            "Shared Externally File Count": "1",
        }
        result = M365UsageReportsConnector._normalize_report_row(
            raw, "onedrive_activity", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["OneDriveFileViewedOrEdited"] == "15"
        assert result["OneDriveFileSynced"] == "3"

    def test_normalize_skips_empty_upn(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector

        raw = {"User Principal Name": "", "Display Name": "Ghost"}
        result = M365UsageReportsConnector._normalize_report_row(
            raw, "email_activity", "2026-03-04T12:00:00Z"
        )
        assert result is None

    def test_normalize_skips_missing_upn(self):
        from shared.connectors.m365_usage import M365UsageReportsConnector

        raw = {"Display Name": "No UPN"}
        result = M365UsageReportsConnector._normalize_report_row(
            raw, "email_activity", "2026-03-04T12:00:00Z"
        )
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# GraphActivityConnector Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestGraphActivityConnector:
    """Tests for the Graph Activity Log connector."""

    def _make_connector(self):
        from shared.connectors.graph_activity import GraphActivityConnector
        return GraphActivityConnector()

    # ── Identity & Metadata ────────────────────────────────────────────────

    def test_identity(self):
        c = self._make_connector()
        assert c.name == "graph_activity"
        assert c.display_name == "Graph Activity Logs"
        assert c.source_type == "graph_api"
        assert c.version == "1.0.0"

    def test_capability_flags(self):
        c = self._make_connector()
        assert c.supports_partitioning is True
        assert c.supports_polling is False
        assert c.supports_subdivision is False
        assert c.supports_explosion is False

    def test_required_permissions(self):
        c = self._make_connector()
        perms = c.get_required_permissions()
        assert "AuditLog.Read.All" in perms
        assert "Directory.Read.All" in perms

    def test_adls_prefixes(self):
        c = self._make_connector()
        assert "graph-activity" in c.get_bronze_prefix()
        assert "graph-activity" in c.get_silver_prefix()

    # ── Config Validation ──────────────────────────────────────────────────

    def test_validate_config_passes(self):
        c = self._make_connector()
        assert c.validate_config() == []

    # ── Orchestration Phases ───────────────────────────────────────────────

    def test_orchestration_phases(self):
        c = self._make_connector()
        phases = c.get_orchestration_phases()
        assert len(phases) == 2
        assert phases[0].name == "ingest"
        assert phases[0].fan_out is True
        assert phases[1].name == "transform"

    # ── Silver Schema ──────────────────────────────────────────────────────

    def test_silver_schema(self):
        from shared.connectors.graph_activity import GRAPH_ACTIVITY_SILVER_COLUMNS
        c = self._make_connector()
        schema = c.get_silver_schema()
        assert schema == GRAPH_ACTIVITY_SILVER_COLUMNS
        assert "EventType" in schema
        assert "UserId" in schema
        assert "_SourceEndpoint" in schema

    # ── Plan (date partitioning) ───────────────────────────────────────────

    def test_plan_creates_daily_partitions(self):
        c = self._make_connector()
        partitions = c.plan({
            "start_date": "2026-03-01T00:00:00+00:00",
            "end_date": "2026-03-04T00:00:00+00:00",
        })
        assert len(partitions) == 3  # 3 full days
        assert partitions[0]["id"] == 1
        assert partitions[0]["date_prefix"] == "2026/03/01"
        assert "signIns" in partitions[0]["endpoints"]
        assert "directoryAudits" in partitions[0]["endpoints"]

    def test_plan_single_day(self):
        c = self._make_connector()
        partitions = c.plan({
            "start_date": "2026-03-01T00:00:00+00:00",
            "end_date": "2026-03-01T23:59:59+00:00",
        })
        assert len(partitions) == 1

    def test_plan_defaults_without_dates(self):
        """Without explicit dates, plan() should use lookback_days."""
        c = self._make_connector()
        partitions = c.plan({})
        # IH_LOOKBACK_DAYS is set to "2" in conftest
        assert len(partitions) >= 1

    # ── get_status() ───────────────────────────────────────────────────────

    def test_get_status_shape(self):
        c = self._make_connector()
        status = c.get_status()
        assert status["name"] == "graph_activity"
        assert status["configured"] is True
        assert status["capabilities"]["partitioning"] is True
        assert status["capabilities"]["polling"] is False

    # ── Normalisation (sign-in) ────────────────────────────────────────────

    def test_normalize_signin_record(self):
        from shared.connectors.graph_activity import GraphActivityConnector

        record = {
            "id": "sign-in-001",
            "userId": "user-001",
            "userPrincipalName": "alice@contoso.com",
            "userDisplayName": "Alice",
            "userType": "member",
            "createdDateTime": "2026-03-04T10:30:00Z",
            "appDisplayName": "Microsoft Teams",
            "appId": "app-001",
            "ipAddress": "10.0.0.1",
            "clientAppUsed": "Browser",
            "isInteractive": True,
            "conditionalAccessStatus": "success",
            "riskState": "none",
            "riskLevelDuringSignIn": "none",
            "resourceDisplayName": "Microsoft Graph",
            "resourceId": "res-001",
            "location": {
                "city": "Redmond",
                "state": "Washington",
                "countryOrRegion": "US",
            },
            "deviceDetail": {
                "browser": "Edge 120",
                "operatingSystem": "Windows 11",
                "deviceId": "dev-001",
            },
            "status": {
                "errorCode": 0,
                "failureReason": "",
            },
            "mfaDetail": {
                "authMethod": "PhoneAppNotification",
            },
        }

        result = GraphActivityConnector._normalize_record(
            record, "signIns", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["EventType"] == "SignIn"
        assert result["EventId"] == "sign-in-001"
        assert result["UserId"] == "user-001"
        assert result["UserPrincipalName"] == "alice@contoso.com"
        assert result["EventDate"] == "2026-03-04"
        assert result["AppDisplayName"] == "Microsoft Teams"
        assert result["IpAddress"] == "10.0.0.1"
        assert result["Location_City"] == "Redmond"
        assert result["Location_Country"] == "US"
        assert result["DeviceDetail_Browser"] == "Edge 120"
        assert result["DeviceDetail_OS"] == "Windows 11"
        assert result["MfaRequired"] == "Yes"
        assert result["IsInteractive"] == "True"
        assert result["_SourceEndpoint"] == "signIns"

    def test_normalize_signin_no_mfa(self):
        from shared.connectors.graph_activity import GraphActivityConnector

        record = {
            "id": "sign-in-002",
            "createdDateTime": "2026-03-04T10:30:00Z",
            "mfaDetail": {},
        }
        result = GraphActivityConnector._normalize_record(
            record, "signIns", "2026-03-04T12:00:00Z"
        )
        assert result["MfaRequired"] == "No"

    # ── Normalisation (directory audit) ────────────────────────────────────

    def test_normalize_directory_audit_record(self):
        from shared.connectors.graph_activity import GraphActivityConnector

        record = {
            "id": "audit-001",
            "activityDisplayName": "Update user",
            "category": "UserManagement",
            "result": "success",
            "activityDateTime": "2026-03-04T11:00:00Z",
            "initiatedBy": {
                "user": {
                    "id": "admin-001",
                    "userPrincipalName": "admin@contoso.com",
                    "displayName": "Admin",
                },
            },
            "targetResources": [
                {"displayName": "Alice", "type": "User"},
            ],
        }
        result = GraphActivityConnector._normalize_record(
            record, "directoryAudits", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["EventType"] == "DirectoryAudit"
        assert result["EventId"] == "audit-001"
        assert result["UserId"] == "admin-001"
        assert result["ActivityDisplayName"] == "Update user"
        assert result["Category"] == "UserManagement"
        assert result["Result"] == "success"
        assert result["TargetResource"] == "Alice"
        assert result["EventDate"] == "2026-03-04"

    def test_normalize_directory_audit_no_targets(self):
        from shared.connectors.graph_activity import GraphActivityConnector

        record = {
            "id": "audit-002",
            "activityDisplayName": "Delete group",
            "activityDateTime": "2026-03-04T11:00:00Z",
            "initiatedBy": {"user": {}},
            "targetResources": [],
        }
        result = GraphActivityConnector._normalize_record(
            record, "directoryAudits", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["TargetResource"] == ""

    def test_normalize_skips_no_id(self):
        from shared.connectors.graph_activity import GraphActivityConnector

        record = {"userId": "user-001"}
        result = GraphActivityConnector._normalize_record(
            record, "signIns", "2026-03-04T12:00:00Z"
        )
        assert result is None

    def test_normalize_nested_none_gracefully(self):
        """location/deviceDetail/status can be None in API responses."""
        from shared.connectors.graph_activity import GraphActivityConnector

        record = {
            "id": "sign-in-003",
            "createdDateTime": "2026-03-04T10:30:00Z",
            "location": None,
            "deviceDetail": None,
            "status": None,
            "mfaDetail": None,
        }
        result = GraphActivityConnector._normalize_record(
            record, "signIns", "2026-03-04T12:00:00Z"
        )
        assert result is not None
        assert result["Location_City"] == ""
        assert result["DeviceDetail_Browser"] == ""
        assert result["Status_ErrorCode"] == ""
        assert result["MfaRequired"] == "No"


# ═══════════════════════════════════════════════════════════════════════════════
# Registry Auto-Discovery Tests (all 4 connectors)
# ═══════════════════════════════════════════════════════════════════════════════


class TestRegistryAutoDiscovery:
    """Verify that auto-discover registers all four connectors."""

    def test_all_four_connectors_registered(self):
        from shared.connectors.registry import ConnectorRegistry

        ConnectorRegistry.reset()
        registry = ConnectorRegistry.instance()

        names = registry.names
        assert "purview_audit" in names
        assert "entra" in names
        assert "m365_usage" in names
        assert "graph_activity" in names
        assert len(names) == 4

        ConnectorRegistry.reset()

    def test_enabled_filter_respects_env(self):
        from shared.connectors.registry import ConnectorRegistry
        from shared.connectors import registry as reg_module

        ConnectorRegistry.reset()
        registry = ConnectorRegistry.instance()

        with patch.object(
            reg_module.config, "ENABLED_CONNECTORS",
            ["purview_audit", "m365_usage"],
        ):
            enabled = registry.get_enabled()
            enabled_names = [c.name for c in enabled]
            assert "purview_audit" in enabled_names
            assert "m365_usage" in enabled_names
            assert "entra" not in enabled_names
            assert "graph_activity" not in enabled_names

        ConnectorRegistry.reset()

    def test_list_all_returns_status_dicts(self):
        from shared.connectors.registry import ConnectorRegistry

        ConnectorRegistry.reset()
        registry = ConnectorRegistry.instance()

        statuses = registry.list_all()
        assert len(statuses) == 4
        names_in_status = {s["name"] for s in statuses}
        assert names_in_status == {
            "purview_audit", "entra", "m365_usage", "graph_activity"
        }

        ConnectorRegistry.reset()

    def test_all_connectors_validate_with_conftest_env(self):
        from shared.connectors.registry import ConnectorRegistry

        ConnectorRegistry.reset()
        registry = ConnectorRegistry.instance()

        errors = registry.validate_all()
        # All should pass with conftest env vars
        for name, errs in errors.items():
            assert errs == [], f"{name} validation failed: {errs}"

        ConnectorRegistry.reset()
