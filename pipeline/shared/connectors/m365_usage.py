"""
Insight Harbor — M365 Usage Reports Connector
===============================================
Fetches Microsoft 365 usage activity reports from the Graph API
``/reports`` endpoint.  These reports provide aggregate and per-user
usage data for Teams, Exchange, SharePoint, and OneDrive.

Supported Reports
~~~~~~~~~~~~~~~~~
- ``getM365AppUserDetail``  — per-user app adoption
- ``getTeamsUserActivityUserDetail``  — Teams activity per user
- ``getEmailActivityUserDetail``  — Exchange activity per user
- ``getSharePointActivityUserDetail``  — SharePoint activity per user
- ``getOneDriveActivityUserDetail``  — OneDrive activity per user

These complement the Purview audit log (which captures Copilot events)
by adding workload adoption and usage metrics.

Data Flow
~~~~~~~~~
Graph ``/reports/getXxxDetail(period='D7')`` → CSV response → Bronze →
transform → Silver CSV on ADLS.

Required App Registration Permission
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``Reports.Read.All``  (application-level)
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone

from shared.adls_client import ADLSClient
from shared.config import config
from shared.connectors.base import BaseConnector, ConnectorPhase
from shared.graph_client import GraphClient

logger = logging.getLogger("ih.connectors.m365_usage")


# ═══════════════════════════════════════════════════════════════════════════════
# Report definitions
# ═══════════════════════════════════════════════════════════════════════════════

M365_USAGE_REPORTS: dict[str, str] = {
    "m365_app_user_detail": "getM365AppUserDetail",
    "teams_user_activity": "getTeamsUserActivityUserDetail",
    "email_activity": "getEmailActivityUserDetail",
    "sharepoint_activity": "getSharePointActivityUserDetail",
    "onedrive_activity": "getOneDriveActivityUserDetail",
}

# Silver schema — normalised columns across all usage reports
M365_USAGE_SILVER_COLUMNS: list[str] = [
    "UserPrincipalName",
    "DisplayName",
    "ReportType",
    "ReportPeriod",
    "ReportRefreshDate",
    "LastActivityDate",
    "IsDeleted",
    "IsLicensed",
    # App adoption (from getM365AppUserDetail)
    "HasTeams",
    "HasOutlook",
    "HasWord",
    "HasExcel",
    "HasPowerPoint",
    "HasOneNote",
    "HasOneDrive",
    "HasSharePoint",
    # Teams metrics (from getTeamsUserActivityUserDetail)
    "TeamsChatMessageCount",
    "TeamsCallCount",
    "TeamsMeetingCount",
    "TeamsPrivateChatMessageCount",
    # Email metrics (from getEmailActivityUserDetail)
    "EmailSendCount",
    "EmailReceiveCount",
    "EmailReadCount",
    # SharePoint metrics
    "SharePointFileViewedOrEdited",
    "SharePointFileSynced",
    "SharePointFileSharedInternally",
    "SharePointFileSharedExternally",
    "SharePointPagesVisited",
    # OneDrive metrics
    "OneDriveFileViewedOrEdited",
    "OneDriveFileSynced",
    "OneDriveFileSharedInternally",
    "OneDriveFileSharedExternally",
    # Metadata
    "_SourceReport",
    "_ReportDate",
    "_LoadedAtUtc",
]


# ═══════════════════════════════════════════════════════════════════════════════
# M365UsageReportsConnector
# ═══════════════════════════════════════════════════════════════════════════════


class M365UsageReportsConnector(BaseConnector):
    """Connector for Microsoft 365 Usage Activity Reports (Graph API).

    Fetches per-user usage reports for Teams, Exchange, SharePoint,
    OneDrive, and M365 App adoption, normalises them into a single
    Silver schema, and stores in ADLS.
    """

    # ── Identity ───────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "m365_usage"

    @property
    def display_name(self) -> str:
        return "M365 Usage Reports"

    @property
    def source_type(self) -> str:
        return "graph_api"

    @property
    def version(self) -> str:
        return "1.0.0"

    # ── Configuration ──────────────────────────────────────────────────────

    def validate_config(self) -> list[str]:
        errors: list[str] = []
        if not config.TENANT_ID:
            errors.append("IH_TENANT_ID is not set")
        if not config.CLIENT_ID:
            errors.append("IH_CLIENT_ID is not set")
        if not config.CLIENT_SECRET:
            errors.append("IH_CLIENT_SECRET is not set")
        if not config.ADLS_ACCOUNT_NAME:
            errors.append("IH_ADLS_ACCOUNT_NAME is not set")
        return errors

    def get_required_permissions(self) -> list[str]:
        return ["Reports.Read.All"]

    # ── ADLS Paths ─────────────────────────────────────────────────────────

    def get_bronze_prefix(self) -> str:
        return config.BRONZE_M365_USAGE_PREFIX

    def get_silver_prefix(self) -> str:
        return config.SILVER_M365_USAGE_PREFIX

    # ── Orchestration ──────────────────────────────────────────────────────

    def get_orchestration_phases(self) -> list[ConnectorPhase]:
        return [
            ConnectorPhase(
                name="ingest",
                activity_name="ingest_m365_usage",
                retry_max_attempts=3,
                retry_interval_ms=5_000,
            ),
            ConnectorPhase(
                name="transform",
                activity_name="transform_m365_usage",
                retry_max_attempts=3,
                retry_interval_ms=5_000,
            ),
        ]

    # ── Lifecycle Methods ──────────────────────────────────────────────────

    def ingest(self, input_data: dict) -> dict:
        """Fetch all M365 usage reports and write to ADLS Bronze.

        Input:
            {
                "period": "D7" | "D30" | "D90" | "D180" (default "D7"),
                "date_prefix": "2026/03/04" (optional),
                "reports": ["m365_app_user_detail", ...] (optional, default: all),
            }

        Returns:
            {
                "status": "completed",
                "bronze_paths": {"report_key": "bronze/m365-usage/...", ...},
                "records_per_report": {"report_key": N, ...},
            }
        """
        period = input_data.get("period", "D7")
        now = datetime.now(timezone.utc)
        date_prefix = input_data.get("date_prefix", now.strftime("%Y/%m/%d"))
        report_keys = input_data.get("reports", list(M365_USAGE_REPORTS.keys()))

        graph = GraphClient()
        adls = ADLSClient()

        bronze_paths: dict[str, str] = {}
        records_per_report: dict[str, int] = {}

        for report_key in report_keys:
            endpoint_name = M365_USAGE_REPORTS.get(report_key)
            if not endpoint_name:
                logger.warning("Unknown report key: %s — skipping", report_key)
                continue

            try:
                url = (
                    f"{config.GRAPH_BASE_URL}/v1.0/reports/"
                    f"{endpoint_name}(period='{period}')"
                )
                csv_text = graph.fetch_report_csv(url)

                if not csv_text or len(csv_text.strip()) == 0:
                    logger.warning("Empty report for %s — skipping", report_key)
                    continue

                # Count rows (minus header)
                row_count = csv_text.strip().count("\n")
                records_per_report[report_key] = row_count

                # Write to ADLS Bronze
                blob_path = (
                    f"{config.BRONZE_M365_USAGE_PREFIX}/{date_prefix}/"
                    f"{report_key}.csv"
                )
                adls.upload_csv(blob_path, csv_text)
                bronze_paths[report_key] = blob_path

                logger.info(
                    "Ingested %s: %d rows → %s",
                    report_key, row_count, blob_path,
                )

            except Exception as exc:
                logger.error("Failed to fetch %s: %s", report_key, exc)
                records_per_report[report_key] = 0

        adls.close()

        return {
            "status": "completed",
            "bronze_paths": bronze_paths,
            "records_per_report": records_per_report,
        }

    def transform_to_silver(self, input_data: dict) -> dict:
        """Transform Bronze usage reports to a unified Silver schema.

        Input:
            {
                "bronze_paths": {"report_key": "bronze/m365-usage/...", ...},
            }

        Returns:
            {
                "output_blob_path": "silver/m365-usage/silver_m365_usage.csv",
                "new_records": N,
            }
        """
        bronze_paths: dict[str, str] = input_data.get("bronze_paths", {})
        now = datetime.now(timezone.utc)
        loaded_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        adls = ADLSClient()
        all_rows: list[dict] = []

        for report_key, blob_path in bronze_paths.items():
            try:
                csv_text = adls.download_text(blob_path)
                reader = csv.DictReader(io.StringIO(csv_text))

                for raw_row in reader:
                    silver_row = self._normalize_report_row(
                        raw_row, report_key, loaded_at
                    )
                    if silver_row:
                        all_rows.append(silver_row)

            except Exception as exc:
                logger.error(
                    "Failed to transform %s (%s): %s",
                    report_key, blob_path, exc,
                )

        # Write Silver CSV
        output_path = f"{config.SILVER_M365_USAGE_PREFIX}/silver_m365_usage.csv"

        if all_rows:
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=M365_USAGE_SILVER_COLUMNS,
                lineterminator="\n",
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(all_rows)
            adls.upload_csv(output_path, output.getvalue())

        adls.close()

        return {
            "output_blob_path": output_path,
            "new_records": len(all_rows),
        }

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_silver_schema(self) -> list[str]:
        return list(M365_USAGE_SILVER_COLUMNS)

    # ── Internal helpers ───────────────────────────────────────────────────

    @staticmethod
    def _normalize_report_row(
        raw_row: dict, report_key: str, loaded_at: str
    ) -> dict | None:
        """Map a Graph report CSV row to the unified Silver schema.

        Different reports have different columns; this normaliser
        extracts the known fields and fills the rest with empty strings.
        """
        # Skip header row artefacts or truly empty rows
        upn = raw_row.get("User Principal Name", "").strip()
        if not upn:
            return None

        silver: dict = {col: "" for col in M365_USAGE_SILVER_COLUMNS}

        # Common fields
        silver["UserPrincipalName"] = upn
        silver["DisplayName"] = raw_row.get("Display Name", "")
        silver["ReportType"] = report_key
        silver["ReportPeriod"] = raw_row.get("Report Period", "")
        silver["ReportRefreshDate"] = raw_row.get("Report Refresh Date", "")
        silver["LastActivityDate"] = raw_row.get("Last Activity Date", "")
        silver["IsDeleted"] = raw_row.get("Is Deleted", "")
        silver["IsLicensed"] = raw_row.get("Is Licensed", "")

        # M365 App adoption
        if report_key == "m365_app_user_detail":
            for app in ("Teams", "Outlook", "Word", "Excel",
                        "PowerPoint", "OneNote", "OneDrive", "SharePoint"):
                # Graph returns fields like "Has Teams" or "Has Outlook"
                silver[f"Has{app}"] = raw_row.get(f"Has {app}", "")

        # Teams activity
        elif report_key == "teams_user_activity":
            silver["TeamsChatMessageCount"] = raw_row.get("Chat Message Count", "")
            silver["TeamsCallCount"] = raw_row.get("Call Count", "")
            silver["TeamsMeetingCount"] = raw_row.get("Meeting Count", "")
            silver["TeamsPrivateChatMessageCount"] = raw_row.get(
                "Private Chat Message Count", ""
            )

        # Email activity
        elif report_key == "email_activity":
            silver["EmailSendCount"] = raw_row.get("Send Count", "")
            silver["EmailReceiveCount"] = raw_row.get("Receive Count", "")
            silver["EmailReadCount"] = raw_row.get("Read Count", "")

        # SharePoint activity
        elif report_key == "sharepoint_activity":
            silver["SharePointFileViewedOrEdited"] = raw_row.get(
                "Viewed Or Edited File Count", ""
            )
            silver["SharePointFileSynced"] = raw_row.get("Synced File Count", "")
            silver["SharePointFileSharedInternally"] = raw_row.get(
                "Shared Internally File Count", ""
            )
            silver["SharePointFileSharedExternally"] = raw_row.get(
                "Shared Externally File Count", ""
            )
            silver["SharePointPagesVisited"] = raw_row.get(
                "Visited Page Count", ""
            )

        # OneDrive activity
        elif report_key == "onedrive_activity":
            silver["OneDriveFileViewedOrEdited"] = raw_row.get(
                "Viewed Or Edited File Count", ""
            )
            silver["OneDriveFileSynced"] = raw_row.get("Synced File Count", "")
            silver["OneDriveFileSharedInternally"] = raw_row.get(
                "Shared Internally File Count", ""
            )
            silver["OneDriveFileSharedExternally"] = raw_row.get(
                "Shared Externally File Count", ""
            )

        # Metadata
        silver["_SourceReport"] = report_key
        silver["_ReportDate"] = raw_row.get("Report Refresh Date", "")
        silver["_LoadedAtUtc"] = loaded_at

        return silver
