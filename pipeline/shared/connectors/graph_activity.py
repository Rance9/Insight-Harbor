"""
Insight Harbor — Graph Activity Log Connector
===============================================
Fetches sign-in and directory audit logs from the Graph API
``/auditLogs`` endpoints.  These complement Purview audit data
by providing authentication events and directory changes.

Supported Endpoints
~~~~~~~~~~~~~~~~~~~
- ``/auditLogs/signIns``     — user sign-in events
- ``/auditLogs/directoryAudits``  — directory change events

Data Flow
~~~~~~~~~
Graph ``/auditLogs/signIns?$filter=...`` (paginated) → JSONL Bronze →
transform → Silver CSV on ADLS.

Required App Registration Permissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- ``AuditLog.Read.All``      (read audit logs)
- ``Directory.Read.All``     (directory audit access)
"""

from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime, timezone, timedelta

from shared.adls_client import ADLSClient
from shared.config import config
from shared.connectors.base import BaseConnector, ConnectorPhase
from shared.graph_client import GraphClient

logger = logging.getLogger("ih.connectors.graph_activity")


# ═══════════════════════════════════════════════════════════════════════════════
# Silver Schema
# ═══════════════════════════════════════════════════════════════════════════════

GRAPH_ACTIVITY_SILVER_COLUMNS: list[str] = [
    # Identity
    "UserId",
    "UserPrincipalName",
    "DisplayName",
    "UserType",
    # Event
    "EventType",
    "EventId",
    "EventTime",
    "EventDate",
    # Sign-in specific
    "AppDisplayName",
    "AppId",
    "IpAddress",
    "Location_City",
    "Location_State",
    "Location_Country",
    "ClientAppUsed",
    "DeviceDetail_Browser",
    "DeviceDetail_OS",
    "DeviceDetail_DeviceId",
    "IsInteractive",
    "ResourceDisplayName",
    "ResourceId",
    "ConditionalAccessStatus",
    "RiskState",
    "RiskLevelDuringSignIn",
    "MfaRequired",
    "Status_ErrorCode",
    "Status_FailureReason",
    # Directory audit specific
    "ActivityDisplayName",
    "Category",
    "Result",
    "TargetResource",
    # Metadata
    "_SourceEndpoint",
    "_LoadedAtUtc",
]


# ═══════════════════════════════════════════════════════════════════════════════
# GraphActivityConnector
# ═══════════════════════════════════════════════════════════════════════════════


class GraphActivityConnector(BaseConnector):
    """Connector for Graph API audit/sign-in logs.

    Supports daily partitioning to bound the volume of sign-in
    events returned per API call.
    """

    # ── Identity ───────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "graph_activity"

    @property
    def display_name(self) -> str:
        return "Graph Activity Logs"

    @property
    def source_type(self) -> str:
        return "graph_api"

    @property
    def version(self) -> str:
        return "1.0.0"

    # ── Capability flags ───────────────────────────────────────────────────

    @property
    def supports_partitioning(self) -> bool:
        return True

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
        return ["AuditLog.Read.All", "Directory.Read.All"]

    # ── ADLS Paths ─────────────────────────────────────────────────────────

    def get_bronze_prefix(self) -> str:
        return config.BRONZE_GRAPH_ACTIVITY_PREFIX

    def get_silver_prefix(self) -> str:
        return config.SILVER_GRAPH_ACTIVITY_PREFIX

    # ── Orchestration ──────────────────────────────────────────────────────

    def get_orchestration_phases(self) -> list[ConnectorPhase]:
        return [
            ConnectorPhase(
                name="ingest",
                activity_name="ingest_graph_activity",
                fan_out=True,  # one per day partition
                retry_max_attempts=3,
                retry_interval_ms=5_000,
            ),
            ConnectorPhase(
                name="transform",
                activity_name="transform_graph_activity",
                retry_max_attempts=3,
                retry_interval_ms=5_000,
            ),
        ]

    # ── Lifecycle Methods ──────────────────────────────────────────────────

    def plan(self, input_data: dict) -> list[dict]:
        """Create one work-item per day in the date range.

        Sign-in logs are date-filtered, so daily partitions keep
        API result sets manageable.
        """
        start_str = input_data.get("start_date", "")
        end_str = input_data.get("end_date", "")
        now = datetime.now(timezone.utc)

        if start_str:
            start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        else:
            start = now - timedelta(days=config.DEFAULT_LOOKBACK_DAYS)

        if end_str:
            end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        else:
            end = now

        partitions: list[dict] = []
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        part_id = 1

        while current < end:
            next_day = current + timedelta(days=1)
            partitions.append({
                "id": part_id,
                "start": current.isoformat(),
                "end": min(next_day, end).isoformat(),
                "date_prefix": current.strftime("%Y/%m/%d"),
                "endpoints": ["signIns", "directoryAudits"],
            })
            part_id += 1
            current = next_day

        return partitions

    def ingest(self, input_data: dict) -> dict:
        """Fetch sign-in and directory audit logs for one day partition.

        Uses ``$filter`` on ``createdDateTime`` to scope the results.
        Paginates until all data is fetched and writes JSONL to Bronze.

        Input:
            {
                "id": 1,
                "start": "2026-03-04T00:00:00+00:00",
                "end": "2026-03-05T00:00:00+00:00",
                "date_prefix": "2026/03/04",
                "endpoints": ["signIns", "directoryAudits"],
            }
        """
        start = input_data["start"]
        end = input_data["end"]
        date_prefix = input_data.get("date_prefix", "")
        endpoints = input_data.get("endpoints", ["signIns"])
        partition_id = input_data.get("id", 0)

        graph = GraphClient()
        adls = ADLSClient()

        bronze_paths: dict[str, str] = {}
        total_records = 0

        for endpoint in endpoints:
            filter_expr = (
                f"createdDateTime ge {start} and createdDateTime lt {end}"
            )
            url = (
                f"{config.GRAPH_BASE_URL}/v1.0/auditLogs/{endpoint}"
                f"?$filter={filter_expr}&$top=999"
            )

            all_records: list[dict] = []
            try:
                for page in graph.paginate(url):
                    all_records.extend(page)
            except Exception as exc:
                logger.error(
                    "Failed to fetch %s for partition %d: %s",
                    endpoint, partition_id, exc,
                )
                continue

            if all_records:
                # Write as JSONL to Bronze
                jsonl_lines = [json.dumps(r) for r in all_records]
                jsonl_text = "\n".join(jsonl_lines) + "\n"

                blob_path = (
                    f"{config.BRONZE_GRAPH_ACTIVITY_PREFIX}/{date_prefix}/"
                    f"{endpoint}_P{partition_id:03d}.jsonl"
                )
                adls.upload_csv(blob_path, jsonl_text)
                bronze_paths[endpoint] = blob_path
                total_records += len(all_records)

                logger.info(
                    "Ingested %s: %d records → %s",
                    endpoint, len(all_records), blob_path,
                )

        adls.close()

        return {
            "status": "completed",
            "partition_id": partition_id,
            "bronze_paths": bronze_paths,
            "records": total_records,
        }

    def transform_to_silver(self, input_data: dict) -> dict:
        """Transform Bronze JSONL to Silver CSV.

        Input:
            {
                "bronze_paths": [
                    {"endpoint": "signIns", "path": "bronze/..."},
                    ...
                ]
            }
        """
        bronze_items = input_data.get("bronze_paths", [])
        now = datetime.now(timezone.utc)
        loaded_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        adls = ADLSClient()
        all_rows: list[dict] = []

        for item in bronze_items:
            endpoint = item.get("endpoint", "signIns")
            blob_path = item.get("path", "")
            if not blob_path:
                continue

            try:
                jsonl_text = adls.download_text(blob_path)
                for line in jsonl_text.strip().split("\n"):
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    silver_row = self._normalize_record(
                        record, endpoint, loaded_at
                    )
                    if silver_row:
                        all_rows.append(silver_row)
            except Exception as exc:
                logger.error("Failed to transform %s: %s", blob_path, exc)

        output_path = (
            f"{config.SILVER_GRAPH_ACTIVITY_PREFIX}/silver_graph_activity.csv"
        )

        if all_rows:
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=GRAPH_ACTIVITY_SILVER_COLUMNS,
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
        return list(GRAPH_ACTIVITY_SILVER_COLUMNS)

    # ── Internal helpers ───────────────────────────────────────────────────

    @staticmethod
    def _normalize_record(
        record: dict, endpoint: str, loaded_at: str
    ) -> dict | None:
        """Normalise a Graph API audit/sign-in record to Silver schema."""
        silver: dict = {col: "" for col in GRAPH_ACTIVITY_SILVER_COLUMNS}

        event_id = record.get("id", "")
        if not event_id:
            return None

        silver["EventId"] = event_id
        silver["_SourceEndpoint"] = endpoint
        silver["_LoadedAtUtc"] = loaded_at

        if endpoint == "signIns":
            silver["EventType"] = "SignIn"
            silver["UserId"] = record.get("userId", "")
            silver["UserPrincipalName"] = record.get("userPrincipalName", "")
            silver["DisplayName"] = record.get("userDisplayName", "")
            silver["UserType"] = record.get("userType", "")

            created = record.get("createdDateTime", "")
            silver["EventTime"] = created
            if created:
                silver["EventDate"] = created[:10]

            silver["AppDisplayName"] = record.get("appDisplayName", "")
            silver["AppId"] = record.get("appId", "")
            silver["IpAddress"] = record.get("ipAddress", "")
            silver["ClientAppUsed"] = record.get("clientAppUsed", "")
            silver["IsInteractive"] = str(record.get("isInteractive", ""))
            silver["ConditionalAccessStatus"] = record.get(
                "conditionalAccessStatus", ""
            )
            silver["RiskState"] = record.get("riskState", "")
            silver["RiskLevelDuringSignIn"] = record.get(
                "riskLevelDuringSignIn", ""
            )
            silver["ResourceDisplayName"] = record.get(
                "resourceDisplayName", ""
            )
            silver["ResourceId"] = record.get("resourceId", "")

            # Nested objects
            location = record.get("location", {}) or {}
            silver["Location_City"] = location.get("city", "")
            silver["Location_State"] = location.get("state", "")
            silver["Location_Country"] = location.get("countryOrRegion", "")

            device = record.get("deviceDetail", {}) or {}
            silver["DeviceDetail_Browser"] = device.get("browser", "")
            silver["DeviceDetail_OS"] = device.get("operatingSystem", "")
            silver["DeviceDetail_DeviceId"] = device.get("deviceId", "")

            status = record.get("status", {}) or {}
            silver["Status_ErrorCode"] = str(status.get("errorCode", ""))
            silver["Status_FailureReason"] = status.get("failureReason", "")

            mfa_detail = record.get("mfaDetail", {}) or {}
            silver["MfaRequired"] = (
                "Yes" if mfa_detail.get("authMethod") else "No"
            )

        elif endpoint == "directoryAudits":
            silver["EventType"] = "DirectoryAudit"

            initiated_by = record.get("initiatedBy", {}) or {}
            user = initiated_by.get("user", {}) or {}
            silver["UserId"] = user.get("id", "")
            silver["UserPrincipalName"] = user.get("userPrincipalName", "")
            silver["DisplayName"] = user.get("displayName", "")

            silver["ActivityDisplayName"] = record.get(
                "activityDisplayName", ""
            )
            silver["Category"] = record.get("category", "")
            silver["Result"] = record.get("result", "")

            created = record.get("activityDateTime", "")
            silver["EventTime"] = created
            if created:
                silver["EventDate"] = created[:10]

            targets = record.get("targetResources", [])
            if targets:
                silver["TargetResource"] = targets[0].get("displayName", "")

        return silver
