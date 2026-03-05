"""
Insight Harbor — Purview Audit Log Connector
==============================================
Wraps the existing Purview audit pipeline (create → poll → fetch → explode
→ transform) into the BaseConnector interface.

This connector uses the Graph API Purview audit-log endpoint
(``/security/auditLog/queries``) and supports:
  • Time-window partitioning
  • Async query-poll loop
  • Automatic subdivision for large partitions
  • 153-column JSON → CSV explosion
  • Silver transform with Entra enrichment + deduplication
"""

from __future__ import annotations

import logging

from shared.config import config
from shared.connectors.base import BaseConnector, ConnectorPhase
from shared.constants import PURVIEW_COMPUTED_COLUMNS, ENTRA_ENRICHMENT_COLUMNS
from shared.explosion import PURVIEW_EXPLODED_HEADER

logger = logging.getLogger("ih.connectors.purview_audit")


class PurviewAuditConnector(BaseConnector):
    """Connector for Microsoft Purview Audit Log (Graph API).

    Orchestration Phases
    --------------------
    1. ``plan_partitions`` — split time range into 6-hour windows
    2. ``process_partition`` — sub-orchestrator: create → poll → subdivide
       → fetch → cleanup (one per partition)
    3. ``explode_partition`` — flatten nested JSON to 153-column CSV
    4. ``transform_silver`` — enrichment, dedup, computed columns
    """

    # ── Identity ───────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "purview_audit"

    @property
    def display_name(self) -> str:
        return "Purview Audit Log"

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

    @property
    def supports_polling(self) -> bool:
        return True

    @property
    def supports_subdivision(self) -> bool:
        return True

    @property
    def supports_explosion(self) -> bool:
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
        return [
            "AuditLogsQuery.Read.All",  # Create/poll/fetch audit queries
        ]

    # ── ADLS Paths ─────────────────────────────────────────────────────────

    def get_bronze_prefix(self) -> str:
        return config.BRONZE_PURVIEW_PREFIX

    def get_silver_prefix(self) -> str:
        return config.SILVER_COPILOT_USAGE_PREFIX

    # ── Orchestration Phases ───────────────────────────────────────────────

    def get_orchestration_phases(self) -> list[ConnectorPhase]:
        return [
            ConnectorPhase(
                name="plan",
                activity_name="plan_partitions",
            ),
            ConnectorPhase(
                name="ingest",
                activity_name="process_partition",
                fan_out=True,
                retry_max_attempts=3,
                retry_interval_ms=5_000,
            ),
            ConnectorPhase(
                name="explode",
                activity_name="explode_partition",
                fan_out=True,
                parallel_group="post_ingest",
                retry_max_attempts=3,
                retry_interval_ms=5_000,
            ),
            ConnectorPhase(
                name="transform",
                activity_name="transform_silver",
                retry_max_attempts=3,
                retry_interval_ms=10_000,
            ),
        ]

    # ── Lifecycle Methods ──────────────────────────────────────────────────

    def plan(self, input_data: dict) -> list[dict]:
        """Delegate to the plan_partitions activity implementation."""
        from activities.plan_partitions import plan_partitions
        result = plan_partitions(input_data)
        return result.get("partitions", [])

    def ingest(self, input_data: dict) -> dict:
        """Run the full ingestion cycle for one partition.

        In practice, this is handled by the ``process_partition``
        sub-orchestrator, which manages the create → poll → fetch →
        cleanup lifecycle with Durable Functions timers.

        This method is provided for direct invocation (e.g. testing)
        and delegates to the individual activity implementations.
        """
        from activities.create_query import create_query
        from activities.fetch_records import fetch_records
        from activities.poll_query import poll_query
        from activities.cleanup_queries import cleanup_query

        # Step 1: Create the audit query
        query_result = create_query({
            "start_time": input_data.get("start"),
            "end_time": input_data.get("end"),
            "activity_types": input_data.get("activity_types", []),
            "partition_id": input_data.get("id", 0),
        })
        query_id = query_result["query_id"]

        # Step 2: Poll (simplified — no Durable timers)
        import time
        for _ in range(config.MAX_POLL_ATTEMPTS):
            time.sleep(config.POLL_MIN_SECONDS)
            poll_result = poll_query({"query_id": query_id})
            if poll_result["status"] == "succeeded":
                break
            if poll_result["status"] == "failed":
                raise RuntimeError(f"Query {query_id} failed on server")
        else:
            cleanup_query({"query_id": query_id})
            return {"status": "failed", "error": "Query timed out"}

        # Step 3: Fetch records
        fetch_result = fetch_records({
            "query_id": query_id,
            "partition_id": input_data.get("id", 0),
            "date_prefix": input_data.get("date_prefix", ""),
        })

        # Step 4: Cleanup
        cleanup_query({"query_id": query_id})

        return {
            "status": "completed",
            "partition_id": input_data.get("id", 0),
            "blob_path": fetch_result.get("blob_path", ""),
            "records": fetch_result.get("records_written", 0),
        }

    def explode(self, input_data: dict) -> dict:
        """Delegate to the explode_partition activity implementation."""
        from activities.explode_partition import explode_partition
        return explode_partition(input_data)

    def transform_to_silver(self, input_data: dict) -> dict:
        """Delegate to the transform_silver activity implementation."""
        from activities.transform_silver import transform_silver
        return transform_silver(input_data)

    def cleanup(self, input_data: dict) -> dict:
        """Delegate to the cleanup_queries activity implementation."""
        from activities.cleanup_queries import cleanup_query
        return cleanup_query(input_data)

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_bronze_schema(self) -> list[str]:
        """Return the 153-column exploded header."""
        return list(PURVIEW_EXPLODED_HEADER)

    def get_silver_schema(self) -> list[str]:
        """Return Silver columns (Bronze + computed + enrichment)."""
        silver = list(PURVIEW_EXPLODED_HEADER)
        for col in PURVIEW_COMPUTED_COLUMNS + ENTRA_ENRICHMENT_COLUMNS:
            if col not in silver:
                silver.append(col)
        return silver
