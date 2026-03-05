"""
Insight Harbor — Create Query Activity
========================================
Creates a Purview audit log query via the Graph API.

Maps to PAX's Invoke-GraphAuditQuery function.
Handles: display name generation, filter body construction, error raising.
"""

from __future__ import annotations

import logging

from pipeline.shared.config import config
from pipeline.shared.graph_client import GraphClient

logger = logging.getLogger("ih.activity.create_query")


def create_query(input_data: dict) -> dict:
    """Create a Graph API audit log query.

    Input:
        {
            "partition_id": 1,
            "start_time": "2026-03-04T00:00:00+00:00",
            "end_time": "2026-03-04T06:00:00+00:00",
            "activity_types": ["CopilotInteraction"],
            "record_types": ["CopilotInteraction"] (optional),
            "service_filter": "Exchange" (optional),
        }

    Returns:
        {"query_id": "...", "display_name": "...", "partition_id": 1}
    """
    partition_id = input_data["partition_id"]
    start_time = input_data["start_time"]
    end_time = input_data["end_time"]
    activity_types = input_data["activity_types"]

    display_name = f"IH_P{partition_id}_{start_time[:13].replace(':', '')}"

    client = GraphClient()
    result = client.create_audit_query(
        display_name=display_name,
        start_time=start_time,
        end_time=end_time,
        activity_types=activity_types,
        record_types=input_data.get("record_types"),
        user_principal_names=config.USER_IDS if config.USER_IDS else None,
        service_filter=input_data.get("service_filter"),
    )

    logger.info(
        "Created query %s for partition %d (%s → %s)",
        result["query_id"], partition_id, start_time[:16], end_time[:16],
    )

    return {
        "query_id": result["query_id"],
        "display_name": result["display_name"],
        "partition_id": partition_id,
    }
