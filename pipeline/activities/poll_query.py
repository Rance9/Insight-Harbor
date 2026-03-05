"""
Insight Harbor — Poll Query Activity
======================================
Polls a Purview audit log query for completion status.

Pure status check — no side effects (idempotent).
Called in a polling loop by the process_partition sub-orchestrator.
"""

from __future__ import annotations

import logging

from pipeline.shared.graph_client import GraphClient

logger = logging.getLogger("ih.activity.poll_query")


def poll_query(input_data: dict) -> dict:
    """Poll the status of a Graph API audit log query.

    Input:
        {"query_id": "..."}

    Returns:
        {"query_id": "...", "status": "succeeded|running|failed|cancelled",
         "record_count": 4500}
    """
    query_id = input_data["query_id"]

    client = GraphClient()
    result = client.poll_audit_query(query_id)

    logger.info(
        "Query %s status: %s (records: %d)",
        query_id, result["status"], result["record_count"],
    )

    return result
