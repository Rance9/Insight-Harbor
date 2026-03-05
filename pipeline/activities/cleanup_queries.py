"""
Insight Harbor — Cleanup Queries Activity
==========================================
Deletes completed audit log queries from Graph API to free the
10-concurrent-query slot limit.

Matches PAX's post-fetch cleanup behavior.
"""

from __future__ import annotations

import logging

from pipeline.shared.graph_client import GraphClient

logger = logging.getLogger("ih.activity.cleanup_queries")


def cleanup_query(input_data: dict) -> dict:
    """Delete a completed audit query to free the 10-query slot.

    Input:
        {"query_id": "..."}

    Returns:
        {"query_id": "...", "deleted": true}
    """
    query_id = input_data["query_id"]

    client = GraphClient()
    client.delete_audit_query(query_id)

    logger.info("Cleaned up query %s", query_id)

    return {
        "query_id": query_id,
        "deleted": True,
    }
