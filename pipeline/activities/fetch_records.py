"""
Insight Harbor — Fetch Records Activity
=========================================
Fetches paginated audit records from a completed query and streams
them to ADLS as JSONL.

Matches PAX's per-page streaming model:
  • One page in memory at a time (~2 MB)
  • Each page appended to ADLS append blob
  • Zero local file system usage
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pipeline.shared.adls_client import ADLSClient
from pipeline.shared.config import config
from pipeline.shared.graph_client import GraphClient

logger = logging.getLogger("ih.activity.fetch_records")


def fetch_records(input_data: dict) -> dict:
    """Fetch all records from a completed audit query and stream to ADLS.

    Input:
        {
            "query_id": "...",
            "partition_id": 1,
            "date_prefix": "2026/03/04",
        }

    Returns:
        {
            "query_id": "...",
            "partition_id": 1,
            "blob_path": "bronze/purview/2026/03/04/P001.jsonl",
            "records_written": 4500,
            "pages_fetched": 5,
        }
    """
    query_id = input_data["query_id"]
    partition_id = input_data["partition_id"]
    date_prefix = input_data["date_prefix"]

    # Build the ADLS blob path
    blob_path = (
        f"{config.BRONZE_PURVIEW_PREFIX}/{date_prefix}"
        f"/P{partition_id:03d}.jsonl"
    )

    graph = GraphClient()
    adls = ADLSClient()

    # Create the append blob for streaming writes
    adls.create_append_blob(blob_path, content_type="application/x-ndjson")

    total_records = 0
    pages_fetched = 0

    try:
        for page in graph.fetch_audit_records(query_id):
            pages_fetched += 1
            total_records += len(page)

            # Stream this page to ADLS immediately (matching PAX's per-page flush)
            adls.append_jsonl(blob_path, page)

            logger.debug(
                "Partition %d — page %d: %d records (total: %d)",
                partition_id, pages_fetched, len(page), total_records,
            )
    finally:
        adls.close()

    logger.info(
        "Partition %d — fetched %d records (%d pages) → %s",
        partition_id, total_records, pages_fetched, blob_path,
    )

    return {
        "query_id": query_id,
        "partition_id": partition_id,
        "blob_path": blob_path,
        "records_written": total_records,
        "pages_fetched": pages_fetched,
    }
