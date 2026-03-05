"""
Insight Harbor — Explode Partition Activity
=============================================
Reads a raw JSONL partition blob from ADLS, runs the explosion logic
to produce the 153-column flat schema, and streams the exploded CSV
back to ADLS.

Memory-safe: processes in chunks of 50 MB.
Zero local file system usage.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pipeline.shared.adls_client import ADLSClient
from pipeline.shared.config import config
from pipeline.shared.explosion import (
    PURVIEW_EXPLODED_HEADER,
    explode_records_from_jsonl,
    rows_to_csv_string,
)

logger = logging.getLogger("ih.activity.explode_partition")


def explode_partition(input_data: dict) -> dict:
    """Explode a raw JSONL bronze partition into a flat 153-column CSV.

    Input:
        {
            "bronze_blob_path": "bronze/purview/2026/03/04/P001.jsonl",
            "date_prefix": "2026/03/04",
            "partition_id": 1,
        }

    Returns:
        {
            "input_blob_path": "bronze/purview/2026/03/04/P001.jsonl",
            "output_blob_path": "bronze/exploded/2026/03/04/P001_exploded.csv",
            "records_exploded": 12350,
        }
    """
    bronze_path = input_data["bronze_blob_path"]
    date_prefix = input_data["date_prefix"]
    partition_id = input_data["partition_id"]

    output_path = (
        f"{config.BRONZE_EXPLODED_PREFIX}/{date_prefix}"
        f"/P{partition_id:03d}_exploded.csv"
    )

    adls = ADLSClient()

    # Create append blob for streaming CSV output
    adls.create_append_blob(output_path, content_type="text/csv")

    # Write CSV header first
    header_line = ",".join(PURVIEW_EXPLODED_HEADER) + "\n"
    adls.append_to_blob(output_path, header_line)

    total_exploded = 0
    chunk_lines: list[str] = []
    chunk_count = 0

    try:
        for line in adls.download_lines(bronze_path):
            chunk_lines.append(line)

            # Process in chunks to manage memory (~5000 lines per chunk)
            if len(chunk_lines) >= 5000:
                chunk_count += 1
                exploded = explode_records_from_jsonl(
                    chunk_lines,
                    prompt_filter=config.PROMPT_FILTER if config.PROMPT_FILTER else None,
                )
                if exploded:
                    csv_chunk = rows_to_csv_string(
                        exploded, include_header=False
                    )
                    adls.append_to_blob(output_path, csv_chunk)
                    total_exploded += len(exploded)

                chunk_lines = []
                logger.debug(
                    "Partition %d — chunk %d: %d rows exploded (total: %d)",
                    partition_id, chunk_count, len(exploded), total_exploded,
                )

        # Process remaining lines
        if chunk_lines:
            exploded = explode_records_from_jsonl(
                chunk_lines,
                prompt_filter=config.PROMPT_FILTER if config.PROMPT_FILTER else None,
            )
            if exploded:
                csv_chunk = rows_to_csv_string(
                    exploded, include_header=False
                )
                adls.append_to_blob(output_path, csv_chunk)
                total_exploded += len(exploded)
    finally:
        adls.close()

    logger.info(
        "Partition %d — exploded %d records → %s",
        partition_id, total_exploded, output_path,
    )

    return {
        "input_blob_path": bronze_path,
        "output_blob_path": output_path,
        "records_exploded": total_exploded,
    }
