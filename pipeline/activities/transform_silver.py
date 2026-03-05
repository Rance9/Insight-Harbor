"""
Insight Harbor — Transform Silver Activity
============================================
Reads exploded Bronze CSV blobs from ADLS, applies the Silver schema
with Entra enrichment and deduplication, and uploads the final
Silver Copilot Usage CSV.

Memory-safe: streams exploded files and uses dedup key set (~50 MB for 5M rows).
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone

from pipeline.shared.adls_client import ADLSClient
from pipeline.shared.config import config
from pipeline.shared.constants import SILVER_COPILOT_USAGE_FILENAME
from pipeline.shared.entra_transforms import load_entra_lookup_from_csv
from pipeline.shared.explosion import PURVIEW_EXPLODED_HEADER
from pipeline.shared.transforms import (
    build_silver_columns,
    load_dedup_keys_from_csv,
    rows_to_csv_string,
    transform_batch,
)

logger = logging.getLogger("ih.activity.transform_silver")


def transform_silver(input_data: dict) -> dict:
    """Apply Bronze-to-Silver transform with Entra enrichment and dedup.

    Input:
        {
            "exploded_blob_paths": ["bronze/exploded/2026/03/04/P001_exploded.csv", ...],
            "entra_silver_path": "silver/entra-users/silver_entra_users.csv",
            "overwrite": false (optional),
        }

    Returns:
        {
            "output_blob_path": "silver/copilot-usage/silver_copilot_usage.csv",
            "records_transformed": 12000,
            "new_records": 11800,
            "duplicates_skipped": 200,
        }
    """
    exploded_paths = input_data["exploded_blob_paths"]
    entra_path = input_data.get("entra_silver_path", "")
    overwrite = input_data.get("overwrite", False)

    now = datetime.now(timezone.utc)
    loaded_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    adls = ADLSClient()
    output_path = (
        f"{config.SILVER_COPILOT_USAGE_PREFIX}/{SILVER_COPILOT_USAGE_FILENAME}"
    )

    # Load Entra lookup for enrichment (~46 KB for demo tenant)
    entra_lookup: dict[str, dict] = {}
    if entra_path:
        try:
            entra_csv = adls.download_text(entra_path)
            entra_lookup = load_entra_lookup_from_csv(entra_csv)
            logger.info("Loaded %d Entra users for enrichment", len(entra_lookup))
        except Exception as exc:
            logger.warning("Could not load Entra Silver: %s — enrichment will be empty", exc)

    # Load existing dedup keys (unless overwrite mode)
    existing_keys: set[tuple] = set()
    if not overwrite and adls.blob_exists(output_path):
        try:
            existing_csv = adls.download_text(output_path)
            existing_keys = load_dedup_keys_from_csv(existing_csv)
            logger.info("Loaded %d existing dedup keys", len(existing_keys))
        except Exception as exc:
            logger.warning("Could not load existing Silver for dedup: %s", exc)

    # Process each exploded blob
    all_new_rows: list[dict] = []
    total_skipped_no_id = 0
    total_skipped_dedup = 0
    total_errors = 0
    bronze_columns: list[str] = list(PURVIEW_EXPLODED_HEADER)

    for blob_path in exploded_paths:
        logger.info("Processing: %s", blob_path)

        try:
            csv_text = adls.download_text(blob_path)
            reader = csv.DictReader(io.StringIO(csv_text))

            if reader.fieldnames:
                # Use actual columns from first file
                if not all_new_rows:
                    bronze_columns = list(reader.fieldnames)

            # Read all rows from this file into a batch
            batch_rows = list(reader)

            source_file = blob_path.split("/")[-1]
            new_rows, no_id, dedup, errors = transform_batch(
                batch_rows, source_file, loaded_at,
                entra_lookup, existing_keys,
            )

            all_new_rows.extend(new_rows)
            total_skipped_no_id += no_id
            total_skipped_dedup += dedup
            total_errors += errors

            logger.info(
                "  %s: %d new, %d dedup, %d no-id, %d errors",
                source_file, len(new_rows), dedup, no_id, errors,
            )

        except Exception as exc:
            logger.error("Failed to process %s: %s", blob_path, exc)
            total_errors += 1

    # Build Silver column list and write to ADLS
    silver_columns = build_silver_columns(bronze_columns)

    if all_new_rows:
        silver_csv = rows_to_csv_string(all_new_rows, silver_columns)
        adls.upload_csv(output_path, silver_csv)
        logger.info(
            "Silver transform complete: %d new rows → %s",
            len(all_new_rows), output_path,
        )
    else:
        logger.info("No new rows — Silver layer is up to date")

    adls.close()

    return {
        "output_blob_path": output_path,
        "records_transformed": len(all_new_rows) + total_skipped_no_id + total_skipped_dedup,
        "new_records": len(all_new_rows),
        "duplicates_skipped": total_skipped_dedup,
        "errors": total_errors,
    }
