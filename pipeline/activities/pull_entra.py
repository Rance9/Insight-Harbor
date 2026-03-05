"""
Insight Harbor — Pull Entra Users Activity
============================================
Fetches all Entra ID (Azure AD) users via Graph API, applies the Silver
transform, and uploads to ADLS.

Matches PAX's -OnlyUserInfo mode:
  • Paginated /users endpoint with $select
  • License extraction + Copilot detection
  • UPN deduplication
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pipeline.shared.adls_client import ADLSClient
from pipeline.shared.config import config
from pipeline.shared.constants import ENTRA_USER_SELECT_FIELDS
from pipeline.shared.entra_transforms import (
    entra_rows_to_csv,
    parse_snapshot_date,
    transform_entra_from_graph,
)
from pipeline.shared.graph_client import GraphClient

logger = logging.getLogger("ih.activity.pull_entra")


def pull_entra(input_data: dict) -> dict:
    """Fetch Entra users from Graph API, transform, and store in ADLS.

    Input:
        {
            "date_prefix": "2026/03/04" (optional — defaults to today),
        }

    Returns:
        {
            "bronze_blob_path": "bronze/entra/2026/03/04/entra_users_raw.csv",
            "silver_blob_path": "silver/entra-users/silver_entra_users.csv",
            "users_count": 250,
        }
    """
    now = datetime.now(timezone.utc)
    date_prefix = input_data.get("date_prefix", now.strftime("%Y/%m/%d"))
    loaded_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    snapshot_date = parse_snapshot_date()

    graph = GraphClient()
    adls = ADLSClient()

    # Collect all users from Graph API
    all_users: list[dict] = []
    for page in graph.fetch_users(ENTRA_USER_SELECT_FIELDS):
        all_users.extend(page)

    logger.info("Fetched %d users from Graph API", len(all_users))

    # Transform to Silver schema
    silver_rows = transform_entra_from_graph(all_users, loaded_at, snapshot_date)
    logger.info("Transformed %d Entra users to Silver", len(silver_rows))

    # Write Silver CSV to ADLS
    silver_csv = entra_rows_to_csv(silver_rows)
    silver_blob = f"{config.SILVER_ENTRA_USERS_PREFIX}/silver_entra_users.csv"
    adls.upload_csv(silver_blob, silver_csv)

    # Also write Bronze snapshot for audit trail
    bronze_blob = f"{config.BRONZE_ENTRA_PREFIX}/{date_prefix}/entra_users_raw.csv"
    adls.upload_csv(bronze_blob, silver_csv)

    adls.close()

    logger.info(
        "Entra pull complete — %d users → %s", len(silver_rows), silver_blob,
    )

    return {
        "bronze_blob_path": bronze_blob,
        "silver_blob_path": silver_blob,
        "users_count": len(silver_rows),
    }
