"""
Insight Harbor — Entra Silver Transform Functions
===================================================
Pure functions extracted from transform/bronze_to_silver_entra.py
for reuse in the Durable Functions pipeline.

All functions are stateless with zero I/O dependencies.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any

from .constants import (
    COPILOT_KEYWORDS,
    ENTRA_SILVER_COLUMNS,
    ENTRA_SOURCE_TO_SILVER,
    LICENSE_TIER_RULES,
)

logger = logging.getLogger("ih.entra_transforms")


# ═══════════════════════════════════════════════════════════════════════════════
# Pure Transform Functions (ported from bronze_to_silver_entra.py)
# ═══════════════════════════════════════════════════════════════════════════════


def build_column_map(fieldnames: list[str]) -> dict[str, str]:
    """Build a case-insensitive mapping from raw field names to Silver columns.

    Returns:
        Dict mapping raw field name → Silver column name.
    """
    col_map: dict[str, str] = {}
    for field in fieldnames:
        lower = field.strip().lower()
        if lower in ENTRA_SOURCE_TO_SILVER:
            col_map[field] = ENTRA_SOURCE_TO_SILVER[lower]
    return col_map


def parse_bool(value: Any) -> str:
    """Convert various boolean representations to 'True'/'False' string."""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return "True" if value.strip().lower() in ("true", "1", "yes") else "False"
    return "False"


def has_copilot_license(licenses_str: str) -> bool:
    """Check if the user has any Copilot-related license.

    Uses keyword matching ('copilot' substring) which is more robust
    than fixed SKU IDs — catches new SKUs automatically.
    """
    if not licenses_str:
        return False
    lower = licenses_str.lower()
    return any(kw in lower for kw in COPILOT_KEYWORDS)


def compute_license_tier(licenses_str: str) -> str:
    """Determine the highest license tier from assigned licenses.

    Priority order (first match wins):
      Copilot > E5 > E3 > F1/F3

    Returns: tier string or "" if no match.
    """
    if not licenses_str:
        return ""
    lower = licenses_str.lower()
    for keyword, tier in LICENSE_TIER_RULES:
        if keyword in lower:
            return tier
    return ""


def parse_snapshot_date() -> str:
    """Generate a snapshot date stamp (current UTC date)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def transform_entra_row(
    raw_row: dict,
    column_map: dict[str, str],
    snapshot_date: str,
    loaded_at: str,
) -> dict | None:
    """Transform one raw Entra user row to the Silver schema.

    Args:
        raw_row: Dict from Graph API /users response or CSV DictReader.
        column_map: Mapping from raw field names to Silver column names.
        snapshot_date: Current date string for _SnapshotDate.
        loaded_at: UTC timestamp string for _LoadedAtUtc.

    Returns:
        Silver row dict or None if row is invalid (no UPN).
    """
    # Map raw fields to Silver columns
    silver_row: dict[str, str] = {}
    for raw_field, silver_col in column_map.items():
        val = raw_row.get(raw_field, "")
        if isinstance(val, (list, dict)):
            silver_row[silver_col] = str(val)
        elif isinstance(val, bool):
            silver_row[silver_col] = str(val)
        else:
            silver_row[silver_col] = str(val).strip() if val is not None else ""

    # Validate — must have UPN
    upn = silver_row.get("UserPrincipalName", "").strip()
    if not upn:
        return None

    # Boolean columns
    if "AccountEnabled" in silver_row:
        silver_row["AccountEnabled"] = parse_bool(silver_row.get("AccountEnabled", ""))

    # License processing
    licenses_str = silver_row.get("AssignedLicenses", "")
    silver_row["HasLicense"] = "True" if licenses_str else "False"
    silver_row["HasCopilotLicense"] = str(has_copilot_license(licenses_str))
    silver_row["LicenseTier"] = compute_license_tier(licenses_str)

    # Metadata columns
    silver_row["_SnapshotDate"] = snapshot_date
    silver_row["_LoadedAtUtc"] = loaded_at

    # Fill missing Silver columns with empty strings
    for col in ENTRA_SILVER_COLUMNS:
        if col not in silver_row:
            silver_row[col] = ""

    return silver_row


def transform_entra_from_graph(
    users: list[dict],
    loaded_at: str,
    snapshot_date: str | None = None,
) -> list[dict]:
    """Transform a batch of Graph API user objects to Silver rows.

    Used by the pull_entra activity which gets data directly from Graph API.

    Args:
        users: List of user dicts from Graph API /users response.
        loaded_at: UTC timestamp for _LoadedAtUtc.
        snapshot_date: Snapshot date string (default: today's UTC date).

    Returns:
        List of Silver row dicts.
    """
    if not snapshot_date:
        snapshot_date = parse_snapshot_date()

    # Build column map from Graph API field names
    if not users:
        return []

    # Graph API returns camelCase; our map handles case-insensitive matching
    sample_keys = list(users[0].keys())
    column_map = build_column_map(sample_keys)

    silver_rows: list[dict] = []
    seen_upns: set[str] = set()

    for user in users:
        # Flatten employeeOrgData if present
        org_data = user.get("employeeOrgData") or {}
        if isinstance(org_data, dict):
            if "division" in org_data:
                user["employeeorgdata_division"] = org_data["division"]
            if "costCenter" in org_data:
                user["employeeorgdata_costcenter"] = org_data["costCenter"]

        # Flatten assignedLicenses to a string for processing
        assigned = user.get("assignedLicenses") or []
        if isinstance(assigned, list):
            sku_ids = [
                lic.get("skuId", "") for lic in assigned if isinstance(lic, dict)
            ]
            user["assignedLicenses"] = ";".join(sku_ids)

        silver_row = transform_entra_row(user, column_map, snapshot_date, loaded_at)
        if silver_row is None:
            continue

        # Dedup on UPN (case-insensitive)
        upn_lower = silver_row["UserPrincipalName"].lower()
        if upn_lower in seen_upns:
            continue
        seen_upns.add(upn_lower)

        silver_rows.append(silver_row)

    return silver_rows


def entra_rows_to_csv(
    rows: list[dict],
    *,
    include_header: bool = True,
) -> str:
    """Serialize Entra Silver rows to CSV string."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=ENTRA_SILVER_COLUMNS,
        lineterminator="\n",
        extrasaction="ignore",
    )
    if include_header:
        writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def load_entra_lookup_from_csv(csv_text: str) -> dict[str, dict]:
    """Load Entra user lookup from a Silver CSV string.

    Returns:
        Dict mapping lowercase UPN → enrichment column values.
    """
    from .constants import ENTRA_ENRICHMENT_COLUMNS

    lookup: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        upn = row.get("UserPrincipalName", "").strip().lower()
        if upn:
            lookup[upn] = {
                col: row.get(col, "") for col in ENTRA_ENRICHMENT_COLUMNS
            }
    return lookup
