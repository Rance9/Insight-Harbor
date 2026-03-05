"""
Insight Harbor — Purview Silver Transform Functions
====================================================
Pure functions extracted from transform/bronze_to_silver_purview.py
for reuse in the Durable Functions pipeline.

All functions are stateless with zero I/O dependencies — they operate
on in-memory dicts and return transformed results.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from .constants import (
    DEDUP_KEY_COLS,
    ENTRA_ENRICHMENT_COLUMNS,
    PURVIEW_COMPUTED_COLUMNS,
    PURVIEW_INT_COLUMNS,
)

logger = logging.getLogger("ih.transforms")


# ═══════════════════════════════════════════════════════════════════════════════
# Pure Transform Functions (ported from bronze_to_silver_purview.py)
# ═══════════════════════════════════════════════════════════════════════════════


def parse_creation_time(raw: str) -> datetime | None:
    """Parse CreationTime from audit record.

    Handles multiple ISO 8601 formats:
      - 2026-03-04T12:34:56Z
      - 2026-03-04T12:34:56.1234567Z
      - 2026-03-04 12:34:56
    """
    if not raw:
        return None
    # Strip trailing 'Z' and any sub-second precision beyond 6 digits
    cleaned = raw.rstrip("Z").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(cleaned[:26], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def compute_prompt_type(is_prompt_raw: str) -> str:
    """Compute PromptType from Message_isPrompt field.

    Returns: "Prompt", "Response", or "" (unknown/empty).
    """
    val = is_prompt_raw.strip().lower()
    if val == "true":
        return "Prompt"
    elif val == "false":
        return "Response"
    return ""


def compute_is_agent(agent_id: str) -> str:
    """Determine if the interaction is from an agent.

    Returns: "Yes" if AgentId is non-empty, "No" otherwise.
    """
    return "Yes" if agent_id.strip() else "No"


def safe_int(value: Any) -> int | str:
    """Cast value to int if possible, else return empty string."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return ""
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return ""


def make_dedup_key(row: dict) -> tuple:
    """Create a composite dedup key from RecordId + Message_Id."""
    return tuple(str(row.get(col, "")).strip() for col in DEDUP_KEY_COLS)


def enrich_row_with_entra(
    silver_row: dict, entra_lookup: dict[str, dict]
) -> None:
    """Add Entra enrichment columns to a Silver row (in-place).

    Looks up the user by UserId (case-insensitive UPN match).
    """
    user_id = silver_row.get("UserId", "").strip().lower()
    entra = entra_lookup.get(user_id)
    if entra:
        for col in ENTRA_ENRICHMENT_COLUMNS:
            silver_row[col] = entra.get(col, "")
    else:
        for col in ENTRA_ENRICHMENT_COLUMNS:
            silver_row[col] = ""


def transform_row(
    raw_row: dict, source_file: str, loaded_at: str
) -> dict | None:
    """Map one exploded Bronze row to a Silver row.

    ALL Bronze columns are preserved (pass-through). Computed columns
    are added/overwritten on top.

    Returns None if the row is invalid (missing RecordId).
    """
    record_id = raw_row.get("RecordId", "").strip()
    if not record_id:
        return None

    # Full copy of every Bronze column
    silver_row: dict = {
        k: (v.strip() if isinstance(v, str) else v)
        for k, v in raw_row.items()
    }

    # Computed / overwritten columns
    creation_time_raw = raw_row.get("CreationTime", "").strip()
    dt = parse_creation_time(creation_time_raw)
    silver_row["UsageDate"] = dt.strftime("%Y-%m-%d") if dt else ""
    silver_row["UsageHour"] = str(dt.hour) if dt else ""

    agent_id = raw_row.get("AgentId", "").strip()
    is_prompt_raw = raw_row.get("Message_isPrompt", "").strip()

    silver_row["PromptType"] = compute_prompt_type(is_prompt_raw)
    silver_row["IsAgent"] = compute_is_agent(agent_id)

    # Cast numeric columns
    for col in PURVIEW_INT_COLUMNS:
        if col in silver_row:
            silver_row[col] = safe_int(silver_row[col])

    # Metadata columns
    silver_row["_SourceFile"] = source_file
    silver_row["_LoadedAtUtc"] = loaded_at

    return silver_row


# ═══════════════════════════════════════════════════════════════════════════════
# Batch Processing Helpers
# ═══════════════════════════════════════════════════════════════════════════════


def build_silver_columns(
    bronze_columns: list[str],
) -> list[str]:
    """Build the dynamic Silver column list.

    Order: all Bronze columns first, then computed columns not already present,
    then Entra enrichment columns not already present.
    """
    silver_columns = list(bronze_columns)
    for col in PURVIEW_COMPUTED_COLUMNS + ENTRA_ENRICHMENT_COLUMNS:
        if col not in silver_columns:
            silver_columns.append(col)
    return silver_columns


def transform_batch(
    rows: list[dict],
    source_file: str,
    loaded_at: str,
    entra_lookup: dict[str, dict],
    existing_keys: set[tuple],
) -> tuple[list[dict], int, int, int]:
    """Transform a batch of Bronze rows to Silver.

    Returns:
        (new_rows, skipped_no_id, skipped_dedup, error_count)
    """
    new_rows: list[dict] = []
    skipped_no_id = 0
    skipped_dedup = 0
    error_count = 0

    for raw_row in rows:
        try:
            silver_row = transform_row(raw_row, source_file, loaded_at)
            if silver_row is None:
                skipped_no_id += 1
                continue

            dedup_key = make_dedup_key(silver_row)
            if dedup_key in existing_keys:
                skipped_dedup += 1
                continue

            enrich_row_with_entra(silver_row, entra_lookup)
            existing_keys.add(dedup_key)
            new_rows.append(silver_row)
        except Exception as exc:
            error_count += 1
            if error_count <= 5:
                logger.warning("Row transform error: %s", exc)

    return new_rows, skipped_no_id, skipped_dedup, error_count


def load_dedup_keys_from_csv(csv_text: str) -> set[tuple]:
    """Load dedup keys from an existing Silver CSV.

    Only loads RecordId + Message_Id columns to minimize memory usage.
    For 5M records, this uses ~50 MB vs ~5 GB for full CSV.
    """
    keys: set[tuple] = set()
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        key = make_dedup_key(row)
        keys.add(key)
    return keys


def rows_to_csv_string(
    rows: list[dict],
    columns: list[str],
    *,
    include_header: bool = True,
) -> str:
    """Serialize rows to a CSV string."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=columns,
        lineterminator="\n",
        extrasaction="ignore",
    )
    if include_header:
        writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()
