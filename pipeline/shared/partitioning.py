"""
Insight Harbor — Time Window Partitioning
==========================================
Generates time-window partitions for Purview audit log queries.
Ports PAX's partition generation and subdivision logic.

Matches PAX behavior:
  • Splits date range into blocks of configurable hours (default 6h)
  • Exclusive end-time semantics
  • Minimum 2-minute granularity for subdivision
  • Halving strategy for oversized partitions
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from .config import config
from .models import Partition

logger = logging.getLogger("ih.partitioning")


def generate_partitions(
    start: datetime,
    end: datetime,
    *,
    partition_hours: int | None = None,
    activity_types: list[str] | None = None,
) -> list[Partition]:
    """Generate time-window partitions from start to end.

    Args:
        start: Start of the date range (inclusive).
        end: End of the date range (exclusive).
        partition_hours: Hours per partition (default: config.PARTITION_HOURS).
        activity_types: Activity types to include in each partition.

    Returns:
        List of Partition objects with sequential IDs.
    """
    hours = partition_hours or config.PARTITION_HOURS
    activities = activity_types or config.ACTIVITY_TYPES
    delta = timedelta(hours=hours)

    partitions: list[Partition] = []
    current = start
    partition_id = 1

    while current < end:
        partition_end = min(current + delta, end)

        partitions.append(
            Partition(
                id=partition_id,
                start=current.isoformat(),
                end=partition_end.isoformat(),
                activity_types=list(activities),
                date_prefix=current.strftime("%Y/%m/%d"),
            )
        )

        current = partition_end
        partition_id += 1

    logger.info(
        "Generated %d partitions (%dh each) for %s → %s",
        len(partitions),
        hours,
        start.isoformat(),
        end.isoformat(),
    )

    # Warn on extreme ranges (matching PAX behavior)
    total_days = (end - start).days
    if total_days > 30:
        logger.warning(
            "Large date range: %d days. Consider reducing lookback to avoid "
            "throttling. Partition count: %d",
            total_days,
            len(partitions),
        )

    return partitions


def subdivide_partition(
    partition: Partition,
    *,
    factor: int = 2,
    min_minutes: int = 2,
) -> list[Partition]:
    """Subdivide an oversized partition into smaller windows.

    Uses halving strategy matching PAX's recursive subdivision.
    Minimum granularity: 2 minutes (same as PAX).

    Args:
        partition: The partition to subdivide.
        factor: Number of sub-partitions to create (default: halving).
        min_minutes: Minimum partition width in minutes.

    Returns:
        List of sub-partitions. Returns [original] if can't subdivide further.
    """
    start = datetime.fromisoformat(partition.start)
    end = datetime.fromisoformat(partition.end)
    total_seconds = (end - start).total_seconds()
    sub_seconds = total_seconds / factor

    # Check minimum granularity
    if sub_seconds < min_minutes * 60:
        logger.warning(
            "Cannot subdivide partition %d further — "
            "sub-window (%.0fs) would be below %d-minute minimum",
            partition.id,
            sub_seconds,
            min_minutes,
        )
        return [partition]

    sub_partitions: list[Partition] = []
    current = start
    base_id = partition.id * 100  # e.g., partition 3 → sub-partitions 301, 302

    for i in range(factor):
        sub_end = current + timedelta(seconds=sub_seconds)
        if i == factor - 1:
            sub_end = end  # Ensure last sub-partition aligns to original end

        sub_partitions.append(
            Partition(
                id=base_id + i + 1,
                start=current.isoformat(),
                end=sub_end.isoformat(),
                activity_types=list(partition.activity_types),
                record_types=partition.record_types,
                service_filter=partition.service_filter,
                date_prefix=current.strftime("%Y/%m/%d"),
            )
        )
        current = sub_end

    logger.info(
        "Subdivided partition %d into %d sub-partitions (%.0fs each)",
        partition.id,
        len(sub_partitions),
        sub_seconds,
    )

    return sub_partitions


def compute_date_range(
    lookback_days: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[datetime, datetime]:
    """Compute the effective date range for the pipeline run.

    Priority:
      1. Explicit start_date / end_date if provided
      2. Default lookback from config.DEFAULT_LOOKBACK_DAYS

    Returns:
        (start, end) as timezone-aware UTC datetimes.
    """
    now = datetime.now(timezone.utc)

    if start_date and end_date:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
    elif start_date:
        start = datetime.fromisoformat(start_date)
        end = now
    else:
        days = lookback_days or config.DEFAULT_LOOKBACK_DAYS
        start = now - timedelta(days=days)
        end = now

    # Ensure timezone-aware
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    return start, end


def should_subdivide(record_count: int) -> bool:
    """Check if a partition's record count exceeds the subdivision threshold."""
    return record_count >= config.effective_subdivision_threshold
