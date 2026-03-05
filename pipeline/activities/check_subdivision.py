"""
Insight Harbor — Check Subdivision Activity
=============================================
Evaluates whether a partition's record count exceeds the subdivision
threshold and generates sub-partitions if needed.

Matches PAX's subdivision behavior:
  • Threshold: 950,000 records (normal) or 10,000 (AutoCompleteness)
  • Halving strategy with 2-minute minimum granularity
"""

from __future__ import annotations

import logging

from pipeline.shared.config import config
from pipeline.shared.models import Partition
from pipeline.shared.partitioning import should_subdivide, subdivide_partition

logger = logging.getLogger("ih.activity.check_subdivision")


def check_subdivision(input_data: dict) -> dict:
    """Check if a partition needs subdivision based on record count.

    Input:
        {
            "partition": {...},  # Partition model dict
            "record_count": 960000,
        }

    Returns:
        {
            "should_subdivide": true,
            "sub_partitions": [...],
            "original_partition_id": 1,
        }
    """
    record_count = input_data["record_count"]
    partition = Partition(**input_data["partition"])

    needs_split = should_subdivide(record_count)

    if not needs_split:
        return {
            "should_subdivide": False,
            "sub_partitions": [],
            "original_partition_id": partition.id,
        }

    logger.warning(
        "Partition %d has %d records (threshold: %d) — subdividing",
        partition.id,
        record_count,
        config.effective_subdivision_threshold,
    )

    sub_partitions = subdivide_partition(partition)

    return {
        "should_subdivide": True,
        "sub_partitions": [sp.model_dump() for sp in sub_partitions],
        "original_partition_id": partition.id,
    }
