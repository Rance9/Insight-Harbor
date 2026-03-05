"""
Insight Harbor — Plan Partitions Activity
==========================================
Generates time-window partitions for the pipeline run.
Checks for incomplete previous runs and resumes if needed.

Durable Functions activity: stateless, deterministic.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pipeline.shared.adls_client import ADLSClient
from pipeline.shared.config import config
from pipeline.shared.models import Partition, PartitionState, RunState
from pipeline.shared.partitioning import compute_date_range, generate_partitions

logger = logging.getLogger("ih.activity.plan_partitions")


def plan_partitions(input_data: dict) -> dict:
    """Plan partitions for the pipeline run.

    Input:
        {
            "start_date": "2026-03-04T00:00:00+00:00" (optional),
            "end_date": "2026-03-05T00:00:00+00:00" (optional),
            "lookback_days": 1 (optional),
            "partition_hours": 6 (optional),
        }

    Returns:
        {
            "partitions": [...],
            "date_range_start": "...",
            "date_range_end": "...",
            "is_resume": false,
            "previous_run_id": null,
        }
    """
    start, end = compute_date_range(
        lookback_days=input_data.get("lookback_days"),
        start_date=input_data.get("start_date"),
        end_date=input_data.get("end_date"),
    )

    # Check for incomplete previous run
    adls = ADLSClient()
    date_key = start.strftime("%Y%m%d")
    previous_state = adls.load_run_state(date_key)
    adls.close()

    if previous_state and previous_state.get("status") == "in_progress":
        # Resume: only return partitions that are not yet completed
        run_state = RunState(**previous_state)
        incomplete = [
            p for p in run_state.partitions
            if p.status not in ("completed",)
        ]

        if incomplete:
            logger.info(
                "Resuming run %s — %d/%d partitions remaining",
                run_state.run_id,
                len(incomplete),
                len(run_state.partitions),
            )

            partitions = [
                Partition(
                    id=p.id,
                    start=p.start,
                    end=p.end,
                    activity_types=config.ACTIVITY_TYPES,
                    date_prefix=datetime.fromisoformat(p.start).strftime("%Y/%m/%d"),
                ).model_dump()
                for p in incomplete
            ]

            return {
                "partitions": partitions,
                "date_range_start": run_state.date_range_start,
                "date_range_end": run_state.date_range_end,
                "is_resume": True,
                "previous_run_id": run_state.run_id,
            }

    # Fresh run: generate partitions
    partition_hours = input_data.get("partition_hours", config.PARTITION_HOURS)
    partitions = generate_partitions(
        start, end, partition_hours=partition_hours
    )

    return {
        "partitions": [p.model_dump() for p in partitions],
        "date_range_start": start.isoformat(),
        "date_range_end": end.isoformat(),
        "is_resume": False,
        "previous_run_id": None,
    }
