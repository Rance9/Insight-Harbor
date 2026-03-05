"""
Insight Harbor — Run State Activity
=====================================
Manages pipeline run state in ADLS for cross-run resume capability.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pipeline.shared.adls_client import ADLSClient
from pipeline.shared.config import config
from pipeline.shared.models import PartitionState, RunMetadata, RunState, RunStatus

logger = logging.getLogger("ih.activity.run_state")


def update_run_state(input_data: dict) -> dict:
    """Update the run state file in ADLS after a partition completes.

    Input:
        {
            "run_id": "run_20260304_020000",
            "date_range_start": "...",
            "date_range_end": "...",
            "partition_id": 1,
            "status": "completed",
            "records": 4500,
            "bronze_blob": "bronze/purview/2026/03/04/P001.jsonl",
            "exploded_blob": "bronze/exploded/2026/03/04/P001_exploded.csv",
            "error": null,
        }

    Returns:
        {"run_id": "...", "state_path": "pipeline/state/run_20260304_020000.json"}
    """
    run_id = input_data["run_id"]

    adls = ADLSClient()

    # Load existing state or create new
    existing = adls.load_run_state(run_id.replace("run_", ""))
    if existing:
        state = RunState(**existing)
    else:
        state = RunState(
            run_id=run_id,
            started_at=datetime.now(timezone.utc).isoformat(),
            date_range_start=input_data.get("date_range_start", ""),
            date_range_end=input_data.get("date_range_end", ""),
        )

    # Update or add partition state
    partition_id = input_data["partition_id"]
    partition_state = PartitionState(
        id=partition_id,
        start=input_data.get("partition_start", ""),
        end=input_data.get("partition_end", ""),
        status=input_data["status"],
        records=input_data.get("records", 0),
        bronze_blob=input_data.get("bronze_blob", ""),
        exploded_blob=input_data.get("exploded_blob", ""),
        error=input_data.get("error"),
    )

    # Replace existing partition or append
    found = False
    for i, p in enumerate(state.partitions):
        if p.id == partition_id:
            state.partitions[i] = partition_state
            found = True
            break
    if not found:
        state.partitions.append(partition_state)

    # Save to ADLS
    state_path = adls.save_run_state(run_id, state.model_dump())
    adls.close()

    logger.info(
        "Updated run state: partition %d → %s (%s)",
        partition_id, input_data["status"], state_path,
    )

    return {"run_id": run_id, "state_path": state_path}


def finalize_run_state(input_data: dict) -> dict:
    """Finalize the run state and write run metadata to ADLS.

    Input:
        {
            "run_id": "run_20260304_020000",
            "status": "completed",
            "partitions_total": 16,
            "partitions_completed": 16,
            "partitions_failed": 0,
            "total_records_ingested": 12000,
            "total_records_exploded": 36000,
            "total_records_silver": 11800,
            "activity_types": ["CopilotInteraction"],
            "date_range_start": "...",
            "date_range_end": "...",
            "started_at": "...",
        }

    Returns:
        {"run_id": "...", "metadata_path": "pipeline/history/..."}
    """
    run_id = input_data["run_id"]
    now = datetime.now(timezone.utc)

    adls = ADLSClient()

    # Update run state to final status
    existing = adls.load_run_state(run_id.replace("run_", ""))
    if existing:
        state = RunState(**existing)
        state.status = RunStatus(input_data["status"])
        state.completed_at = now.isoformat()
        state.silver_status = "completed"
        adls.save_run_state(run_id, state.model_dump())

    # Write run metadata for analytics
    from pipeline.shared.constants import EXIT_CODE_MAP
    metadata = RunMetadata(
        run_id=run_id,
        started_at=input_data.get("started_at", ""),
        completed_at=now.isoformat(),
        partitions_total=input_data.get("partitions_total", 0),
        partitions_completed=input_data.get("partitions_completed", 0),
        partitions_failed=input_data.get("partitions_failed", 0),
        total_records_ingested=input_data.get("total_records_ingested", 0),
        total_records_exploded=input_data.get("total_records_exploded", 0),
        total_records_silver=input_data.get("total_records_silver", 0),
        activity_types=input_data.get("activity_types", []),
        date_range_start=input_data.get("date_range_start", ""),
        date_range_end=input_data.get("date_range_end", ""),
        exit_code=EXIT_CODE_MAP.get(input_data["status"], 99),
    )

    metadata_path = adls.save_run_metadata(run_id, metadata.model_dump())
    adls.close()

    logger.info(
        "Finalized run %s — status: %s, metadata: %s",
        run_id, input_data["status"], metadata_path,
    )

    return {"run_id": run_id, "metadata_path": metadata_path}
