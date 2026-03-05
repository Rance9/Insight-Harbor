"""
Insight Harbor — Pipeline Orchestrator (Main)
===============================================
Replaces run-pipeline-local.ps1 + PAX orchestration.

Phases:
  0. Resume check — pick up incomplete runs from ADLS state
  1. Plan partitions — divide date range into time windows
  2. Ingest — batched fan-out of process_partition sub-orchestrators
  2b. Sequential retry — failed partitions retried one-at-a-time
  3. Explode — fan-out explosion of bronze JSONL → 153-column CSV
  4. Entra pull — fetch user directory data (parallel with Phase 3)
  5. Silver transform — bronze → silver with Entra enrichment + dedup
  6. Finalize — persist run metadata + Teams notification
"""

from __future__ import annotations

import logging
from datetime import timedelta

import azure.durable_functions as df

logger = logging.getLogger("ih.orchestrator.pipeline")


def _chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def pipeline_orchestrator(ctx: df.DurableOrchestrationContext):
    """Main pipeline orchestrator — daily audit log ingestion pipeline.

    Input (dict, optional):
        {
            "start_date": "2026-03-04T00:00:00Z"  (optional),
            "end_date": "2026-03-04T23:59:59Z"     (optional),
            "overwrite": false                     (optional),
        }

    Returns:
        {
            "status": "completed" | "partial_success" | "failed",
            "run_id": "...",
            "partitions_total": 16,
            "partitions_completed": 16,
            "partitions_failed": 0,
            "total_records_ingested": 50000,
            "total_records_exploded": 150000,
            "total_records_silver": 49800,
        }
    """
    run_id = ctx.instance_id
    input_data = ctx.get_input() or {}
    start_time = ctx.current_utc_datetime
    overwrite = input_data.get("overwrite", False)

    # ── PHASE 0 + 1: Plan Partitions (with resume check) ─────────────────
    plan_result = yield ctx.call_activity(
        "plan_partitions",
        {
            "start_date": input_data.get("start_date"),
            "end_date": input_data.get("end_date"),
            "run_id": run_id,
        },
    )

    partitions = plan_result["partitions"]
    date_range_start = plan_result["date_range_start"]
    date_range_end = plan_result["date_range_end"]
    is_resume = plan_result.get("is_resume", False)

    if not partitions:
        return {
            "status": "completed",
            "run_id": run_id,
            "partitions_total": 0,
            "partitions_completed": 0,
            "partitions_failed": 0,
            "total_records_ingested": 0,
            "total_records_exploded": 0,
            "total_records_silver": 0,
        }

    # ── PHASE 2: Ingest Audit Logs (batched fan-out/fan-in) ──────────────
    batch_size = 4  # matches host.json maxConcurrentActivityFunctions
    all_completed: list[dict] = []
    all_failed: list[dict] = []

    for batch_idx, batch in enumerate(_chunks(partitions, batch_size)):
        # Stagger batch launches to avoid thundering herd
        if batch_idx > 0:
            stagger_at = ctx.current_utc_datetime + timedelta(seconds=15)
            yield ctx.create_timer(stagger_at)

        # Fan out sub-orchestrators for this batch
        tasks = []
        for i, partition in enumerate(batch):
            part_input = {
                **partition,
                "run_id": run_id,
            }

            # Stagger individual partitions within a batch (5s apart)
            if i > 0:
                inner_stagger = ctx.current_utc_datetime + timedelta(
                    seconds=5 * i
                )
                yield ctx.create_timer(inner_stagger)

            tasks.append(
                ctx.call_sub_orchestrator("process_partition", part_input)
            )

        # Wait for entire batch
        results = yield ctx.task_all(tasks)

        for result in results:
            if result.get("status") == "completed":
                all_completed.append(result)
            else:
                all_failed.append(result)

    # ── PHASE 2b: Sequential Retry for Failed Partitions ─────────────────
    retry_completed: list[dict] = []
    permanently_failed: list[dict] = []

    for failed in all_failed:
        # Find the original partition definition
        orig_partition = None
        for p in partitions:
            if p["id"] == failed.get("partition_id"):
                orig_partition = p
                break

        if not orig_partition:
            permanently_failed.append(failed)
            continue

        try:
            retry_result = yield ctx.call_sub_orchestrator(
                "process_partition",
                {
                    **orig_partition,
                    "run_id": run_id,
                    "sequential_mode": True,
                },
            )
            if retry_result.get("status") == "completed":
                retry_completed.append(retry_result)
            else:
                permanently_failed.append(retry_result)
        except Exception:
            permanently_failed.append(failed)

    all_completed.extend(retry_completed)

    # Collect all successfully-fetched bronze paths
    all_bronze_paths = _collect_blob_paths(all_completed)
    total_records_ingested = sum(
        r.get("records", 0) for r in all_completed
    )

    # ── PHASE 3 + 4: Explode + Entra Pull (parallel) ─────────────────────
    # Build explosion tasks
    explode_tasks = []
    explode_retry = df.RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=3,
    )

    for bronze_path in all_bronze_paths:
        # Extract partition info from path for date_prefix
        # Path format: bronze/purview/YYYY/MM/DD/P001.jsonl
        parts = bronze_path.split("/")
        date_prefix = "/".join(parts[2:5]) if len(parts) >= 5 else ""
        # Extract partition ID from filename
        filename = parts[-1] if parts else ""
        pid = 0
        if filename.startswith("P") and "_" not in filename:
            try:
                pid = int(filename.replace("P", "").replace(".jsonl", ""))
            except ValueError:
                pass

        explode_tasks.append(
            ctx.call_activity_with_retry(
                "explode_partition",
                explode_retry,
                {
                    "bronze_blob_path": bronze_path,
                    "date_prefix": date_prefix,
                    "partition_id": pid,
                },
            )
        )

    # Entra pull runs in parallel with explosion
    entra_task = ctx.call_activity_with_retry(
        "pull_entra",
        explode_retry,
        {"date_prefix": date_range_start.replace("-", "/")[:10] if date_range_start else ""},
    )

    # Wait for all explosion + Entra in parallel
    all_parallel_tasks = explode_tasks + [entra_task]
    parallel_results = yield ctx.task_all(all_parallel_tasks)

    # Split results
    exploded_results = parallel_results[:-1]
    entra_result = parallel_results[-1]

    exploded_paths = [r["output_blob_path"] for r in exploded_results if r.get("output_blob_path")]
    total_records_exploded = sum(
        r.get("records_exploded", 0) for r in exploded_results
    )
    entra_silver_path = entra_result.get("silver_blob_path", "")

    # ── PHASE 5: Bronze-to-Silver Transform ──────────────────────────────
    transform_retry = df.RetryOptions(
        first_retry_interval_in_milliseconds=10_000,
        max_number_of_attempts=3,
    )

    silver_result = yield ctx.call_activity_with_retry(
        "transform_silver",
        transform_retry,
        {
            "exploded_blob_paths": exploded_paths,
            "entra_silver_path": entra_silver_path,
            "overwrite": overwrite,
        },
    )

    total_records_silver = silver_result.get("new_records", 0)

    # ── PHASE 6: Finalize ────────────────────────────────────────────────
    end_time = ctx.current_utc_datetime
    duration_minutes = (end_time - start_time).total_seconds() / 60.0

    # Determine final status
    if permanently_failed:
        if all_completed:
            final_status = "partial_success"
        else:
            final_status = "failed"
    else:
        final_status = "completed"

    # Finalize run state
    yield ctx.call_activity(
        "finalize_run_state",
        {
            "run_id": run_id,
            "status": final_status,
            "partitions_total": len(partitions),
            "partitions_completed": len(all_completed),
            "partitions_failed": len(permanently_failed),
            "total_records_ingested": total_records_ingested,
            "total_records_exploded": total_records_exploded,
            "total_records_silver": total_records_silver,
            "activity_types": partitions[0].get("activity_types", []) if partitions else [],
            "date_range_start": date_range_start,
            "date_range_end": date_range_end,
            "started_at": start_time.isoformat(),
        },
    )

    # Send Teams notification
    errors_list = [
        f"Partition {f.get('partition_id', '?')}: {f.get('error', 'unknown')}"
        for f in permanently_failed
    ]

    yield ctx.call_activity(
        "notify_completion",
        {
            "run_id": run_id,
            "status": final_status,
            "partitions_processed": len(all_completed),
            "records_ingested": total_records_ingested,
            "records_transformed": total_records_silver,
            "duration_minutes": duration_minutes,
            "errors": errors_list[:10],  # Cap at 10 errors for notification
        },
    )

    return {
        "status": final_status,
        "run_id": run_id,
        "partitions_total": len(partitions),
        "partitions_completed": len(all_completed),
        "partitions_failed": len(permanently_failed),
        "total_records_ingested": total_records_ingested,
        "total_records_exploded": total_records_exploded,
        "total_records_silver": total_records_silver,
    }


def _collect_blob_paths(results: list[dict]) -> list[str]:
    """Recursively collect blob_path from results, including sub_results."""
    paths = []
    for r in results:
        if r.get("blob_path"):
            paths.append(r["blob_path"])
        if r.get("sub_results"):
            paths.extend(_collect_blob_paths(r["sub_results"]))
    return paths
