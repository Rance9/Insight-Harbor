"""
Insight Harbor — Process Partition Sub-Orchestrator
=====================================================
Handles the full lifecycle of a single time-window partition:
    CREATE query → POLL until complete → SUBDIVISION check → FETCH records → CLEANUP

Implements the 3-phase Graph API audit query lifecycle from the PAX script,
adapted for Durable Functions with deterministic replay safety.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import azure.durable_functions as df

logger = logging.getLogger("ih.orchestrator.process_partition")


def process_partition(ctx: df.DurableOrchestrationContext):
    """Sub-orchestrator: process one time partition end-to-end.

    Input (dict):
        {
            "id": 1,
            "start": "2026-03-04T00:00:00Z",
            "end": "2026-03-04T06:00:00Z",
            "activity_types": ["CopilotInteraction"],
            "record_types": null,
            "service_filter": null,
            "date_prefix": "2026/03/04",
            "run_id": "run_20260304_020000",
            "sequential_mode": false,
        }

    Returns (dict):
        {
            "status": "completed" | "failed",
            "partition_id": 1,
            "blob_path": "bronze/purview/2026/03/04/P001.jsonl",
            "records": 4500,
            "error": null,
            "sub_results": null,
        }
    """
    partition = ctx.get_input()
    partition_id = partition["id"]
    run_id = partition.get("run_id", ctx.instance_id)

    # ── Retry options for activity calls ──────────────────────────────────
    retry_opts = df.RetryOptions(
        first_retry_interval_in_milliseconds=5_000,      # 5s first retry
        max_number_of_attempts=3,
    )
    # For fetch (longer retry)
    fetch_retry = df.RetryOptions(
        first_retry_interval_in_milliseconds=10_000,     # 10s first retry
        max_number_of_attempts=5,
    )

    try:
        # ── PHASE 1: Create audit query ───────────────────────────────────
        query_result = yield ctx.call_activity_with_retry(
            "create_query",
            retry_opts,
            {
                "start_time": partition["start"],
                "end_time": partition["end"],
                "activity_types": partition.get("activity_types", []),
                "record_types": partition.get("record_types"),
                "service_filter": partition.get("service_filter"),
                "partition_id": partition_id,
            },
        )

        query_id = query_result["query_id"]

        # ── PHASE 2: Poll until succeeded or failed ──────────────────────
        # PAX uses randomized 30-90s intervals, but orchestrators must be
        # deterministic. We use a fixed 60s interval (safe for replay).
        max_poll_attempts = 120  # ~2 hours at 60s
        poll_interval_seconds = 60
        poll_status = None

        for attempt in range(max_poll_attempts):
            # Durable timer — suspends orchestrator, no billing
            fire_at = ctx.current_utc_datetime + timedelta(
                seconds=poll_interval_seconds
            )
            yield ctx.create_timer(fire_at)

            # Check query status
            poll_result = yield ctx.call_activity(
                "poll_query",
                {"query_id": query_id},
            )

            poll_status = poll_result["status"]

            if poll_status == "succeeded":
                record_count = poll_result.get("record_count", 0)

                # ── SUBDIVISION CHECK ─────────────────────────────────
                subdivision = yield ctx.call_activity(
                    "check_subdivision",
                    {
                        "start": partition["start"],
                        "end": partition["end"],
                        "record_count": record_count,
                        "partition_id": partition_id,
                    },
                )

                if subdivision["should_subdivide"]:
                    # Clean up the oversized query first
                    yield ctx.call_activity(
                        "cleanup_queries",
                        {"query_id": query_id},
                    )

                    # Recursively process sub-partitions
                    sub_tasks = []
                    for sub_part in subdivision["sub_partitions"]:
                        sub_part["run_id"] = run_id
                        sub_part["activity_types"] = partition.get(
                            "activity_types", []
                        )
                        sub_part["record_types"] = partition.get("record_types")
                        sub_part["service_filter"] = partition.get(
                            "service_filter"
                        )
                        sub_tasks.append(
                            ctx.call_sub_orchestrator(
                                "process_partition", sub_part
                            )
                        )

                    sub_results = yield ctx.task_all(sub_tasks)

                    return {
                        "status": "completed",
                        "partition_id": partition_id,
                        "blob_path": "",
                        "records": sum(
                            r.get("records", 0) for r in sub_results
                        ),
                        "error": None,
                        "sub_results": sub_results,
                    }

                # Normal volume — proceed to fetch
                break

            elif poll_status == "failed":
                raise RuntimeError(
                    f"Query {query_id} failed on server side"
                )

        else:
            # Exhausted poll attempts
            yield ctx.call_activity(
                "cleanup_queries", {"query_id": query_id}
            )
            return {
                "status": "failed",
                "partition_id": partition_id,
                "blob_path": "",
                "records": 0,
                "error": f"Query {query_id} timed out after {max_poll_attempts} polls",
                "sub_results": None,
            }

        # ── PHASE 3: Fetch records to ADLS ───────────────────────────────
        fetch_result = yield ctx.call_activity_with_retry(
            "fetch_records",
            fetch_retry,
            {
                "query_id": query_id,
                "partition_id": partition_id,
                "date_prefix": partition.get("date_prefix", ""),
            },
        )

        blob_path = fetch_result["blob_path"]
        records_written = fetch_result["records_written"]

        # ── CLEANUP: Delete query to free slot ────────────────────────────
        yield ctx.call_activity(
            "cleanup_queries",
            {"query_id": query_id},
        )

        # ── Update run state ──────────────────────────────────────────────
        yield ctx.call_activity(
            "update_run_state",
            {
                "run_id": run_id,
                "partition_id": partition_id,
                "status": "completed",
                "records": records_written,
                "bronze_blob": blob_path,
                "partition_start": partition["start"],
                "partition_end": partition["end"],
                "date_range_start": partition.get("start", ""),
                "date_range_end": partition.get("end", ""),
            },
        )

        return {
            "status": "completed",
            "partition_id": partition_id,
            "blob_path": blob_path,
            "records": records_written,
            "error": None,
            "sub_results": None,
        }

    except Exception as exc:
        # Attempt cleanup on failure
        try:
            if "query_id" in dir():
                yield ctx.call_activity(
                    "cleanup_queries", {"query_id": query_id}
                )
        except Exception:
            pass  # Best-effort cleanup

        # Update run state with failure
        try:
            yield ctx.call_activity(
                "update_run_state",
                {
                    "run_id": run_id,
                    "partition_id": partition_id,
                    "status": "failed",
                    "records": 0,
                    "error": str(exc),
                    "date_range_start": partition.get("start", ""),
                    "date_range_end": partition.get("end", ""),
                },
            )
        except Exception:
            pass  # Best-effort state update

        return {
            "status": "failed",
            "partition_id": partition_id,
            "blob_path": "",
            "records": 0,
            "error": str(exc),
            "sub_results": None,
        }
