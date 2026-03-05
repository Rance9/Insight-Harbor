"""
Insight Harbor — Pipeline Function App Entry Point
=====================================================
Azure Durable Functions v2 Python programming model.

Registers:
  • Timer trigger   — daily CRON schedule (default 2:00 AM UTC)
  • HTTP trigger    — manual run endpoint (POST /api/run-pipeline)
  • HTTP trigger    — status check endpoint (GET /api/pipeline-status/{id})
  • Orchestrators   — pipeline_orchestrator, process_partition
  • Activities      — all 11 activity functions
"""

from __future__ import annotations

import json
import logging

import azure.durable_functions as df
import azure.functions as func

from pipeline.shared.config import config

# ═══════════════════════════════════════════════════════════════════════════════
# App Initialization
# ═══════════════════════════════════════════════════════════════════════════════

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

logger = logging.getLogger("ih.pipeline")


# ═══════════════════════════════════════════════════════════════════════════════
# Triggers — Timer (scheduled) + HTTP (manual)
# ═══════════════════════════════════════════════════════════════════════════════


@app.schedule(
    schedule=config.SCHEDULE_CRON,
    arg_name="timer",
    run_on_startup=False,
)
@app.durable_client_input(client_name="client")
async def scheduled_pipeline(timer: func.TimerRequest, client):
    """Daily scheduled pipeline trigger (default: 2:00 AM UTC)."""
    if timer.past_due:
        logger.warning("Timer trigger is past due — running anyway")

    instance_id = await client.start_new("pipeline_orchestrator", None, None)
    logger.info("Scheduled pipeline started: %s", instance_id)


@app.route(route="run-pipeline", methods=["POST"])
@app.durable_client_input(client_name="client")
async def manual_pipeline(req: func.HttpRequest, client):
    """Manual HTTP trigger for on-demand pipeline runs.

    POST /api/run-pipeline
    Body (optional JSON):
        {
            "start_date": "2026-03-04T00:00:00Z",
            "end_date": "2026-03-04T23:59:59Z",
            "overwrite": false
        }
    """
    try:
        body = req.get_json() if req.get_body() else {}
    except ValueError:
        body = {}

    instance_id = await client.start_new(
        "pipeline_orchestrator", None, body
    )

    logger.info("Manual pipeline started: %s (input: %s)", instance_id, body)
    return client.create_check_status_response(req, instance_id)


@app.route(route="pipeline-status/{instance_id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def pipeline_status(req: func.HttpRequest, client):
    """Check pipeline run status.

    GET /api/pipeline-status/{instance_id}
    """
    instance_id = req.route_params.get("instance_id", "")
    if not instance_id:
        return func.HttpResponse(
            json.dumps({"error": "instance_id is required"}),
            status_code=400,
            mimetype="application/json",
        )

    status = await client.get_status(instance_id)
    if not status:
        return func.HttpResponse(
            json.dumps({"error": f"Instance {instance_id} not found"}),
            status_code=404,
            mimetype="application/json",
        )

    return func.HttpResponse(
        json.dumps(
            {
                "instance_id": status.instance_id,
                "runtime_status": status.runtime_status.value
                if status.runtime_status
                else None,
                "output": status.output,
                "created_time": str(status.created_time)
                if status.created_time
                else None,
                "last_updated_time": str(status.last_updated_time)
                if status.last_updated_time
                else None,
            }
        ),
        status_code=200,
        mimetype="application/json",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Orchestrators
# ═══════════════════════════════════════════════════════════════════════════════


@app.orchestration_trigger(context_name="context")
def pipeline_orchestrator(context: df.DurableOrchestrationContext):
    """Main pipeline orchestrator — registered with Durable Functions."""
    from pipeline.orchestrators.pipeline_orchestrator import (
        pipeline_orchestrator as _impl,
    )

    return _impl(context)


@app.orchestration_trigger(context_name="context")
def process_partition(context: df.DurableOrchestrationContext):
    """Sub-orchestrator for single partition processing."""
    from pipeline.orchestrators.process_partition import (
        process_partition as _impl,
    )

    return _impl(context)


# ═══════════════════════════════════════════════════════════════════════════════
# Activity Functions
# ═══════════════════════════════════════════════════════════════════════════════


@app.activity_trigger(input_name="inputData")
def plan_partitions(inputData: dict) -> dict:
    """Plan time partitions for the pipeline run."""
    from pipeline.activities.plan_partitions import plan_partitions as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def create_query(inputData: dict) -> dict:
    """Create a Graph API audit query."""
    from pipeline.activities.create_query import create_query as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def poll_query(inputData: dict) -> dict:
    """Poll a Graph API audit query status."""
    from pipeline.activities.poll_query import poll_query as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def fetch_records(inputData: dict) -> dict:
    """Fetch records from a completed query and stream to ADLS."""
    from pipeline.activities.fetch_records import fetch_records as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def check_subdivision(inputData: dict) -> dict:
    """Check if a partition needs subdivision based on record count."""
    from pipeline.activities.check_subdivision import check_subdivision as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def cleanup_queries(inputData: dict) -> dict:
    """Delete a completed audit query to free the slot."""
    from pipeline.activities.cleanup_queries import cleanup_queries as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def explode_partition(inputData: dict) -> dict:
    """Explode a raw JSONL partition into 153-column CSV."""
    from pipeline.activities.explode_partition import explode_partition as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def pull_entra(inputData: dict) -> dict:
    """Fetch Entra users from Graph API, transform, and store."""
    from pipeline.activities.pull_entra import pull_entra as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def transform_silver(inputData: dict) -> dict:
    """Bronze-to-Silver transform with Entra enrichment and dedup."""
    from pipeline.activities.transform_silver import transform_silver as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def notify_completion(inputData: dict) -> dict:
    """Send Teams webhook notification."""
    from pipeline.activities.notify import notify_completion as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def update_run_state(inputData: dict) -> dict:
    """Update run state in ADLS."""
    from pipeline.activities.run_state import update_run_state as _impl

    return _impl(inputData)


@app.activity_trigger(input_name="inputData")
def finalize_run_state(inputData: dict) -> dict:
    """Finalize run state and write metadata."""
    from pipeline.activities.run_state import finalize_run_state as _impl

    return _impl(inputData)
