"""
Insight Harbor — Pipeline Data Models
======================================
Pydantic models for pipeline state, partition definitions, query results,
and run metadata. Used for type-safe serialization across Durable Functions
activity boundaries.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════════════════
# Enums
# ═══════════════════════════════════════════════════════════════════════════════


class PartitionStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SUBDIVIDED = "subdivided"
    SKIPPED = "skipped"


class QueryStatus(str, Enum):
    NOT_STARTED = "notStarted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    CIRCUIT_BREAKER = "circuit_breaker"


# ═══════════════════════════════════════════════════════════════════════════════
# Partition & Query Models
# ═══════════════════════════════════════════════════════════════════════════════


class Partition(BaseModel):
    """A time window partition for audit log queries."""
    id: int
    start: str  # ISO 8601 datetime string
    end: str    # ISO 8601 datetime string
    activity_types: list[str] = Field(default_factory=list)
    record_types: Optional[list[str]] = None
    service_filter: Optional[str] = None
    date_prefix: str = ""  # e.g., "2026/03/04" for ADLS path
    estimated_size: Optional[int] = None
    status: PartitionStatus = PartitionStatus.PENDING


class QueryPass(BaseModel):
    """A single Graph API query pass (one per service type, or single pass)."""
    activities: list[str]
    record_types: Optional[list[str]] = None
    service_filter: Optional[str] = None


class QueryResult(BaseModel):
    """Result from creating a Graph API audit query."""
    query_id: str
    display_name: str
    status: QueryStatus = QueryStatus.NOT_STARTED


class PollResult(BaseModel):
    """Result from polling a Graph API query status."""
    query_id: str
    status: QueryStatus
    record_count: int = 0


class FetchResult(BaseModel):
    """Result from fetching and storing query records."""
    query_id: str
    blob_path: str
    records_written: int = 0
    pages_fetched: int = 0


class SubdivisionResult(BaseModel):
    """Result from subdividing an oversized partition."""
    original_partition_id: int
    sub_partitions: list[Partition] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════════
# Partition Processing Result
# ═══════════════════════════════════════════════════════════════════════════════


class PartitionResult(BaseModel):
    """Complete result from processing a single partition."""
    partition_id: int
    status: PartitionStatus
    blob_path: str = ""
    records_written: int = 0
    error: Optional[str] = None
    sub_results: Optional[list["PartitionResult"]] = None


# ═══════════════════════════════════════════════════════════════════════════════
# Explosion & Transform Results
# ═══════════════════════════════════════════════════════════════════════════════


class ExplosionResult(BaseModel):
    """Result from exploding a bronze JSONL partition."""
    input_blob_path: str
    output_blob_path: str
    records_exploded: int = 0


class EntraResult(BaseModel):
    """Result from pulling Entra user data."""
    blob_path: str
    users_count: int = 0


class SilverTransformResult(BaseModel):
    """Result from the Bronze-to-Silver transform."""
    output_blob_path: str
    records_transformed: int = 0
    new_records: int = 0
    duplicates_skipped: int = 0


# ═══════════════════════════════════════════════════════════════════════════════
# Run State (persisted to ADLS for cross-run resume)
# ═══════════════════════════════════════════════════════════════════════════════


class PartitionState(BaseModel):
    """State of a single partition within a run (persisted to ADLS)."""
    id: int
    start: str
    end: str
    status: PartitionStatus = PartitionStatus.PENDING
    records: int = 0
    bronze_blob: str = ""
    exploded_blob: str = ""
    error: Optional[str] = None


class RunState(BaseModel):
    """Complete run state persisted to ADLS for cross-run resume."""
    run_id: str
    started_at: str
    status: RunStatus = RunStatus.IN_PROGRESS
    date_range_start: str = ""
    date_range_end: str = ""
    partitions: list[PartitionState] = Field(default_factory=list)
    silver_status: str = "pending"
    completed_at: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════════
# Run Metadata (analytics / metrics)
# ═══════════════════════════════════════════════════════════════════════════════


class RunMetadata(BaseModel):
    """Consolidated run metrics — written to ADLS after completion."""
    version: str = "1.0"
    run_id: str = ""
    started_at: str = ""
    completed_at: str = ""
    partitions_total: int = 0
    partitions_completed: int = 0
    partitions_failed: int = 0
    total_records_ingested: int = 0
    total_records_exploded: int = 0
    total_records_silver: int = 0
    throttle_count: int = 0
    subdivision_count: int = 0
    activity_types: list[str] = Field(default_factory=list)
    date_range_start: str = ""
    date_range_end: str = ""
    exit_code: int = 0


# ═══════════════════════════════════════════════════════════════════════════════
# Notification
# ═══════════════════════════════════════════════════════════════════════════════


class NotificationPayload(BaseModel):
    """Payload for Teams webhook notification."""
    run_id: str
    status: str
    partitions_processed: int = 0
    records_ingested: int = 0
    records_transformed: int = 0
    duration_minutes: float = 0.0
    errors: list[str] = Field(default_factory=list)
