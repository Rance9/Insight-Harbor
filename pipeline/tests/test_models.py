"""
Tests for pipeline.shared.models
"""

import pytest

from pipeline.shared.models import (
    NotificationPayload,
    Partition,
    PartitionResult,
    PartitionState,
    PartitionStatus,
    QueryStatus,
    RunMetadata,
    RunState,
    RunStatus,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Enum Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestEnums:
    def test_partition_status_values(self):
        assert PartitionStatus.COMPLETED == "completed"
        assert PartitionStatus.SUBDIVIDED == "subdivided"

    def test_query_status_values(self):
        assert QueryStatus.SUCCEEDED == "succeeded"
        assert QueryStatus.RUNNING == "running"

    def test_run_status_values(self):
        assert RunStatus.COMPLETED == "completed"
        assert RunStatus.PARTIAL_SUCCESS == "partial_success"


# ═══════════════════════════════════════════════════════════════════════════════
# Partition Model
# ═══════════════════════════════════════════════════════════════════════════════


class TestPartition:
    def test_minimal_partition(self):
        p = Partition(
            id=1,
            start="2026-03-04T00:00:00Z",
            end="2026-03-04T06:00:00Z",
        )
        assert p.status == PartitionStatus.PENDING
        assert p.activity_types == []

    def test_full_partition(self):
        p = Partition(
            id=2,
            start="2026-03-04T00:00:00Z",
            end="2026-03-04T06:00:00Z",
            activity_types=["CopilotInteraction"],
            date_prefix="2026/03/04",
        )
        assert p.activity_types == ["CopilotInteraction"]

    def test_serialization_roundtrip(self):
        p = Partition(
            id=1,
            start="2026-03-04T00:00:00Z",
            end="2026-03-04T06:00:00Z",
        )
        data = p.model_dump()
        p2 = Partition(**data)
        assert p2.id == p.id
        assert p2.start == p.start


# ═══════════════════════════════════════════════════════════════════════════════
# PartitionResult
# ═══════════════════════════════════════════════════════════════════════════════


class TestPartitionResult:
    def test_completed_result(self):
        r = PartitionResult(
            partition_id=1,
            status=PartitionStatus.COMPLETED,
            blob_path="bronze/purview/2026/03/04/P001.jsonl",
            records_written=5000,
        )
        assert r.error is None
        assert r.sub_results is None

    def test_failed_result(self):
        r = PartitionResult(
            partition_id=2,
            status=PartitionStatus.FAILED,
            error="Timeout",
        )
        assert r.records_written == 0

    def test_subdivided_result(self):
        sub1 = PartitionResult(partition_id=101, status=PartitionStatus.COMPLETED, records_written=500)
        sub2 = PartitionResult(partition_id=102, status=PartitionStatus.COMPLETED, records_written=600)
        r = PartitionResult(
            partition_id=1,
            status=PartitionStatus.COMPLETED,
            sub_results=[sub1, sub2],
        )
        assert len(r.sub_results) == 2


# ═══════════════════════════════════════════════════════════════════════════════
# RunState
# ═══════════════════════════════════════════════════════════════════════════════


class TestRunState:
    def test_initial_state(self):
        state = RunState(
            run_id="run_20260304_020000",
            started_at="2026-03-04T02:00:00Z",
        )
        assert state.status == RunStatus.IN_PROGRESS
        assert len(state.partitions) == 0

    def test_with_partitions(self):
        ps = PartitionState(id=1, start="a", end="b", status=PartitionStatus.COMPLETED)
        state = RunState(
            run_id="run_test",
            started_at="now",
            partitions=[ps],
        )
        assert len(state.partitions) == 1
        assert state.partitions[0].status == PartitionStatus.COMPLETED


# ═══════════════════════════════════════════════════════════════════════════════
# NotificationPayload
# ═══════════════════════════════════════════════════════════════════════════════


class TestNotificationPayload:
    def test_minimal(self):
        p = NotificationPayload(run_id="r1", status="completed")
        assert p.partitions_processed == 0
        assert p.errors == []

    def test_with_errors(self):
        p = NotificationPayload(
            run_id="r2",
            status="partial_success",
            errors=["Partition 3 failed"],
        )
        assert len(p.errors) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# RunMetadata
# ═══════════════════════════════════════════════════════════════════════════════


class TestRunMetadata:
    def test_defaults(self):
        m = RunMetadata()
        assert m.version == "1.0"
        assert m.exit_code == 0
        assert m.partitions_total == 0
