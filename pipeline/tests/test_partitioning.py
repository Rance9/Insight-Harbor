"""
Tests for pipeline.shared.partitioning
"""

from datetime import datetime, timedelta, timezone

import pytest

from pipeline.shared.models import Partition
from pipeline.shared.partitioning import (
    compute_date_range,
    generate_partitions,
    should_subdivide,
    subdivide_partition,
)


# ═══════════════════════════════════════════════════════════════════════════════
# generate_partitions
# ═══════════════════════════════════════════════════════════════════════════════


class TestGeneratePartitions:
    """Tests for generate_partitions."""

    def test_single_partition_exact_fit(self):
        """6-hour range with 6-hour window → exactly 1 partition."""
        start = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 3, 4, 6, 0, tzinfo=timezone.utc)
        parts = generate_partitions(start, end, partition_hours=6)
        assert len(parts) == 1
        assert parts[0].id == 1
        assert parts[0].start == start.isoformat()
        assert parts[0].end == end.isoformat()

    def test_four_partitions_for_24h(self):
        """24-hour range with 6-hour windows → 4 partitions."""
        start = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc)
        parts = generate_partitions(start, end, partition_hours=6)
        assert len(parts) == 4
        # IDs are sequential
        assert [p.id for p in parts] == [1, 2, 3, 4]

    def test_partial_last_partition(self):
        """7-hour range with 6-hour windows → 2 partitions, last is 1h."""
        start = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 3, 4, 7, 0, tzinfo=timezone.utc)
        parts = generate_partitions(start, end, partition_hours=6)
        assert len(parts) == 2
        # Last partition ends at the actual end time
        assert parts[1].end == end.isoformat()

    def test_custom_partition_hours(self):
        """Custom 3-hour partition size → 8 partitions for 24h."""
        start = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc)
        parts = generate_partitions(start, end, partition_hours=3)
        assert len(parts) == 8

    def test_activity_types_propagated(self):
        """Activity types are carried through to each partition."""
        start = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 3, 4, 6, 0, tzinfo=timezone.utc)
        types = ["CopilotInteraction", "MailItemsAccessed"]
        parts = generate_partitions(start, end, activity_types=types)
        assert parts[0].activity_types == types

    def test_date_prefix_format(self):
        """Date prefix follows YYYY/MM/DD format."""
        start = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 3, 4, 6, 0, tzinfo=timezone.utc)
        parts = generate_partitions(start, end)
        assert parts[0].date_prefix == "2026/03/04"

    def test_empty_range(self):
        """Start == end → no partitions."""
        t = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        parts = generate_partitions(t, t)
        assert len(parts) == 0

    def test_reverse_range(self):
        """Start > end → no partitions."""
        start = datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 3, 4, 0, 0, tzinfo=timezone.utc)
        parts = generate_partitions(start, end)
        assert len(parts) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# subdivide_partition
# ═══════════════════════════════════════════════════════════════════════════════


class TestSubdividePartition:
    """Tests for subdivide_partition."""

    def test_halving(self):
        """6-hour partition halved → two 3-hour sub-partitions."""
        p = Partition(
            id=3,
            start="2026-03-04T00:00:00+00:00",
            end="2026-03-04T06:00:00+00:00",
            activity_types=["CopilotInteraction"],
        )
        subs = subdivide_partition(p)
        assert len(subs) == 2
        # Sub-partition IDs: 301, 302
        assert subs[0].id == 301
        assert subs[1].id == 302
        # Second half starts where first ends
        assert subs[0].end == subs[1].start

    def test_min_granularity_protection(self):
        """Cannot subdivide below 2-minute minimum."""
        p = Partition(
            id=1,
            start="2026-03-04T00:00:00+00:00",
            end="2026-03-04T00:03:00+00:00",  # 3 minutes total
            activity_types=["CopilotInteraction"],
        )
        # Halving 3 min → 1.5 min each (below 2 min) → returns original
        subs = subdivide_partition(p)
        assert len(subs) == 1
        assert subs[0].id == p.id

    def test_activity_types_carried(self):
        """Sub-partitions inherit activity types."""
        p = Partition(
            id=2,
            start="2026-03-04T00:00:00+00:00",
            end="2026-03-04T12:00:00+00:00",
            activity_types=["CopilotInteraction", "Send"],
        )
        subs = subdivide_partition(p)
        for sub in subs:
            assert sub.activity_types == ["CopilotInteraction", "Send"]

    def test_last_sub_aligns_to_end(self):
        """Last sub-partition exactly matches the original end time."""
        p = Partition(
            id=5,
            start="2026-03-04T00:00:00+00:00",
            end="2026-03-04T07:00:00+00:00",  # 7h — not evenly divisible
            activity_types=[],
        )
        subs = subdivide_partition(p)
        assert subs[-1].end == p.end


# ═══════════════════════════════════════════════════════════════════════════════
# compute_date_range
# ═══════════════════════════════════════════════════════════════════════════════


class TestComputeDateRange:
    """Tests for compute_date_range."""

    def test_explicit_dates(self):
        """Explicit start and end dates are used directly."""
        start, end = compute_date_range(
            start_date="2026-03-04T00:00:00+00:00",
            end_date="2026-03-05T00:00:00+00:00",
        )
        assert start.day == 4
        assert end.day == 5

    def test_start_only_defaults_end_to_now(self):
        """Only start date → end defaults to now."""
        start, end = compute_date_range(
            start_date="2026-03-04T00:00:00+00:00",
        )
        assert start.day == 4
        # end should be close to now
        assert (datetime.now(timezone.utc) - end).total_seconds() < 5

    def test_lookback_defaults(self):
        """No dates → lookback from config."""
        start, end = compute_date_range(lookback_days=2)
        diff = end - start
        assert abs(diff.total_seconds() - 2 * 86400) < 5

    def test_timezone_aware(self):
        """Return values are always timezone-aware UTC."""
        start, end = compute_date_range(
            start_date="2026-03-04T00:00:00",
            end_date="2026-03-05T00:00:00",
        )
        assert start.tzinfo is not None
        assert end.tzinfo is not None


# ═══════════════════════════════════════════════════════════════════════════════
# should_subdivide
# ═══════════════════════════════════════════════════════════════════════════════


class TestShouldSubdivide:
    """Tests for should_subdivide."""

    def test_below_threshold(self):
        assert should_subdivide(500_000) is False

    def test_at_threshold(self):
        assert should_subdivide(950_000) is True

    def test_above_threshold(self):
        assert should_subdivide(1_000_000) is True

    def test_zero_records(self):
        assert should_subdivide(0) is False
