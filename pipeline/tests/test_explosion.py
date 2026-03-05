"""
Tests for pipeline.shared.explosion
"""

import json

import pytest

from pipeline.shared.explosion import (
    PURVIEW_EXPLODED_HEADER,
    _categorize_agent,
    _extract_base_fields,
    explode_records_from_jsonl,
    rows_to_csv_string,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Header Schema
# ═══════════════════════════════════════════════════════════════════════════════


class TestPurviewExplodedHeader:
    def test_has_expected_columns(self):
        # Verify header has the expected column count
        assert len(PURVIEW_EXPLODED_HEADER) == 129

    def test_starts_with_record_id(self):
        assert PURVIEW_EXPLODED_HEADER[0] == "RecordId"

    def test_contains_copilot_fields(self):
        assert "CopilotEventData_AppHost" in PURVIEW_EXPLODED_HEADER
        assert "Message_Content" in PURVIEW_EXPLODED_HEADER
        assert "AgentId" in PURVIEW_EXPLODED_HEADER

    def test_no_duplicates(self):
        assert len(PURVIEW_EXPLODED_HEADER) == len(set(PURVIEW_EXPLODED_HEADER))


# ═══════════════════════════════════════════════════════════════════════════════
# _extract_base_fields
# ═══════════════════════════════════════════════════════════════════════════════


class TestExtractBaseFields:
    def test_basic_fields(self):
        # _extract_base_fields uses keys: Id, CreationTime, Operation, UserId
        record = {
            "Id": "rec-001",
            "CreationTime": "2026-03-04T14:30:00Z",
            "RecordType": "CopilotInteraction",
            "Operation": "CopilotInteraction",
            "UserId": "user@contoso.com",
        }
        result = _extract_base_fields(record)
        assert result["RecordId"] == "rec-001"
        assert result["CreationTime"] == "2026-03-04T14:30:00Z"
        assert result["Operation"] == "CopilotInteraction"
        assert result["UserId"] == "user@contoso.com"

    def test_missing_fields_default_empty(self):
        result = _extract_base_fields({})
        assert result["RecordId"] == ""
        assert result["Operation"] == ""


# ═══════════════════════════════════════════════════════════════════════════════
# _categorize_agent
# ═══════════════════════════════════════════════════════════════════════════════


class TestCategorizeAgent:
    def test_teams_agent(self):
        # host_map keys are lowercase; matching uses 'keyword in app_host'
        result = _categorize_agent("teams_app", {})
        assert result["AgentType"] == "Teams"

    def test_word_agent(self):
        result = _categorize_agent("word_doc", {})
        assert result["AgentType"] == "Word"

    def test_unknown_apphost(self):
        result = _categorize_agent("SomeNewApp", {})
        assert result["AgentType"] == ""

    def test_empty_returns_empty(self):
        result = _categorize_agent("", {})
        assert result["AgentType"] == ""
        assert result["AgentId"] == ""

    def test_extracts_agent_id_from_contexts(self):
        copilot_data = {"Contexts": [{"Id": "agent-xyz"}]}
        result = _categorize_agent("Teams", copilot_data)
        assert result["AgentId"] == "agent-xyz"


# ═══════════════════════════════════════════════════════════════════════════════
# explode_records_from_jsonl
# ═══════════════════════════════════════════════════════════════════════════════


class TestExplodeRecordsFromJsonl:
    def _make_copilot_record(self, record_id="r1", messages=None):
        """Helper to create a minimal CopilotInteraction record."""
        if messages is None:
            messages = [
                {"Content": "Hello", "isPrompt": True},
                {"Content": "Hi there", "isPrompt": False},
            ]
        return json.dumps({
            "Id": record_id,
            "CreationTime": "2026-03-04T14:30:00Z",
            "RecordType": "CopilotInteraction",
            "Operation": "CopilotInteraction",
            "UserId": "user@contoso.com",
            "CopilotEventData": {
                "AppHost": "teams",
                "Messages": messages,
                "Contexts": [{"Type": "File", "Id": "doc1"}],
            },
        })

    def test_two_messages_two_rows(self):
        lines = [self._make_copilot_record()]
        rows = explode_records_from_jsonl(lines)
        # Each message becomes a separate row
        assert len(rows) == 2

    def test_no_messages_one_row(self):
        lines = [self._make_copilot_record(messages=[])]
        rows = explode_records_from_jsonl(lines)
        assert len(rows) == 1  # Still creates one base row

    def test_invalid_json_skipped(self):
        lines = ["not valid json", self._make_copilot_record()]
        rows = explode_records_from_jsonl(lines)
        # Invalid line skipped, valid line processed
        assert len(rows) >= 1

    def test_empty_input(self):
        rows = explode_records_from_jsonl([])
        assert len(rows) == 0

    def test_non_copilot_record(self):
        line = json.dumps({
            "Id": "r-usage",
            "CreationTime": "2026-03-04T10:00:00Z",
            "RecordType": "ExchangeItem",
            "Operation": "MailItemsAccessed",
            "UserId": "user@contoso.com",
        })
        rows = explode_records_from_jsonl([line])
        assert len(rows) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# rows_to_csv_string
# ═══════════════════════════════════════════════════════════════════════════════


class TestRowsToCsvString:
    def test_with_header(self):
        rows = [{"A": "1", "B": "2"}]
        csv = rows_to_csv_string(rows, include_header=True)
        lines = csv.strip().split("\n")
        assert len(lines) == 2  # header + 1 row
        assert "A" in lines[0]

    def test_without_header(self):
        rows = [{"A": "1", "B": "2"}]
        csv = rows_to_csv_string(rows, include_header=False)
        lines = csv.strip().split("\n")
        assert len(lines) == 1  # just data

    def test_empty_rows_no_header(self):
        csv = rows_to_csv_string([], include_header=False)
        assert csv == ""

    def test_empty_rows_with_header(self):
        csv = rows_to_csv_string([])
        assert csv.startswith("RecordId")
