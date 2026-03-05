"""
Tests for pipeline.shared.transforms
"""

import pytest

from pipeline.shared.transforms import (
    compute_is_agent,
    compute_prompt_type,
    enrich_row_with_entra,
    make_dedup_key,
    parse_creation_time,
    safe_int,
    transform_row,
)


# ═══════════════════════════════════════════════════════════════════════════════
# compute_prompt_type
# ═══════════════════════════════════════════════════════════════════════════════


class TestComputePromptType:
    def test_true_returns_prompt(self):
        assert compute_prompt_type("true") == "Prompt"
        assert compute_prompt_type("True") == "Prompt"
        assert compute_prompt_type(" TRUE ") == "Prompt"

    def test_false_returns_response(self):
        assert compute_prompt_type("false") == "Response"
        assert compute_prompt_type("False") == "Response"

    def test_empty_returns_empty(self):
        assert compute_prompt_type("") == ""

    def test_unknown_returns_empty(self):
        assert compute_prompt_type("maybe") == ""


# ═══════════════════════════════════════════════════════════════════════════════
# compute_is_agent
# ═══════════════════════════════════════════════════════════════════════════════


class TestComputeIsAgent:
    def test_with_agent_id(self):
        assert compute_is_agent("abc-123") == "Yes"

    def test_empty_agent_id(self):
        assert compute_is_agent("") == "No"

    def test_whitespace_agent_id(self):
        assert compute_is_agent("   ") == "No"


# ═══════════════════════════════════════════════════════════════════════════════
# safe_int
# ═══════════════════════════════════════════════════════════════════════════════


class TestSafeInt:
    def test_integer_string(self):
        assert safe_int("42") == 42

    def test_float_string(self):
        assert safe_int("3.14") == 3

    def test_empty_string(self):
        assert safe_int("") == ""

    def test_none(self):
        assert safe_int(None) == ""

    def test_non_numeric(self):
        assert safe_int("abc") == ""

    def test_actual_int(self):
        assert safe_int(7) == 7


# ═══════════════════════════════════════════════════════════════════════════════
# parse_creation_time
# ═══════════════════════════════════════════════════════════════════════════════


class TestParseCreationTime:
    def test_iso_format_with_z(self):
        dt = parse_creation_time("2026-03-04T14:30:00Z")
        assert dt is not None
        assert dt.hour == 14
        assert dt.minute == 30

    def test_iso_format_with_millis(self):
        dt = parse_creation_time("2026-03-04T14:30:00.123456Z")
        assert dt is not None
        assert dt.hour == 14

    def test_invalid_format(self):
        dt = parse_creation_time("not-a-date")
        assert dt is None

    def test_empty_string(self):
        dt = parse_creation_time("")
        assert dt is None


# ═══════════════════════════════════════════════════════════════════════════════
# make_dedup_key
# ═══════════════════════════════════════════════════════════════════════════════


class TestMakeDedupKey:
    def test_normal_key(self):
        row = {"RecordId": "abc-123", "Message_Id": "msg-456"}
        key = make_dedup_key(row)
        assert key == ("abc-123", "msg-456")

    def test_missing_fields(self):
        key = make_dedup_key({})
        assert key == ("", "")

    def test_whitespace_stripped(self):
        row = {"RecordId": "  abc ", "Message_Id": " msg "}
        key = make_dedup_key(row)
        assert key == ("abc", "msg")


# ═══════════════════════════════════════════════════════════════════════════════
# enrich_row_with_entra
# ═══════════════════════════════════════════════════════════════════════════════


class TestEnrichRowWithEntra:
    def test_matching_user(self):
        silver_row = {"UserId": "user@contoso.com"}
        # Lookup must include ENTRA_ENRICHMENT_COLUMNS keys
        lookup = {
            "user@contoso.com": {
                "Department": "IT",
                "JobTitle": "Engineer",
                "Country": "",
                "City": "",
                "ManagerDisplayName": "",
                "Division": "",
                "CostCenter": "",
                "HasCopilotLicense": "True",
                "LicenseTier": "Copilot",
                "CompanyName": "Contoso",
            }
        }
        enrich_row_with_entra(silver_row, lookup)
        assert silver_row.get("Department") == "IT"
        assert silver_row.get("JobTitle") == "Engineer"
        assert silver_row.get("CompanyName") == "Contoso"

    def test_case_insensitive_match(self):
        silver_row = {"UserId": "USER@Contoso.com"}
        lookup = {"user@contoso.com": {"Department": "Sales"}}
        enrich_row_with_entra(silver_row, lookup)
        assert silver_row.get("Department") == "Sales"

    def test_no_match_fills_empty(self):
        silver_row = {"UserId": "unknown@contoso.com"}
        enrich_row_with_entra(silver_row, {})
        # Enrichment columns should be empty strings, not KeyError


# ═══════════════════════════════════════════════════════════════════════════════
# transform_row
# ═══════════════════════════════════════════════════════════════════════════════


class TestTransformRow:
    def test_valid_row_transforms(self):
        raw = {
            "RecordId": "abc-123",
            "CreationTime": "2026-03-04T14:30:00Z",
            "Message_isPrompt": "true",
            "AgentId": "agent-xyz",
        }
        result = transform_row(raw, "test.csv", "2026-03-04T00:00:00Z")
        assert result is not None
        assert result["UsageDate"] == "2026-03-04"
        assert result["UsageHour"] == "14"
        assert result["PromptType"] == "Prompt"
        assert result["IsAgent"] == "Yes"
        assert result["_SourceFile"] == "test.csv"

    def test_missing_record_id_returns_none(self):
        raw = {"RecordId": "", "CreationTime": "2026-03-04T14:30:00Z"}
        assert transform_row(raw, "test.csv", "now") is None

    def test_no_record_id_key(self):
        raw = {"CreationTime": "2026-03-04T14:30:00Z"}
        assert transform_row(raw, "test.csv", "now") is None

    def test_no_agent_no_prompt(self):
        raw = {
            "RecordId": "xyz",
            "CreationTime": "2026-03-04T10:00:00Z",
            "Message_isPrompt": "",
            "AgentId": "",
        }
        result = transform_row(raw, "src.csv", "now")
        assert result is not None
        assert result["PromptType"] == ""
        assert result["IsAgent"] == "No"
