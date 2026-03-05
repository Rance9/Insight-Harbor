"""
Tests for pipeline.shared.entra_transforms
"""

import pytest

from pipeline.shared.entra_transforms import (
    build_column_map,
    compute_license_tier,
    entra_rows_to_csv,
    has_copilot_license,
    parse_bool,
    parse_snapshot_date,
    transform_entra_from_graph,
    transform_entra_row,
)
from pipeline.shared.constants import ENTRA_SOURCE_TO_SILVER


# ═══════════════════════════════════════════════════════════════════════════════
# parse_bool
# ═══════════════════════════════════════════════════════════════════════════════


class TestParseBool:
    def test_true_values(self):
        assert parse_bool(True) == "True"
        assert parse_bool("true") == "True"
        assert parse_bool("True") == "True"
        assert parse_bool("1") == "True"
        assert parse_bool("yes") == "True"

    def test_false_values(self):
        assert parse_bool(False) == "False"
        assert parse_bool("false") == "False"
        assert parse_bool("0") == "False"
        assert parse_bool("no") == "False"
        assert parse_bool("") == "False"

    def test_none(self):
        assert parse_bool(None) == "False"


# ═══════════════════════════════════════════════════════════════════════════════
# has_copilot_license
# ═══════════════════════════════════════════════════════════════════════════════


class TestHasCopilotLicense:
    def test_copilot_keyword_match(self):
        assert has_copilot_license("Microsoft 365 Copilot") is True

    def test_case_insensitive(self):
        assert has_copilot_license("MICROSOFT COPILOT STUDIO") is True

    def test_no_copilot(self):
        assert has_copilot_license("Microsoft 365 E5") is False

    def test_empty_string(self):
        assert has_copilot_license("") is False

    def test_copilot_in_sku_name(self):
        assert has_copilot_license(
            "[{'skuPartNumber': 'Microsoft_365_Copilot'}]"
        ) is True


# ═══════════════════════════════════════════════════════════════════════════════
# compute_license_tier
# ═══════════════════════════════════════════════════════════════════════════════


class TestComputeLicenseTier:
    def test_copilot_highest_priority(self):
        assert compute_license_tier("Microsoft 365 Copilot") == "Copilot"

    def test_e5_tier(self):
        # LICENSE_TIER_RULES uses SKU part numbers, not display names
        assert compute_license_tier("SPE_E5") == "E5"
        assert compute_license_tier("ENTERPRISEPREMIUM") == "E5"

    def test_e3_tier(self):
        assert compute_license_tier("SPE_E3") == "E3"

    def test_no_match(self):
        assert compute_license_tier("some random sku") == ""

    def test_empty(self):
        assert compute_license_tier("") == ""


# ═══════════════════════════════════════════════════════════════════════════════
# build_column_map
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuildColumnMap:
    def test_maps_known_fields(self):
        fieldnames = ["userPrincipalName", "displayName", "department"]
        col_map = build_column_map(fieldnames)
        assert "userPrincipalName" in col_map
        assert col_map["userPrincipalName"] == "UserPrincipalName"

    def test_case_insensitive_matching(self):
        fieldnames = ["USERPRINCIPALNAME"]
        col_map = build_column_map(fieldnames)
        assert "USERPRINCIPALNAME" in col_map

    def test_unknown_fields_excluded(self):
        fieldnames = ["totally_unknown_field"]
        col_map = build_column_map(fieldnames)
        assert len(col_map) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# transform_entra_row
# ═══════════════════════════════════════════════════════════════════════════════


class TestTransformEntraRow:
    def test_valid_row(self):
        raw = {
            "userPrincipalName": "user@contoso.com",
            "displayName": "Test User",
            "accountEnabled": True,
            "assignedLicenses": "[copilot]",
        }
        col_map = build_column_map(list(raw.keys()))
        result = transform_entra_row(raw, col_map, "2026-03-04", "2026-03-04T00:00:00Z")
        assert result is not None
        assert result["UserPrincipalName"] == "user@contoso.com"
        assert result["_SnapshotDate"] == "2026-03-04"

    def test_missing_upn_returns_none(self):
        raw = {"displayName": "No UPN User"}
        col_map = build_column_map(list(raw.keys()))
        result = transform_entra_row(raw, col_map, "2026-03-04", "now")
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# transform_entra_from_graph
# ═══════════════════════════════════════════════════════════════════════════════


class TestTransformEntraFromGraph:
    def test_basic_transform(self):
        users = [
            {
                "userPrincipalName": "user1@contoso.com",
                "displayName": "User One",
                "accountEnabled": True,
                "assignedLicenses": [],
            },
            {
                "userPrincipalName": "user2@contoso.com",
                "displayName": "User Two",
                "accountEnabled": False,
                "assignedLicenses": [],
            },
        ]
        rows = transform_entra_from_graph(users, "2026-03-04T00:00:00Z", "2026-03-04")
        assert len(rows) == 2

    def test_dedup_by_upn(self):
        users = [
            {
                "userPrincipalName": "dupe@contoso.com",
                "displayName": "First",
                "accountEnabled": True,
                "assignedLicenses": [],
            },
            {
                "userPrincipalName": "DUPE@contoso.com",
                "displayName": "Second",
                "accountEnabled": True,
                "assignedLicenses": [],
            },
        ]
        rows = transform_entra_from_graph(users, "now", "2026-03-04")
        assert len(rows) == 1  # Second one deduplicated

    def test_empty_input(self):
        rows = transform_entra_from_graph([], "now", "2026-03-04")
        assert len(rows) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# entra_rows_to_csv
# ═══════════════════════════════════════════════════════════════════════════════


class TestEntraRowsToCsv:
    def test_csv_output(self):
        rows = [
            {"UserPrincipalName": "user@contoso.com", "DisplayName": "User"},
        ]
        csv_str = entra_rows_to_csv(rows)
        assert "UserPrincipalName" in csv_str
        assert "user@contoso.com" in csv_str

    def test_empty_rows(self):
        # Empty rows still produce header (include_header=True default)
        csv_str = entra_rows_to_csv([], include_header=False)
        assert csv_str == ""


# ═══════════════════════════════════════════════════════════════════════════════
# parse_snapshot_date
# ═══════════════════════════════════════════════════════════════════════════════


class TestParseSnapshotDate:
    def test_returns_date_string(self):
        result = parse_snapshot_date()
        assert len(result) == 10  # YYYY-MM-DD
        assert "-" in result
