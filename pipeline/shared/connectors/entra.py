"""
Insight Harbor — Entra ID (Azure AD) Connector
================================================
Fetches user directory data from Microsoft Entra ID via the Graph API
``/users`` endpoint.  Unlike the Purview connector, Entra ingestion is
a single-phase pull with no partitioning, polling, or explosion.

Data Flow
~~~~~~~~~
Graph ``/users`` (paginated) → transform → Bronze snapshot + Silver CSV

Silver Schema
~~~~~~~~~~~~~
30 columns including UPN, display name, department, license status,
Copilot license detection, and employee org data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from shared.config import config
from shared.connectors.base import BaseConnector, ConnectorPhase
from shared.constants import ENTRA_SILVER_COLUMNS, ENTRA_USER_SELECT_FIELDS

logger = logging.getLogger("ih.connectors.entra")


class EntraConnector(BaseConnector):
    """Connector for Microsoft Entra ID user directory.

    Orchestration Phases
    --------------------
    1. ``pull_entra`` — single activity that fetches, transforms, and
       uploads Entra user data in one shot.

    This connector does **not** support partitioning, polling,
    subdivision, or explosion.
    """

    # ── Identity ───────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "entra"

    @property
    def display_name(self) -> str:
        return "Entra ID Users"

    @property
    def source_type(self) -> str:
        return "graph_api"

    @property
    def version(self) -> str:
        return "1.0.0"

    # ── Configuration ──────────────────────────────────────────────────────

    def validate_config(self) -> list[str]:
        errors: list[str] = []
        if not config.TENANT_ID:
            errors.append("IH_TENANT_ID is not set")
        if not config.CLIENT_ID:
            errors.append("IH_CLIENT_ID is not set")
        if not config.CLIENT_SECRET:
            errors.append("IH_CLIENT_SECRET is not set")
        return errors

    def get_required_permissions(self) -> list[str]:
        return [
            "User.Read.All",       # Read user profiles
            "Directory.Read.All",  # Read directory data (licenses)
        ]

    # ── ADLS Paths ─────────────────────────────────────────────────────────

    def get_bronze_prefix(self) -> str:
        return config.BRONZE_ENTRA_PREFIX

    def get_silver_prefix(self) -> str:
        return config.SILVER_ENTRA_USERS_PREFIX

    # ── Orchestration Phases ───────────────────────────────────────────────

    def get_orchestration_phases(self) -> list[ConnectorPhase]:
        return [
            ConnectorPhase(
                name="ingest",
                activity_name="pull_entra",
                retry_max_attempts=3,
                retry_interval_ms=5_000,
            ),
        ]

    # ── Lifecycle Methods ──────────────────────────────────────────────────

    def ingest(self, input_data: dict) -> dict:
        """Fetch Entra users, transform, and upload to ADLS.

        Delegates to the existing ``pull_entra`` activity implementation.
        """
        from activities.pull_entra import pull_entra
        result = pull_entra(input_data)
        return {
            "status": "completed",
            "blob_path": result.get("silver_blob_path", ""),
            "records": result.get("users_count", 0),
            **result,
        }

    def transform_to_silver(self, input_data: dict) -> dict:
        """Entra transform is embedded in the ingest phase.

        The ``pull_entra`` activity already transforms raw Graph API
        output to Silver schema in a single pass, so this method is
        a no-op for this connector.
        """
        return {
            "output_blob_path": input_data.get("silver_blob_path", ""),
            "new_records": input_data.get("users_count", 0),
        }

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_bronze_schema(self) -> list[str]:
        """Raw Graph API user fields."""
        return list(ENTRA_USER_SELECT_FIELDS)

    def get_silver_schema(self) -> list[str]:
        """30-column Entra Silver schema."""
        return list(ENTRA_SILVER_COLUMNS)
