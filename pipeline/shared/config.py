"""
Insight Harbor — Pipeline Configuration
========================================
Loads all pipeline configuration from environment variables (Function App Settings).
Provides typed defaults matching PAX parameter equivalents.
"""

from __future__ import annotations

import os


class PipelineConfig:
    """Typed configuration loaded from environment variables."""

    # ── Azure identity ──────────────────────────────────────────────────────
    TENANT_ID: str = os.getenv("IH_TENANT_ID", "")
    CLIENT_ID: str = os.getenv("IH_CLIENT_ID", "")
    CLIENT_SECRET: str = os.getenv("IH_CLIENT_SECRET", "")

    # ── ADLS Gen2 ───────────────────────────────────────────────────────────
    ADLS_ACCOUNT_NAME: str = os.getenv("IH_ADLS_ACCOUNT_NAME", "ihstoragepoc01")
    ADLS_CONTAINER: str = os.getenv("IH_ADLS_CONTAINER", "insight-harbor")

    # ── Pipeline schedule / window ──────────────────────────────────────────
    DEFAULT_LOOKBACK_DAYS: int = int(os.getenv("IH_DEFAULT_LOOKBACK_DAYS", "1"))
    PARTITION_HOURS: int = int(os.getenv("IH_PARTITION_HOURS", "6"))
    MAX_CONCURRENCY: int = int(os.getenv("IH_MAX_CONCURRENCY", "4"))
    TARGET_RECORDS_PER_BLOCK: int = int(os.getenv("IH_TARGET_RECORDS_PER_BLOCK", "5000"))
    SCHEDULE_CRON: str = os.getenv("IH_SCHEDULE_CRON", "0 0 2 * * *")

    # ── Graph API ───────────────────────────────────────────────────────────
    GRAPH_API_VERSION: str = os.getenv("IH_GRAPH_API_VERSION", "v1.0")
    GRAPH_BASE_URL: str = "https://graph.microsoft.com"

    # ── Activity types & filters ────────────────────────────────────────────
    ACTIVITY_TYPES: list[str] = [
        s.strip()
        for s in os.getenv("IH_ACTIVITY_TYPES", "CopilotInteraction").split(",")
        if s.strip()
    ]
    EXPLOSION_MODE: str = os.getenv("IH_EXPLOSION_MODE", "raw")

    # ── M365 usage / DSPM switches (PAX equivalents) ───────────────────────
    INCLUDE_M365_USAGE: bool = os.getenv("IH_INCLUDE_M365_USAGE", "false").lower() == "true"
    INCLUDE_DSPM_AI: bool = os.getenv("IH_INCLUDE_DSPM_AI", "false").lower() == "true"
    EXCLUDE_COPILOT_INTERACTION: bool = (
        os.getenv("IH_EXCLUDE_COPILOT_INTERACTION", "false").lower() == "true"
    )
    AUTO_COMPLETENESS: bool = os.getenv("IH_AUTO_COMPLETENESS", "false").lower() == "true"

    # ── Subdivision / polling ───────────────────────────────────────────────
    SUBDIVISION_THRESHOLD: int = int(os.getenv("IH_SUBDIVISION_THRESHOLD", "950000"))
    POLL_MIN_SECONDS: int = int(os.getenv("IH_POLL_MIN_SECONDS", "30"))
    POLL_MAX_SECONDS: int = int(os.getenv("IH_POLL_MAX_SECONDS", "90"))
    MAX_POLL_ATTEMPTS: int = int(os.getenv("IH_MAX_POLL_ATTEMPTS", "120"))

    # ── Content filters (PAX -AgentId, -AgentsOnly, etc.) ──────────────────
    AGENT_ID_FILTER: str = os.getenv("IH_AGENT_ID_FILTER", "")
    AGENTS_ONLY: bool = os.getenv("IH_AGENTS_ONLY", "false").lower() == "true"
    EXCLUDE_AGENTS: bool = os.getenv("IH_EXCLUDE_AGENTS", "false").lower() == "true"
    PROMPT_FILTER: str = os.getenv("IH_PROMPT_FILTER", "")  # Prompt|Response|Both|Null
    USER_IDS: list[str] = [
        u.strip()
        for u in os.getenv("IH_USER_IDS", "").split(",")
        if u.strip()
    ]
    SERVICE_TYPES: list[str] = [
        s.strip()
        for s in os.getenv("IH_SERVICE_TYPES", "").split(",")
        if s.strip()
    ]
    RECORD_TYPES: list[str] = [
        r.strip()
        for r in os.getenv("IH_RECORD_TYPES", "").split(",")
        if r.strip()
    ]

    # ── Notifications ───────────────────────────────────────────────────────
    TEAMS_WEBHOOK_URL: str = os.getenv("IH_TEAMS_WEBHOOK_URL", "")

    # ── Durable Functions ───────────────────────────────────────────────────
    DURABLE_TASK_HUB: str = os.getenv("IH_DURABLE_TASK_HUB", "ihpipelinehub")

    # ── ADLS path constants ─────────────────────────────────────────────────
    BRONZE_PURVIEW_PREFIX: str = "bronze/purview"
    BRONZE_EXPLODED_PREFIX: str = "bronze/exploded"
    BRONZE_ENTRA_PREFIX: str = "bronze/entra"
    SILVER_COPILOT_USAGE_PREFIX: str = "silver/copilot-usage"
    SILVER_ENTRA_USERS_PREFIX: str = "silver/entra-users"
    PIPELINE_STATE_PREFIX: str = "pipeline/state"
    PIPELINE_HISTORY_PREFIX: str = "pipeline/history"

    # ── Memory / streaming ──────────────────────────────────────────────────
    STREAM_CHUNK_SIZE_MB: int = 50
    STREAM_CHUNK_SIZE_BYTES: int = STREAM_CHUNK_SIZE_MB * 1024 * 1024

    @property
    def effective_subdivision_threshold(self) -> int:
        """AutoCompleteness uses 10K; normal uses configured value (default 950K)."""
        return 10_000 if self.AUTO_COMPLETENESS else self.SUBDIVISION_THRESHOLD

    @property
    def graph_audit_url(self) -> str:
        """Base URL for Purview audit log queries endpoint."""
        return f"{self.GRAPH_BASE_URL}/{self.GRAPH_API_VERSION}/security/auditLog/queries"

    @property
    def adls_account_url(self) -> str:
        """Full ADLS blob service URL."""
        return f"https://{self.ADLS_ACCOUNT_NAME}.blob.core.windows.net"


# Module-level singleton — import this from activities/orchestrators
config = PipelineConfig()
