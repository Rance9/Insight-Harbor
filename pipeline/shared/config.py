"""
Insight Harbor — Pipeline Configuration
========================================
Loads all pipeline configuration from environment variables (Function App Settings).
Provides typed defaults matching PAX parameter equivalents.
"""

from __future__ import annotations

import os


class PipelineConfig:
    """Typed configuration loaded from environment variables.

    All env-var reads happen in ``__init__`` so that each instance
    reflects the *current* ``os.environ`` — critical for tests that
    use ``monkeypatch.setenv`` after the module is imported.
    """

    def __init__(self) -> None:
        # ── Azure identity ──────────────────────────────────────────────
        self.TENANT_ID: str = os.getenv("IH_TENANT_ID", "")
        self.CLIENT_ID: str = os.getenv("IH_CLIENT_ID", "")
        self.CLIENT_SECRET: str = os.getenv("IH_CLIENT_SECRET", "")

        # ── ADLS Gen2 ──────────────────────────────────────────────────
        self.ADLS_ACCOUNT_NAME: str = os.getenv("IH_ADLS_ACCOUNT_NAME", "ihstoragepoc01")
        self.ADLS_CONTAINER: str = os.getenv("IH_ADLS_CONTAINER", "insight-harbor")

        # ── Pipeline schedule / window ──────────────────────────────────
        self.DEFAULT_LOOKBACK_DAYS: int = int(os.getenv("IH_DEFAULT_LOOKBACK_DAYS", "1"))
        self.PARTITION_HOURS: int = int(os.getenv("IH_PARTITION_HOURS", "6"))
        self.MAX_CONCURRENCY: int = int(os.getenv("IH_MAX_CONCURRENCY", "4"))
        self.TARGET_RECORDS_PER_BLOCK: int = int(os.getenv("IH_TARGET_RECORDS_PER_BLOCK", "5000"))
        self.SCHEDULE_CRON: str = os.getenv("IH_SCHEDULE_CRON", "0 0 2 * * *")

        # ── Graph API ──────────────────────────────────────────────────
        self.GRAPH_API_VERSION: str = os.getenv("IH_GRAPH_API_VERSION", "beta")
        self.GRAPH_BASE_URL: str = "https://graph.microsoft.com"

        # ── Activity types & filters ───────────────────────────────────
        self.ACTIVITY_TYPES: list[str] = [
            s.strip()
            for s in os.getenv("IH_ACTIVITY_TYPES", "CopilotInteraction").split(",")
            if s.strip()
        ]
        self.EXPLOSION_MODE: str = os.getenv("IH_EXPLOSION_MODE", "raw")

        # ── M365 usage / DSPM switches (PAX equivalents) ──────────────
        self.INCLUDE_M365_USAGE: bool = os.getenv("IH_INCLUDE_M365_USAGE", "false").lower() == "true"
        self.INCLUDE_DSPM_AI: bool = os.getenv("IH_INCLUDE_DSPM_AI", "false").lower() == "true"
        self.EXCLUDE_COPILOT_INTERACTION: bool = (
            os.getenv("IH_EXCLUDE_COPILOT_INTERACTION", "false").lower() == "true"
        )
        self.AUTO_COMPLETENESS: bool = os.getenv("IH_AUTO_COMPLETENESS", "false").lower() == "true"

        # ── Subdivision / polling ──────────────────────────────────────
        self.SUBDIVISION_THRESHOLD: int = int(os.getenv("IH_SUBDIVISION_THRESHOLD", "950000"))
        self.POLL_MIN_SECONDS: int = int(os.getenv("IH_POLL_MIN_SECONDS", "30"))
        self.POLL_MAX_SECONDS: int = int(os.getenv("IH_POLL_MAX_SECONDS", "90"))
        self.MAX_POLL_ATTEMPTS: int = int(os.getenv("IH_MAX_POLL_ATTEMPTS", "120"))

        # ── Content filters (PAX -AgentId, -AgentsOnly, etc.) ─────────
        self.AGENT_ID_FILTER: str = os.getenv("IH_AGENT_ID_FILTER", "")
        self.AGENTS_ONLY: bool = os.getenv("IH_AGENTS_ONLY", "false").lower() == "true"
        self.EXCLUDE_AGENTS: bool = os.getenv("IH_EXCLUDE_AGENTS", "false").lower() == "true"
        self.PROMPT_FILTER: str = os.getenv("IH_PROMPT_FILTER", "")  # Prompt|Response|Both|Null
        self.USER_IDS: list[str] = [
            u.strip()
            for u in os.getenv("IH_USER_IDS", "").split(",")
            if u.strip()
        ]
        self.SERVICE_TYPES: list[str] = [
            s.strip()
            for s in os.getenv("IH_SERVICE_TYPES", "").split(",")
            if s.strip()
        ]
        self.RECORD_TYPES: list[str] = [
            r.strip()
            for r in os.getenv("IH_RECORD_TYPES", "").split(",")
            if r.strip()
        ]

        # ── Notifications ─────────────────────────────────────────────
        self.TEAMS_WEBHOOK_URL: str = os.getenv("IH_TEAMS_WEBHOOK_URL", "")

        # ── Durable Functions ─────────────────────────────────────────
        self.DURABLE_TASK_HUB: str = os.getenv("IH_DURABLE_TASK_HUB", "ihpipelinehub")

        # ── Connector Framework ───────────────────────────────────────
        # Comma-separated connector names to enable.  Empty = all registered.
        self.ENABLED_CONNECTORS: list[str] = [
            c.strip()
            for c in os.getenv("IH_ENABLED_CONNECTORS", "").split(",")
            if c.strip()
        ]

        # ── ADLS path constants ───────────────────────────────────────
        self.BRONZE_PURVIEW_PREFIX: str = "bronze/purview"
        self.BRONZE_EXPLODED_PREFIX: str = "bronze/exploded"
        self.BRONZE_ENTRA_PREFIX: str = "bronze/entra"
        self.SILVER_COPILOT_USAGE_PREFIX: str = "silver/copilot-usage"
        self.SILVER_ENTRA_USERS_PREFIX: str = "silver/entra-users"
        self.PIPELINE_STATE_PREFIX: str = "pipeline/state"
        self.PIPELINE_HISTORY_PREFIX: str = "pipeline/history"

        # ── Multi-source ADLS paths (Gap 5) ───────────────────────────
        self.BRONZE_M365_USAGE_PREFIX: str = "bronze/m365-usage"
        self.SILVER_M365_USAGE_PREFIX: str = "silver/m365-usage"
        self.BRONZE_GRAPH_ACTIVITY_PREFIX: str = "bronze/graph-activity"
        self.SILVER_GRAPH_ACTIVITY_PREFIX: str = "silver/graph-activity"
        self.SCHEMA_CATALOG_PREFIX: str = "metadata/schema-catalog"

        # ── Memory / streaming ────────────────────────────────────────
        self.STREAM_CHUNK_SIZE_MB: int = 50
        self.STREAM_CHUNK_SIZE_BYTES: int = self.STREAM_CHUNK_SIZE_MB * 1024 * 1024

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
