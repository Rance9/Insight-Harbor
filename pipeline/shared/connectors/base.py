"""
Insight Harbor — BaseConnector Abstract Class
===============================================
Defines the abstract interface every data connector must implement.

A connector encapsulates the full lifecycle of a single data source:
  • Configuration validation
  • Ingestion planning (partitions / work-items)
  • Data ingestion (API calls → ADLS Bronze)
  • Optional explosion (nested JSON → flat CSV)
  • Silver transformation (enrichment, dedup, schema normalisation)
  • Cleanup of temporary resources

Architecture
~~~~~~~~~~~~
Connectors are **not** Durable Functions themselves — they are plain
Python classes whose methods are *called by* registered activity
functions.  The ``ConnectorRegistry`` resolves the correct connector
at runtime, and the orchestrator iterates over enabled connectors to
drive the pipeline.

Adding a new connector
~~~~~~~~~~~~~~~~~~~~~~
1. Subclass ``BaseConnector``.
2. Implement all abstract methods and override optional hooks.
3. Register in ``ConnectorRegistry._auto_discover()``.
4. Add corresponding activity functions in ``function_app.py``.

See ``docs/adding-a-connector.md`` for a full walkthrough.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


# ═══════════════════════════════════════════════════════════════════════════════
# ConnectorPhase — describes one orchestration step
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class ConnectorPhase:
    """Metadata for one phase in a connector's orchestration sequence.

    Parameters
    ----------
    name : str
        Logical phase name (e.g. ``"ingest"``, ``"explode"``, ``"transform"``).
    activity_name : str
        The Durable Functions activity function name to call.
    fan_out : bool
        If ``True``, the orchestrator will invoke this activity once per
        work-item (partition / blob) and wait for all to complete.
    parallel_group : str | None
        Phases sharing the same ``parallel_group`` value may run
        concurrently via ``task_all``.
    retry_max_attempts : int
        Maximum retry attempts for transient failures.
    retry_interval_ms : int
        Initial back-off interval in milliseconds.
    optional : bool
        If ``True``, the phase can be skipped when there is nothing to do.
    """

    name: str
    activity_name: str
    fan_out: bool = False
    parallel_group: str | None = None
    retry_max_attempts: int = 3
    retry_interval_ms: int = 5_000
    optional: bool = False


# ═══════════════════════════════════════════════════════════════════════════════
# BaseConnector — abstract interface every connector must satisfy
# ═══════════════════════════════════════════════════════════════════════════════


class BaseConnector(ABC):
    """Abstract base class for Insight Harbor data connectors.

    Lifecycle Methods (override as needed)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ``validate_config()``
        Check that all required environment variables / secrets are set.

    ``plan(input_data)``
        Produce a list of work-items (e.g. time-window partitions).

    ``ingest(input_data)``
        Fetch data for **one** work-item and write to Bronze in ADLS.

    ``explode(input_data)``
        Flatten raw (JSONL / JSON) data into tabular CSV.

    ``transform_to_silver(input_data)``
        Apply schema normalisation, Entra enrichment, deduplication.

    ``cleanup(input_data)``
        Release server-side resources (e.g. delete audit queries).

    Properties (must implement)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ``name``  — unique identifier (snake_case).
    ``display_name``  — human-readable label.
    ``source_type``  — data-source category.
    """

    # ── Identity ───────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique connector identifier (e.g. ``'purview_audit'``)."""
        ...

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name (e.g. ``'Purview Audit Log'``)."""
        ...

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Data-source category (``'graph_api'``, ``'rest_api'``, ``'file'``)."""
        ...

    @property
    def version(self) -> str:
        """Connector semantic version."""
        return "1.0.0"

    # ── Capability flags ───────────────────────────────────────────────────

    @property
    def supports_partitioning(self) -> bool:
        """If ``True``, ``plan()`` may return multiple work-items."""
        return False

    @property
    def supports_polling(self) -> bool:
        """If ``True``, ingestion involves an async query → poll loop."""
        return False

    @property
    def supports_subdivision(self) -> bool:
        """If ``True``, large partitions can be split mid-flight."""
        return False

    @property
    def supports_explosion(self) -> bool:
        """If ``True``, the ``explode`` phase is used."""
        return False

    # ── Configuration ──────────────────────────────────────────────────────

    @abstractmethod
    def validate_config(self) -> list[str]:
        """Check required configuration.

        Returns
        -------
        list[str]
            Empty list if valid, otherwise error messages describing
            what is missing or misconfigured.
        """
        ...

    @abstractmethod
    def get_required_permissions(self) -> list[str]:
        """Graph API / Azure RBAC permissions this connector needs.

        Used for documentation and the admin health-check endpoint.
        """
        ...

    # ── ADLS Path Convention ───────────────────────────────────────────────

    @abstractmethod
    def get_bronze_prefix(self) -> str:
        """ADLS path prefix for raw (Bronze) data."""
        ...

    @abstractmethod
    def get_silver_prefix(self) -> str:
        """ADLS path prefix for Silver (curated) data."""
        ...

    # ── Orchestration Contract ─────────────────────────────────────────────

    @abstractmethod
    def get_orchestration_phases(self) -> list[ConnectorPhase]:
        """Return the ordered sequence of phases for this connector.

        Each ``ConnectorPhase`` maps to a Durable Functions activity
        that the orchestrator should call.
        """
        ...

    # ── Lifecycle Methods ──────────────────────────────────────────────────

    def plan(self, input_data: dict) -> list[dict]:
        """Plan ingestion work-items.

        Override for connectors that split work into partitions,
        time-windows, or paged batches.  Default returns a single
        work-item containing the full input.

        Parameters
        ----------
        input_data : dict
            Pipeline trigger payload (``start_date``, ``end_date``, etc.).

        Returns
        -------
        list[dict]
            One dict per work-item.
        """
        return [input_data]

    @abstractmethod
    def ingest(self, input_data: dict) -> dict:
        """Execute one ingestion unit.

        Fetch data from the source API and write to ADLS Bronze.

        Parameters
        ----------
        input_data : dict
            Work-item produced by ``plan()`` or the orchestrator.

        Returns
        -------
        dict
            Must contain at least ``{"status": "completed"|"failed"}``.
        """
        ...

    def explode(self, input_data: dict) -> dict:
        """Flatten raw data into tabular format.

        Default is a pass-through (no explosion required).  Override
        for connectors whose raw format is deeply nested JSON.

        Parameters
        ----------
        input_data : dict
            Must contain ``{"bronze_blob_path": "..."}`` at minimum.

        Returns
        -------
        dict
            Must contain ``{"output_blob_path": "..."}``.
        """
        return input_data

    @abstractmethod
    def transform_to_silver(self, input_data: dict) -> dict:
        """Transform Bronze data to Silver schema.

        Apply enrichment, deduplication, computed columns, and type
        casting.

        Parameters
        ----------
        input_data : dict
            Connector-specific payload (e.g. list of exploded paths).

        Returns
        -------
        dict
            Must contain ``{"output_blob_path": "...", "new_records": N}``.
        """
        ...

    def cleanup(self, input_data: dict) -> dict:
        """Release server-side resources after ingestion.

        Default is a no-op.  Override for connectors that create
        temporary queries or sessions that must be deleted.
        """
        return {"cleaned": True}

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_bronze_schema(self) -> list[str]:
        """Return Bronze column names (optional — used by schema catalog)."""
        return []

    def get_silver_schema(self) -> list[str]:
        """Return Silver column names (optional — used by schema catalog)."""
        return []

    # ── Status / Health ────────────────────────────────────────────────────

    def get_status(self) -> dict:
        """Return a serialisable status dict for admin endpoints.

        Includes configuration validity, connector metadata, and
        capability flags.
        """
        errors = self.validate_config()
        return {
            "name": self.name,
            "display_name": self.display_name,
            "source_type": self.source_type,
            "version": self.version,
            "configured": len(errors) == 0,
            "validation_errors": errors,
            "capabilities": {
                "partitioning": self.supports_partitioning,
                "polling": self.supports_polling,
                "subdivision": self.supports_subdivision,
                "explosion": self.supports_explosion,
            },
            "permissions": self.get_required_permissions(),
            "adls_paths": {
                "bronze": self.get_bronze_prefix(),
                "silver": self.get_silver_prefix(),
            },
        }

    # ── Repr ───────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name={self.name!r})>"
