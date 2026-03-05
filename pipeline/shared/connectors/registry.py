"""
Insight Harbor — Connector Registry
=====================================
Singleton registry that discovers, validates, and manages connectors.

Usage::

    from shared.connectors.registry import ConnectorRegistry

    registry = ConnectorRegistry.instance()

    # List all registered connectors
    registry.list_all()

    # Get only the ones enabled by config
    for connector in registry.get_enabled():
        print(connector.name)

    # Look up a specific connector
    purview = registry.get("purview_audit")
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from shared.config import config

if TYPE_CHECKING:
    from .base import BaseConnector

logger = logging.getLogger("ih.connectors.registry")


class ConnectorRegistry:
    """Thread-safe singleton registry for data connectors.

    Connectors are auto-discovered on first access.  The set of
    *enabled* connectors is controlled by the ``IH_ENABLED_CONNECTORS``
    environment variable (comma-separated names).  If the variable is
    empty or unset, **all** registered connectors are enabled.
    """

    _instance: ConnectorRegistry | None = None
    _connectors: dict[str, BaseConnector]

    def __init__(self) -> None:
        self._connectors = {}

    # ── Singleton access ───────────────────────────────────────────────────

    @classmethod
    def instance(cls) -> ConnectorRegistry:
        """Return the singleton ``ConnectorRegistry``, creating it on first call."""
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._auto_discover()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (for testing)."""
        cls._instance = None

    # ── Registration ───────────────────────────────────────────────────────

    def register(self, connector: BaseConnector) -> None:
        """Register a connector instance.

        Overwrites any existing connector with the same ``name``.
        """
        self._connectors[connector.name] = connector
        logger.info("Registered connector: %s (%s)", connector.name, connector.display_name)

    def unregister(self, name: str) -> bool:
        """Remove a connector by name.  Returns True if it existed."""
        return self._connectors.pop(name, None) is not None

    # ── Look-up ────────────────────────────────────────────────────────────

    def get(self, name: str) -> BaseConnector | None:
        """Look up a connector by its unique name."""
        return self._connectors.get(name)

    def get_enabled(self) -> list[BaseConnector]:
        """Return the list of connectors that are enabled by configuration.

        Respects ``IH_ENABLED_CONNECTORS`` (comma-separated names).
        If the variable is empty, all registered connectors are enabled.
        """
        enabled_names = config.ENABLED_CONNECTORS
        if not enabled_names:
            return list(self._connectors.values())
        return [
            c
            for name, c in self._connectors.items()
            if name in enabled_names
        ]

    def list_all(self) -> list[dict]:
        """Return serialisable status dicts for every registered connector."""
        return [c.get_status() for c in self._connectors.values()]

    def list_enabled(self) -> list[dict]:
        """Return serialisable status dicts for enabled connectors only."""
        return [c.get_status() for c in self.get_enabled()]

    @property
    def names(self) -> list[str]:
        """All registered connector names."""
        return list(self._connectors.keys())

    # ── Validation ─────────────────────────────────────────────────────────

    def validate_all(self) -> dict[str, list[str]]:
        """Validate configuration for every registered connector.

        Returns
        -------
        dict[str, list[str]]
            Mapping of connector name → list of error messages.
            Connectors with no errors will have an empty list.
        """
        return {
            name: c.validate_config()
            for name, c in self._connectors.items()
        }

    # ── Auto-discovery ─────────────────────────────────────────────────────

    def _auto_discover(self) -> None:
        """Import and register all built-in connectors.

        External / third-party connectors can be registered manually
        via ``registry.register(MyConnector())``.
        """
        # Lazy imports to avoid circular dependencies
        try:
            from .purview_audit import PurviewAuditConnector
            self.register(PurviewAuditConnector())
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to register PurviewAuditConnector: %s", exc)

        try:
            from .entra import EntraConnector
            self.register(EntraConnector())
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to register EntraConnector: %s", exc)

        try:
            from .m365_usage import M365UsageReportsConnector
            self.register(M365UsageReportsConnector())
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to register M365UsageReportsConnector: %s", exc)

        try:
            from .graph_activity import GraphActivityConnector
            self.register(GraphActivityConnector())
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to register GraphActivityConnector: %s", exc)

    # ── Repr ───────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        names = ", ".join(self._connectors.keys())
        return f"<ConnectorRegistry(connectors=[{names}])>"

    def __len__(self) -> int:
        return len(self._connectors)
