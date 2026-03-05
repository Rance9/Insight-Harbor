"""
Insight Harbor — Extensible Connector Framework
=================================================
Provides a pluggable architecture for ingesting data from multiple
M365 and Graph API data sources.

Usage::

    from shared.connectors.registry import ConnectorRegistry

    registry = ConnectorRegistry.instance()
    for connector in registry.get_enabled():
        connector.ingest(...)
"""

from .base import BaseConnector, ConnectorPhase
from .registry import ConnectorRegistry

__all__ = ["BaseConnector", "ConnectorPhase", "ConnectorRegistry"]
