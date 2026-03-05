"""
Insight Harbor — Semantic Query Engine
=======================================
Provides schema-aware query generation and execution against
Silver-tier data in ADLS Gen2.

Modules
-------
- ``schema_catalog``    — YAML-based schema metadata registry
- ``query_generator``   — Natural-language → DSL (pandas query) translator
- ``query_executor``    — Runs generated queries against Silver CSV data
- ``viz_recommender``   — Rule-based chart type recommender
- ``narrative``         — AI narrative / insight generator
"""

from shared.query.schema_catalog import SchemaCatalog
from shared.query.query_generator import QueryGenerator
from shared.query.query_executor import QueryExecutor
from shared.query.viz_recommender import VizRecommender
from shared.query.narrative import NarrativeGenerator

__all__ = [
    "SchemaCatalog",
    "QueryGenerator",
    "QueryExecutor",
    "VizRecommender",
    "NarrativeGenerator",
]
