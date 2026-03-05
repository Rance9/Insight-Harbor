"""
Insight Harbor — Schema Catalog
=================================
Loads YAML schema definitions for each Silver dataset and provides
a unified API for the query engine to discover columns, types,
semantic tags, and natural-language aliases.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import yaml                           # PyYAML — already in requirements.txt via pydantic

logger = logging.getLogger("ih.query.schema_catalog")

# ── Default schema directory (sibling to this module) ─────────────────────────
_SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"


class SchemaCatalog:
    """Registry of Silver dataset schemas.

    Usage::

        catalog = SchemaCatalog()                   # auto-loads from schemas/
        catalog = SchemaCatalog("/custom/path")     # explicit directory

        for ds in catalog.list_datasets():
            print(ds)

        schema = catalog.get_schema("copilot_usage")
        cols   = catalog.get_columns("copilot_usage")
        qcols  = catalog.get_queryable_columns("copilot_usage")
    """

    def __init__(self, schemas_dir: str | Path | None = None) -> None:
        self._schemas_dir = Path(schemas_dir) if schemas_dir else _SCHEMAS_DIR
        self._schemas: dict[str, dict[str, Any]] = {}
        self._aliases: dict[str, str] = {}          # global alias → hint
        self._load()

    # ── Loading ──────────────────────────────────────────────────────────

    def _load(self) -> None:
        """Discover and load all ``*.yaml`` files in the schemas directory."""
        if not self._schemas_dir.is_dir():
            logger.warning("Schema catalog dir not found: %s", self._schemas_dir)
            return

        for fp in sorted(self._schemas_dir.glob("*.yaml")):
            try:
                with open(fp, "r", encoding="utf-8") as fh:
                    doc = yaml.safe_load(fh)
                if not isinstance(doc, dict) or "dataset" not in doc:
                    logger.warning("Skipping invalid schema file: %s", fp.name)
                    continue

                ds_name = doc["dataset"]
                self._schemas[ds_name] = doc

                # Merge aliases into global map (prefix with dataset for disambiguation)
                for alias, hint in (doc.get("aliases") or {}).items():
                    self._aliases[alias.lower()] = hint

                logger.debug("Loaded schema: %s (%s)", ds_name, fp.name)

            except Exception as exc:
                logger.error("Failed to load schema %s: %s", fp.name, exc)

        logger.info("Schema catalog loaded: %d datasets", len(self._schemas))

    def reload(self) -> None:
        """Clear and reload all schemas."""
        self._schemas.clear()
        self._aliases.clear()
        self._load()

    # ── Queries ──────────────────────────────────────────────────────────

    def list_datasets(self) -> list[str]:
        """Return sorted list of dataset names."""
        return sorted(self._schemas.keys())

    def get_schema(self, dataset: str) -> dict[str, Any] | None:
        """Return full schema dict for *dataset*, or ``None``."""
        return self._schemas.get(dataset)

    def get_columns(self, dataset: str) -> list[dict[str, Any]]:
        """Return column definitions for *dataset*."""
        schema = self._schemas.get(dataset)
        if not schema:
            return []
        return schema.get("columns", [])

    def get_column_names(self, dataset: str) -> list[str]:
        """Return ordered list of column names."""
        return [c["name"] for c in self.get_columns(dataset)]

    def get_queryable_columns(self, dataset: str) -> list[dict[str, Any]]:
        """Return only columns marked ``queryable: true``."""
        return [c for c in self.get_columns(dataset) if c.get("queryable")]

    def get_column(self, dataset: str, column_name: str) -> dict[str, Any] | None:
        """Lookup a single column definition by name."""
        for c in self.get_columns(dataset):
            if c["name"] == column_name:
                return c
        return None

    def get_silver_path(self, dataset: str) -> str:
        """Return the ADLS Silver blob path for *dataset*."""
        schema = self._schemas.get(dataset)
        return schema.get("silver_path", "") if schema else ""

    def get_display_name(self, dataset: str) -> str:
        schema = self._schemas.get(dataset)
        return schema.get("display_name", dataset) if schema else dataset

    def get_description(self, dataset: str) -> str:
        schema = self._schemas.get(dataset)
        return schema.get("description", "") if schema else ""

    def get_grain(self, dataset: str) -> str:
        schema = self._schemas.get(dataset)
        return schema.get("grain", "") if schema else ""

    # ── Alias resolution ─────────────────────────────────────────────────

    def resolve_alias(self, text: str) -> str | None:
        """Return the query hint for a natural-language alias, or ``None``."""
        return self._aliases.get(text.lower().strip())

    def get_all_aliases(self) -> dict[str, str]:
        """Return the full alias → hint map."""
        return dict(self._aliases)

    # ── Schema summary (for LLM context) ─────────────────────────────────

    def build_context_prompt(self, dataset: str | None = None) -> str:
        """Build a concise schema description suitable for an LLM system prompt.

        If *dataset* is ``None``, includes all datasets.
        """
        datasets = [dataset] if dataset else self.list_datasets()
        parts: list[str] = []

        for ds in datasets:
            schema = self._schemas.get(ds)
            if not schema:
                continue

            lines = [
                f"## Dataset: {schema.get('display_name', ds)} ({ds})",
                f"Description: {schema.get('description', '').strip()}",
                f"Grain: {schema.get('grain', 'N/A')}",
                f"Silver path: {schema.get('silver_path', 'N/A')}",
                "",
                "Queryable columns:",
            ]
            for col in self.get_queryable_columns(ds):
                ctype = col.get("type", "string")
                desc = col.get("description", "")
                vals = col.get("common_values", [])
                val_str = f" (values: {', '.join(str(v) for v in vals)})" if vals else ""
                lines.append(f"  - {col['name']} ({ctype}): {desc}{val_str}")

            aliases = schema.get("aliases", {})
            if aliases:
                lines.append("")
                lines.append("Aliases:")
                for alias, hint in aliases.items():
                    lines.append(f"  \"{alias}\" → {hint}")

            parts.append("\n".join(lines))

        return "\n\n".join(parts)

    # ── Serialisation ────────────────────────────────────────────────────

    def to_summary(self) -> list[dict[str, Any]]:
        """Return a JSON-serialisable summary of all datasets."""
        result = []
        for ds in self.list_datasets():
            schema = self._schemas[ds]
            result.append({
                "dataset": ds,
                "display_name": schema.get("display_name", ds),
                "connector": schema.get("connector", ""),
                "description": schema.get("description", "").strip(),
                "grain": schema.get("grain", ""),
                "silver_path": schema.get("silver_path", ""),
                "column_count": len(schema.get("columns", [])),
                "queryable_column_count": len(self.get_queryable_columns(ds)),
            })
        return result
