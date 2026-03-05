# Adding a New Connector to Insight Harbor

This guide walks through creating a new data connector for the Insight Harbor pipeline.  
Connectors are pluggable modules that encapsulate all logic for a specific data source — from API authentication through Silver-layer transformation.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│  ConnectorRegistry (singleton)                      │
│  ┌───────────────┐  ┌────────────┐  ┌───────────┐  │
│  │PurviewAudit   │  │ Entra      │  │ YourNew   │  │
│  │Connector      │  │ Connector  │  │ Connector │  │
│  └───────┬───────┘  └──────┬─────┘  └─────┬─────┘  │
│          │                 │               │        │
│          ▼                 ▼               ▼        │
│  ┌─────────────────────────────────────────────┐    │
│  │           BaseConnector (ABC)               │    │
│  │  • validate_config()                        │    │
│  │  • plan() → work items                      │    │
│  │  • ingest() → Bronze                        │    │
│  │  • explode() → flat CSV                     │    │
│  │  • transform_to_silver() → Silver           │    │
│  │  • get_orchestration_phases()               │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

Each connector is a Python class that inherits from `BaseConnector` and implements its abstract methods.

---

## Step 1: Create the Connector File

Create a new file at `pipeline/shared/connectors/your_source.py`:

```python
"""
Insight Harbor — Your New Data Source Connector
"""
from __future__ import annotations
import logging

from shared.config import config
from shared.connectors.base import BaseConnector, ConnectorPhase

logger = logging.getLogger("ih.connectors.your_source")


class YourSourceConnector(BaseConnector):

    # ── Identity ─────────────────────────────────────────────
    @property
    def name(self) -> str:
        return "your_source"  # unique snake_case identifier

    @property
    def display_name(self) -> str:
        return "Your Data Source"

    @property
    def source_type(self) -> str:
        return "graph_api"  # or "rest_api", "file", etc.

    # ── Configuration ────────────────────────────────────────
    def validate_config(self) -> list[str]:
        errors = []
        if not config.TENANT_ID:
            errors.append("IH_TENANT_ID is not set")
        # Add checks for your source-specific env vars
        return errors

    def get_required_permissions(self) -> list[str]:
        return ["Reports.Read.All"]  # Graph API permissions

    # ── ADLS Paths ───────────────────────────────────────────
    def get_bronze_prefix(self) -> str:
        return "bronze/your-source"

    def get_silver_prefix(self) -> str:
        return "silver/your-source"

    # ── Orchestration ────────────────────────────────────────
    def get_orchestration_phases(self) -> list[ConnectorPhase]:
        return [
            ConnectorPhase(
                name="ingest",
                activity_name="ingest_your_source",
                retry_max_attempts=3,
            ),
            ConnectorPhase(
                name="transform",
                activity_name="transform_your_source",
            ),
        ]

    # ── Lifecycle ────────────────────────────────────────────
    def ingest(self, input_data: dict) -> dict:
        """Fetch data and write to ADLS Bronze."""
        # Your API fetch logic here
        return {"status": "completed", "blob_path": "...", "records": 0}

    def transform_to_silver(self, input_data: dict) -> dict:
        """Transform Bronze to Silver schema."""
        # Your transform logic here
        return {"output_blob_path": "...", "new_records": 0}
```

---

## Step 2: Register the Connector

Edit `pipeline/shared/connectors/registry.py` and add your connector to `_auto_discover()`:

```python
def _auto_discover(self) -> None:
    # ... existing registrations ...

    try:
        from .your_source import YourSourceConnector
        self.register(YourSourceConnector())
    except Exception as exc:
        logger.error("Failed to register YourSourceConnector: %s", exc)
```

---

## Step 3: Add Activity Functions

Create activity function files in `pipeline/activities/`:

```python
# pipeline/activities/ingest_your_source.py
def ingest_your_source(input_data: dict) -> dict:
    from shared.connectors.registry import ConnectorRegistry
    connector = ConnectorRegistry.instance().get("your_source")
    return connector.ingest(input_data)
```

Register them in `pipeline/function_app.py`:

```python
@app.activity_trigger(input_name="inputData")
def ingest_your_source(inputData: dict) -> dict:
    from activities.ingest_your_source import ingest_your_source as _impl
    return _impl(inputData)
```

---

## Step 4: Add ADLS Path Constants (Optional)

If your connector uses fixed ADLS paths, add them to `pipeline/shared/config.py`:

```python
class PipelineConfig:
    # ...existing paths...
    BRONZE_YOUR_SOURCE_PREFIX: str = "bronze/your-source"
    SILVER_YOUR_SOURCE_PREFIX: str = "silver/your-source"
```

---

## Step 5: Add Configuration

Add your connector to the enabled list via environment variable:

```
IH_ENABLED_CONNECTORS=purview_audit,entra,your_source
```

If `IH_ENABLED_CONNECTORS` is empty, **all** registered connectors are enabled by default.

---

## Step 6: Add Unit Tests

Create `pipeline/tests/test_your_source_connector.py`:

```python
class TestYourSourceConnector:
    def test_identity(self):
        from shared.connectors.your_source import YourSourceConnector
        c = YourSourceConnector()
        assert c.name == "your_source"

    def test_config_validation(self):
        from shared.connectors.your_source import YourSourceConnector
        c = YourSourceConnector()
        errors = c.validate_config()
        assert errors == []  # with test env vars

    def test_orchestration_phases(self):
        from shared.connectors.your_source import YourSourceConnector
        c = YourSourceConnector()
        phases = c.get_orchestration_phases()
        assert len(phases) >= 1
```

---

## Step 7: Add Schema Metadata (Optional, for Semantic Query)

If you want your connector's data visible to the semantic query layer, create a YAML schema file at `pipeline/metadata/schemas/your_source.yaml`:

```yaml
source: your_source
display_name: Your Data Source
description: Data from your source
silver_table: silver/your-source/output.csv
columns:
  - name: UserId
    type: string
    description: User principal name
    searchable: true
  - name: EventTime
    type: datetime
    description: When the event occurred
```

---

## Capability Flags

Override these properties to declare what your connector supports:

| Property | Default | Description |
|---|---|---|
| `supports_partitioning` | `False` | `plan()` may return multiple work-items |
| `supports_polling` | `False` | Ingestion uses an async query-poll loop |
| `supports_subdivision` | `False` | Large partitions can be auto-split |
| `supports_explosion` | `False` | Raw data needs JSON→CSV flattening |

---

## ADLS Data Lake Structure

```
insight-harbor/
├── bronze/
│   ├── purview/          ← PurviewAuditConnector
│   ├── exploded/         ← PurviewAuditConnector (exploded)
│   ├── entra/            ← EntraConnector
│   ├── m365-usage/       ← M365UsageReportsConnector
│   ├── graph-activity/   ← GraphActivityConnector
│   └── your-source/      ← YourSourceConnector
├── silver/
│   ├── copilot-usage/    ← PurviewAuditConnector
│   ├── entra-users/      ← EntraConnector
│   ├── m365-usage/       ← M365UsageReportsConnector
│   ├── graph-activity/   ← GraphActivityConnector
│   └── your-source/      ← YourSourceConnector
├── metadata/
│   └── schema-catalog/   ← Schema definitions for semantic query
└── pipeline/
    ├── state/            ← Run state checkpoints
    └── history/          ← Historical run metadata
```

---

## Built-in Connectors Reference

| Connector | Name | Source | Capabilities |
|---|---|---|---|
| **PurviewAuditConnector** | `purview_audit` | Graph `/security/auditLog/queries` | Partition, Poll, Subdivide, Explode |
| **EntraConnector** | `entra` | Graph `/users` | — |
| **M365UsageReportsConnector** | `m365_usage` | Graph `/reports` | — |
| **GraphActivityConnector** | `graph_activity` | Graph `/auditLogs/signIns` | Partition |
