# Silver Schema: `silver_copilot_usage`

**Source:** `bronze/exploded/YYYY/MM/DD/*.csv` (153-column output from Python explosion processor)
**Output:** `silver/copilot-usage/silver_copilot_usage.csv` (append, deduplicated)
**Produced by:** `transform/bronze_to_silver_purview.py`

## Column Pass-Through Strategy

**All Bronze columns are preserved in the Silver output — no trimming.**

The Silver layer dynamically inherits every column from the exploded Bronze CSV.
If the explosion processor adds new columns in the future, they flow through to
Silver automatically. On top of the Bronze columns, the transform adds computed
columns and Entra enrichment columns (listed below).

---

## Computed Columns (added / overwritten by transform)

| Column | Logic |
|---|---|
| `UsageDate` | `CreationTime.date()` (UTC) |
| `UsageHour` | `CreationTime.hour` (UTC, 0–23) |
| `PromptType` | `"Prompt"` if `Message_isPrompt == True`; `"Response"` if `False`; `"Interaction"` if null |
| `IsAgent` | `"TRUE"` if `AgentId` is not null/empty; otherwise `"FALSE"` |
| `_SourceFile` | Filename only (no path) of the source Bronze exploded CSV |
| `_LoadedAtUtc` | `datetime.now(UTC)` at time of Silver transform run |

## Entra Enrichment Columns (from LEFT JOIN with `silver_entra_users`)

| Column | Source |
|---|---|
| `Department` | Entra user profile |
| `JobTitle` | Entra user profile |
| `Country` | Entra user profile |
| `City` | Entra user profile |
| `ManagerDisplayName` | Entra manager relationship |
| `Division` | Entra `employeeOrgData.division` |
| `CostCenter` | Entra `employeeOrgData.costCenter` |
| `HasCopilotLicense` | Computed from `AssignedLicenses` |
| `LicenseTier` | Computed license tier grouping |
| `CompanyName` | Entra user profile |

## Numeric Columns (cast to integer)

`TurnNumber`, `TokensTotal`, `TokensInput`, `TokensOutput`, `DurationMs`

---

## Deduplication Key

Rows are deduplicated on **`RecordId` + `Message_Id`** (composite):
- `RecordId` alone is not sufficient because one audit record may produce multiple rows after explosion (one per Copilot message).
- `Message_Id` can be null for non-message rows (resource context rows). For those, `RecordId` + `Message_Id` (null) is treated as a single composite key.

---

## Local Artifact Cleanup

After successful ADLS upload, the transform automatically cleans up local
intermediate files (Silver CSV, metadata JSON). The orchestrator
(`run-pipeline-local.ps1`) performs a final sweep of `ingestion/output/` to
remove exploded CSVs, logs, and other pipeline artifacts. PAX checkpoint files
and original source data are preserved.

---

## Future Silver Tables

| Table | Source Script | Phase |
|---|---|---|
| `silver_entra_users` | PAX Graph / Entra API | Phase 4+ |
| `silver_graph_audit` | `PAX_Graph_Audit_Log_Processor.ps1` | Future |
| `silver_copilot_content` | `PAX_CopilotInteractions_Content_Audit_Log_Processor.ps1` | Future |

The `silver_entra_users` table joins to `silver_copilot_usage` on `UserId` to enrich records with `Department`, `JobTitle`, `Manager`, `Country`, `LicenseTier`, and `HasCopilotLicense`. This join enables the **Adoption by Department**, **Adoption Rate**, and **Geographic Distribution** metrics.

---

## Power BI Measures Based on This Schema

| Measure Name | DAX Pattern |
|---|---|
| `[Monthly Active Users]` | `DISTINCTCOUNT(silver_copilot_usage[UserId])` scoped to month |
| `[Daily Active Users]` | `DISTINCTCOUNT(silver_copilot_usage[UserId])` scoped to date |
| `[Total Interactions]` | `COUNTROWS(silver_copilot_usage)` filtered to `PromptType = "Interaction"` or all rows |
| `[Agent Adoption %]` | `DIVIDE(CALCULATE(COUNTROWS(...), IsAgent=TRUE), COUNTROWS(...))` |
| `[Avg Response Latency]` | `AVERAGEX(FILTER(..., DurationMs <> BLANK()), DurationMs)` |
| `[Avg Tokens per Interaction]` | `AVERAGEX(FILTER(..., TokensTotal <> BLANK()), TokensTotal)` |
