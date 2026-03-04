# Ingestion Layer — Drop-In Contract

This folder is the **drop zone for modified PAX PowerShell scripts**. Scripts placed here are the versions that have been modified by the PAX solution's AI to support Insight Harbor's config-file-driven, ADLS-aware pipeline.

> **Do not place unmodified PAX scripts here.** Reference copies are in `resources/PAX_archive/` (gitignored).

---

## Pipeline Flow

```
PAX Script (this folder)
    │
    │  Runs WITHOUT -ExplodeArrays or -ExplodeDeep flags
    │  Outputs: RAW CSV with AuditData JSON column intact
    ▼
ingestion/output/<timestamp>_RAW.csv
    │
    ▼
transform/explosion/  (Python processor — ~50x faster than PAX built-in explosion)
    │
    │  Outputs: Flat 35-column exploded CSV
    ▼
ingestion/output/<timestamp>_EXPLODED.csv
    │
    ▼
transform/bronze_to_silver_purview.py
    │
    │  Outputs: Silver CSV uploaded to ADLS
    ▼
ADLS: silver/copilot-usage/
```

---

## Script Drop-In Requirements

When dropping a modified PAX script into this folder, it **must** support all of the following:

### 1. Config File Parameter
```powershell
-ConfigFile <path>
```
Accepts a path to `insight-harbor-config.json`. All of the following must be readable from the config file:
- `auth.tenantId`
- `auth.clientId`
- `auth.clientSecret` (or `auth.certificateThumbprint`)
- `adls.storageAccountName`
- `adls.storageAccountKey`
- `adls.containerName`
- `ingestion.outputLocalPath`

Individual parameters (e.g., `-TenantId`, `-ClientId`) must still work as overrides when provided directly.

### 2. Output Destination Routing
The script must check `pax.outputDestination` from the config:
- `"Local"` — write CSV to `ingestion/output/` only (existing script behavior, unchanged).
- `"ADLS"` — write CSV to local output path **and** upload to ADLS using the Az.Storage PowerShell module, at the path:
  ```
  bronze/purview/YYYY/MM/DD/<scriptname>_<timestamp>_RAW.csv
  ```

### 3. RAW Mode Only
Scripts **must not** use `-ExplodeArrays` or `-ExplodeDeep` internally when called from Insight Harbor. The `pax.explosionMode` field in config is always `"raw"` — explosion is delegated to the Python processor.

### 4. Run Metadata File
On each successful run, output a `_run_metadata.json` alongside the CSV:
```json
{
  "scriptName": "PAX_Purview_Audit_Log_Processor.ps1",
  "scriptVersion": "1.10.7",
  "runTimestamp": "2026-03-02T02:00:00Z",
  "startDate": "2026-03-01",
  "endDate": "2026-03-02",
  "activityTypes": ["CopilotInteraction"],
  "recordCount": 4821,
  "outputFile": "PAX_Purview_20260302_020000_RAW.csv",
  "outputDestination": "ADLS",
  "adlsPath": "bronze/purview/2026/03/02/PAX_Purview_20260302_020000_RAW.csv"
}
```

### 5. Version Header in Config
The `pax.releaseVersion` field in `insight-harbor-config.json` must match the version string at the top of this script. The `check-pax-version.ps1` script uses this for upstream update detection.

---

## Currently Active Scripts

| Script File | Source PAX Script | Status |
|---|---|---|
| *(pending drop-in)* | `PAX_Purview_Audit_Log_Processor_v1.10.7.ps1` | Awaiting PAX AI mods |

---

## Adding a New Data Source

1. Get the modified PAX script from the PAX AI (using prompts in `docs/pax-ai-prompts.md`).
2. Drop it into this folder.
3. Set `sources.<sourceName>.enabled = true` in `insight-harbor-config.json`.
4. Add a new corresponding Automation Runbook following the pattern in `docs/app-registration-setup.md`.
5. If the new source requires a new Silver table, define the schema in `transform/schema/` before writing the transform.
