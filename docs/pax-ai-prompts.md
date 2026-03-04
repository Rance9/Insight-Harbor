# PAX AI Prompts — Insight Harbor Script Modifications

> These prompts are designed to be given to the **PAX solution's AI chat** to produce PAX script modifications compatible with the Insight Harbor drop-in contract defined in `ingestion/README.md`.
>
> **How to use:**
> 1. Open the PAX repository in VS Code (or wherever PAX AI is available).
> 2. Give Prompt 0 first (context-setting) — **always include this before any other prompt**.
> 3. Follow with the numbered prompts in order for the target script.
> 4. Review the diff carefully. Only accept changes described in the prompt — reject anything else.
> 5. Drop the resulting script into `ingestion/` in this repo.
>
> **Ground rule for all prompts:** Existing script behavior must be fully preserved when `-ConfigFile` is NOT provided. All new functionality is additive and gated on the presence of the config file and its values.

---

## Prompt 0 — Context Setting (Always Send First)

Send this before any modification prompt to establish context:

```
I am integrating the PAX Purview Audit Log Processor script into a solution called "Insight Harbor."
Insight Harbor is a modular M365 Copilot analytics pipeline that uses PAX scripts for data collection,
followed by a separate Python script for audit log explosion/flattening (the
Purview_M365_Usage_Bundle_Explosion_Processor), and then a Bronze-to-Silver Python transform that
loads data into Azure Data Lake Storage Gen2.

Because Insight Harbor handles explosion separately via a Python processor that is ~50x faster than
the built-in explosion, the PAX script in this integration must ALWAYS output in RAW mode (no
-ExplodeArrays or -ExplodeDeep). The Python processor will handle all explosion downstream.

I need you to make additive, non-breaking modifications to PAX_Purview_Audit_Log_Processor.ps1.
All existing behavior must be preserved exactly when the new -ConfigFile parameter is not used.
All new functionality is gated behind the presence of the -ConfigFile parameter.

The script version is v1.10.7. Please confirm you understand before I give you the specific changes.
```

---

## Prompt 1 — Add `-ConfigFile` Parameter and Config Loading

```
Modification 1 of 4: Add -ConfigFile parameter and config loading.

Add a new optional parameter `-ConfigFile` (string, default empty) to the script's param block.
When -ConfigFile is provided:
  - Read and parse the JSON file at that path using ConvertFrom-Json.
  - If the file does not exist or is not valid JSON, write an error message and exit with code 1.
  - Store the parsed object in a script-scoped variable: $script:IHConfig

From $script:IHConfig, extract and apply the following values IF the corresponding
standard script parameters were not already explicitly provided by the caller
(i.e., only set them from config when the parameter was left at its default value):
  - auth.tenantId          → maps to the script's -TenantId / -Organization parameter
  - auth.clientId          → maps to the script's -AppId / -ClientId parameter
  - auth.clientSecret      → maps to the script's -ClientSecret parameter
  - auth.certificateThumbprint → maps to the script's -CertificateThumbprint parameter
  - ingestion.outputLocalPath → maps to -OutputPath (only if OutputPath was not explicitly provided)

If -ConfigFile is not provided, $script:IHConfig is $null and the script runs entirely as before.

Show me only the param block changes and the config-loading block you would add (near the top of
the script, after param block and before the main execution logic). Do not show the full script.
```

---

## Prompt 2 — Add ADLS Upload Function and Output Routing

```
Modification 2 of 4: Add ADLS upload function and post-export output routing.

After the script successfully writes a CSV output file, add the following behavior when
$script:IHConfig is not $null AND $script:IHConfig.pax.outputDestination -eq "ADLS":

1. Construct the ADLS blob path using this pattern:
   bronze/purview/{YYYY}/{MM}/{DD}/{OriginalFileName}
   where YYYY/MM/DD is the UTC date the script ran (not the data date range).
   Example: bronze/purview/2026/03/02/PAX_Purview_CopilotInteraction_20260302_020000.csv

2. Upload the CSV to ADLS Gen2 using the Az.Storage PowerShell module:
   - Storage account name: $script:IHConfig.adls.storageAccountName
   - Storage account key:  $script:IHConfig.adls.storageAccountKey
   - Container name:       $script:IHConfig.adls.containerName
   - Blob path:            constructed in step 1

   Use this pattern:
   $storageCtx = New-AzStorageContext `
       -StorageAccountName $script:IHConfig.adls.storageAccountName `
       -StorageAccountKey  $script:IHConfig.adls.storageAccountKey
   Set-AzStorageBlobContent `
       -Context   $storageCtx `
       -Container $script:IHConfig.adls.containerName `
       -File      $localCsvPath `
       -Blob      $adlsBlobPath `
       -Force

3. Write a status line to the console indicating success or failure of the upload.
   On failure, write a warning but do NOT exit — the local CSV must still be preserved.

4. If $script:IHConfig.pax.outputDestination -eq "Local" or $script:IHConfig is $null,
   skip all ADLS upload logic entirely.

5. Check if the Az.Storage module is available before attempting the upload.
   If not installed, write a warning message:
   "WARNING: Az.Storage module not found. ADLS upload skipped. Install with: Install-Module Az.Storage -Scope CurrentUser"
   Then skip the upload without erroring.

Show me only the new ADLS upload function and the call site where it is invoked
after CSV export completion. Do not show unrelated sections.
```

---

## Prompt 3 — Emit Run Metadata JSON

```
Modification 3 of 4: Emit a run metadata JSON file.

After each successful CSV export (and after any ADLS upload attempt), when $script:IHConfig
is not $null, write a companion metadata file alongside the local CSV output.

The metadata file name must be: {OriginalCsvFileNameWithoutExtension}_run_metadata.json
Example: PAX_Purview_CopilotInteraction_20260302_020000_run_metadata.json

The metadata file content must be this exact JSON structure:
{
  "scriptName":        "<the .ps1 filename without path>",
  "scriptVersion":     "<script version string from the version variable at top of script>",
  "insightHarborVersion": "<$script:IHConfig.solution.version or 'unknown' if not present>",
  "runTimestampUtc":   "<ISO 8601 UTC timestamp of run start>",
  "startDate":         "<-StartDate value used>",
  "endDate":           "<-EndDate value used>",
  "activityTypes":     ["<activity type 1>", "<activity type 2>"],
  "recordCount":       <integer record count exported>,
  "outputFile":        "<csv filename only, no path>",
  "outputLocalPath":   "<full local path to csv>",
  "outputDestination": "<'Local' or 'ADLS'>",
  "adlsBlobPath":      "<the blob path if uploaded to ADLS, otherwise null>",
  "uploadSuccess":     <true|false|null — null if destination is Local>
}

- If $script:IHConfig is $null (no config file), skip the metadata file entirely.
- Write the metadata file using ConvertTo-Json -Depth 5 | Out-File with UTF8 encoding.
- If writing the metadata file fails, write a warning but do not exit.

Show me only the metadata-writing code block and where it is placed in the script.
```

---

## Prompt 4 — Block Explosion Flags When Called from Insight Harbor

```
Modification 4 of 4: Enforce RAW-only output mode when called from Insight Harbor config.

When $script:IHConfig is not $null AND $script:IHConfig.pax.explosionMode -eq "raw":
  - If the caller passed -ExplodeArrays or -ExplodeDeep as parameters, print a warning:
    "WARNING [Insight Harbor]: explosionMode is set to 'raw' in config. -ExplodeArrays and -ExplodeDeep
     are disabled. Explosion is handled downstream by the Python processor. Proceeding in RAW mode."
  - Then force both $ExplodeArrays and $ExplodeDeep to $false regardless of what was passed.
  - This ensures the output is always a RAW CSV with the AuditData JSON column intact.

When $script:IHConfig is $null, this block must be completely bypassed — existing explosion
behavior is unchanged.

This check must happen early in the script execution flow, before any explosion logic runs.

Show me only this guard block and where it should be placed.
```

---

## Prompt 5 — Final Review Request

After all four modifications are applied, send this final prompt:

```
Please provide a summary of all changes made across the four modifications, confirming:
1. The param block additions (new -ConfigFile parameter)
2. The config loading block location in the script
3. The ADLS upload function name and call site
4. The metadata file write location
5. The explosion guard block location
6. Confirmation that no existing parameters, logic, or output behavior was changed when -ConfigFile is not used
7. Any Az module dependencies added and how they are handled if not installed

Also confirm the script still works correctly when called with all existing parameter combinations
(e.g., -StartDate, -EndDate, -ActivityTypes, -ExplodeArrays without -ConfigFile).
```

---

## Notes on Script Scope

The PAX Purview script is approximately **17,000 lines**. When providing these prompts:
- Ask PAX AI to show **only the changed sections and their surrounding context** (not the full script).
- Review each diff carefully before accepting.
- The ADLS upload and metadata write are the only places where **new lines of code** are injected into existing execution paths. Everything else is purely additive.

## Expected Output Files After Integration

When the script runs via Insight Harbor config (`OutputDestination = "ADLS"`), the `ingestion/output/` folder will contain:

```
ingestion/output/
├── PAX_Purview_CopilotInteraction_20260302_020000.csv          ← RAW (AuditData JSON preserved)
└── PAX_Purview_CopilotInteraction_20260302_020000_run_metadata.json
```

These are then consumed by `transform/explosion/` (Python) and `transform/bronze_to_silver_purview.py`.

