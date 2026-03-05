# PAX Purview Audit Log Processor v1.10.7 (IH) — Exhaustive Audit Report

> **Script**: `ingestion/PAX_Purview_Audit_Log_Processor_v1.10.7_IH.ps1`  
> **Lines**: 17,417  
> **Version**: 1.10.7  
> **Audit Date**: 2025-06-11

---

## Table of Contents

1. [All Script Parameters](#1-all-script-parameters)
2. [-IncludeM365Usage Switch](#2--includem365usage-switch)
3. [-OnlyUserInfo Switch](#3--onlyuserinfo-switch)
4. [Pipeline Invocation Details](#4-pipeline-invocation-details)
5. [Output Format Details](#5-output-format-details)
6. [Key Behavioral Switches](#6-key-behavioral-switches)
7. [Record Processing Pipeline](#7-record-processing-pipeline)
8. [Insight Harbor Modifications](#8-insight-harbor-modifications)

---

## 1. All Script Parameters

The `param()` block spans **lines 1025–1312**. Every parameter is `[Parameter(Mandatory = $false)]`. Parameters are grouped by functional area below.

### 1.1 Date Range

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$StartDate` | `string` | *(none)* | — | Start of audit query window (parsed to DateTime internally) |
| `$EndDate` | `string` | *(none)* | — | End of audit query window |

### 1.2 Output

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$OutputPath` | `string` | `"C:\Temp\"` | — | Directory for all output files |

### 1.3 Authentication

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$Auth` | `string` | `'WebLogin'` | `ValidateSet('WebLogin','DeviceCode','Credential','Silent','AppRegistration')` | Auth method |
| `$TenantId` | `string` | *(none)* | — | Azure AD tenant ID |
| `$ClientId` | `string` | *(none)* | — | App registration client ID |
| `$ClientSecret` | `string` | *(none)* | — | App registration client secret |
| `$ClientCertificateThumbprint` | `string` | *(none)* | — | Client certificate thumbprint for cert-based auth |
| `$ClientCertificateStoreLocation` | `string` | `'CurrentUser'` | — | Certificate store location |
| `$ClientCertificatePath` | `string` | *(none)* | — | Path to PFX certificate file |
| `$ClientCertificatePassword` | `SecureString` | *(none)* | — | Password for PFX certificate |

### 1.4 Query Partitioning & Throttling

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$BlockHours` | `double` | `0.5` | `ValidateRange(0.016667, 24)` | Time-window width per query block (hours). Min ~1 minute, max 24h |
| `$PartitionHours` | `int` | `0` | `ValidateRange(1, 72)` | Override: fixed partition window size in hours (0 = auto) |
| `$MaxPartitions` | `int` | `160` | `ValidateRange(1, 1000)` | Max number of time-window partitions created |
| `$ResultSize` | `int` | `10000` | `ValidateRange(1, 10000)` | Max records per API page (Graph API hard cap: 10,000) |
| `$PacingMs` | `int` | `0` | `ValidateRange(0, 10000)` | Artificial delay (ms) between API calls to avoid throttling |

### 1.5 Activity / Record / Service Filters

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$ActivityTypes` | `string[]` | `@('CopilotInteraction')` | — | Activity type operations to query. Comma-separated values auto-split |
| `$RecordTypes` | `string[]` | *(none)* | — | Audit record types filter. Canonicalized through `$recordTypeCanonicalMap` |
| `$ServiceTypes` | `string[]` | *(none)* | — | Service/workload filter for Graph API `serviceFilter` |

### 1.6 Explosion (Column Expansion)

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$ExplodeArrays` | `switch` | `$false` | — | Expand AuditData arrays into separate rows (153-column schema) |
| `$ExplodeDeep` | `switch` | `$false` | — | Deep explosion: extends ExplodeArrays with dynamic nested property flattening |
| `$FlatDepth` | `int` | `120` | — | Max nested depth for JSON flattening during explosion |
| `$ExplosionThreads` | `int` | `0` | `ValidateRange(0, 32)` | Thread count for parallel explosion: 0 = auto-detect (2–16 based on CPU), 1 = serial |

### 1.7 Parallel Processing

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$MaxConcurrency` | `int` | `10` | — | Concurrent Graph API partition queries or EOM serial query limit |
| `$EnableParallel` | `switch` | `$false` | — | (Legacy) Enable parallel processing. Superseded by `$ParallelMode` |
| `$MaxParallelGroups` | `int` | `8` | `ValidateRange(0, 50)` | Max concurrent activity-type groups processed simultaneously |
| `$ParallelMode` | `string` | `'Auto'` | `ValidateSet('Off','On','Auto')` | `Auto` = PS7+ auto-engages parallel; `Off` = serial; `On` = force parallel |

### 1.8 Adaptive Concurrency Controls

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$DisableAdaptive` | `switch` | `$false` | — | Disable all adaptive safeguards (memory, latency, concurrency smoothing) |
| `$ProgressSmoothingAlpha` | `double` | `0.3` | `ValidateRange(0.0, 1.0)` | EMA weight for smoothing dynamic progress total. 0 = off |
| `$HighLatencyMs` | `int` | `90000` | `ValidateRange(1000, 600000)` | Partition avg latency (ms) that triggers mild concurrency reduction |
| `$MemoryPressureMB` | `int` | `1500` | `ValidateRange(256, 32768)` | Working-set (MB) threshold that triggers concurrency reduction |
| `$MaxMemoryMB` | `int` | `-1` | `ValidateRange(-1, 65536)` | Max process memory (MB) before flushing to disk. -1 = auto 75%, 0 = disabled |
| `$StatusIntervalSeconds` | `int` | `60` | `ValidateRange(30, 600)` | Status display interval during polling and backpressure waits |
| `$LowLatencyMs` | `int` | `20000` | `ValidateRange(100, 600000)` | Sustained low-latency threshold for concurrency step-up consideration |
| `$LowLatencyConsecutive` | `int` | `2` | `ValidateRange(1, 10)` | Consecutive low-latency groups required before stepping up concurrency |
| `$ThroughputDropPct` | `int` | `15` | `ValidateRange(1, 100)` | Percent throughput drop vs baseline (with high latency) to justify reduction |
| `$ThroughputSmoothingAlpha` | `double` | `0.3` | `ValidateRange(0.0, 1.0)` | EMA smoothing for throughput baseline |
| `$AdaptiveConcurrencyCeiling` | `int` | `6` | `ValidateRange(1, 50)` | Upper bound for adaptive concurrency step-ups |

### 1.9 Streaming Export

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$StreamingSchemaSample` | `int` | `5000` | `ValidateRange(100, 50000)` | Number of records sampled to determine schema for streaming CSV |
| `$StreamingChunkSize` | `int` | `5000` | `ValidateRange(100, 50000)` | Batch size for streaming writes to disk |
| `$ExportProgressInterval` | `int` | `10` | `ValidateRange(1, 10000)` | Progress display interval during CSV export |

### 1.10 Content Filtering (Copilot-specific)

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$AgentId` | `string[]` | *(none)* | — | Filter to specific Copilot Agent IDs |
| `$AgentsOnly` | `switch` | `$false` | — | Only include records with a non-null AgentId |
| `$ExcludeAgents` | `switch` | `$false` | — | Exclude all records with an AgentId |
| `$PromptFilter` | `string` | *(none)* | `ValidateSet('Prompt','Response','Both','Null')` | Filter Copilot interaction content type |
| `$UserIds` | `string[]` | *(none)* | — | Filter to specific user principal names |
| `$GroupNames` | `string[]` | *(none)* | — | Filter to members of specific Entra ID groups |

### 1.11 Reliability / Circuit Breaker

| Parameter | Type | Default | Validation | Description |
|-----------|------|---------|------------|-------------|
| `$CircuitBreakerThreshold` | `int` | `5` | `ValidateRange(1, 50)` | Consecutive block failures before tripping circuit breaker |
| `$CircuitBreakerCooldownSeconds` | `int` | `120` | `ValidateRange(5, 3600)` | Cooldown duration (seconds) after breaker trips |
| `$BackoffBaseSeconds` | `double` | `1.0` | `ValidateRange(0.1, 120)` | Base seconds for exponential backoff between block retries |
| `$BackoffMaxSeconds` | `int` | `45` | `ValidateRange(1, 600)` | Max cap for exponential backoff delay |
| `$MaxNetworkOutageMinutes` | `int` | `30` | — | Max continuous network outage tolerated during async polling/retrieval |

### 1.12 Activity Type Composition Switches

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `$IncludeCopilotInteraction` | `switch` | `$false` | Explicitly add `CopilotInteraction` to activity list (redundant since it's the default) |
| `$IncludeM365Usage` | `switch` | `$false` | Add the full M365 usage activity bundle (~120 activity types) — see Section 2 |
| `$IncludeDSPMForAI` | `switch` | `$false` | Add DSPM activity types: `ConnectedAIAppInteraction`, `AIInteraction`, `AIAppInteraction` |
| `$ExcludeCopilotInteraction` | `switch` | `$false` | Remove `CopilotInteraction` from the final list (overrides all inclusion logic) |

### 1.13 Output Mode

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `$ExportWorkbook` | `switch` | `$false` | Export to `.xlsx` (requires `ImportExcel` module) instead of `.csv` |
| `$AppendFile` | `string` | *(none)* | Append data to an existing CSV or Excel file. File must already exist |
| `$CombineOutput` | `switch` | `$false` | Merge all activity types into a single output file/tab instead of per-type files |

### 1.14 Operational Flags

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `$Help` | `switch` | `$false` | Display help text and exit |
| `$Force` | `switch` | `$false` | Bypass all interactive prompts (PAYG warning, conflict resolution) |
| `$SkipDiagnostics` | `switch` | `$false` | Skip pre-query capability diagnostic checks |
| `$UseEOM` | `switch` | `$false` | Use Exchange Online Management (`Search-UnifiedAuditLog`) instead of Graph API |
| `$IncludeUserInfo` | `switch` | `$false` | Add Entra user directory + license data as a separate output file/tab |
| `$OnlyUserInfo` | `switch` | `$false` | Export ONLY Entra user data — skip all audit log retrieval (see Section 3) |
| `$IncludeTelemetry` | `switch` | `$false` | Export Graph API partition telemetry CSV for timing analysis |

### 1.15 Offline / Replay

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `$RAWInputCSV` | `string` | *(none)* | Path to a raw CSV for offline replay (bypasses live API calls) |

### 1.16 Metrics

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `$EmitMetricsJson` | `switch` | `$false` | Emit structured `_metrics_<timestamp>.json` alongside output |
| `$MetricsPath` | `string` | *(none)* | Override metrics output path |
| `$AutoCompleteness` | `switch` | `$false` | Aggressively subdivide windows still at server 10K limit until below threshold |

### 1.17 Resume (Manual Parsing)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `$RemainingArgs` | `string[]` | *(none)* | `ValueFromRemainingArguments`. Captures `-Resume` and its optional path argument |

> **`-Resume` is NOT a standard param-block parameter.** It is parsed manually from `$RemainingArgs` (lines 1316–1330). Supports two forms:
> - `-Resume` — auto-discover checkpoint in `$OutputPath`
> - `-Resume "path\to\checkpoint"` — explicit checkpoint file

### 1.18 Insight Harbor Extension

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `$ConfigFile` | `string` | `""` | Path to Insight Harbor JSON configuration file. **Added by IH MOD.** |

### Non-Existent Parameters (Clarification)

The following parameters were investigated and **DO NOT exist** in the script's param block:

| Name | Status | Correct Alternative |
|------|--------|-------------------|
| `-LookbackDays` | **Does not exist** | `ingestion.defaultLookbackDays` in IH config JSON; the pipeline orchestrator computes `$StartDate`/`$EndDate` from it |
| `-UseGraphApi` | **Does not exist** | Graph API is the default. Use `-UseEOM` to switch to Exchange Online Management |
| `-RawJsonOutput` | **Does not exist** | Config `pax.explosionMode = "raw"` controls raw output via IH MOD 4 |
| `-CheckpointPath` | **Does not exist as a param** | `$script:CheckpointPath` is an internal script-scoped variable (not user-configurable) |
| `-ResumeFromCheckpoint` | **Does not exist** | Use `-Resume` or `-Resume "path"` (manual parsing from `$RemainingArgs`) |
| `-IncludeEntraUsers` | **Does not exist** | The correct parameter is `-IncludeUserInfo` |

---

## 2. -IncludeM365Usage Switch

**Location**: Param block line ~1275; activation logic lines 1436–1462; bundle definition lines 1740–1830.

### What It Does

When `-IncludeM365Usage` is present, the script:

1. **Appends the `$m365UsageActivityBundle`** (~120+ unique activity types) to `$ActivityTypes`
2. **Sets `$RecordTypes`** to the `$m365UsageRecordBundle` (14 record types), merged with any user-supplied RecordTypes
3. **Forces `$ServiceTypes = $null`** — sends NO `serviceFilter` to Graph API so all workloads return in a single pass per partition (critical performance optimization)
4. `CopilotInteraction` is still auto-included unless `-ExcludeCopilotInteraction` is also specified

### M365 Usage Activity Bundle (`$m365UsageActivityBundle`)

Defined at lines 1740–1830, covering 8+ workloads:

**Exchange/Email (8)**:
`MailboxLogin`, `MailItemsAccessed`, `Send`, `SendOnBehalf`, `SoftDelete`, `HardDelete`, `MoveToDeletedItems`, `CopyToFolder`

**SharePoint/OneDrive — Files (11)**:
`FileAccessed`, `FileDownloaded`, `FileUploaded`, `FileModified`, `FileDeleted`, `FileMoved`, `FileCheckedIn`, `FileCheckedOut`, `FileRecycled`, `FileRestored`, `FileVersionsAllDeleted`

**SharePoint/OneDrive — Sharing (8)**:
`SharingSet`, `SharingInvitationCreated`, `SharingInvitationAccepted`, `SharedLinkCreated`, `SharingRevoked`, `AddedToSecureLink`, `RemovedFromSecureLink`, `SecureLinkUsed`

**Groups/Unified Groups (2)**:
`AddMemberToUnifiedGroup`, `RemoveMemberFromUnifiedGroup`

**Teams — Team/Channel Management (17)**:
`TeamCreated`, `TeamDeleted`, `TeamArchived`, `TeamSettingChanged`, `TeamMemberAdded`, `TeamMemberRemoved`, `MemberAdded`, `MemberRemoved`, `MemberRoleChanged`, `ChannelAdded`, `ChannelDeleted`, `ChannelSettingChanged`, `ChannelOwnerResponded`, `ChannelMessageSent`, `ChannelMessageDeleted`, `BotAddedToTeam`, `BotRemovedFromTeam`, `TabAdded`, `TabRemoved`, `TabUpdated`, `ConnectorAdded`, `ConnectorRemoved`, `ConnectorUpdated`

**Teams — Chat/Messaging (13)**:
`TeamsSessionStarted`, `ChatCreated`, `ChatRetrieved`, `ChatUpdated`, `MessageSent`, `MessageRead`, `MessageDeleted`, `MessageUpdated`, `MessagesListed`, `MessageCreation`, `MessageCreatedHasLink`, `MessageEditedHasLink`, `MessageHostedContentRead`, `MessageHostedContentsListed`, `SensitiveContentShared`

**Teams — Meeting Lifecycle (14)**:
`MeetingCreated`, `MeetingUpdated`, `MeetingDeleted`, `MeetingStarted`, `MeetingEnded`, `MeetingParticipantJoined`, `MeetingParticipantLeft`, `MeetingParticipantRoleChanged`, `MeetingRecordingStarted`, `MeetingRecordingEnded`, `MeetingDetail`, `MeetingParticipantDetail`, `LiveNotesUpdate`, `AINotesUpdate`, `RecordingExported`, `TranscriptsExported`

**Teams — Apps/Approvals (5)**:
`AppInstalled`, `AppUpgraded`, `AppUninstalled`, `CreatedApproval`, `ApprovedRequest`, `RejectedApprovalRequest`, `CanceledApprovalRequest`

**Office Apps — Word/Excel/PowerPoint (5)**:
`Create`, `Edit`, `Open`, `Save`, `Print`

**Microsoft Forms (8)**:
`CreateForm`, `EditForm`, `DeleteForm`, `ViewForm`, `CreateResponse`, `SubmitResponse`, `ViewResponse`, `DeleteResponse`

**Microsoft Stream (4)**:
`StreamModified`, `StreamViewed`, `StreamDeleted`, `StreamDownloaded`

**Planner (8)**:
`PlanCreated`, `PlanDeleted`, `PlanModified`, `TaskCreated`, `TaskDeleted`, `TaskModified`, `TaskAssigned`, `TaskCompleted`

**Power Apps (5)**:
`LaunchedApp`, `CreatedApp`, `EditedApp`, `DeletedApp`, `PublishedApp`

**Copilot (1)**:
`CopilotInteraction`

### M365 Usage Record Type Bundle (`$m365UsageRecordBundle`)

14 record types (line 1738):
```
ExchangeAdmin, ExchangeItem, ExchangeMailbox, SharePointFileOperation,
SharePointSharingOperation, SharePoint, OneDrive, MicrosoftTeams,
OfficeNative, MicrosoftForms, MicrosoftStream, PlannerPlan, PlannerTask, PowerAppsApp
```

### ServiceTypes Behavior

When `-IncludeM365Usage` is active, `$ServiceTypes` is explicitly forced to `$null` (line 1453). The code comment explains:

> *"CRITICAL: Do NOT set ServiceTypes for M365 usage mode - Graph API should get ALL workloads in single pass. Multiple serviceFilter values cause unnecessary workload splits."*

---

## 3. -OnlyUserInfo Switch

**Location**: Param block line ~1295; validation lines 1502–1600; filename logic line 7665.

### Behavior

When `-OnlyUserInfo` is specified:

1. **Sets `$IncludeUserInfo = $true`** (forces Entra user retrieval)
2. **Sets `$ActivityTypes = @()`** (empty array — no audit log retrieval)
3. **Skips all audit query logic** — no time windows, no Graph API queries, no record processing
4. **Output filename**: `EntraUsers_MAClicensing_YYYYMMDD_HHMMSS.csv` (or `.xlsx` with `-ExportWorkbook`)

### Incompatible Parameters

The script performs exhaustive validation (lines 1508–1592) and **exits with error** if any of the following are combined with `-OnlyUserInfo`:

**Date Parameters**: `StartDate`, `EndDate`  
**Activity Config**: `ActivityTypes`, `IncludeM365Usage`, `IncludeDSPMForAI`, `ExcludeCopilotInteraction`  
**Query Settings**: `BlockHours` (non-default), `PartitionHours` (non-default), `MaxPartitions` (non-default), `ResultSize` (non-default), `PacingMs` (non-default), `AutoCompleteness`, `StreamingSchemaSample` (non-default), `StreamingChunkSize` (non-default), `ExportProgressInterval` (non-default)  
**Filters**: `AgentId`, `AgentsOnly`, `ExcludeAgents`, `PromptFilter`, `UserIds`, `GroupNames`, `RecordTypes`, `ServiceTypes`  
**Processing Modes**: `ExplodeArrays`, `ExplodeDeep`, `RAWInputCSV`  
**Parallel**: `EnableParallel`, `MaxConcurrency` (non-default), `MaxParallelGroups` (non-default), `ParallelMode` (non-default), `DisableAdaptive`, and all adaptive tuning params at non-default values  
**Reliability** (non-default): `CircuitBreakerThreshold`, `CircuitBreakerCooldownSeconds`, `BackoffBaseSeconds`, `BackoffMaxSeconds`  
**Modes**: `UseEOM`, `CombineOutput`, `AppendFile`

### Compatible Parameters

Only these are allowed with `-OnlyUserInfo`:
- `OutputPath`, `Auth`, `ExportWorkbook`, `Force`, `MaxNetworkOutageMinutes`, `EmitMetricsJson`, `MetricsPath`, `SkipDiagnostics`, `ConfigFile`

### Entra Users Schema (47 columns)

Defined at lines 8900–8930 in `$EntraUsersHeader`:

**Core User Properties (30)**:
`userPrincipalName`, `DisplayName`, `id`, `Email`, `givenName`, `surname`, `JobTitle`, `department`, `employeeType`, `employeeId`, `employeeHireDate`, `officeLocation`, `city`, `state`, `Country`, `postalCode`, `companyName`, `employeeOrgData_division`, `employeeOrgData_costCenter`, `accountEnabled`, `userType`, `createdDateTime`, `usageLocation`, `preferredLanguage`, `onPremisesSyncEnabled`, `onPremisesImmutableId`, `externalUserState`, `proxyAddresses_Primary`, `proxyAddresses_Count`, `proxyAddresses_All`

**Manager Properties (5)**:
`manager_id`, `manager_displayName`, `manager_userPrincipalName`, `manager_mail`, `manager_jobTitle`

**License Properties (2)**:
`assignedLicenses`, `HasLicense`

**Power BI Template Compatibility (10)**:
`ManagerID`, `BusinessAreaLabel`, `CountryofEmployment`, `CompanyCodeLabel`, `CostCentreLabel`, `UserName`, `EffectiveDate`, `FunctionType`, `BusinessAreaCode`, `OrgLevel_3Label`

> The last 4 fields (`EffectiveDate`, `FunctionType`, `BusinessAreaCode`, `OrgLevel_3Label`) are null placeholders for Viva Insights compatibility.

---

## 4. Pipeline Invocation Details

**Orchestrator**: `scripts/run-pipeline-local.ps1` (515 lines, 4 stages + cleanup)

### Stage 1 — Purview Audit Retrieval (line ~274)

```powershell
pwsh -File $paxScript.FullName -ConfigFile $resolvedConfigFile -Auth AppRegistration
```

- Invokes PAX script with **only** `-ConfigFile` and `-Auth AppRegistration`
- All other settings (TenantId, ClientId, ClientSecret, OutputPath) are resolved from the config file via **IH MOD 1**
- `explosionMode = "raw"` in config → IH MOD 4 disables `-ExplodeArrays` and `-ExplodeDeep`
- Output: Raw 8-column CSV in `OutputPath`

### Stage 1B — Entra User Directory (line ~308)

```powershell
pwsh -File $paxScript.FullName -ConfigFile $resolvedConfigFile -OnlyUserInfo -Auth AppRegistration
```

- Same as Stage 1 but with **`-OnlyUserInfo`** added
- Skips all audit retrieval, exports only Entra user/license data
- Output: `EntraUsers_MAClicensing_YYYYMMDD_HHMMSS.csv`

### Stage 2 — Python Explosion (`transform/explosion/pipeline_explode.py`)

- Takes the raw 8-column CSV from Stage 1
- Performs Python-based column explosion (equivalent of `-ExplodeArrays` but in Python)
- Output: Exploded CSV with 153+ columns

### Stage 2B — Entra Bronze→Silver (`transform/bronze_to_silver_entra.py`)

- Transforms Entra user CSV from Stage 1B to Silver schema

### Stage 3 — Purview Bronze→Silver (`transform/bronze_to_silver_purview.py`)

- Transforms exploded Purview CSV from Stage 2 to Silver schema

### Cleanup

- Removes intermediate files but **preserves checkpoint files**
- Sends Teams notification (Adaptive Card via webhook) on success or failure

### Config-Driven Date Range

The pipeline orchestrator reads `ingestion.defaultLookbackDays` from `config/insight-harbor-config.json` and computes `$StartDate`/`$EndDate` before invoking PAX. The PAX script itself has no `-LookbackDays` parameter.

---

## 5. Output Format Details

### 5.1 Filename Patterns

All filenames include `$global:ScriptRunTimestamp` = `yyyyMMdd_HHmmss`.

| Mode | Filename Pattern |
|------|-----------------|
| Standard (no explosion) | `Purview_Audit_YYYYMMDD_HHMMSS.csv` |
| CombineOutput | `Purview_Audit_CombinedUsageActivity_YYYYMMDD_HHMMSS.csv` |
| CombineOutput + IncludeUserInfo | `Purview_Audit_CombinedUsageActivity_EntraUsers_MAClicensing_YYYYMMDD_HHMMSS.xlsx` |
| OnlyUserInfo | `EntraUsers_MAClicensing_YYYYMMDD_HHMMSS.csv` |
| ExportWorkbook (multi-tab) | `Purview_Audit_MultiTab_YYYYMMDD_HHMMSS.xlsx` |
| AppendFile | Uses existing file path as-is (must already exist) |
| Per-activity split | `Purview_Audit_<ActivityType>_YYYYMMDD_HHMMSS.csv` (one per activity type) |

### 5.2 Non-Explosion Schema (8 columns)

When neither `-ExplodeArrays` nor `-ExplodeDeep` is set (the default in IH pipeline):

```
RecordId, CreationDate, RecordType, Operation, UserId, AuditData,
AssociatedAdminUnits, AssociatedAdminUnitsNames
```

The `AuditData` column contains the full raw JSON string (compressed, `ConvertTo-Json -Depth 100 -Compress`). This is the format used by the Insight Harbor pipeline (`explosionMode = "raw"`).

### 5.3 Exploded Schema (153 columns)

When `-ExplodeArrays` is active, each record is expanded via `Convert-ToPurviewExplodedRecords` (line ~8963) into the `$PurviewExplodedHeader` schema (lines 8760–8816). This includes flattened AuditData properties across:

- Core audit fields (RecordId, CreationDate, etc.)
- User/tenant fields (UserId, UserType, OrganizationId, etc.)
- Copilot-specific fields (AppHost, Contexts, ThreadId, etc.)
- CopilotEventData nested fields (Messages array expanded)
- AccessedResources array expanded

### 5.4 Incremental Storage (JSONL)

During Graph API retrieval, records are stored incrementally as JSONL:
- **Per-page memory flush**: `<incrementalDir>/Part<N>_<timestamp>_qid-<queryId>_<jobRunId>.jsonl`  
- **Rolling safety snapshots**: Every 500 pages, a backup snapshot `.jsonl` is written
- After successful completion, JSONL files from the current run are cleaned up

### 5.5 Log File

Every run produces a `.log` file adjacent to the output file. All `Write-Host` calls are mirrored to the log via a global `Write-Host` override function (lines 1880–1910).

### 5.6 Metrics JSON

With `-EmitMetricsJson`, a `_metrics_<timestamp>.json` is written containing:
- Script version, parameters snapshot
- Total structured rows, elapsed time, partition telemetry

### 5.7 Run Metadata JSON (IH MOD 3)

For Insight Harbor runs, a `_run_metadata.json` is written per-CSV with:
`scriptName`, `scriptVersion`, `insightHarborVersion`, `runTimestampUtc`, `startDate`, `endDate`, `activityTypes`, `recordCount`, `outputFile`, `outputLocalPath`, `outputDestination`, `adlsBlobPath`, `uploadSuccess`

---

## 6. Key Behavioral Switches

### Authentication Flow (`-Auth`)

| Value | Method |
|-------|--------|
| `WebLogin` | Interactive browser-based MSAL login (default) |
| `DeviceCode` | Device-code flow for headless environments |
| `Credential` | Username/password (legacy, limited support) |
| `Silent` | MSAL silent token with cached credentials |
| `AppRegistration` | Client credentials (secret or certificate) — **used by IH pipeline** |

### API Mode (`-UseEOM`)

| Flag | API | Behavior |
|------|-----|----------|
| Default (no flag) | **Microsoft Graph Security API** | Async queries, parallel partitions, v1.0/beta auto-detection |
| `-UseEOM` | **Exchange Online Management** | `Search-UnifiedAuditLog`, serial-only, EOM module required |

### Parallel Execution (`-ParallelMode`)

- `Auto` (default): PS 7+ automatically uses parallel execution; PS 5.1 falls back to serial
- `On`: Forces parallel even if auto-detection says otherwise
- `Off`: Forces serial execution

### Adaptive Concurrency (default: enabled)

Enabled unless `-DisableAdaptive` is specified. Monitors:
- **Memory pressure** → reduces concurrency when working set exceeds `$MemoryPressureMB` (1500 MB)
- **High latency** → reduces concurrency when partition avg exceeds `$HighLatencyMs` (90s)
- **Low latency** → steps up concurrency after `$LowLatencyConsecutive` (2) consecutive groups below `$LowLatencyMs` (20s)
- **Throughput drop** → correlates latency with throughput to avoid premature reduction

### Circuit Breaker

Trips after `$CircuitBreakerThreshold` (5) consecutive block failures. Enters cooldown for `$CircuitBreakerCooldownSeconds` (120s). Uses exponential backoff from `$BackoffBaseSeconds` (1.0) to `$BackoffMaxSeconds` (45).

### Memory Management (`-MaxMemoryMB`)

- `-1` (default): Auto-limit to 75% of available system memory; flush `$allLogs` to JSONL when exceeded
- `0`: Disabled — all records kept in memory
- Positive value: Explicit MB threshold

### DSPM for AI (`-IncludeDSPMForAI`)

Adds 3 activity types: `ConnectedAIAppInteraction`, `AIInteraction`, `AIAppInteraction`. These are DSPM for AI billing events (paid, not included in standard M365 Copilot licensing).

### AutoCompleteness

When enabled, aggressively subdivides any time window that returns the server 10K record limit. Continues subdividing until the result count drops below the limit or the minimum window size is reached.

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `10` | Hit 10K or 1M record limit without `-AutoCompleteness` |
| `20` | Circuit breaker tripped |

---

## 7. Record Processing Pipeline

### 7.1 Overview (Graph API Mode — Default)

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ Create Async     │────▶│ Poll for Query   │────▶│ Paginate &       │────▶│ Normalize to     │
│ Audit Query      │     │ Completion       │     │ Retrieve Records │     │ EOM Format       │
└─────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
                                                                                │
                                                                                ▼
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ Final CSV/XLSX   │◀────│ Streaming Export  │◀────│ Apply Filters    │◀────│ Store JSONL      │
│ Output           │     │ (Convert-To*)    │     │ (Agent/User/etc) │     │ Incrementally    │
└─────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
```

### 7.2 Step 1: Time Partitioning

The date range (`$StartDate` → `$EndDate`) is divided into partitions:
- Default: `$BlockHours = 0.5` (30 minutes each)
- Override: `$PartitionHours` for fixed-size partitions
- Max: `$MaxPartitions = 160`

### 7.3 Step 2: Async Query Creation (Graph API)

For each partition, the script sends a POST to Microsoft Graph Security API:
```
POST /security/auditLog/queries
```
With filters for `activityDateTime` range, `operationFilters` (from `$ActivityTypes`), `recordTypeFilters` (from `$RecordTypes`), and `serviceFilter` (from `$ServiceTypes`).

Queries are dispatched in parallel (up to `$MaxConcurrency` concurrent) via `Start-ThreadJob` (PS 7+).

### 7.4 Step 3: Poll for Completion

Each async query is polled via:
```
GET /security/auditLog/queries/{queryId}
```
Until `status` = `succeeded`. Polling includes throttle detection (429/503), network outage handling with progressive backoff, and token refresh on 401.

### 7.5 Step 4: Record Retrieval

Records are paginated via `@odata.nextLink`:
```
GET /security/auditLog/queries/{queryId}/records?$top=10000
```

Each Graph API record is **normalized to EOM-compatible format** inline (line ~12239):

```powershell
$normalized = [PSCustomObject]@{
    RecordType          = $record.auditLogRecordType
    CreationDate        = [datetime]::Parse($record.createdDateTime, ...)
    UserIds             = $record.userPrincipalName
    Operations          = $record.operation
    AuditData           = $record.auditData | ConvertTo-Json -Depth 100 -Compress
    _ParsedAuditData    = $record.auditData  # Already-parsed object (avoids re-parsing)
    ResultIndex         = ...
    ResultCount         = 1
    Identity            = $record.id
    IsValid             = $true
    ObjectState         = 'Unchanged'
}
```

### 7.6 Step 5: Incremental JSONL Storage

Records are flushed to disk as JSONL after each page (when `$memoryFlushEnabled`):
```
.pax_incremental/Part<N>_<timestamp>_qid-<queryId>_<jobRunId>.jsonl
```

Safety snapshots are written every 500 pages even when memory flush is not enabled.

### 7.7 Step 6: Streaming Export

After all partitions complete, the JSONL files are read back and streamed through `Convert-ToStructuredRecord` or `Convert-ToPurviewExplodedRecords`:

**Non-explosion path** (`Convert-ToStructuredRecord`, line ~9580):
Produces 8-column rows: `RecordId`, `CreationDate`, `RecordType`, `Operation`, `UserId`, `AuditData`, `AssociatedAdminUnits`, `AssociatedAdminUnitsNames`

**Explosion path** (`Convert-ToPurviewExplodedRecords`, line ~8963):
Produces 153-column rows by:
1. Parsing `AuditData` JSON (or using cached `_ParsedAuditData`)
2. Flattening nested objects (CopilotEventData, AccessedResources, Contexts, etc.)
3. Expanding arrays into multiple rows
4. Mapping to the fixed `$PurviewExplodedHeader` schema

### 7.8 Content Filters Applied (Post-Retrieval)

- **UserIds filter**: Applied during retrieval (line ~12257) — records not matching `$userIds` are discarded
- **Agent filters**: `$AgentId`, `$AgentsOnly`, `$ExcludeAgents` — applied via `Test-AgentIdFilter` function
- **PromptFilter**: Filters based on CopilotEventData message types

### 7.9 EOM Fallback Mode

When `-UseEOM` is specified, the pipeline uses `Search-UnifiedAuditLog` instead of Graph API. This path:
- Is serial-only (no parallel partitions)
- Uses EOM module connection
- Records already arrive in the 8-column format (no normalization needed)
- `_ParsedAuditData` is set from the already-parsed `auditData` object when available

---

## 8. Insight Harbor Modifications

Four modifications (MODs) are injected into the upstream PAX script, clearly marked with `[INSIGHT HARBOR]` comments.

### MOD 1 — Config File Loading (Line 1346)

**Purpose**: Load settings from `insight-harbor-config.json` and apply them as defaults.

**Behavior**:
1. Validates `$ConfigFile` path exists and is valid JSON
2. Parses JSON into `$script:IHConfig`
3. Applies config values **only if the caller did not explicitly pass** the corresponding parameter (`$PSBoundParameters` check):
   - `$TenantId` ← `auth.tenantId`
   - `$ClientId` ← `auth.clientId`
   - `$ClientSecret` ← `auth.clientSecret`
   - `$ClientCertificateThumbprint` ← `auth.certificateThumbprint`
   - `$OutputPath` ← `ingestion.outputLocalPath`
4. Also promotes auth params to `$script:` scope for function access

**Key Vault Resolution**: The config may contain `@Microsoft.KeyVault(SecretUri=...)` references (e.g., `auth.clientSecret`). Resolution happens elsewhere in the pipeline orchestrator, not inside the PAX script.

### MOD 4 — Raw-Only Explosion Guard (Line 1382)

**Purpose**: Prevent PAX-native explosion when the Insight Harbor pipeline uses Python-based explosion.

**Behavior**:
```powershell
if ($script:IHConfig -and $script:IHConfig.pax.explosionMode -eq 'raw') {
    $ExplodeArrays = [switch]$false
    $ExplodeDeep   = [switch]$false
}
```

Emits a warning if either switch was explicitly provided. This ensures the 8-column raw format is output for downstream Python processing.

### MOD 2 — ADLS Upload (Line 17191)

**Purpose**: Upload output CSV files to Azure Data Lake Storage Gen2.

**Behavior**:
1. Only runs when `$script:IHConfig` exists AND `$ExportWorkbook` is false
2. Computes ADLS path: `bronze/purview/YYYY/MM/DD/<filename>.csv`
3. Checks for Az.Storage module availability
4. Uses `New-AzStorageContext` with storage account name + key from config
5. Uploads via `Set-AzStorageBlobContent` with `-Force`
6. Sets `$script:IHUploadSuccess` to `$true` or `$false`
7. Handles both per-activity split files (`$script:CsvSplitFiles`) and single combined output

### MOD 3 — Run Metadata JSON (Line 17218)

**Purpose**: Write a structured metadata file alongside each output CSV for pipeline traceability.

**Output file**: `<OutputCSVName>_run_metadata.json`

**Schema**:
```json
{
    "scriptName": "PAX_Purview_Audit_Log_Processor_v1.10.7_IH.ps1",
    "scriptVersion": "1.10.7",
    "insightHarborVersion": "<from config.solution.version>",
    "runTimestampUtc": "2025-06-11T00:00:00.0000000Z",
    "startDate": "2025-06-10",
    "endDate": "2025-06-11",
    "activityTypes": ["CopilotInteraction"],
    "recordCount": 12345,
    "outputFile": "Purview_Audit_20250611_000000.csv",
    "outputLocalPath": "C:\\Temp\\Purview_Audit_20250611_000000.csv",
    "outputDestination": "ADLS",
    "adlsBlobPath": "bronze/purview/2025/06/11/Purview_Audit_20250611_000000.csv",
    "uploadSuccess": true
}
```

### Summary of IH Modifications

| MOD | Line | Purpose | Impact |
|-----|------|---------|--------|
| MOD 1 | 1346 | Config file loading | Injects auth + output settings from JSON config |
| MOD 4 | 1382 | Raw explosion guard | Forces raw 8-column output when `explosionMode = "raw"` |
| MOD 2 | 17191 | ADLS upload | Uploads CSV to `bronze/purview/YYYY/MM/DD/` in ADLS |
| MOD 3 | 17218 | Run metadata | Writes `_run_metadata.json` per output file |

### Config File Reference (`config/insight-harbor-config.json`)

```json
{
    "solution": { "name": "Insight Harbor", "version": "1.0.0" },
    "auth": {
        "tenantId": "...",
        "clientId": "...",
        "clientSecret": "@Microsoft.KeyVault(SecretUri=...)"
    },
    "pax": {
        "explosionMode": "raw",
        "outputDestination": "ADLS"
    },
    "adls": {
        "storageAccountName": "...",
        "containerName": "insight-harbor",
        "paths": { "bronze": "bronze/", "silver": "silver/" }
    },
    "ingestion": {
        "defaultLookbackDays": 1,
        "outputLocalPath": "C:\\Temp\\InsightHarbor"
    }
}
```

---

*End of audit report.*
