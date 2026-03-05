# Insight Harbor — Durable Functions Pipeline Implementation Plan

> **Version:** 1.1  
> **Date:** 2025-07-08 (Updated: 2025-07-09)  
> **Status:** DRAFT — Pending Review (Appendix C added: PAX Intelligence Gap Analysis)  
> **Scope:** Replace the local PAX PowerShell pipeline with a zero-local-storage Azure Durable Functions pipeline

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Azure Resource Plan](#3-azure-resource-plan)
4. [Project Structure](#4-project-structure)
5. [PAX Logic → Durable Functions Mapping](#5-pax-logic--durable-functions-mapping)
   - 5.1 [Authentication & Token Management](#51-authentication--token-management)
   - 5.2 [Graph API 3-Phase Query Lifecycle](#52-graph-api-3-phase-query-lifecycle)
   - 5.3 [Time Window & Partition Management](#53-time-window--partition-management)
   - 5.4 [Rate Limiting & Backpressure](#54-rate-limiting--backpressure)
   - 5.5 [Error Handling & Multi-Layer Retry](#55-error-handling--multi-layer-retry)
   - 5.6 [Data Processing & Explosion](#56-data-processing--explosion)
   - 5.7 [Checkpoint & Resume](#57-checkpoint--resume)
   - 5.8 [Search Strategy & Query Planning](#58-search-strategy--query-planning)
   - 5.9 [Entra User Enrichment](#59-entra-user-enrichment)
   - 5.10 [Bronze-to-Silver Transform](#510-bronze-to-silver-transform)
6. [Orchestration Flow](#6-orchestration-flow)
7. [Reuse Assessment](#7-reuse-assessment)
8. [Configuration Strategy](#8-configuration-strategy)
9. [Implementation Phases](#9-implementation-phases)
10. [Testing Strategy](#10-testing-strategy)
11. [Monitoring & Alerting](#11-monitoring--alerting)
12. [Cost Model](#12-cost-model)
13. [Risks & Mitigations](#13-risks--mitigations)
14. [Decision Log](#14-decision-log)

**Appendices:**
- [Appendix A: PAX Feature Coverage Matrix](#appendix-a-pax-feature-coverage-matrix)
- [Appendix B: ADLS Path Structure](#appendix-b-adls-path-structure-new)
- [Appendix C: PAX Intelligence Gap Analysis](#appendix-c-pax-intelligence-gap-analysis) *(NEW — v1.1)*

---

## 1. Executive Summary

### What We're Replacing

The current Insight Harbor pipeline runs on a local machine:

```
PAX PowerShell (17,417 lines) → local CSV (~5 GB)
    → Python explosion → local CSV (~5+ GB)
    → Python Entra transform → local CSV (~46 KB)
    → Python Silver transform (JOIN) → local CSV (~5 GB)
    → ADLS uploads
Peak local disk: ~15 GB
```

### What We're Building

A fully serverless Azure Durable Functions pipeline with **zero local storage**:

```
Timer Trigger (daily schedule)
    → Orchestrator: plan partitions, fan-out
    → Activity: authenticate (MSAL) → Graph API
    → Activity[N]: create/poll/fetch audit queries → stream to ADLS bronze/
    → Activity[N]: explode 153-column schema → ADLS bronze/exploded/
    → Activity: Entra user pull → ADLS silver/entra-users/
    → Activity: bronze-to-silver transform (JOIN + dedup) → ADLS silver/copilot-usage/
    → Activity: notify Teams
Zero local files. All I/O through ADLS Gen2.
```

### Why Durable Functions

| Criterion | Fit |
|---|---|
| Async Purview queries (10-60 min) | Built-in `CreateTimer` for polling without burning compute |
| Parallel partition processing | Native fan-out/fan-in pattern |
| Checkpoint/resume | Built-in orchestration replay — survives crashes |
| 429 throttle waits | Timer-based waits at zero cost |
| Memory management | Each Activity runs in isolation; no GC concerns |
| Cost | ~$0.50/mo additional on Consumption plan |
| Monitoring | Reuses existing App Insights (`ih-app-insights`) |

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  ih-pipeline-func01  (Azure Functions — Consumption Plan, Python)   │
│                                                                      │
│  ┌─────────────┐    ┌─────────────────────────────────┐              │
│  │ Timer       │───▶│ pipeline_orchestrator            │              │
│  │ Trigger     │    │  (Durable Orchestrator)          │              │
│  │ (0 2 * * *) │    │                                  │              │
│  └─────────────┘    │  ┌─ plan_partitions()            │              │
│                     │  │                               │              │
│  ┌─────────────┐    │  ├─ fan-out: process_partition() │              │
│  │ HTTP        │───▶│  │   ├─ create_query()           │              │
│  │ Trigger     │    │  │   ├─ poll_query_status()      │              │
│  │ (manual)    │    │  │   ├─ fetch_records()           │              │
│  └─────────────┘    │  │   └─ upload_bronze()          │              │
│                     │  │                               │              │
│                     │  ├─ fan-out: explode_partition()  │              │
│                     │  │                               │              │
│                     │  ├─ pull_entra_users()            │              │
│                     │  │                               │              │
│                     │  ├─ transform_to_silver()         │              │
│                     │  │                               │              │
│                     │  └─ notify_completion()           │              │
│                     └─────────────────────────────────┘              │
│                                                                      │
│  ┌─────────────────────────────────────────────────────┐            │
│  │ Shared Modules                                       │            │
│  │  ├─ graph_client.py     (MSAL + Graph API wrapper)  │            │
│  │  ├─ adls_client.py      (ADLS streaming I/O)        │            │
│  │  ├─ explosion.py        (153-col schema processor)  │            │
│  │  ├─ transforms.py       (Silver transform logic)    │            │
│  │  ├─ partitioning.py     (Time window planner)       │            │
│  │  └─ models.py           (Pydantic data models)      │            │
│  └─────────────────────────────────────────────────────┘            │
└──────────────────────────────────────────────────────────────────────┘
         │                    │                     │
         ▼                    ▼                     ▼
┌────────────────┐  ┌─────────────────┐  ┌──────────────────┐
│ Microsoft Graph│  │  ADLS Gen2      │  │  Key Vault       │
│  /security/    │  │  ihstoragepoc01 │  │  ih-keyvault-    │
│  auditLog/     │  │  insight-harbor │  │    poc01         │
│  queries       │  │  ├ bronze/      │  │  (client creds)  │
│                │  │  ├ silver/      │  │                  │
│                │  │  └ pipeline/    │  │                  │
└────────────────┘  └─────────────────┘  └──────────────────┘
```

### Key Design Principles

1. **Zero local files** — All data flows through ADLS Gen2 blob storage
2. **Streaming I/O** — Download/upload in chunks (50 MB) to stay within 1.5 GB Consumption memory limit
3. **Idempotent activities** — Every activity can be safely retried
4. **ADLS as checkpoint store** — Pipeline state files in `pipeline/state/` supplement Durable's built-in replay
5. **Managed Identity for ADLS/Key Vault** — Client credentials (from Key Vault) for Graph API only

---

## 3. Azure Resource Plan

### New Resources to Create

| Resource | Name | SKU/Plan | Region | Monthly Cost |
|---|---|---|---|---|
| Function App | `ih-pipeline-func01` | Consumption (Y1) | Central US | ~$0.00-0.50 |
| Storage Account | `ihpipelinestor01` | Standard_LRS, StorageV2 | Central US | ~$0.05 |
| App Service Plan | *(Consumption — auto-created)* | Y1 | Central US | Included |

> **Total estimated additional cost: ~$0.10-0.55/mo**

### Existing Resources Reused

| Resource | Purpose |
|---|---|
| `ihstoragepoc01` (ADLS Gen2) | Lake house — all pipeline data |
| `ih-keyvault-poc01` | Client credentials for Graph API |
| `ih-app-insights` | Monitoring, tracing, alerting |
| `ih-log-analytics` | Log aggregation |

### Resource Configuration

**Function App Settings:**
```
FUNCTIONS_WORKER_RUNTIME=python
AzureWebJobsFeatureFlags=EnableWorkerIndexing
PYTHON_ISOLATE_WORKER_DEPENDENCIES=1

# ADLS (Managed Identity — no keys needed)
IH_ADLS_ACCOUNT_NAME=ihstoragepoc01
IH_ADLS_CONTAINER=insight-harbor

# Graph API (client credentials via Key Vault reference)
IH_TENANT_ID=579e8f66-10ec-4646-a923-b9dc013cc0a7
IH_CLIENT_ID=01e187b7-264a-4ce1-8cc1-32c977e0a302
IH_CLIENT_SECRET=@Microsoft.KeyVault(VaultName=ih-keyvault-poc01;SecretName=IH-CLIENT-SECRET)

# Pipeline Config
IH_DEFAULT_LOOKBACK_DAYS=1
IH_PARTITION_HOURS=6
IH_MAX_CONCURRENCY=4
IH_TARGET_RECORDS_PER_BLOCK=5000
IH_ACTIVITY_TYPES=CopilotInteraction
IH_EXPLOSION_MODE=raw

# Notifications
IH_TEAMS_WEBHOOK_URL=@Microsoft.KeyVault(VaultName=ih-keyvault-poc01;SecretName=IH-TEAMS-WEBHOOK)

# Durable Functions
IH_DURABLE_TASK_HUB=ihpipelinehub
```

### IAM Role Assignments for Function App Managed Identity

| Scope | Role | Purpose |
|---|---|---|
| `ihstoragepoc01` | Storage Blob Data Contributor | Read/write pipeline data |
| `ih-keyvault-poc01` | Key Vault Secrets User | Read client credentials |
| `ihpipelinestor01` | Storage Blob Data Owner | Durable Functions internal state |

---

## 4. Project Structure

```
pipeline/                              # New top-level folder
├── function_app.py                    # Function App entry point (triggers)
├── host.json                          # Host configuration
├── requirements.txt                   # Python dependencies
├── local.settings.json                # Local dev settings (gitignored)
│
├── orchestrators/
│   └── pipeline_orchestrator.py       # Main Durable orchestrator
│
├── activities/
│   ├── auth_activity.py               # MSAL token acquisition
│   ├── plan_partitions_activity.py    # Time window partition planner
│   ├── create_query_activity.py       # POST /auditLog/queries
│   ├── poll_query_activity.py         # GET query status
│   ├── fetch_records_activity.py      # GET records + pagination → ADLS
│   ├── check_subdivision_activity.py  # Volume check → subdivision decision
│   ├── explode_partition_activity.py  # 153-column explosion → ADLS
│   ├── pull_entra_activity.py         # Entra user directory pull → ADLS
│   ├── transform_silver_activity.py   # Bronze → Silver with Entra JOIN
│   ├── cleanup_queries_activity.py    # DELETE completed queries
│   └── notify_activity.py            # Teams webhook notification
│
├── shared/
│   ├── __init__.py
│   ├── graph_client.py               # MSAL + Graph API wrapper
│   ├── adls_client.py                # ADLS streaming read/write
│   ├── explosion.py                  # 153-column schema logic
│   ├── transforms.py                 # Silver transform functions
│   ├── entra_transforms.py           # Entra Silver transform functions
│   ├── partitioning.py               # Time window generation
│   ├── models.py                     # Pydantic models
│   ├── constants.py                  # Column schemas, activity bundles
│   └── config.py                     # Config from environment variables
│
└── tests/
    ├── __init__.py
    ├── test_graph_client.py
    ├── test_partitioning.py
    ├── test_explosion.py
    ├── test_transforms.py
    └── test_orchestrator.py
```

### Dependencies (`requirements.txt`)

```
azure-functions>=1.18.0
azure-functions-durable>=1.2.9
azure-identity>=1.15.0
azure-storage-blob>=12.19.0
azure-keyvault-secrets>=4.8.0
azure-monitor-opentelemetry>=1.6.0
msal>=1.28.0
requests>=2.31.0
pandas>=2.0.0
orjson>=3.9.0
pydantic>=2.5.0
```

---

## 5. PAX Logic → Durable Functions Mapping

### 5.1 Authentication & Token Management

#### What PAX Does (Lines ~3001-3800)

PAX implements 6 authentication methods, with elaborate token lifecycle management:

| PAX Feature | Detail |
|---|---|
| 6 auth methods | WebLogin, DeviceCode, Credential, Silent, AppRegistration-Secret, AppRegistration-Cert |
| JWT extraction | Parses `access_token` base64 payload for `exp`, `upn`, `aud`, `app_displayname` |
| Proactive refresh | Triggers at 30 min remaining (configurable via `BufferMinutes`) |
| Cross-thread sharing | `SharedAuthState` synchronized hashtable with `IsRefreshing` flag |
| Thread-side validation | `Test-TokenValid` polls `Wait-ForTokenRefresh` when main thread is refreshing |
| AppReg silent re-auth | `Invoke-TokenRefresh` uses stored client secret/cert for non-interactive renewal |
| Retry on 401 | Catches unauthorized, refreshes token, retries the request |

#### Durable Functions Equivalent

```
┌─────────────────────────────────────────────────────────────┐
│  Auth Strategy: MSAL ConfidentialClientApplication          │
│                                                             │
│  • Managed Identity → ADLS + Key Vault (zero credentials)  │
│  • Client credentials → Graph API only (from Key Vault)    │
│  • MSAL handles token cache + proactive refresh internally  │
│  • No cross-thread concerns (activities are isolated)       │
│  • Token acquired per-activity, cached by MSAL in-memory   │
└─────────────────────────────────────────────────────────────┘
```

**Implementation Plan:**

1. **`shared/graph_client.py`** — Central Graph API client class:
   ```python
   class GraphClient:
       """Wraps MSAL ConfidentialClientApplication for Graph API access."""
       
       def __init__(self, tenant_id, client_id, client_secret):
           self._app = msal.ConfidentialClientApplication(
               client_id,
               authority=f"https://login.microsoftonline.com/{tenant_id}",
               client_credential=client_secret
           )
           self._scopes = ["https://graph.microsoft.com/.default"]
       
       def get_token(self) -> str:
           """Acquire token with automatic cache + refresh."""
           # MSAL handles: cache lookup → silent refresh → new token
           result = self._app.acquire_token_for_client(scopes=self._scopes)
           if "access_token" not in result:
               raise AuthenticationError(result.get("error_description"))
           return result["access_token"]
       
       def graph_request(self, method, url, **kwargs) -> dict:
           """Make authenticated Graph API request with retry on 401."""
           # ... retry logic with token refresh ...
   ```

2. **Why 6 auth methods → 1**: PAX supports interactive (WebLogin, DeviceCode) for human operators. Durable Functions runs unattended — only `client_credentials` flow is needed. MSAL's `ConfidentialClientApplication.acquire_token_for_client()` handles the full lifecycle:
   - Returns cached token if valid
   - Silently acquires new token when cache expires
   - No JWT parsing needed — MSAL tracks expiry internally

3. **Cross-thread token sharing eliminated**: PAX uses `SharedAuthState` because ThreadJobs share a process but each has its own runspace. In Durable Functions, each Activity is an independent invocation. The orchestrator doesn't hold tokens — each activity acquires its own (MSAL cache makes this fast: <1ms for cached tokens).

4. **401 retry preserved**: The `graph_request()` wrapper catches 401, calls `get_token()` (which forces MSAL cache refresh), and retries. This matches PAX's `Invoke-TokenRefresh` behavior.

5. **Token lifetime safety**: 
   - PAX concern: Long-running partitions (60+ min) may outlive the token
   - DF solution: Each `fetch_records` activity call runs for a bounded time (one page of results). If pagination takes longer than the token lifetime, the next `graph_request()` call auto-refreshes via MSAL
   - Additional safety: Activities have a default timeout (configurable via `host.json`); if an activity hangs, Durable restarts it

**PAX Complexity Captured:** ✅ All 6 concerns (auth, cache, refresh, cross-thread, retry, expiry) are addressed — 4 by MSAL natively, 2 by Durable Functions architecture.

---

### 5.2 Graph API 3-Phase Query Lifecycle

#### What PAX Does (Lines ~6300-6800)

The Purview audit log API is asynchronous with three phases:

```
Phase 1 — CREATE (Invoke-GraphAuditQuery)
    POST /security/auditLog/queries
    Body: { displayName, filterStartDateTime, filterEndDateTime, 
            recordTypeFilters, operationFilters, ... }
    Returns: { id, status: "notStarted" }

Phase 2 — POLL (Get-GraphAuditQueryStatus)
    GET /security/auditLog/queries/{id}
    Poll until status == "succeeded" or "failed"
    Randomized intervals: 30-90 seconds
    Returns: { id, status, recordCount }
    
    Special behavior:
    - At ≥9,500 records → preemptive subdivision warning
    - At exactly 1,000,000 → hard limit reached, MUST subdivide

Phase 3 — FETCH (Get-GraphAuditRecords)
    GET /security/auditLog/queries/{id}/records
    Paginated via @odata.nextLink
    Each page: up to 1,000 records (Graph) or 5,000 (EOM)
    Stream-writes JSONL per page to prevent memory overflow
```

#### Durable Functions Equivalent

```
┌──────────────────────────────────────────────────────────┐
│  Orchestrator: process_partition sub-orchestration        │
│                                                          │
│  1. call_activity("create_query", partition_params)       │
│     → returns query_id                                   │
│                                                          │
│  2. LOOP: poll with durable timer                        │
│     │  call_activity("poll_query", query_id)             │
│     │  → returns {status, record_count}                  │
│     │                                                    │
│     │  if status == "succeeded":                         │
│     │      if record_count >= 950_000:                   │
│     │          → trigger SUBDIVISION sub-orchestration   │
│     │      else:                                         │
│     │          → break to Phase 3                        │
│     │                                                    │
│     │  if status == "failed":                            │
│     │      → raise RetryableError                        │
│     │                                                    │
│     │  ctx.create_timer(next_poll_time)  ← ZERO COST     │
│     │  (randomized 30-90s interval)                      │
│     └────────────────────────────────────────────────────│
│                                                          │
│  3. call_activity("fetch_records", {query_id, blob_path})│
│     → paginated fetch, streams each page to ADLS         │
│     → returns {records_written, blob_path}               │
│                                                          │
│  4. call_activity("cleanup_query", query_id)             │
│     → DELETE /auditLog/queries/{id}                      │
└──────────────────────────────────────────────────────────┘
```

**Implementation Plan:**

1. **`activities/create_query_activity.py`**:
   - Accepts: `{start_time, end_time, activity_types, display_name}`
   - Builds the filter body matching PAX's `Invoke-GraphAuditQuery`
   - POST to `/security/auditLog/queries`
   - Returns: `{query_id, display_name}`
   - Handles: 429 (raise for Durable retry), 401 (MSAL refresh + retry), 500 (raise for Durable retry)

2. **`activities/poll_query_activity.py`**:
   - Accepts: `{query_id}`
   - GET `/security/auditLog/queries/{query_id}`
   - Returns: `{query_id, status, record_count}`
   - Pure status check — no side effects (idempotent)

3. **`activities/fetch_records_activity.py`**:
   - Accepts: `{query_id, adls_blob_path}`
   - Paginated GET `/security/auditLog/queries/{query_id}/records`
   - Follows `@odata.nextLink` until exhausted
   - **Streaming write**: Each page of records is appended to ADLS blob using `AppendBlobClient`
     - This matches PAX's per-page JSONL flush that prevents memory overflow
     - Each page: ~1,000 records × ~2 KB = ~2 MB (well within memory)
   - Returns: `{records_written, blob_path, pages_fetched}`
   - **Memory safety**: Only one page in memory at a time

4. **Polling Timer Cost**: PAX's 30-90s polling burns an active PowerShell process. Durable Functions `ctx.create_timer()` is **free** — the function is completely unloaded during the wait, zero compute cost.

5. **10-concurrent-query limit**: PAX tracks this via `MaxConcurrency` with backpressure polling. The Durable orchestrator controls this via batched fan-out (see Section 5.4).

**PAX Complexity Captured:** ✅ All 3 phases faithfully replicated. Timer-based polling is more efficient. Pagination streaming preserved. SubDivision trigger at 950K preserved.

---

### 5.3 Time Window & Partition Management

#### What PAX Does (Lines ~7350-11100)

PAX implements sophisticated time window management:

| Feature | PAX Behavior |
|---|---|
| **Partition generation** | Splits date range into blocks of `$PartitionHours` (default 6h) |
| **Adaptive block sizing** | `Get-OptimalBlockSize` / `Update-LearnedBlockSize` — starts at target ~5000 records, adjusts based on actual returns |
| **Smart subdivision** | When a block hits 1M limit: analyzes timestamp distribution, splits into sub-partitions (min 2-minute granularity), re-indexes partition numbers |
| **Extreme volume warnings** | >30 day ranges get console warnings |
| **EOM time partitioning** | Different partitioning for EOM (Exchange Online Management) fallback queries |
| **Block overlap prevention** | Ensures partition boundaries don't overlap (EndTime exclusive) |

#### Durable Functions Equivalent

```
┌──────────────────────────────────────────────────────────────┐
│  Activity: plan_partitions                                    │
│                                                              │
│  Input: {start_date, end_date, partition_hours,              │
│          target_records_per_block}                            │
│                                                              │
│  Logic:                                                      │
│  1. Generate initial partitions (same algorithm as PAX)      │
│  2. Return: [{partition_id, start, end, estimated_size}, ...]│
│                                                              │
│  No adaptive resizing at plan time — that happens during     │
│  execution via the subdivision sub-orchestration             │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  Sub-Orchestration: subdivide_partition                       │
│                                                              │
│  Triggered when poll_query returns record_count >= 950,000   │
│                                                              │
│  Logic:                                                      │
│  1. Delete the oversized query                               │
│  2. Split time window using halving strategy                 │
│     (minimum granularity: 2 minutes, matching PAX)           │
│  3. Fan-out: process_partition() for each sub-partition      │
│  4. Recursion: if a sub-partition ALSO hits limit,           │
│     subdivide again (matching PAX's recursive subdivision)   │
│                                                              │
│  Returns: [{blob_path, records_written}, ...] from all subs  │
└──────────────────────────────────────────────────────────────┘
```

**Implementation Plan:**

1. **`shared/partitioning.py`** — Port PAX's partition logic:
   - `generate_partitions(start, end, hours)` → list of `(start, end)` tuples
   - `subdivide_partition(start, end, factor=2)` → smaller partitions
   - `estimate_optimal_hours(historical_counts)` → adaptive block sizing (uses ADLS state from previous runs)
   - Boundary handling: exactly match PAX's exclusive end-time semantics

2. **`activities/plan_partitions_activity.py`**:
   - Loads historical run data from `pipeline/state/block_history.json` in ADLS
   - Generates partitions with learned optimal block size
   - Returns partition list to orchestrator for fan-out

3. **`activities/check_subdivision_activity.py`**:
   - Called after poll returns record_count
   - Decision logic: `record_count >= 950_000` → subdivide, else proceed
   - Returns: `{should_subdivide: bool, sub_partitions: [...]}`

4. **Adaptive Learning Across Runs**: PAX adjusts block size within a single run. The Durable version persists `block_history.json` to ADLS, learning across runs. Each completed partition writes its `{time_range, record_count, duration}` to this file. The planner reads it on next run.

**PAX Complexity Captured:** ✅ Partition generation, adaptive sizing, recursive subdivision, minimum 2-min granularity, extreme volume handling — all preserved.

---

### 5.4 Rate Limiting & Backpressure

#### What PAX Does (Lines ~6200-12600)

| Feature | PAX Behavior |
|---|---|
| **429 detection** | 3-layer: HTTP status, `Retry-After` header, response body pattern match (`Test-Is429`) |
| **429 handling** | Unlimited retries; waits `Retry-After` seconds or exponential backoff 60→120→240→300s cap |
| **Concurrency control** | `MaxConcurrency` param (default 3); backpressure polling (500ms) when limit reached |
| **Staggered launch** | 10-25s random jitter between launching parallel partitions |
| **Circuit breaker** | After N consecutive 429s, reduces concurrency and increases wait times |
| **10-query limit** | Purview API allows max 10 concurrent search jobs per user; PAX respects this |

#### Durable Functions Equivalent

```
Orchestrator concurrency control:

    # Fan-out in controlled batches
    BATCH_SIZE = 4  # Max parallel partitions (< 10 query limit)
    
    for batch in chunks(partitions, BATCH_SIZE):
        # Staggered launch within batch
        tasks = []
        for i, partition in enumerate(batch):
            if i > 0:
                # 10-25s stagger (matches PAX)
                yield ctx.create_timer(ctx.current_utc_datetime 
                    + timedelta(seconds=random.randint(10, 25)))
            tasks.append(
                ctx.call_sub_orchestrator("process_partition", partition)
            )
        
        # Wait for entire batch before starting next
        results = yield ctx.task_all(tasks)
```

**Implementation Plan:**

1. **429 Handling**: Activity functions raise a custom `ThrottledError` on 429. Durable Functions retry policy handles the wait:
   ```python
   # In activity: raise on 429
   if response.status_code == 429:
       retry_after = int(response.headers.get("Retry-After", 60))
       raise ThrottledError(f"429 received, retry after {retry_after}s",
                            retry_after_seconds=retry_after)
   
   # In orchestrator: retry policy on activities
   retry_opts = df.RetryOptions(
       first_retry_interval_in_milliseconds=60_000,  # 60s first retry
       max_number_of_attempts=10,                     # Generous retry budget
       backoff_coefficient=2.0,                       # 60→120→240→300 cap
       max_retry_interval_in_milliseconds=300_000,    # 300s cap (matches PAX)
   )
   result = yield ctx.call_activity_with_retry("fetch_records", retry_opts, input)
   ```

2. **Concurrency**: The orchestrator batches fan-out to 4 parallel partitions (below Purview's 10-query limit, with headroom for retries). PAX defaults to 3; we use 4 since we don't have thread overhead.

3. **Staggered Launch**: Durable timers between partition starts within a batch. Zero compute cost during stagger waits.

4. **Circuit Breaker**: If a batch experiences >3 consecutive 429s across all partitions, the orchestrator:
   - Reduces batch size to 2
   - Doubles the stagger interval
   - Logs a warning to App Insights
   - Persists the reduced state so subsequent batches respect it

5. **10-Query Cleanup**: After each partition completes fetch, immediately DELETE the query to free the slot (matching PAX's cleanup behavior).

**PAX Complexity Captured:** ✅ All rate limiting, backpressure, staggered launch, and circuit breaker behaviors preserved. Timer-based waits are more efficient than PAX's busy-polling.

---

### 5.5 Error Handling & Multi-Layer Retry

#### What PAX Does

PAX implements a 5-layer retry architecture:

```
Layer 1: In-thread retry (3 attempts per HTTP call)
    └─ Catches transient errors, 429, 500, network timeouts
    
Layer 2: Partition retry pass (up to 5 attempts)
    └─ Re-runs failed partitions with reduced concurrency
    
Layer 3: Final safety net (sequential processing)
    └─ Last-resort serial processing of still-failed partitions
    
Layer 4: Zero-record recovery
    └─ Checks QueryIds directly for partitions with 0 records
    
Layer 5: Checkpoint/resume (across runs)
    └─ Resumes from last successful partition on next run
```

Additional error handling:
- 401 → Token refresh + retry (not counted as error)
- 403 → Permanent failure, log and skip (permissions issue)
- Graceful Ctrl+C → Saves checkpoint, cleans up queries
- Network timeout → Treated as transient, retried

#### Durable Functions Equivalent

```
Layer 1: Activity-level retry policy (built-in)
    └─ RetryOptions: 3 attempts, exponential backoff
    
Layer 2: Sub-orchestration retry loop
    └─ process_partition catches ActivityFailure,
       re-invokes with reduced concurrency flag
       Up to 5 attempts (matching PAX)
    
Layer 3: Orchestrator fallback pass
    └─ Collects failed partitions from fan-out,
       runs them sequentially (batch_size=1)
    
Layer 4: Zero-record verification activity
    └─ For partitions returning 0 records,
       re-polls the query status to verify

Layer 5: Durable Functions built-in checkpoint
    └─ Orchestration state survives crashes, restarts,
       and scale-in events. Resumes exactly where it stopped.
    └─ PLUS: ADLS state file for cross-run resumption
```

**Implementation Plan:**

1. **Layer 1 — Activity Retry Policy**:
   ```python
   retry_options = df.RetryOptions(
       first_retry_interval_in_milliseconds=5_000,
       max_number_of_attempts=3,
       backoff_coefficient=2.0,
       max_retry_interval_in_milliseconds=30_000
   )
   ```

2. **Layer 2 — Partition Retry** (in `process_partition` sub-orchestration):
   ```python
   for attempt in range(5):
       try:
           result = yield ctx.call_activity_with_retry(
               "fetch_records", retry_options, input_data)
           break
       except Exception as e:
           if attempt < 4:
               # Reduced concurrency on retry (matches PAX)
               input_data["reduced_concurrency"] = True
               yield ctx.create_timer(
                   ctx.current_utc_datetime + timedelta(seconds=30 * (attempt + 1)))
           else:
               failed_partitions.append(input_data)
   ```

3. **Layer 3 — Sequential Fallback** (in main orchestrator):
   ```python
   if failed_partitions:
       logging.warning(f"Sequential retry for {len(failed_partitions)} partitions")
       for partition in failed_partitions:
           result = yield ctx.call_sub_orchestrator(
               "process_partition", 
               {**partition, "sequential_mode": True})
   ```

4. **Layer 4 — Zero-Record Verification**:
   - Activity checks if a "succeeded" query truly has 0 records vs. a fetch failure
   - Re-polls query status, if still "succeeded" with recordCount > 0, retries fetch

5. **Layer 5 — Cross-Run Resume**:
   - After each partition completes, write its status to `pipeline/state/run_{date}.json` in ADLS
   - On orchestrator start, check for incomplete run state files
   - Resume from the last incomplete partition
   - Durable Functions' built-in replay handles within-run crashes automatically

6. **403 Handling**: Logged as permanent failure, partition marked as `"skipped_permission_denied"`, run continues. Matches PAX behavior.

7. **Graceful Termination**: Durable Functions handles this natively — if the function app scales in or restarts, the orchestration replays from its last checkpoint. No explicit Ctrl+C handling needed (that was a local-execution concern).

**PAX Complexity Captured:** ✅ All 5 retry layers faithfully mapped. Durable Functions actually improves Layer 5 (built-in crash recovery vs. PAX's manual checkpoint files).

---

### 5.6 Data Processing & Explosion

#### What PAX Does (Lines ~8700-16200)

| Feature | Detail |
|---|---|
| **153-column schema** | `$PurviewExplodedHeader` — flat schema for audit records |
| **`Convert-ToPurviewExplodedRecords`** | Recursively flattens nested JSON, extracts Copilot message details, categorizes agents |
| **Copilot message expansion** | Parses `CopilotEventData.Messages[]`, creates one row per message with turn tracking |
| **Agent categorization** | Maps `AppHost` values to agent types (Teams, Word, Excel, PowerPoint, etc.) |
| **Parallel explosion** | ThreadJob with 2-8 workers, auto-scaled by CPU cores |
| **CSV streaming** | 1MB buffer, writes in chunks to prevent memory overflow |
| **Deduplication** | `RecordId` HashSet to skip already-processed records |
| **Graph→EOM normalization** | Transforms EOM record format to match Graph API format |

#### Durable Functions Equivalent

**Key Decision**: The existing Python explosion module (`Purview_M365_Usage_Bundle_Explosion_Processor`) already implements the 153-column schema and explosion logic. We **reuse** this module with modifications for ADLS I/O.

**Implementation Plan:**

1. **`shared/explosion.py`** — Adapted from existing explosion processor:
   - Import and wrap the core `run_explosion()` logic
   - Replace local file reads with ADLS blob stream reads
   - Replace local file writes with ADLS append blob writes
   - Keep the 153-column schema (`PURVIEW_EXPLODED_HEADER`) as-is
   - Keep `convert_to_exploded_records()` logic as-is
   - Keep Copilot message expansion and agent categorization as-is

2. **`activities/explode_partition_activity.py`**:
   - Input: `{bronze_blob_path, output_blob_path}`
   - Downloads bronze JSONL from ADLS in chunks (50 MB)
   - Processes records through explosion logic
   - Streams exploded CSV to ADLS append blob (matching PAX's 1MB buffered writes)
   - Returns: `{records_exploded, output_blob_path}`

3. **Parallel Explosion**: The orchestrator fans out explosion activities — one per bronze partition blob. Each activity runs independently (equivalent to PAX's ThreadJob workers, but fully isolated with no shared memory concerns).

4. **Memory Management**:
   - PAX: 75% RAM auto-detection, per-page JSONL flush, `[GC]::Collect()`
   - DF: Each activity has ~1.5 GB (Consumption plan). Process in chunks:
     - Read 50 MB of JSONL from ADLS
     - Explode in memory (~3x expansion = ~150 MB)
     - Write exploded chunk to ADLS append blob
     - Release memory for next chunk
   - Worst case: 50 MB input × 3x = ~150 MB peak — well within 1.5 GB

5. **Deduplication**: Maintained via `RecordId` set within each activity (per-partition). Cross-partition dedup happens at the Silver transform stage (matching current pipeline).

**PAX Complexity Captured:** ✅ 153-column schema, Copilot message expansion, agent categorization, streaming writes, memory management — all preserved. Parallel explosion via fan-out is architecturally cleaner than ThreadJob.

---

### 5.7 Checkpoint & Resume

#### What PAX Does (Lines ~3800-5400)

| Feature | Detail |
|---|---|
| **v2 checkpoint JSON** | `{Version: 2, RunId, CompletedPartitions: [], FailedPartitions: [], ...}` |
| **Atomic writes** | Write to `.tmp` → rename (prevents corruption) |
| **`_PARTIAL` file naming** | Output files named `*_PARTIAL.csv` until processing completes, then renamed |
| **Incremental JSONL saves** | Per-partition results saved to `.pax_incremental/` as JSONL files |
| **Streaming merge** | On resume, merges all incremental JSONL files with dedup |
| **Checkpoint save points** | Saved at: query creation, query completion, retry start, Ctrl+C, partition complete |
| **Token refresh prompts** | On resume, prompts user to re-authenticate if token expired |

#### Durable Functions Equivalent

Durable Functions has **built-in checkpointing** via orchestration replay. This replaces PAX's entire manual checkpoint system:

```
┌──────────────────────────────────────────────────────────────┐
│  Built-in Checkpoint (Durable Functions)                      │
│                                                              │
│  • Every yield/await in the orchestrator is a checkpoint     │
│  • If the function crashes and restarts, it REPLAYS from     │
│    the beginning, but all completed activities return        │
│    their cached results instantly (no re-execution)          │
│  • Works across: process crashes, VM evictions, scale-in     │
│                                                              │
│  This is architecturally superior to PAX's manual checkpoint │
│  because it checkpoints at EVERY step, not just save points  │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  ADLS State File (cross-run resume)                           │
│                                                              │
│  pipeline/state/run_{YYYYMMDD}_{HHmmss}.json                │
│  {                                                           │
│    "run_id": "...",                                          │
│    "started_at": "...",                                      │
│    "status": "in_progress | completed | failed",            │
│    "date_range": {"start": "...", "end": "..."},            │
│    "partitions": [                                           │
│      {"id": 1, "start": "...", "end": "...",                │
│       "status": "completed", "records": 4500,               │
│       "bronze_blob": "...", "exploded_blob": "..."},        │
│      {"id": 2, "status": "failed", "error": "..."}         │
│    ],                                                        │
│    "silver_status": "completed | pending",                   │
│    "completed_at": "..."                                     │
│  }                                                           │
└──────────────────────────────────────────────────────────────┘
```

**Implementation Plan:**

1. **Within-run checkpointing**: Fully automatic. No code needed. Every `yield` is a checkpoint.

2. **Cross-run resume**: The orchestrator checks for incomplete run state files at startup:
   ```python
   # In orchestrator
   last_run = yield ctx.call_activity("check_previous_run", today)
   if last_run and last_run["status"] == "in_progress":
       # Resume: only process partitions not yet completed
       partitions = [p for p in last_run["partitions"] 
                     if p["status"] != "completed"]
       logging.info(f"Resuming run {last_run['run_id']}, "
                    f"{len(partitions)} partitions remaining")
   ```

3. **ADLS state writes**: After each partition completes, update the run state file:
   ```python
   yield ctx.call_activity("update_run_state", {
       "run_id": run_id,
       "partition_id": partition.id,
       "status": "completed",
       "records": result.records_written,
       "blob_path": result.blob_path
   })
   ```

4. **No `_PARTIAL` file naming needed**: In PAX, `_PARTIAL` prevents downstream consumers from reading incomplete files. In Durable Functions, the orchestrator controls the pipeline — Silver transform only runs after ALL partitions complete (enforced by `task_all`). No race condition possible.

5. **No token refresh prompts**: PAX prompts the user on resume because interactive tokens expired. Durable Functions uses client credentials — MSAL refreshes automatically. No human interaction needed.

**PAX Complexity Captured:** ✅ All checkpoint/resume functionality preserved. Durable Functions' built-in replay is strictly superior to PAX's manual checkpoint system. Cross-run resume via ADLS state files preserves the ability to pick up where a failed run left off.

---

### 5.8 Search Strategy & Query Planning

#### What PAX Does (Lines ~1300-2400)

| Feature | Detail |
|---|---|
| **Activity bundles** | ~100+ curated activity types grouped into logical bundles (`CopilotInteraction`, `SharePointFileOperation`, etc.) |
| **7-step resolution** | Resolves user input (`-ActivityTypes`) through bundle expansion, validation, dedup |
| **Query planning** | Graph API: single combined filter; EOM: per-activity queries |
| **Service workload splitting** | Splits activities by service (Exchange, SharePoint, AzureAD, etc.) for EOM |
| **User/group filtering** | `RecordTypeFilters`, `UserPrincipalNameFilters` |

#### Durable Functions Equivalent

**Implementation Plan:**

1. **`shared/constants.py`** — Port activity bundle definitions:
   ```python
   ACTIVITY_BUNDLES = {
       "CopilotInteraction": ["CopilotInteraction"],
       "SharePointFileOperation": [
           "FileAccessed", "FileModified", "FileDeleted", ...
       ],
       # ... all ~100+ bundles from PAX
   }
   ```

2. **`activities/plan_partitions_activity.py`** — Includes query planning:
   - Resolves activity types from config (matching PAX's 7-step pipeline)
   - For Insight Harbor's use case: `CopilotInteraction` is the primary (and usually only) activity type
   - Returns resolved activity list alongside partition plan

3. **EOM fallback**: PAX falls back to Exchange Online Management (EOM) API when Graph is unavailable. For the Durable Functions implementation:
   - **Phase 1**: Graph API only (EOM adds significant complexity and is not needed for our tenant)
   - **Future**: EOM can be added as an alternative activity path if Graph API is consistently unavailable
   - Decision rationale: Our demo tenant uses Graph API successfully; EOM is a legacy fallback

4. **User/group filtering**: Passed through as query parameters in `create_query_activity`. Matches PAX's `UserPrincipalNameFilters` and `RecordTypeFilters`.

**PAX Complexity Captured:** ✅ Activity bundle definitions, resolution pipeline, and query planning preserved. EOM fallback deferred to future phase (documented, not dropped).

---

### 5.9 Entra User Enrichment

#### What PAX Does

PAX has a separate `-OnlyUserInfo` mode that:
- Calls Graph API `/users` endpoint with `$select` for 37+ user properties
- Handles pagination (`@odata.nextLink`)
- Extracts license SKUs, department, job title, country, etc.
- Writes to CSV: `EntraUsers_{tenant}_{date}.csv`

#### Durable Functions Equivalent

**Implementation Plan:**

1. **`activities/pull_entra_activity.py`**:
   - Uses same `GraphClient` for authentication
   - GET `/users?$select=id,userPrincipalName,displayName,department,jobTitle,...&$top=999`
   - Follows `@odata.nextLink` pagination
   - Extracts license assignments via `/users/{id}/licenseDetails` (batched)
   - Streams results as CSV to ADLS `bronze/entra/YYYY/MM/DD/entra_users_raw.csv`
   - Returns: `{users_count, blob_path}`

2. **Entra Silver Transform**: Reuse existing `bronze_to_silver_entra.py` logic:
   - 30-column Silver schema
   - `HasCopilotLicense` / `LicenseTier` computation
   - UPN deduplication
   - Upload to `silver/entra-users/silver_entra_users.csv`

3. **Integration**: The orchestrator runs Entra pull in parallel with audit log processing (they're independent), then the Silver transform joins them.

**PAX Complexity Captured:** ✅ Full Entra enrichment pipeline preserved.

---

### 5.10 Bronze-to-Silver Transform

#### What the Current Pipeline Does

- Reads exploded Bronze CSVs from ADLS `bronze/exploded/`
- Applies Silver schema: `UsageDate`, `UsageHour`, `PromptType`, `IsAgent`, computed columns
- Casts numeric columns (`TurnNumber`, `TokensTotal`, etc.)
- LEFT JOINs Entra user data (10 columns: Department, JobTitle, Country, etc.)
- Deduplicates on `(RecordId, Message_Id)` against existing Silver in ADLS
- Uploads to `silver/copilot-usage/silver_copilot_usage.csv`

#### Durable Functions Equivalent

**Implementation Plan:**

1. **`activities/transform_silver_activity.py`**:
   - Input: `{exploded_blob_paths: [...], entra_blob_path, existing_silver_path}`
   - Downloads Entra Silver lookup from ADLS (small: ~46 KB)
   - Downloads existing Silver from ADLS for dedup keys
   - Streams each exploded Bronze blob in chunks:
     - Apply `transform_row()` (reused from existing code)
     - Apply Entra enrichment (reused from existing code)
     - Dedup against existing keys
   - Uploads final Silver CSV to ADLS
   - Returns: `{records_transformed, new_records, duplicates_skipped}`

2. **Reused Functions** (extracted from existing `bronze_to_silver_purview.py`):
   - `transform_row()` — pure function, zero I/O dependencies
   - `compute_prompt_type()` — pure function
   - `compute_is_agent()` — pure function
   - `safe_int()` — pure function
   - `make_dedup_key()` — pure function
   - `enrich_row_with_entra()` — pure function
   - `load_entra_lookup()` — modified to read from ADLS only

3. **Memory Management for Large Silver Files**:
   - Current Silver file can be ~5 GB. Loading entirely into memory won't work in 1.5 GB Consumption plan.
   - Strategy: **Incremental append with streaming dedup**
     - Load only the dedup key set from existing Silver (just `RecordId` + `Message_Id` columns — ~50 MB for 5M records)
     - Stream new records through transform, check against dedup set
     - Append new records to Silver blob
   - Alternative: If Silver grows beyond what fits in memory, switch to **partitioned Silver** files (one per day/month) — dedup only needs the current partition's keys

**PAX Complexity Captured:** ✅ All transform logic, Entra enrichment, and deduplication preserved. Memory management adapted for serverless constraints.

---

## 6. Orchestration Flow

### Complete Orchestrator Pseudocode

```python
@df.orchestrator_trigger
async def pipeline_orchestrator(ctx: df.DurableOrchestrationContext):
    """Main pipeline orchestrator — replaces run-pipeline-local.ps1 + PAX."""
    
    run_id = ctx.instance_id
    input_data = ctx.get_input()
    start_date = input_data.get("start_date")  # or computed from lookback
    end_date = input_data.get("end_date")       # or now()
    
    # ── PHASE 0: Resume Check ──────────────────────────────────────
    previous_run = yield ctx.call_activity("check_previous_run", {
        "date": start_date
    })
    if previous_run and previous_run["status"] == "in_progress":
        # Resume incomplete run
        partitions = previous_run["incomplete_partitions"]
        completed_bronze = previous_run["completed_bronze_paths"]
    else:
        completed_bronze = []
        
        # ── PHASE 1: Plan Partitions ──────────────────────────────
        partitions = yield ctx.call_activity("plan_partitions", {
            "start_date": start_date,
            "end_date": end_date,
            "partition_hours": config.PARTITION_HOURS,
            "activity_types": config.ACTIVITY_TYPES,
            "target_records": config.TARGET_RECORDS_PER_BLOCK
        })
    
    # ── PHASE 2: Ingest Audit Logs (fan-out/fan-in) ───────────────
    BATCH_SIZE = config.MAX_CONCURRENCY  # 4
    all_bronze_paths = list(completed_bronze)
    failed_partitions = []
    
    for batch in chunks(partitions, BATCH_SIZE):
        tasks = []
        for i, partition in enumerate(batch):
            if i > 0:
                # Staggered launch (10-25s jitter)
                stagger = random.randint(10, 25)
                yield ctx.create_timer(
                    ctx.current_utc_datetime + timedelta(seconds=stagger))
            
            tasks.append(ctx.call_sub_orchestrator(
                "process_partition", partition))
        
        # Wait for batch to complete
        results = yield ctx.task_all(tasks)
        
        for result in results:
            if result["status"] == "completed":
                all_bronze_paths.append(result["blob_path"])
            else:
                failed_partitions.append(result["partition"])
    
    # ── PHASE 2B: Retry Failed Partitions (sequential) ───────────
    if failed_partitions:
        for partition in failed_partitions:
            try:
                result = yield ctx.call_sub_orchestrator(
                    "process_partition",
                    {**partition, "sequential_mode": True})
                all_bronze_paths.append(result["blob_path"])
            except Exception as e:
                logging.error(f"Partition {partition['id']} permanently failed: {e}")
    
    # ── PHASE 3: Explode Bronze → Exploded Bronze (fan-out) ──────
    explode_tasks = []
    for bronze_path in all_bronze_paths:
        explode_tasks.append(ctx.call_activity_with_retry(
            "explode_partition", retry_options, {
                "bronze_blob_path": bronze_path,
                "output_prefix": "bronze/exploded/"
            }))
    
    exploded_results = yield ctx.task_all(explode_tasks)
    exploded_paths = [r["output_blob_path"] for r in exploded_results]
    
    # ── PHASE 4: Entra User Pull (parallel with Phase 3) ─────────
    # Note: In practice, Entra pull runs in parallel with explosion
    # since they're independent. Shown sequentially for clarity.
    entra_result = yield ctx.call_activity_with_retry(
        "pull_entra_users", retry_options, {
            "output_prefix": "bronze/entra/"
        })
    
    # Entra Bronze → Silver transform
    entra_silver = yield ctx.call_activity_with_retry(
        "transform_entra_silver", retry_options, {
            "entra_bronze_path": entra_result["blob_path"],
            "output_path": "silver/entra-users/silver_entra_users.csv"
        })
    
    # ── PHASE 5: Bronze-to-Silver Transform ───────────────────────
    silver_result = yield ctx.call_activity_with_retry(
        "transform_silver", retry_options, {
            "exploded_blob_paths": exploded_paths,
            "entra_silver_path": entra_silver["blob_path"],
            "output_path": "silver/copilot-usage/silver_copilot_usage.csv"
        })
    
    # ── PHASE 6: Notify ──────────────────────────────────────────
    yield ctx.call_activity("notify_completion", {
        "run_id": run_id,
        "partitions_processed": len(all_bronze_paths),
        "records_ingested": sum(r.get("records", 0) for r in results),
        "records_transformed": silver_result["records_transformed"],
        "duration_minutes": (ctx.current_utc_datetime - start_time).total_seconds() / 60,
        "status": "success" if not failed_partitions else "partial_success"
    })
    
    return {
        "status": "completed",
        "run_id": run_id,
        "partitions": len(all_bronze_paths),
        "total_records": silver_result["records_transformed"]
    }
```

### Sub-Orchestrator: `process_partition`

```python
@df.orchestrator_trigger
async def process_partition(ctx: df.DurableOrchestrationContext):
    """Process a single time partition: CREATE → POLL → FETCH."""
    
    partition = ctx.get_input()
    
    # Phase 1: Create query
    query = yield ctx.call_activity_with_retry(
        "create_query", retry_opts, {
            "start_time": partition["start"],
            "end_time": partition["end"],
            "activity_types": partition["activity_types"],
            "display_name": f"IH-{partition['id']}"
        })
    
    # Phase 2: Poll until complete
    MAX_POLL_ATTEMPTS = 120  # ~2 hours at 60s intervals
    for attempt in range(MAX_POLL_ATTEMPTS):
        # Randomized poll interval (30-90s, matching PAX)
        if not ctx.is_replaying:
            poll_interval = random.randint(30, 90)
        else:
            poll_interval = 60  # Deterministic during replay
        
        yield ctx.create_timer(
            ctx.current_utc_datetime + timedelta(seconds=poll_interval))
        
        status = yield ctx.call_activity("poll_query", {
            "query_id": query["query_id"]
        })
        
        if status["status"] == "succeeded":
            # Check for subdivision trigger
            if status["record_count"] >= 950_000:
                # Subdivide: split time window and recurse
                yield ctx.call_activity("cleanup_query", query["query_id"])
                
                sub_partitions = yield ctx.call_activity(
                    "subdivide_partition", {
                        "start": partition["start"],
                        "end": partition["end"],
                        "record_count": status["record_count"]
                    })
                
                # Recursive fan-out for sub-partitions
                sub_tasks = [
                    ctx.call_sub_orchestrator("process_partition", sp)
                    for sp in sub_partitions
                ]
                sub_results = yield ctx.task_all(sub_tasks)
                return {"status": "completed", "sub_results": sub_results}
            
            break  # Normal completion — proceed to fetch
        
        elif status["status"] == "failed":
            raise RetryableError(f"Query {query['query_id']} failed")
    else:
        raise TimeoutError(f"Query {query['query_id']} did not complete in time")
    
    # Phase 3: Fetch records (paginated, streaming to ADLS)
    blob_path = f"bronze/purview/{partition['date_prefix']}/partition_{partition['id']}.jsonl"
    
    fetch_result = yield ctx.call_activity_with_retry(
        "fetch_records", retry_opts, {
            "query_id": query["query_id"],
            "output_blob_path": blob_path
        })
    
    # Cleanup: DELETE the query to free the slot
    yield ctx.call_activity("cleanup_query", {"query_id": query["query_id"]})
    
    # Update run state
    yield ctx.call_activity("update_run_state", {
        "partition_id": partition["id"],
        "status": "completed",
        "records": fetch_result["records_written"],
        "blob_path": blob_path
    })
    
    return {
        "status": "completed",
        "partition": partition,
        "blob_path": blob_path,
        "records": fetch_result["records_written"]
    }
```

---

## 7. Reuse Assessment

### Components Reused From Existing Pipeline

| Component | Source File | Reuse Level | Modifications |
|---|---|---|---|
| Explosion core logic | `transform/explosion/` processor module | **High** | Replace local I/O with ADLS streams |
| `transform_row()` (purview) | `transform/bronze_to_silver_purview.py` | **As-is** | None — pure function |
| `compute_prompt_type()` | `transform/bronze_to_silver_purview.py` | **As-is** | None — pure function |
| `compute_is_agent()` | `transform/bronze_to_silver_purview.py` | **As-is** | None — pure function |
| `safe_int()` | `transform/bronze_to_silver_purview.py` | **As-is** | None — pure function |
| `make_dedup_key()` | `transform/bronze_to_silver_purview.py` | **As-is** | None — pure function |
| `enrich_row_with_entra()` | `transform/bronze_to_silver_purview.py` | **As-is** | None — pure function |
| `load_entra_lookup()` | `transform/bronze_to_silver_purview.py` | **Moderate** | Read from ADLS only (drop local path) |
| `transform_row()` (entra) | `transform/bronze_to_silver_entra.py` | **As-is** | None — pure function |
| `build_column_map()` | `transform/bronze_to_silver_entra.py` | **As-is** | None — pure function |
| `has_copilot_license()` | `transform/bronze_to_silver_entra.py` | **As-is** | None — pure function |
| `compute_license_tier()` | `transform/bronze_to_silver_entra.py` | **As-is** | None — pure function |
| `SILVER_COLUMNS` / `SOURCE_TO_SILVER` | `transform/bronze_to_silver_entra.py` | **As-is** | None — constants |
| `upload_csv_to_adls()` | Multiple files (3 copies) | **Consolidate** | One shared `adls_client.py` |
| ADLS blob access pattern | `dashboard/api/function_app.py` | **Pattern reuse** | Same `BlobServiceClient` approach |

### Components Dropped/Replaced

| Component | Reason |
|---|---|
| `run-pipeline-local.ps1` (PowerShell orchestrator) | Replaced by Durable orchestrator |
| PAX PowerShell script (17,417 lines) | Replaced by activities + shared modules |
| `_resolve_keyvault_refs()` (3 copies) | Replaced by Managed Identity + native KV refs |
| All `argparse` CLI code | Replaced by activity function parameters |
| Local file read/write logic | Replaced by ADLS streaming I/O |
| Local file cleanup logic | No local files to clean up |
| PAX checkpoint system | Replaced by Durable Functions built-in replay |

### New Components to Build

| Component | Complexity | Lines (est.) |
|---|---|---|
| `shared/graph_client.py` | Medium | ~150 |
| `shared/adls_client.py` | Medium | ~200 |
| `shared/partitioning.py` | Medium | ~250 |
| `shared/explosion.py` (adapted) | Low (mostly reuse) | ~100 adapter + existing |
| `shared/transforms.py` (extracted) | Low (mostly reuse) | ~80 adapter + existing |
| `shared/entra_transforms.py` (extracted) | Low (mostly reuse) | ~60 adapter + existing |
| `shared/models.py` | Low | ~100 |
| `shared/constants.py` | Low | ~200 (activity bundles) |
| `shared/config.py` | Low | ~50 |
| `orchestrators/pipeline_orchestrator.py` | High | ~300 |
| `activities/` (11 activity functions) | Medium each | ~100 each ≈ 1,100 total |
| `function_app.py` (entry point) | Low | ~60 |
| Tests | Medium | ~500 |
| **Total estimated new code** | | **~3,150 lines** |

---

## 8. Configuration Strategy

### Current → New Configuration Mapping

| Current (JSON config file) | New (Function App Settings) | Notes |
|---|---|---|
| `auth.tenantId` | `IH_TENANT_ID` | App Setting |
| `auth.clientId` | `IH_CLIENT_ID` | App Setting |
| `auth.clientSecret` (`@KeyVault:`) | `IH_CLIENT_SECRET` (`@Microsoft.KeyVault(...)`) | Native Key Vault reference |
| `adls.storageAccountName` | `IH_ADLS_ACCOUNT_NAME` | App Setting |
| `adls.storageAccountKey` | *(removed)* | Managed Identity — no key needed |
| `adls.containerName` | `IH_ADLS_CONTAINER` | App Setting |
| `adls.paths.*` | `IH_ADLS_PATH_*` | App Settings (or hardcoded constants) |
| `ingestion.defaultLookbackDays` | `IH_DEFAULT_LOOKBACK_DAYS` | App Setting |
| `pax.defaultActivityTypes` | `IH_ACTIVITY_TYPES` | App Setting |
| `notifications.teamsWebhookUrl` | `IH_TEAMS_WEBHOOK_URL` (`@Microsoft.KeyVault(...)`) | Native Key Vault reference |

### New Configuration Parameters

| Setting | Default | Description |
|---|---|---|
| `IH_PARTITION_HOURS` | `6` | Time window partition size in hours |
| `IH_MAX_CONCURRENCY` | `4` | Max parallel partition processing |
| `IH_TARGET_RECORDS_PER_BLOCK` | `5000` | Adaptive block sizing target |
| `IH_SUBDIVISION_THRESHOLD` | `950000` | Record count that triggers subdivision |
| `IH_POLL_MIN_SECONDS` | `30` | Minimum poll interval |
| `IH_POLL_MAX_SECONDS` | `90` | Maximum poll interval |
| `IH_MAX_POLL_ATTEMPTS` | `120` | Max poll cycles before timeout |
| `IH_EXPLOSION_MODE` | `raw` | Explosion mode (raw = Python handles) |
| `IH_DURABLE_TASK_HUB` | `ihpipelinehub` | Durable Functions task hub name |
| `IH_SCHEDULE_CRON` | `0 0 2 * * *` | Daily at 2:00 AM UTC |

---

## 9. Implementation Phases

### Phase 1: Foundation (Est. 2-3 sessions)

**Goal:** Function App infrastructure + core shared modules

| Step | Task | Detail |
|---|---|---|
| 1.1 | Create Azure resources | `ih-pipeline-func01`, `ihpipelinestor01`, IAM roles |
| 1.2 | Scaffold project | `pipeline/` folder structure, `host.json`, `requirements.txt` |
| 1.3 | Build `shared/config.py` | Env var loader with defaults |
| 1.4 | Build `shared/models.py` | Pydantic models for partitions, queries, results |
| 1.5 | Build `shared/graph_client.py` | MSAL `ConfidentialClientApplication` wrapper |
| 1.6 | Build `shared/adls_client.py` | `DefaultAzureCredential` + streaming blob I/O |
| 1.7 | Unit tests for shared modules | Mock MSAL + ADLS |
| 1.8 | Deploy empty Function App | Verify deployment, App Insights connection |

**Deliverable:** Deployable Function App skeleton with tested shared modules.

### Phase 2: Ingestion Activities (Est. 3-4 sessions)

**Goal:** Replicate PAX's Graph API lifecycle in Python activities

| Step | Task | Detail |
|---|---|---|
| 2.1 | Build `activities/plan_partitions_activity.py` | Port PAX partition logic |
| 2.2 | Build `shared/partitioning.py` | Time window generation + adaptive sizing |
| 2.3 | Build `activities/create_query_activity.py` | POST `/auditLog/queries` |
| 2.4 | Build `activities/poll_query_activity.py` | GET query status |
| 2.5 | Build `activities/fetch_records_activity.py` | Paginated GET + ADLS streaming |
| 2.6 | Build `activities/check_subdivision_activity.py` | Volume threshold logic |
| 2.7 | Build `activities/cleanup_queries_activity.py` | DELETE completed queries |
| 2.8 | Integration test: single partition | End-to-end: create → poll → fetch → ADLS |
| 2.9 | Build `shared/constants.py` | Activity bundle definitions from PAX |

**Deliverable:** Can ingest audit logs for a single time partition to ADLS.

### Phase 3: Orchestration (Est. 2-3 sessions)

**Goal:** Wire activities into Durable Functions orchestrator with fan-out/fan-in

| Step | Task | Detail |
|---|---|---|
| 3.1 | Build `process_partition` sub-orchestrator | CREATE → POLL → FETCH lifecycle |
| 3.2 | Build main `pipeline_orchestrator` | Partition planning, batched fan-out |
| 3.3 | Add staggered launch + concurrency control | Timer-based jitter, batch sizing |
| 3.4 | Add retry layers 2-3 | Partition retry loop + sequential fallback |
| 3.5 | Add subdivision sub-orchestration | Recursive split on volume threshold |
| 3.6 | Add cross-run resume logic | ADLS state file check on startup |
| 3.7 | Integration test: multi-partition run | Fan-out with 2+ partitions |
| 3.8 | Add timer trigger + HTTP manual trigger | `function_app.py` entry points |

**Deliverable:** Full ingestion pipeline running as Durable Functions with retry + resume.

### Phase 4: Transform Pipeline (Est. 2-3 sessions)

**Goal:** Explosion + Silver transforms as activities

| Step | Task | Detail |
|---|---|---|
| 4.1 | Build `shared/explosion.py` | Adapt existing explosion processor for ADLS I/O |
| 4.2 | Build `activities/explode_partition_activity.py` | 153-col explosion → ADLS |
| 4.3 | Build `activities/pull_entra_activity.py` | Graph `/users` pull → ADLS |
| 4.4 | Build `shared/entra_transforms.py` | Extract Entra Silver logic |
| 4.5 | Build `shared/transforms.py` | Extract Purview Silver logic |
| 4.6 | Build `activities/transform_silver_activity.py` | Bronze → Silver with JOIN + dedup |
| 4.7 | Integrate into orchestrator | Add explosion + transform phases |
| 4.8 | Integration test: end-to-end | Ingest → Explode → Transform → Silver in ADLS |

**Deliverable:** Complete pipeline: audit logs → Silver analytics in ADLS, zero local files.

### Phase 5: Notifications & Polish (Est. 1-2 sessions)

**Goal:** Teams notifications, monitoring, edge cases

| Step | Task | Detail |
|---|---|---|
| 5.1 | Build `activities/notify_activity.py` | Teams Adaptive Card webhook |
| 5.2 | Configure App Insights alerts | Failed orchestration, long-running, 429 spikes |
| 5.3 | Add zero-record recovery (Layer 4 retry) | Verify 0-record partitions |
| 5.4 | Circuit breaker implementation | Reduce concurrency on repeated 429s |
| 5.5 | Production `host.json` tuning | Timeouts, concurrency limits, Durable config |
| 5.6 | Documentation | README, runbook, architecture diagram |
| 5.7 | Dashboard cache warming | POST to API `/health` after successful run |

**Deliverable:** Production-ready pipeline with monitoring and alerting.

### Phase 6: Validation & Cutover (Est. 1-2 sessions)

**Goal:** Validate output parity with current pipeline, then switch

| Step | Task | Detail |
|---|---|---|
| 6.1 | Parallel run | Run both pipelines for same date range |
| 6.2 | Output comparison | Diff Silver CSVs for record parity |
| 6.3 | Performance benchmarking | Compare ingestion time, throughput |
| 6.4 | Cost verification | Monitor actual Azure costs for 1 week |
| 6.5 | Cutover | Disable local pipeline, enable timer trigger |
| 6.6 | Post-cutover monitoring | Watch for 48 hours, verify dashboard data |

**Deliverable:** Pipeline fully running in Azure. Local execution retired.

---

## 10. Testing Strategy

### Unit Tests

| Module | Test Focus |
|---|---|
| `shared/graph_client.py` | Token acquisition, 401 retry, 429 detection |
| `shared/partitioning.py` | Partition generation, subdivision, boundary cases |
| `shared/explosion.py` | 153-column schema, Copilot message expansion, edge cases |
| `shared/transforms.py` | `transform_row`, `compute_prompt_type`, `compute_is_agent`, dedup |
| `shared/adls_client.py` | Streaming read/write, chunk sizing |

### Integration Tests (Against Demo Tenant)

| Test | What It Validates |
|---|---|
| Single partition ingest | CREATE → POLL → FETCH → ADLS bronze blob |
| Multi-partition fan-out | Concurrency control, stagger timing |
| Subdivision trigger | Create known-large query, verify split behavior |
| Explosion pipeline | Bronze JSONL → 153-col CSV in ADLS |
| Silver transform | Bronze → Silver with Entra JOIN, dedup |
| Resume after failure | Kill mid-run, restart, verify continuation |
| 429 handling | Simulate throttling, verify backoff + retry |

### Output Parity Test

```
1. Run current local pipeline for date range D
2. Run new Durable pipeline for same date range D
3. Download both Silver CSVs
4. Sort by (RecordId, Message_Id)
5. Assert row-level equality (ignoring _LoadedAtUtc timestamps)
```

---

## 11. Monitoring & Alerting

### Reuse Existing App Insights (`ih-app-insights`)

| Signal | Alert Rule | Severity |
|---|---|---|
| Orchestration failed | `exceptions` where `cloud_RoleName` = `ih-pipeline-func01` | Sev 1 |
| Orchestration running > 4 hours | Custom metric: duration > 14400s | Sev 2 |
| 429 rate > 10/hour | Custom event: `ThrottledError` count | Sev 3 |
| Zero records ingested | Custom metric: `records_ingested` = 0 | Sev 2 |
| Activity function failure rate > 20% | `requests` success rate < 80% | Sev 2 |

### Custom Telemetry

```python
# In each activity function
from azure.monitor.opentelemetry import configure_azure_monitor
import logging

logger = logging.getLogger("ih-pipeline")

# Example: track partition processing
logger.info("Partition processed", extra={
    "custom_dimensions": {
        "partition_id": partition_id,
        "records_fetched": records,
        "duration_seconds": elapsed,
        "query_id": query_id
    }
})
```

### KQL Queries for Monitoring

```kql
// Pipeline run history
customEvents
| where name == "PipelineCompleted"
| extend records = toint(customDimensions.total_records)
| extend duration = todouble(customDimensions.duration_minutes)
| project timestamp, records, duration
| order by timestamp desc

// Throttling trend
exceptions
| where type == "ThrottledError"
| summarize count() by bin(timestamp, 1h)
| render timechart

// Activity function performance
requests
| where cloud_RoleName == "ih-pipeline-func01"
| summarize avg(duration), percentile(duration, 95), count() 
  by name, bin(timestamp, 1h)
```

---

## 12. Cost Model

### Detailed Consumption Estimate

**Assumptions** (based on current workload):
- Daily run: ~50,000 records ingested
- 8 partitions per run (1 day ÷ 6h blocks × some overlap)
- ~45 min total polling time (8 queries × ~5 min average)
- ~10 min total fetch time (50K records at ~5K/page)
- ~5 min explosion + transform time

| Component | Calculation | Monthly Cost |
|---|---|---|
| **Function Executions** | ~200 executions/day × 30 = 6,000/mo (free grant: 1M) | $0.00 |
| **Execution Duration** | ~60 min/day × 256 MB = ~450,000 GB-s/mo (free: 400,000) | ~$0.008 |
| **ADLS Storage** | ~5 GB new/mo (Cool tier) | ~$0.05 |
| **ADLS Transactions** | ~10,000 ops/mo | ~$0.005 |
| **Durable Storage** | Task hub in `ihpipelinestor01`: ~100 MB | ~$0.002 |
| **Key Vault** | ~200 secret reads/mo (free: 10,000) | $0.00 |
| **App Insights** | ~100 MB telemetry/mo (free: 5 GB) | $0.00 |
| **Total Additional** | | **~$0.06-0.50/mo** |

### Cost Scenarios

| Scenario | Records/Day | Est. Monthly Cost |
|---|---|---|
| Light (PoC) | 5,000 | ~$0.06 |
| Normal (current) | 50,000 | ~$0.15 |
| Heavy (large org) | 500,000 | ~$0.50 |
| Extreme (1M+/day) | 1,000,000+ | ~$1.50 |

> All scenarios well within the $60/mo budget. Even at extreme volumes, the pipeline costs < $2/mo.

---

## 13. Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| **Consumption plan 10-min timeout** | Activity functions processing large partitions may timeout | Medium | Split fetch into multiple activities (one per page batch); use `functionTimeout` override in `host.json` ("02:30:00" for Consumption plan max) |
| **1.5 GB memory limit** | Large Silver file dedup may exceed memory | Medium | Streaming dedup with key-only loading; partitioned Silver files if needed |
| **Durable Functions replay non-determinism** | Random values in orchestrator cause replay errors | High (if not handled) | Use `ctx.new_guid()` for deterministic IDs; fixed poll intervals during replay |
| **MSAL token cache cold starts** | Each activity cold start requires fresh token acquisition | Low | Token acquisition is ~200ms; acceptable overhead |
| **Graph API behavior changes** | Microsoft may change Purview API response format | Low | Defensive parsing; schema validation in fetch activity |
| **ADLS throttling under high write volume** | Many parallel writes may hit storage limits | Low | Standard_LRS supports 20,000 IOPS; our workload is ~100 IOPS |
| **Explosion module compatibility** | Existing Python explosion module may need significant changes | Medium | Test early (Phase 4.1); have fallback plan to port explosion logic directly |
| **EOM fallback not implemented** | Graph API outage with no fallback path | Low | Phase 1 only targets Graph API; document EOM as future enhancement |

### Consumption Plan Timeout Deep Dive

The Consumption plan has a **default 5-minute timeout** per function execution, configurable up to **10 minutes** via `host.json`:

```json
{
    "functionTimeout": "00:10:00"
}
```

**Critical Activity Analysis:**

| Activity | Expected Duration | Risk |
|---|---|---|
| `create_query` | <5s | None |
| `poll_query` | <5s | None |
| `fetch_records` (50K records, paginated) | ~2-5 min | Low — split if needed |
| `explode_partition` (50K records) | ~1-3 min | Low |
| `transform_silver` (50K new records + dedup against 5M existing) | ~3-8 min | **Medium** — may need chunking |
| `pull_entra_users` (pagination for large org) | ~1-3 min | Low |

**Mitigation for `transform_silver`**: If Silver grows beyond what can be processed in 10 minutes, split into a sub-orchestration that processes exploded files one at a time, each appending to Silver incrementally.

---

## 14. Decision Log

| # | Decision | Rationale | Alternatives Considered |
|---|---|---|---|
| D1 | Separate Function App (`ih-pipeline-func01`) | Isolation from dashboard API; independent scaling and deployment | Shared function app (rejected: deployment coupling) |
| D2 | Consumption plan (Y1) | Cost-effective; workload is bursty (1 daily run); free grant covers most usage | Premium plan (rejected: $60/mo minimum); Dedicated (rejected: overkill) |
| D3 | Managed Identity for ADLS/Key Vault | Zero credentials to manage; automatic rotation | Storage account keys (rejected: security risk); SAS tokens (rejected: expiry management) |
| D4 | Client credentials for Graph API | Only auth method suitable for unattended serverless; stored in Key Vault | Managed Identity with Graph (rejected: requires additional Azure AD config not available on demo tenant) |
| D5 | MSAL Python over raw HTTP | Built-in token caching, refresh, retry; matches PAX's auth sophistication | Raw `requests` + JWT (rejected: reinventing the wheel) |
| D6 | JSONL for bronze storage | Matches PAX output format; line-by-line streaming; easy to append | CSV (rejected: escaping issues with JSON payloads); Parquet (rejected: streaming writes not supported) |
| D7 | Append Blob for streaming writes | Enables page-by-page write without holding entire file in memory | Block blob with full upload (rejected: memory overflow risk) |
| D8 | Graph API only (Phase 1) | Demo tenant works with Graph; EOM is legacy fallback | EOM support (deferred: adds significant complexity for no current benefit) |
| D9 | Python 3.11 | Matches existing `ih-api-poc01`; stable; Durable SDK support | Python 3.12 (deferred: Durable SDK verification needed) |
| D10 | Reuse existing explosion module | ~321 lines already tested; adapting is faster than rewriting | Full rewrite (rejected: risk of regression in 153-column schema) |
| D11 | Reuse existing Silver transform functions | Pure functions with no I/O dependencies; proven correct | Rewrite (rejected: no benefit, increased risk) |
| D12 | AppendBlobClient for ADLS writes | Native support in azure-storage-blob; matches streaming pattern | DataLake SDK (rejected: heavier dependency; blob SDK sufficient) |

---

## Appendix A: PAX Feature Coverage Matrix

This matrix confirms every significant PAX feature has a Durable Functions equivalent:

| PAX Feature | PAX Lines | DF Equivalent | Section |
|---|---|---|---|
| 6 auth methods | 3001-3800 | MSAL `ConfidentialClientApplication` | 5.1 |
| JWT token extraction | ~3200 | MSAL handles internally | 5.1 |
| Proactive token refresh | ~3400 | MSAL cache + auto-refresh | 5.1 |
| Cross-thread token sharing | ~3500 | Eliminated (activity isolation) | 5.1 |
| 401 token retry | ~3600 | `graph_request()` wrapper | 5.1 |
| POST create audit query | ~6300 | `create_query_activity` | 5.2 |
| Poll query status | ~6400 | `poll_query_activity` + `create_timer` | 5.2 |
| Paginated record fetch | ~6600 | `fetch_records_activity` + `@odata.nextLink` | 5.2 |
| Query cleanup (DELETE) | ~6800 | `cleanup_queries_activity` | 5.2 |
| Partition generation | ~7350 | `shared/partitioning.py` | 5.3 |
| Adaptive block sizing | ~7500 | ADLS `block_history.json` | 5.3 |
| Smart subdivision | ~7800 | `subdivide_partition` sub-orchestration | 5.3 |
| Extreme volume warnings | ~8000 | Logging + App Insights alert | 5.3 |
| 429 detection (3-layer) | ~6200 | Activity raises `ThrottledError` | 5.4 |
| Exponential backoff (60→300s) | ~6250 | `RetryOptions` with `backoff_coefficient` | 5.4 |
| Concurrency control | ~9000 | Batched fan-out (`BATCH_SIZE=4`) | 5.4 |
| Staggered launch (10-25s) | ~9100 | `create_timer` between batch starts | 5.4 |
| Circuit breaker | ~9200 | Orchestrator state tracking | 5.4 |
| 5-layer retry architecture | Multiple | DF retry policy + sub-orchestration loops | 5.5 |
| 401/403 differentiation | ~6350 | Activity error handling | 5.5 |
| Graceful exit | ~12000 | DF built-in crash recovery | 5.5 |
| 153-column explosion | 8700-16200 | `shared/explosion.py` (adapted) | 5.6 |
| Copilot message expansion | ~9000 | Reused from explosion module | 5.6 |
| Agent categorization | ~9200 | Reused from explosion module | 5.6 |
| Parallel explosion | ~9500 | Fan-out activity pattern | 5.6 |
| CSV streaming (1MB buffer) | ~9700 | Append blob writes | 5.6 |
| RecordId deduplication | ~10000 | Per-activity HashSet + Silver dedup | 5.6 |
| v2 checkpoint format | 3800-5400 | DF built-in replay + ADLS state | 5.7 |
| Atomic writes | ~4000 | ADLS atomic blob operations | 5.7 |
| `_PARTIAL` file naming | ~4200 | Not needed (orchestrator controls flow) | 5.7 |
| Incremental JSONL saves | ~4400 | Per-page ADLS append | 5.7 |
| Resume from checkpoint | ~4600 | DF replay + ADLS state check | 5.7 |
| Activity bundles (~100+) | 1300-2400 | `shared/constants.py` | 5.8 |
| 7-step resolution pipeline | ~1800 | `plan_partitions_activity` | 5.8 |
| Query planning | ~2000 | Partition planner | 5.8 |
| EOM fallback | ~2200 | Deferred (documented) | 5.8 |
| Entra user pull (37+ cols) | ~14000 | `pull_entra_activity` | 5.9 |
| License SKU detection | ~14500 | Reused from `entra_transforms.py` | 5.9 |
| Memory management (75% RAM) | ~15000 | Chunked streaming (50 MB) | 5.6 |
| Metrics/telemetry (60+ fields) | ~15500 | App Insights custom dimensions | 11 |
| Progress tracking | ~16000 | Orchestrator state + logging | 6 |
| Exit codes (0/10/20) | ~16200 | Orchestrator return status | 6 |

---

## Appendix B: ADLS Path Structure (New)

```
insight-harbor/                          (container)
├── bronze/
│   ├── purview/                         (audit log JSONL — NEW FORMAT)
│   │   └── YYYY/MM/DD/
│   │       ├── partition_001.jsonl
│   │       ├── partition_002.jsonl
│   │       └── ...
│   ├── exploded/                        (153-col flat CSV — same as current)
│   │   └── YYYY/MM/DD/
│   │       ├── exploded_partition_001.csv
│   │       └── ...
│   └── entra/                           (raw Entra user data — NEW)
│       └── YYYY/MM/DD/
│           └── entra_users_raw.csv
├── silver/
│   ├── copilot-usage/                   (unchanged)
│   │   └── silver_copilot_usage.csv
│   └── entra-users/                     (unchanged)
│       └── silver_entra_users.csv
└── pipeline/                            (NEW — pipeline metadata)
    ├── state/
    │   ├── run_20250708_020000.json     (run state for resume)
    │   ├── run_20250709_020000.json
    │   └── ...
    └── history/
        └── block_history.json           (adaptive block sizing data)
```

---

## Appendix C: PAX Intelligence Gap Analysis

> **Added:** Post-review audit to ensure every piece of PAX intelligence is captured  
> **Audit Scope:** Complete PAX v1.10.7 parameter space (55+ params, 18 categories)  
> **Reference:** `docs/PAX_Purview_Audit_Report.md` (724-line exhaustive audit)

### Summary

The original plan (Sections 5.1–5.10) covers the **core architectural patterns** faithfully. This appendix identifies **12 specific intelligence items** that were implicit or missing, organized by criticality. All items have been resolved with specific implementation guidance below.

### Gap 1: `-IncludeM365Usage` Full Bundle Support (CRITICAL)

**What PAX does (lines 1425-1470, 1700-1860):**

When `-IncludeM365Usage` is active, PAX:
1. Appends `$m365UsageActivityBundle` (~120 activity types across 12 workload categories) to the requested `ActivityTypes`
2. Merges `$m365UsageRecordBundle` (14 record types) into `RecordTypes`
3. Forces `$ServiceTypes = $null` — critical optimization that eliminates per-service splitting and runs a single pass
4. Adds `CopilotInteraction` as a base activity type (unless `-ExcludeCopilotInteraction` is set)

**What the plan had:** `IH_ACTIVITY_TYPES=CopilotInteraction` with mention of bundles in `shared/constants.py`, but no detail on the M365 usage composition logic or the `ServiceTypes = $null` optimization.

**Resolution — Add to `shared/constants.py`:**

```python
# ═══════════════════════════════════════════════════════════════════
# M365 Usage Activity Bundle (matches PAX $m365UsageActivityBundle)
# ~120 activity types across 12 workload categories
# ═══════════════════════════════════════════════════════════════════

M365_USAGE_ACTIVITY_BUNDLE: list[str] = [
    # ── Exchange (8) ──
    "MailItemsAccessed", "Send", "MailboxLogin",
    "SearchQueryInitiatedExchange", "MoveToDeletedItems",
    "SoftDelete", "HardDelete", "UpdateInboxRules",

    # ── SharePoint / OneDrive Files (11) ──
    "FileAccessed", "FileModified", "FileUploaded",
    "FileDownloaded", "FileDeleted", "FileRenamed",
    "FileMoved", "FileCopied", "FileCheckedOut",
    "FileCheckedIn", "FileRecycled",

    # ── SharePoint / OneDrive Sharing (8) ──
    "SharingSet", "SharingInvitationCreated", "SharingInvitationAccepted",
    "AnonymousLinkCreated", "CompanyLinkCreated",
    "SecureLinkCreated", "SharingRevoked", "SharingInheritanceBroken",

    # ── SharePoint Groups (2) ──
    "AddedToGroup", "RemovedFromGroup",

    # ── Teams Management (17+) ──
    "TeamCreated", "TeamDeleted", "TeamSettingChanged",
    "MemberAdded", "MemberRemoved", "MemberRoleChanged",
    "ChannelAdded", "ChannelDeleted", "ChannelSettingChanged",
    "TabAdded", "TabRemoved", "TabUpdated",
    "ConnectorAdded", "ConnectorRemoved", "ConnectorUpdated",
    "BotAddedToTeam", "BotRemovedFromTeam",

    # ── Teams Chat (13+) ──
    "ChatCreated", "ChatUpdated", "ChatRetrieved",
    "MessageSent", "MessageUpdated", "MessageDeleted",
    "MessageRead", "MessageHostedContentRead",
    "SubscribedToMessages", "UnsubscribedFromMessages",
    "MessageCreatedNotification", "MessageDeletedNotification",
    "MessageUpdatedNotification",

    # ── Teams Meetings (14+) ──
    "MeetingStarted", "MeetingEnded", "MeetingJoined",
    "MeetingLeft", "MeetingParticipantDetail",
    "MeetingDetail", "MeetingPolicyUpdated",
    "MeetingRecordingStarted", "MeetingRecordingStopped",
    "MeetingTranscriptionStarted", "MeetingTranscriptionStopped",
    "MeetingRegistrationCreated", "MeetingRegistrationUpdated",
    "MeetingRegistered",

    # ── Teams Apps (5+) ──
    "AppInstalled", "AppUpgraded", "AppUninstalled",
    "AppPermissionGranted", "AppPermissionRevoked",

    # ── Office Apps (5) ──
    "FileAccessedExtended", "FileSyncUploadFull",
    "FileSyncDownloadFull", "FileModifiedExtended",
    "SearchQueryInitiatedSharePoint",

    # ── Microsoft Forms (8) ──
    "FormCreated", "FormUpdated", "FormDeleted",
    "FormViewed", "FormResponseCreated", "FormResponseUpdated",
    "FormResponseDeleted", "FormSummaryViewed",

    # ── Microsoft Stream (4) ──
    "StreamVideoCreated", "StreamVideoUpdated",
    "StreamVideoDeleted", "StreamVideoViewed",

    # ── Planner (8) ──
    "PlanCreated", "PlanModified", "PlanDeleted",
    "PlanCopied", "PlanRead",
    "TaskCreated", "TaskModified", "TaskDeleted",

    # ── Power Apps (5) ──
    "LaunchPowerApp", "PowerAppPermissionEdited",
    "AppLaunched", "GatewayResourceRequestAction",
    "EnvironmentPropertyChange",
]

# CopilotInteraction is always included by default (unless ExcludeCopilotInteraction)
COPILOT_BASE_ACTIVITY_TYPE = "CopilotInteraction"

# ═══════════════════════════════════════════════════════════════════
# M365 Usage Record Type Bundle (matches PAX $m365UsageRecordBundle)
# 14 record types
# ═══════════════════════════════════════════════════════════════════

M365_USAGE_RECORD_BUNDLE: list[str] = [
    "ExchangeAdmin", "ExchangeItem", "ExchangeMailbox",
    "SharePointFileOperation", "SharePointSharingOperation",
    "SharePoint", "OneDrive",
    "MicrosoftTeams",
    "OfficeNative",
    "MicrosoftForms",
    "MicrosoftStream",
    "PlannerPlan", "PlannerTask",
    "PowerAppsApp",
]

# ═══════════════════════════════════════════════════════════════════
# DSPM for AI Activity Types (matches PAX -IncludeDSPMForAI)
# ═══════════════════════════════════════════════════════════════════

DSPM_AI_ACTIVITY_TYPES: list[str] = [
    "AICompliancePolicyEvent",
    "DlpSensitiveInformationTypeCmdletRecord",
    "SecurityComplianceAlerts",
]

# ═══════════════════════════════════════════════════════════════════
# Record Type → Workload Mapping (matches PAX $recordTypeWorkloadMap)
# Used for service-based query splitting
# ═══════════════════════════════════════════════════════════════════

RECORD_TYPE_WORKLOAD_MAP: dict[str, list[str]] = {
    "ExchangeAdmin": ["Exchange"],
    "ExchangeItem": ["Exchange"],
    "ExchangeMailbox": ["Exchange"],
    "SharePointFileOperation": ["SharePoint", "OneDrive"],
    "SharePointSharingOperation": ["SharePoint", "OneDrive"],
    "SharePoint": ["SharePoint"],
    "OneDrive": ["OneDrive"],
    "MicrosoftTeams": ["MicrosoftTeams"],
    "OfficeNative": [],         # Cross-workload — no service filter
    "MicrosoftForms": [],       # Cross-workload
    "MicrosoftStream": [],      # Cross-workload
    "PlannerPlan": [],          # Cross-workload
    "PlannerTask": [],          # Cross-workload
    "PowerAppsApp": [],         # Cross-workload
}

# ═══════════════════════════════════════════════════════════════════
# Service → Operation Mapping (matches PAX $serviceOperationMap)
# Used to align operationFilters per workload pass
# ═══════════════════════════════════════════════════════════════════

SERVICE_OPERATION_MAP: dict[str, list[str]] = {
    "Exchange": [
        "MailItemsAccessed", "Send", "MailboxLogin",
        "SearchQueryInitiatedExchange", "MoveToDeletedItems",
        "SoftDelete", "HardDelete", "UpdateInboxRules",
    ],
    "SharePoint": [
        "FileAccessed", "FileModified", "FileUploaded",
        "FileDownloaded", "FileDeleted", "FileRenamed",
        "FileMoved", "FileCopied", "FileCheckedOut",
        "FileCheckedIn", "FileRecycled",
        "SharingSet", "SharingInvitationCreated",
        "SharingInvitationAccepted", "AnonymousLinkCreated",
        "CompanyLinkCreated", "SecureLinkCreated",
        "SharingRevoked", "SharingInheritanceBroken",
        "AddedToGroup", "RemovedFromGroup",
        "SearchQueryInitiatedSharePoint",
    ],
    "MicrosoftTeams": [
        "TeamCreated", "TeamDeleted", "TeamSettingChanged",
        "MemberAdded", "MemberRemoved", "MemberRoleChanged",
        "ChannelAdded", "ChannelDeleted", "ChannelSettingChanged",
        "TabAdded", "TabRemoved", "TabUpdated",
        "ConnectorAdded", "ConnectorRemoved", "ConnectorUpdated",
        "BotAddedToTeam", "BotRemovedFromTeam",
        "ChatCreated", "ChatUpdated", "ChatRetrieved",
        "MessageSent", "MessageUpdated", "MessageDeleted",
        "MessageRead", "MessageHostedContentRead",
        "MeetingStarted", "MeetingEnded", "MeetingJoined",
        "MeetingLeft", "MeetingParticipantDetail",
    ],
}

# ═══════════════════════════════════════════════════════════════════
# Copilot SKU IDs (matches PAX $script:CopilotSkuIds)
# Used for license detection in Entra enrichment
# NOTE: The existing Python transform uses keyword matching
# ("copilot" substring) which is MORE robust than fixed IDs,
# catching new SKUs automatically. These IDs are documented
# for reference but the keyword approach is preferred.
# ═══════════════════════════════════════════════════════════════════

COPILOT_SKU_IDS: list[str] = [
    "a55e26b6-c4c0-4667-bf8c-0f6e8ec4f1c1",  # Microsoft 365 Copilot
    "639dec6b-bb19-468b-871c-c5c441c4b0cb",  # Microsoft Copilot Studio
    "e946c3cd-ed45-4810-a223-d5f2e1a8076e",  # Copilot for Microsoft 365
    "cfb76ac1-a8da-4643-a65b-92c8751b8d39",  # Copilot for Microsoft 365 (alt)
    "d56f3deb-4993-4b11-aaab-4c84fb4ef9b1",  # Copilot for Microsoft 365 Enterprise
    "b4135cb0-3ced-4243-a038-7c3d4fbae1e8",  # Microsoft 365 Copilot for Finance
    "8c6fbc8c-3ec2-4657-ba29-1522c0370de2",  # Microsoft 365 Copilot for Sales
    "91484d0c-d3db-40aa-a026-be5c8c72ae6d",  # Microsoft 365 Copilot for Service
    "b39b5fa0-e5b2-46d4-8da8-76d800748c11",  # Microsoft 365 Copilot (GCC)
    "7f12cfab-27f5-4877-a5ee-c1e489f06b0f",  # Microsoft Copilot Studio (viral)
]
```

**Resolution — Add to `shared/config.py`:**

```python
# New configuration parameters for M365 usage mode
IH_INCLUDE_M365_USAGE = os.getenv("IH_INCLUDE_M365_USAGE", "false").lower() == "true"
IH_INCLUDE_DSPM_AI = os.getenv("IH_INCLUDE_DSPM_AI", "false").lower() == "true"
IH_EXCLUDE_COPILOT_INTERACTION = os.getenv("IH_EXCLUDE_COPILOT_INTERACTION", "false").lower() == "true"
```

**Resolution — Update `plan_partitions_activity.py` activity type resolution:**

```python
def resolve_activity_types(config) -> list[str]:
    """
    Resolve effective activity types matching PAX's 7-step pipeline.
    Handles -IncludeM365Usage, -IncludeDSPMForAI, -ExcludeCopilotInteraction.
    """
    activities = list(config.ACTIVITY_TYPES)  # Start with configured types

    # Step 1: Add M365 usage bundle if enabled
    if config.INCLUDE_M365_USAGE:
        activities.extend(M365_USAGE_ACTIVITY_BUNDLE)
        # Add CopilotInteraction base type unless excluded
        if not config.EXCLUDE_COPILOT_INTERACTION:
            if COPILOT_BASE_ACTIVITY_TYPE not in activities:
                activities.append(COPILOT_BASE_ACTIVITY_TYPE)

    # Step 2: Add DSPM for AI types if enabled
    if config.INCLUDE_DSPM_AI:
        activities.extend(DSPM_AI_ACTIVITY_TYPES)

    # Step 3: Remove CopilotInteraction if explicitly excluded
    if config.EXCLUDE_COPILOT_INTERACTION:
        activities = [a for a in activities if a != COPILOT_BASE_ACTIVITY_TYPE]

    # Step 4: Deduplicate while preserving order
    seen = set()
    unique = []
    for a in activities:
        if a not in seen:
            seen.add(a)
            unique.append(a)

    return unique
```

---

### Gap 2: Query Body — Record Type & Service Filters (CRITICAL)

**What PAX does (lines 5530-5625, 11340-11350):**

The Graph API query body supports three filter types:
```json
{
    "displayName": "...",
    "filterStartDateTime": "...",
    "filterEndDateTime": "...",
    "operationFilters": ["CopilotInteraction", ...],
    "recordTypeFilters": ["ExchangeItem", ...],
    "serviceFilter": "Exchange"
}
```

PAX has a **fail-safe sanitizer** (line 5573-5587): when `operationFilters` includes ANY M365 usage operations, `recordTypeFilters` and `serviceFilter` are forcibly dropped. This prevents conflicts because M365 usage operations span multiple record types and workloads.

**What the plan had:** Section 5.2 only showed `operationFilters` in the query body. `recordTypeFilters` and `serviceFilter` were not mentioned in the `create_query_activity` interface.

**Resolution — Update `create_query_activity.py` interface:**

```python
def create_query(input_data: dict) -> dict:
    """
    POST /security/auditLog/queries
    
    Input: {
        start_time: str (ISO 8601),
        end_time: str (ISO 8601),
        activity_types: list[str],        # operationFilters
        record_types: list[str] | None,   # recordTypeFilters (NEW)
        service_filter: str | None,       # serviceFilter (NEW)
        display_name: str,
        include_m365_usage: bool          # Triggers fail-safe sanitizer (NEW)
    }
    """
    body = {
        "displayName": input_data["display_name"],
        "filterStartDateTime": input_data["start_time"],
        "filterEndDateTime": input_data["end_time"],
    }

    # Add operation filters
    if input_data.get("activity_types"):
        body["operationFilters"] = input_data["activity_types"]

    # FAIL-SAFE SANITIZER (matches PAX line 5573-5587):
    # When M365 usage operations are present, DROP record/service filters
    # to avoid cross-workload conflicts
    has_m365_usage_ops = any(
        op in M365_USAGE_ACTIVITY_BUNDLE
        for op in (input_data.get("activity_types") or [])
    )

    if not has_m365_usage_ops:
        if input_data.get("record_types"):
            body["recordTypeFilters"] = input_data["record_types"]
        if input_data.get("service_filter"):
            body["serviceFilter"] = input_data["service_filter"]
    else:
        logger.info("M365 usage ops detected — dropping record/service filters (fail-safe)")

    return graph_client.graph_request("POST", AUDIT_QUERIES_URL, json=body)
```

---

### Gap 3: Service-Based Query Splitting (IMPORTANT)

**What PAX does (lines 10650-10780):**

When `ServiceTypes` is specified (e.g., `["Exchange", "SharePoint", "MicrosoftTeams"]`), PAX runs **multiple passes** — one per service type. For each pass:
1. Filters `operationFilters` to only operations belonging to that service (using `$serviceOperationMap`)
2. Filters `recordTypeFilters` to only record types belonging to that service (using `$recordTypeWorkloadMap`)
3. Sets `serviceFilter` to the current service name

When `-IncludeM365Usage` is active, `$ServiceTypes` is forced to `$null` → single pass, no service splitting.

**What the plan had:** No mention of service-based splitting. The orchestrator assumes a single activity type list per partition.

**Resolution — Add service pass logic to orchestrator:**

```python
# In plan_partitions or orchestrator:

def build_query_passes(config, resolved_activities, resolved_record_types):
    """
    Build per-service query passes matching PAX behavior.
    When include_m365_usage=True, ServiceTypes is null → single pass.
    """
    if config.INCLUDE_M365_USAGE or not config.SERVICE_TYPES:
        # Single pass — all activities in one query (no service filter)
        return [QueryPass(
            activities=resolved_activities,
            record_types=None,   # Dropped by fail-safe when M365 usage present
            service_filter=None,
        )]

    # Multi-pass — split by service type
    passes = []
    for service in config.SERVICE_TYPES:
        svc_ops = SERVICE_OPERATION_MAP.get(service, [])
        filtered_ops = [a for a in resolved_activities if a in svc_ops]
        if not filtered_ops:
            filtered_ops = resolved_activities  # Fallback: all ops

        svc_rts = [
            rt for rt in (resolved_record_types or [])
            if not RECORD_TYPE_WORKLOAD_MAP.get(rt)
            or service in RECORD_TYPE_WORKLOAD_MAP[rt]
        ]

        passes.append(QueryPass(
            activities=filtered_ops,
            record_types=svc_rts or None,
            service_filter=service,
        ))

    return passes
```

> **Impact on current IH config:** Since Insight Harbor uses `CopilotInteraction` only (no `ServiceTypes`), this resolves to a single pass — no behavior change. But the capability is preserved for future `-IncludeM365Usage` activation.

---

### Gap 4: Content Filtering Capabilities (IMPORTANT — Future)

**What PAX does (lines 1100-1200, explosion module):**

PAX supports post-query content filters that refine results AFTER fetching from the API:

| Filter | PAX Param | Applied At | Behavior |
|---|---|---|---|
| `AgentId` | `-AgentId "..."` | Explosion | Keep only records where `CopilotEventData.AgentId` matches |
| `AgentsOnly` | `-AgentsOnly` | Explosion | Keep only records where AgentId is present |
| `ExcludeAgents` | `-ExcludeAgents` | Explosion | Remove records where AgentId is present |
| `PromptFilter` | `-PromptFilter Prompt\|Response\|Both\|Null` | Explosion | Filter exploded rows by message type |
| `UserIds` | `-UserIds @(...)` | API query + post-filter | Filter by user principal name (in query body AND post-fetch) |
| `GroupNames` | `-GroupNames @(...)` | Post-fetch | Resolve AD group → member UPNs, filter results |

**What the plan had:** Section 5.8 mentions "User/group filtering" briefly but doesn't detail the content filter pipeline.

**Resolution — Add to `shared/config.py` and document for future implementation:**

```python
# Content filtering configuration (Phase 2+ feature)
IH_AGENT_ID_FILTER = os.getenv("IH_AGENT_ID_FILTER", "")
IH_AGENTS_ONLY = os.getenv("IH_AGENTS_ONLY", "false").lower() == "true"
IH_EXCLUDE_AGENTS = os.getenv("IH_EXCLUDE_AGENTS", "false").lower() == "true"
IH_PROMPT_FILTER = os.getenv("IH_PROMPT_FILTER", "")  # Prompt|Response|Both|Null
IH_USER_IDS = [u.strip() for u in os.getenv("IH_USER_IDS", "").split(",") if u.strip()]
```

> **Note:** Content filtering is already handled by the existing Python explosion processor (which supports `--prompt-filter`). For `AgentId`, `AgentsOnly`, and `ExcludeAgents`, the explosion module can be extended. `UserIds` filtering can be added to `fetch_records_activity.py` as a post-fetch filter. `GroupNames` requires an additional Graph API call to resolve group membership.

---

### Gap 5: AutoCompleteness Mode (LOWER PRIORITY)

**What PAX does:**

With `-AutoCompleteness`, PAX applies aggressive subdivision when any Query returns ≥10,000 records (instead of the normal 950,000 threshold). This ensures very fine-grained completeness guarantees for compliance-critical scenarios.

**What the plan had:** Subdivision threshold at 950,000 only.

**Resolution — Add configurable subdivision threshold:**

```python
# In config.py
IH_SUBDIVISION_THRESHOLD = int(os.getenv("IH_SUBDIVISION_THRESHOLD", "950000"))
IH_AUTO_COMPLETENESS = os.getenv("IH_AUTO_COMPLETENESS", "false").lower() == "true"

# In poll/subdivision logic:
effective_threshold = 10_000 if config.AUTO_COMPLETENESS else config.SUBDIVISION_THRESHOLD
```

---

### Gap 6: JSONL Bronze Output Format Clarity (IMPORTANT)

**What PAX outputs (lines 9582-9620):**

In non-explosion mode (`explosionMode: "raw"`), PAX outputs an 8-column CSV:

| Column | Source |
|---|---|
| `RecordId` | Record.RecordId \|\| Identity \|\| Id \|\| AuditData.Id |
| `CreationDate` | Record.CreationDate (UTC ISO 8601) |
| `RecordType` | Record.RecordType |
| `Operation` | AuditData.Operation |
| `UserId` | Record.UserId \|\| UserIds |
| `AuditData` | Raw JSON string (the entire audit payload) |
| `AssociatedAdminUnits` | AuditData.AssociatedAdminUnits |
| `AssociatedAdminUnitsNames` | AuditData.AssociatedAdminUnitsNames |

The Python explosion processor then reads this 8-column CSV and expands `AuditData` JSON into the 153-column schema.

**What the plan had:** States "JSONL" for bronze but didn't specify the structure of each line.

**Resolution — Clarify bronze JSONL format:**

Each line of the bronze JSONL file should be the **raw Graph API response record** as JSON. The Graph API returns records with these fields:
```json
{
    "id": "...",
    "createdDateTime": "2025-07-08T14:30:00Z",
    "userPrincipalName": "user@contoso.com",
    "administrativeUnits": [...],
    "auditLogRecordType": "CopilotInteraction",
    "operation": "CopilotInteraction",
    "auditData": { ... }   // The full nested audit payload
}
```

The `fetch_records_activity` writes each page of records as JSONL (one JSON object per line). The explosion activity then reads these and maps them to the 153-column schema — matching the existing Python explosion processor's input expectations.

**Key difference from PAX:** PAX converts to the 8-column CSV format as an intermediate. The DF version stores the raw Graph API response, which contains MORE information (the 8-column format loses some fields). The explosion processor handles both formats, so no data is lost.

---

### Gap 7: Run Metadata & Metrics (LOWER PRIORITY)

**What PAX does:**

PAX writes `_query_metadata.json` at run end with ~60+ metrics fields including:
- Total records, unique users, timestamp ranges
- Per-partition metrics (records, elapsed time, retries)
- Circuit breaker triggers, throttle counts
- Memory high-water marks

**What the plan had:** App Insights custom dimensions for telemetry, but no consolidated metadata file.

**Resolution — Add run metadata activity:**

```python
# Add to orchestrator Phase 6 (after notify):
yield ctx.call_activity("write_run_metadata", {
    "run_id": run_id,
    "blob_path": f"pipeline/state/run_{date_str}_metadata.json",
    "metadata": {
        "version": "1.0",
        "started_at": start_time.isoformat(),
        "completed_at": ctx.current_utc_datetime.isoformat(),
        "partitions_total": len(partitions),
        "partitions_completed": len(all_bronze_paths),
        "partitions_failed": len(failed_partitions),
        "total_records_ingested": total_records,
        "total_records_exploded": sum(r["records"] for r in exploded_results),
        "total_records_silver": silver_result["records_transformed"],
        "throttle_count": throttle_counter,
        "subdivision_count": subdivision_counter,
        "activity_types": resolved_activities,
        "date_range": {"start": start_date, "end": end_date},
    }
})
```

---

### Gap 8: Entra 47-Column Raw Schema (ALREADY COVERED)

**Status:** ✅ No gap. The plan's Section 5.9 covers Entra pull via Graph `/users` endpoint with the same `$select` properties. The existing `bronze_to_silver_entra.py` has the full 30-column Silver schema already defined and tested. The raw 47-column Entra output is an intermediate handled by `pull_entra_activity`.

---

### Gap 9: Copilot SKU IDs (ALREADY COVERED — Python approach is superior)

**Status:** ✅ No gap. PAX uses 10 hardcoded SKU IDs. The existing Python `bronze_to_silver_entra.py` uses `COPILOT_KEYWORDS = ["copilot"]` (case-insensitive substring match), which is MORE robust because it automatically catches new Copilot SKUs without code changes. The Python approach is documented in the constants above for reference.

---

### Gap 10: Graph API Version Negotiation (MINOR)

**What PAX does (lines 2804-2830):**

PAX probes `v1.0` first, then falls back to `beta`:
```
Test: GET https://graph.microsoft.com/v1.0/security/auditLog/queries
If 404 → try: GET https://graph.microsoft.com/beta/security/auditLog/queries
```

**What the plan had:** Implicit use of a single API version.

**Resolution — Add to `shared/graph_client.py`:**

```python
GRAPH_API_VERSIONS = ["v1.0", "beta"]

async def discover_api_version(self) -> str:
    """Probe available Graph audit API version (v1.0 preferred, beta fallback)."""
    for version in GRAPH_API_VERSIONS:
        url = f"https://graph.microsoft.com/{version}/security/auditLog/queries"
        try:
            response = self.graph_request("GET", url)
            self._api_version = version
            return version
        except Exception:
            continue
    raise RuntimeError("No available Graph audit API version found")
```

---

### Gap 11: Exit Code Semantics (MINOR)

**What PAX does:**

| Exit Code | Meaning |
|---|---|
| 0 | Success — all partitions completed |
| 10 | Partial success — hit API limits, some data may be missing |
| 20 | Circuit breaker tripped — significant throttling |

**What the plan had:** Return status `completed | partial_success | failed` in orchestrator.

**Resolution — Map to numeric codes in notification:**

```python
# In notify_activity.py — map status to PAX exit codes for metric parity
EXIT_CODE_MAP = {
    "completed": 0,
    "partial_success": 10,   # Some partitions failed after retries
    "circuit_breaker": 20,   # Circuit breaker tripped
    "failed": 99,            # Total failure
}
```

---

### Gap 12: Per-Page JSONL Memory Flush (ALREADY COVERED)

**Status:** ✅ No gap. The plan's `fetch_records_activity` (Section 5.2) already specifies AppendBlobClient for per-page streaming writes, matching PAX's `.pax_incremental/` JSONL flush pattern. Each page (~1,000 records × ~2 KB = ~2 MB) is appended immediately to ADLS, keeping memory bounded.

---

### Updated Configuration Parameters (Complete List)

Adding to Section 8, the following new config parameters capture all PAX intelligence:

| Setting | Default | Description | PAX Equivalent |
|---|---|---|---|
| `IH_INCLUDE_M365_USAGE` | `false` | Enable M365 usage activity bundle (~120 ops) | `-IncludeM365Usage` |
| `IH_INCLUDE_DSPM_AI` | `false` | Enable DSPM for AI activity types (+3 ops) | `-IncludeDSPMForAI` |
| `IH_EXCLUDE_COPILOT_INTERACTION` | `false` | Remove CopilotInteraction from activity types | `-ExcludeCopilotInteraction` |
| `IH_AUTO_COMPLETENESS` | `false` | Aggressive subdivision at 10K (vs 950K) | `-AutoCompleteness` |
| `IH_AGENT_ID_FILTER` | *(empty)* | Filter by specific agent ID | `-AgentId` |
| `IH_AGENTS_ONLY` | `false` | Keep only agent interactions | `-AgentsOnly` |
| `IH_EXCLUDE_AGENTS` | `false` | Exclude agent interactions | `-ExcludeAgents` |
| `IH_PROMPT_FILTER` | *(empty)* | Filter by message type (Prompt\|Response\|Both\|Null) | `-PromptFilter` |
| `IH_USER_IDS` | *(empty)* | Comma-separated UPNs to filter | `-UserIds` |
| `IH_SERVICE_TYPES` | *(empty)* | Comma-separated service types for multi-pass | `-ServiceTypes` |
| `IH_RECORD_TYPES` | *(empty)* | Comma-separated record types for query filter | `-RecordTypes` |
| `IH_GRAPH_API_VERSION` | `v1.0` | Graph API version (auto-negotiated) | Auto v1.0/beta |

---

### Updated Feature Coverage Matrix (Complete)

Adding to Appendix A, the following PAX features are now also explicitly covered:

| PAX Feature | PAX Location | DF Equivalent | Section |
|---|---|---|---|
| `-IncludeM365Usage` bundle (~120 ops) | Lines 1425-1470, 1700-1860 | `resolve_activity_types()` + `M365_USAGE_ACTIVITY_BUNDLE` | App.C Gap 1 |
| `$m365UsageRecordBundle` (14 types) | Lines 1800-1830 | `M365_USAGE_RECORD_BUNDLE` constant | App.C Gap 1 |
| `$ServiceTypes = $null` optimization | Line 1465 | `build_query_passes()` → single pass | App.C Gap 3 |
| Fail-safe sanitizer (drop filters) | Lines 5573-5587 | `create_query()` M365 usage detection | App.C Gap 2 |
| `$recordTypeWorkloadMap` | Lines 1840-1860 | `RECORD_TYPE_WORKLOAD_MAP` constant | App.C Gap 3 |
| `$serviceOperationMap` | Lines 1850-1860 | `SERVICE_OPERATION_MAP` constant | App.C Gap 3 |
| `-IncludeDSPMForAI` (+3 ops) | Lines 1470-1475 | `DSPM_AI_ACTIVITY_TYPES` + config flag | App.C Gap 1 |
| `-ExcludeCopilotInteraction` | Lines 1460-1465 | `resolve_activity_types()` filter | App.C Gap 1 |
| Content filters (Agent/User/Prompt) | Lines 1100-1200 | Config params + post-fetch filters | App.C Gap 4 |
| `AutoCompleteness` mode | Lines 1235-1260 | Configurable subdivision threshold | App.C Gap 5 |
| Graph API version negotiation | Lines 2804-2830 | `discover_api_version()` | App.C Gap 10 |
| 10 Copilot SKU IDs | Lines 1870-1895 | `COPILOT_SKU_IDS` const (ref only) | App.C Gap 9 |
| `recordTypeFilters` in query body | Lines 5590-5593 | `create_query()` body construction | App.C Gap 2 |
| `serviceFilter` in query body | Lines 5595-5598 | `create_query()` body construction | App.C Gap 2 |
| Service-based multi-pass splitting | Lines 10650-10780 | `build_query_passes()` | App.C Gap 3 |
| Run metadata JSON | End of script | `write_run_metadata` activity | App.C Gap 7 |
| Exit code semantics (0/10/20) | Lines 16200+ | `EXIT_CODE_MAP` in notifications | App.C Gap 11 |
| 8-column compact output format | Lines 9600-9618 | Bronze JSONL (raw API format — superset) | App.C Gap 6 |

---

### Output Parity Assurance

With all 12 gaps addressed, the Durable Functions pipeline will produce **identical analytical output** to running PAX with any combination of switches:

1. **`-IncludeM365Usage`** → Set `IH_INCLUDE_M365_USAGE=true` → resolves to same ~120+1 activity types → same Graph API queries → same records fetched → same explosion → same Silver output

2. **Default (CopilotInteraction only)** → `IH_ACTIVITY_TYPES=CopilotInteraction` → single operation filter → identical output

3. **`-OnlyUserInfo`** → `pull_entra_activity` → same Graph `/users` endpoint with same `$select` → same Entra Silver schema

4. **Content filters** → Same post-fetch filtering logic → same subset of records

5. **Large volumes** → Same partition splitting → same subdivision thresholds → same record coverage

The only difference: bronze storage format changes from PAX's 8-column CSV to raw JSONL (which is a **superset** — more data, not less). The Python explosion processor handles both formats.

---

*End of Implementation Plan — Awaiting Review*
