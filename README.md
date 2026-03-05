# Insight Harbor

**Insight Harbor** is a cloud-native analytics platform that ingests Microsoft 365 Copilot usage telemetry and identity data from a customer's tenant, processes it through a zero-local-storage serverless pipeline, and surfaces insights through an interactive HTML dashboard and Power BI reports.

**Live Dashboard**: [ih.data-analytics.tech](https://ih.data-analytics.tech) · **SWA URL**: `https://lemon-mud-0e797b310.6.azurestaticapps.net`

---

## Architecture Overview

Insight Harbor uses two complementary execution paths:

### Primary: Azure Durable Functions Pipeline (Zero-Local)

The primary ingestion pipeline runs entirely in Azure with no local storage or execution requirements:

```
+----------------------------------------------------------------------+
|  ih-pipeline-func01  (Azure Functions - Consumption Plan, Python)    |
|                                                                      |
|  Timer Trigger (daily 2:00 AM UTC) or HTTP manual trigger            |
|      |                                                               |
|      v                                                               |
|  pipeline_orchestrator (Durable Orchestrator)                        |
|      |                                                               |
|      +- plan_partitions -- Split time window into 6-hour blocks      |
|      |                                                               |
|      +- Fan-out: process_partition (sub-orchestrator) x N            |
|      |   +- create_query -- Graph /security/auditLog/queries (beta)  |
|      |   +- poll_query --- Wait for async Purview query completion   |
|      |   +- fetch_records - Paginated record retrieval -> ADLS bronze|
|      |   +- check_subdivision -- Adaptive partition splitting        |
|      |   +- explode_partition -- 153-col schema explosion -> ADLS    |
|      |   +- cleanup_queries -- Delete completed Purview queries      |
|      |                                                               |
|      +- pull_entra ---- Entra ID user profiles & licensing -> ADLS   |
|      +- transform_silver -- Bronze-to-Silver enrichment -> ADLS      |
|      +- notify -------- Teams webhook (Adaptive Cards)               |
|      +- run_state ----- Pipeline state tracking                      |
|                                                                      |
|  Auth: MSAL client-credential flow (InsightHarbor-PAX app reg)       |
|  Secrets: Azure Key Vault (ih-keyvault-poc01)                        |
|  Monitoring: Application Insights (ih-app-insights)                  |
+----------------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
|  STORAGE: ADLS Gen2 (ihstoragepoc01)                        |
|  bronze/purview/YYYY/MM/DD/HH/      <- raw audit records    |
|  bronze/exploded/YYYY/MM/DD/HH/     <- 153-column explosion |
|  silver/copilot-usage/               <- enriched analytics   |
|  silver/entra-users/                 <- user profiles        |
|                                                             |
|  Lifecycle: bronze cool@30d, delete@90d                     |
|             silver cool@60d, archive@180d, delete@365d      |
|             temp   delete@7d                                |
+-----------------------------+-------------------------------+
                           |
          +----------------+----------------+
          v                                 v
+------------------+        +-----------------------------------+
|  Power BI        |        |  HTML Dashboard                    |
|  (Direct Import  |        |  Azure Static Web App (ih-dashboard|
|   from ADLS CSV) |        |  + Azure Functions API (ih-api-poc0|
+------------------+        |  Custom Domain: ih.data-analytics. |
                            |                                    |
                            |  MSAL -> Entra ID (single-tenant)  |
                            |  InsightHarbor-Viewers SG gate     |
                            |  JWT validation (RS256)            |
                            |  Key Vault (ih-keyvault-poc01)     |
                            |                                    |
                            |  App Insights (ih-app-insights)    |
                            |  Log Analytics (ih-log-analytics)  |
                            +------------------------------------+
```

### Legacy: Local Pipeline (PAX PowerShell)

The original local pipeline is retained for reference and fallback:

```
PAX PowerShell scripts -> local RAW CSV -> Python explosion -> Bronze-to-Silver transform -> ADLS upload
Orchestrated by: scripts/run-pipeline-local.ps1
```

See [ingestion/README.md](ingestion/README.md) for the PAX drop-in contract.

---

## Folder Structure

```
insight-harbor/
+-- .github/workflows/
|   +-- deploy-dashboard.yml          # CI/CD: SWA deploy on push
|   +-- deploy-api.yml                # CI/CD: Function App deploy on push
+-- .gitignore
+-- README.md
+-- pytest.ini                        # Test config (pythonpath = pipeline)
|
+-- pipeline/                         # ** Durable Functions pipeline (primary)
|   +-- function_app.py               # Entry point (16 registered functions)
|   +-- host.json                     # Function host config + Durable task hub
|   +-- requirements.txt              # Python dependencies
|   +-- local.settings.json           # Local dev settings (gitignored values)
|   +-- .funcignore                   # Deployment exclusions
|   +-- shared/                       # Shared modules
|   |   +-- config.py                 # Environment-based typed configuration
|   |   +-- constants.py              # ADLS paths, Graph endpoints, limits
|   |   +-- models.py                 # Pydantic models for pipeline state
|   |   +-- graph_client.py           # MSAL auth + Graph API HTTP client
|   |   +-- adls_client.py            # ADLS Gen2 read/write (DefaultAzureCredential)
|   |   +-- partitioning.py           # Time-window partitioning logic
|   |   +-- explosion.py              # 153-column JSON->flat schema explosion
|   |   +-- transforms.py             # Bronze-to-Silver Copilot usage transforms
|   |   +-- entra_transforms.py       # Entra user profile transforms
|   |   +-- connectors/               # Pluggable connector framework
|   |   |   +-- base.py               # BaseConnector ABC + ConnectorPhase
|   |   |   +-- registry.py           # ConnectorRegistry singleton (auto-discover)
|   |   |   +-- purview_audit.py      # Purview audit log connector
|   |   |   +-- entra.py              # Entra ID user connector
|   |   |   +-- m365_usage.py         # M365 Usage Reports connector (Graph)
|   |   |   +-- graph_activity.py     # Graph sign-in / audit log connector
|   |   +-- query/                    # Semantic query engine
|   |       +-- schemas/*.yaml        # YAML schema catalog (4 datasets)
|   |       +-- schema_catalog.py     # Schema metadata registry
|   |       +-- query_generator.py    # DSL validation → QueryPlan
|   |       +-- query_executor.py     # Pandas-based query execution
|   |       +-- viz_recommender.py    # Rule-based chart recommendation
|   |       +-- narrative.py          # AI narrative / insight generation
|   +-- orchestrators/                # Durable orchestrators (2)
|   |   +-- pipeline_orchestrator.py  # Main orchestrator (fan-out/fan-in)
|   |   +-- process_partition.py      # Per-partition sub-orchestrator
|   +-- activities/                   # Activity functions (11)
|   |   +-- plan_partitions.py        # Time-window partition planning
|   |   +-- create_query.py           # Graph audit query creation
|   |   +-- poll_query.py             # Async query status polling
|   |   +-- fetch_records.py          # Paginated record retrieval
|   |   +-- check_subdivision.py      # Adaptive partition splitting
|   |   +-- cleanup_queries.py        # Purview query cleanup
|   |   +-- explode_partition.py      # Schema explosion per partition
|   |   +-- pull_entra.py             # Entra ID user/license pull
|   |   +-- transform_silver.py       # Bronze->Silver enrichment
|   |   +-- notify.py                 # Teams webhook notifications
|   |   +-- run_state.py              # Pipeline state management
|   +-- tests/                        # Unit tests (288 tests, 11 files)
|   |   +-- conftest.py
|   |   +-- test_partitioning.py
|   |   +-- test_transforms.py
|   |   +-- test_entra_transforms.py
|   |   +-- test_explosion.py
|   |   +-- test_graph_client.py
|   |   +-- test_models.py
|   |   +-- test_connectors.py        # Connector framework tests (45)
|   |   +-- test_multi_source.py      # Multi-source ingestion tests (38)
|   |   +-- test_semantic_query.py    # Schema/query/executor tests (61)
|   |   +-- test_viz_narrative.py     # Viz recommender + narrative tests (31)
|   +-- deploy/
|       +-- provision-azure-resources.ps1  # Azure resource provisioning
|
+-- config/
|   +-- insight-harbor-config.template.json
|   +-- lifecycle-policy.json         # ADLS blob lifecycle rules
|   +-- insight-harbor-config.json    # (gitignored) Runtime config
+-- docs/
|   +-- IMPLEMENTATION_PLAN_Durable_Functions_Pipeline.md  # 2,320-line plan
|   +-- PAX_Purview_Audit_Report.md   # PAX audit & gap analysis (724 lines)
|   +-- adding-a-connector.md         # How to build a new data connector
|   +-- app-registration-setup.md
|   +-- pax-ai-prompts.md
|   +-- powerbi-setup-guide.md
|   +-- secret-rotation-plan.md
|   +-- claude-code-mcp-workflow.md
+-- ingestion/                        # Legacy PAX script drop zone
|   +-- README.md
|   +-- PAX_*.ps1                     # Modified PAX scripts
+-- transform/                        # Legacy local transform scripts
|   +-- explosion/
|   +-- bronze_to_silver_purview.py
|   +-- bronze_to_silver_entra.py
|   +-- schema/
+-- infrastructure/
|   +-- main.bicep                    # Core infrastructure (ADLS, KV, etc.)
|   +-- main-dashboard.bicep          # Dashboard infrastructure (SWA, API)
|   +-- parameters*.json
+-- scripts/
|   +-- run-pipeline-local.ps1        # Legacy local pipeline orchestrator
|   +-- check-pax-version.ps1
|   +-- generate-synthetic.ps1
|   +-- generate-synthetic-entra.ps1
|   +-- export-dashboard-data.py
|   +-- runbook-pipeline-health.ps1
+-- dashboard/
|   +-- html/
|   |   +-- index.html                # Single-page dashboard (~1400 lines)
|   |   +-- admin.html                # Admin console (connectors, runs, health)
|   |   +-- explore.html              # Explore: schema browser + query runner
|   |   +-- staticwebapp.config.json  # SWA routing & security headers
|   |   +-- robots.txt
|   +-- api/
|   |   +-- function_app.py           # Dashboard API (15 endpoints)
|   |   +-- requirements.txt
|   |   +-- host.json
|   +-- powerbi/                      # (Future: Power BI reports)
+-- resources/                        # Reference materials & architecture docs
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.11+ | Durable Functions pipeline + transforms |
| Azure CLI | Infrastructure deployment + `func` CLI |
| Azure Functions Core Tools | Local testing and deployment (`func azure functionapp publish`) |
| Azure Subscription | Visual Studio Enterprise (`bmiddendorf@gmail.com`) |
| M365 Demo Tenant | `M365CPI01318443.onmicrosoft.com` - Global Admin required |
| PowerShell 7+ | Legacy PAX pipeline only (optional) |
| Power BI Desktop | Report development (optional) |

---

## Quick Start - Durable Functions Pipeline

### 1. Provision Azure Resources

```powershell
cd pipeline/deploy
.\provision-azure-resources.ps1
```

This creates `ihpipelinestor01` (Durable task hub storage), `ih-pipeline-func01` (Function App), and assigns RBAC roles for ADLS and Key Vault access.

### 2. Configure App Settings

Set environment variables on the Function App (or in `local.settings.json` for local dev):

| Setting | Description |
|---|---|
| `IH_TENANT_ID` | Entra tenant ID |
| `IH_CLIENT_ID` | App registration client ID |
| `IH_CLIENT_SECRET` | Key Vault reference: `@Microsoft.KeyVault(SecretUri=...)` |
| `IH_ADLS_ACCOUNT_NAME` | ADLS storage account (default: `ihstoragepoc01`) |
| `IH_ADLS_CONTAINER` | ADLS container (default: `insight-harbor`) |
| `IH_DURABLE_TASK_HUB` | Durable Functions task hub name |
| `IH_SCHEDULE_CRON` | CRON expression (default: `0 0 2 * * *`) |

### 3. Deploy

```powershell
cd pipeline
func azure functionapp publish ih-pipeline-func01
```

### 4. Run Manually

```powershell
# Trigger via HTTP
curl -X POST "https://ih-pipeline-func01.azurewebsites.net/api/run-pipeline?code=<function-key>"
```

### 5. Run Tests

```powershell
# From repo root
pytest pipeline/tests/ -v
# 113 tests passing
```

### Legacy Local Pipeline (optional)

```powershell
.\scripts\run-pipeline-local.ps1             # Full run
.\scripts\run-pipeline-local.ps1 -SkipPAX    # Reprocess existing data
.\scripts\run-pipeline-local.ps1 -DryRun     # No ADLS uploads
```

---

## Durable Functions Pipeline

The primary data pipeline is an Azure Durable Functions orchestration (`ih-pipeline-func01`) that replaces all local execution:

### Execution Flow

1. **Timer trigger** fires daily at 2:00 AM UTC (or manual HTTP trigger)
2. **plan_partitions** splits the lookback window into 6-hour blocks
3. **Fan-out**: each partition runs as a `process_partition` sub-orchestrator
   - **create_query** -> Graph `/security/auditLog/queries` (beta API)
   - **poll_query** -> Async polling with Durable timer waits (zero compute cost)
   - **fetch_records** -> Paginated retrieval -> stream to ADLS `bronze/`
   - **check_subdivision** -> Adaptive splitting if partition exceeds threshold
   - **explode_partition** -> 153-column schema explosion -> ADLS `bronze/exploded/`
   - **cleanup_queries** -> Delete completed Purview queries
4. **pull_entra** -> Entra ID user profiles & licensing -> ADLS `silver/entra-users/`
5. **transform_silver** -> Bronze-to-Silver JOIN + dedup -> ADLS `silver/copilot-usage/`
6. **notify** -> Teams webhook with Adaptive Card summary
7. **run_state** -> Pipeline state tracking

### Key Design Decisions

- **Graph API beta endpoint**: Required for `/security/auditLog/queries` (not available in v1.0)
- **MSAL client-credential flow**: Uses `InsightHarbor-PAX` app registration
- **Zero local storage**: All I/O through ADLS Gen2 via `DefaultAzureCredential`
- **153-column explosion**: Full-fidelity schema preservation (source-driven, no semantic assumptions)
- **Adaptive partitioning**: Automatically subdivides time windows when record counts exceed thresholds
- **Checkpoint/resume**: Built-in Durable Functions replay survives crashes and restarts

### Registered Functions (16 total)

| Type | Count | Functions |
|---|---|---|
| Timer trigger | 1 | `scheduled_pipeline` |
| HTTP trigger | 2 | `manual_pipeline`, `pipeline_status` |
| Orchestrator | 2 | `pipeline_orchestrator`, `process_partition` |
| Activity | 11 | `plan_partitions`, `create_query`, `poll_query`, `fetch_records`, `check_subdivision`, `cleanup_queries`, `explode_partition`, `pull_entra`, `transform_silver`, `notify`, `run_state` |

---

## Dashboard

The dashboard is a single-page HTML application (~1400 lines, no build step) served from Azure Static Web Apps.

**Features:**
- Tab navigation: Overview + Teams Deep Dive
- KPI cards, trend line chart, department bar chart, workload doughnut
- Department adoption gauge cards (SVG rings)
- Hourly activity heatmap (CSS grid, 7x24)
- Entra license utilization with stats row
- Mobile responsive (768px + 480px breakpoints)
- PNG export via html2canvas
- MSAL 2.28.0 authentication (Entra ID, security group gated)
- App Insights client-side telemetry (page views, API timing, errors)

---

## Dashboard API Endpoints

All endpoints require a Bearer token (Entra ID) except `/api/health`.

| Endpoint | Method | Description |
|---|---|---|
| `/api/summary` | GET | Top-level KPIs |
| `/api/trend?days=30` | GET | Daily prompt trend |
| `/api/department` | GET | Department breakdown |
| `/api/workload` | GET | Workload breakdown |
| `/api/licensing` | GET | License utilization |
| `/api/hourly` | GET | Hour-of-day heatmap data |
| `/api/health` | GET | Health check (anonymous) |
| `/api/admin/connectors` | GET | Connector status & config (admin) |
| `/api/admin/runs?limit=N` | GET | Pipeline run history (admin) |
| `/api/admin/health` | GET | Extended health (admin) |
| `/api/schema[?dataset=X]` | GET | Schema metadata / column details |
| `/api/query` | POST | Execute structured query DSL |
| `/api/visualize` | POST | Query + viz recommendation + narrative |
| `/api/narrative` | POST | Generate narrative for a result set |

---

## Security

- **Authentication**: Entra ID via MSAL (client-credential for pipeline, redirect flow for dashboard)
- **Authorization**: Security group `InsightHarbor-Viewers` gates dashboard access
- **Pipeline API**: Function-level auth keys for HTTP triggers
- **Dashboard API**: JWT validation (PyJWT, RS256, validates audience/issuer/signature)
- **Secrets Management**: Key Vault (`ih-keyvault-poc01`) with `@Microsoft.KeyVault` references
- **ADLS Access**: `DefaultAzureCredential` with RBAC (Storage Blob Data Contributor)
- **Crawler Blocking**: `robots.txt`, meta `noindex` tags, `X-Robots-Tag` header via SWA config

---

## Data Pipeline

### Primary (Durable Functions - Zero Local)

- **Ingestion**: Graph API `/security/auditLog/queries` (beta) via MSAL client-credential flow
- **Explosion**: Python 153-column schema explosion (full-fidelity, source-driven)
- **Transform**: Bronze-to-Silver enrichment with Entra user JOIN and deduplication
- **Storage**: ADLS Gen2 with hour-partitioned paths (`bronze/purview/YYYY/MM/DD/HH/`)
- **Notifications**: Teams webhook Adaptive Cards on pipeline success/failure
- **Orchestration**: Azure Durable Functions with fan-out/fan-in, timer-based polling

### Legacy (PAX PowerShell - Local)

- **Ingestion**: PAX PowerShell scripts pull raw audit data from Purview/Graph APIs
- **Explosion**: Python processor flattens nested AuditData JSON into 35-column flat CSV
- **Transform**: Bronze-to-Silver scripts cleanse, deduplicate, and enrich data
- **Orchestration**: `run-pipeline-local.ps1` - end-to-end local pipeline

**ADLS Lifecycle Policies:**
| Tier | Cool | Archive | Delete |
|---|---|---|---|
| Bronze | 30 days | - | 90 days |
| Silver | 60 days | 180 days | 365 days |
| Temp | - | - | 7 days |

---

## Data Sources (Connector Framework)

All sources use the pluggable connector framework (`shared/connectors/`). See [docs/adding-a-connector.md](docs/adding-a-connector.md) for how to add new connectors.

| Source | Connector | Data | Status |
|---|---|---|---|
| Purview Audit Logs | `PurviewAuditConnector` | CopilotInteraction events | **Active** |
| Entra User Profiles | `EntraConnector` | User metadata & licensing | **Active** |
| M365 Usage Reports | `M365UsageReportsConnector` | App adoption, Teams, Email, SPO, OD | **Ready** |
| Graph Activity Logs | `GraphActivityConnector` | Sign-in & directory audit events | **Ready** |

---

## CI/CD

GitHub Actions workflows auto-deploy on push:

| Workflow | Trigger Path | Target | Required Secret |
|---|---|---|---|
| `deploy-dashboard.yml` | `dashboard/html/**` | Azure Static Web App (`ih-dashboard`) | `SWA_DEPLOY_TOKEN` |
| `deploy-api.yml` | `dashboard/api/**` | Azure Functions (`ih-api-poc01`) | `AZURE_FUNCTIONAPP_PUBLISH_PROFILE` |

Pipeline deployment is manual via `func azure functionapp publish ih-pipeline-func01`.

---

## Azure Resources

All resources are in resource group: **`insight-harbor-rg`**
Azure Subscription: Visual Studio Enterprise (`bmiddendorf@gmail.com`)
Custom Domain: **`ih.data-analytics.tech`** (via Azure Front Door)

| Resource | Name | Type | Location | Purpose |
|---|---|---|---|---|
| ADLS Gen2 | `ihstoragepoc01` | Storage Account | East US 2 | Bronze & Silver data layers |
| Pipeline Function App | `ih-pipeline-func01` | Azure Functions (Python 3.11) | East US 2 | Durable Functions ingestion pipeline |
| Pipeline Storage | `ihpipelinestor01` | Storage Account | East US 2 | Durable task hub backing store |
| Dashboard API | `ih-api-poc01` | Azure Functions (Python 3.11) | Central US | REST API (15 endpoints) |
| Static Web App | `ih-dashboard` | Static Web App | Central US | HTML dashboard frontend |
| Key Vault | `ih-keyvault-poc01` | Key Vault | Central US | Secrets management |
| App Insights | `ih-app-insights` | Application Insights | Central US | Telemetry & monitoring |
| Log Analytics | `ih-log-analytics` | Log Analytics Workspace | Central US | Log storage for App Insights |
| Automation | `ih-automation` | Automation Account | Central US | Legacy scheduled pipeline runbooks |
| API Function Storage | `ihfuncstor01` | Storage Account | Central US | Dashboard API backing store |

---

## App Registrations

Demo Tenant: `M365CPI01318443.onmicrosoft.com` (`579e8f66-10ec-4646-a923-b9dc013cc0a7`)

| Registration | Client ID | Purpose | Auth Flow |
|---|---|---|---|
| `InsightHarbor-PAX` | `01e187b7-264a-4ce1-8cc1-32c977e0a302` | Pipeline data ingestion | Client secret (MSAL) |
| `InsightHarbor-Dashboard` | - | SPA authentication | MSAL redirect (single-tenant) |

---

## Testing

```powershell
# Run all 288 unit tests
pytest pipeline/tests/ -v

# Run specific test module
pytest pipeline/tests/test_explosion.py -v
```

Test coverage: partitioning logic, explosion schema, transforms, Entra transforms, Graph client, Pydantic models, connector framework (4 connectors), multi-source ingestion, semantic query engine, viz recommender, narrative generator.

---

## Budget Target

**PoC monthly cap: $60** - Expected actual: ~$0.50-$2/month

| Component | Cost |
|---|---|
| App Insights | First 5 GB/month free |
| Static Web App | Free tier |
| Dashboard Function App | Consumption plan (~$0) |
| Pipeline Function App | Consumption plan (~$0.50/mo) |
| ADLS Gen2 | ~$0.02/GB/month |
| Key Vault | ~$0.03/10K operations |
