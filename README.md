# Insight Harbor

**Insight Harbor** is a modular analytics solution that ingests Microsoft 365 Copilot usage and identity data from a customer's tenant, processes it into clean analytics-ready schemas, and surfaces insights through an interactive HTML dashboard frontend and Power BI reports.

**Live Dashboard**: [ih.data-analytics.tech](https://ih.data-analytics.tech) · **SWA URL**: `https://lemon-mud-0e797b310.6.azurestaticapps.net`

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                            │
│  PAX PowerShell Scripts (drop-in, no-modification)          │
│  Scheduled via Azure Automation Account (ih-automation)     │
│  Auth: Client Secret (InsightHarbor-PAX App Registration)   │
│  Teams webhook notifications (Adaptive Cards) on completion │
└──────────────────────────┬──────────────────────────────────┘
                           │ RAW CSV (unexploded AuditData JSON)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  EXPLOSION LAYER                                            │
│  Python: Purview_M365_Usage_Bundle_Explosion_Processor      │
│  Input:  RAW CSV  →  Output: 35-column flat CSV             │
│  ~50x faster than PAX built-in explosion                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ Exploded flat CSV
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORM LAYER                                            │
│  bronze_to_silver_purview.py — Copilot usage cleansing      │
│  bronze_to_silver_entra.py   — Entra user enrichment        │
│  Key Vault secret resolution at runtime                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  STORAGE: ADLS Gen2 (ihstoragepoc01)                        │
│  bronze/purview/YYYY/MM/DD/   ← raw PAX output              │
│  bronze/exploded/YYYY/MM/DD/  ← Python-exploded output      │
│  silver/copilot-usage/        ← cleansed & enriched         │
│  silver/entra-users/          ← user profiles & licensing   │
│                                                             │
│  Lifecycle: bronze cool@30d, delete@90d                     │
│             silver cool@60d, archive@180d, delete@365d      │
│             temp   delete@7d                                │
└──────────────────────────┬──────────────────────────────────┘
                           │
          ┌────────────────┴────────────────┐
          ▼                                 ▼
┌──────────────────┐        ┌───────────────────────────────────┐
│  Power BI        │        │  HTML Dashboard                    │
│  (Direct Import  │        │  Azure Static Web App (ih-dashboard│
│   from ADLS CSV) │        │  + Azure Functions API (ih-api-poc0│
└──────────────────┘        │  Custom Domain: ih.data-analytics. │
                            │                                    │
                            │  ┌───────────────────────────────┐ │
                            │  │ Security                      │ │
                            │  │ MSAL → Entra ID (single-tenant│ │
                            │  │ InsightHarbor-Viewers SG gate │ │
                            │  │ JWT validation (RS256)        │ │
                            │  │ Key Vault (ih-keyvault-poc01) │ │
                            │  └───────────────────────────────┘ │
                            │                                    │
                            │  App Insights (ih-app-insights)    │
                            │  Log Analytics (ih-log-analytics)  │
                            └───────────────────────────────────┘
```

---

## Folder Structure

```
insight-harbor/
├── .github/workflows/
│   ├── deploy-dashboard.yml          # CI/CD: SWA deploy on push
│   └── deploy-api.yml                # CI/CD: Function App deploy on push
├── .gitignore
├── README.md
├── config/
│   ├── insight-harbor-config.template.json
│   ├── lifecycle-policy.json         # ADLS blob lifecycle rules
│   └── insight-harbor-config.json    # (gitignored) Runtime config with @KeyVault: refs
├── docs/
│   ├── app-registration-setup.md
│   └── pax-ai-prompts.md
├── ingestion/
│   └── README.md
├── transform/
│   ├── explosion/
│   ├── bronze_to_silver_purview.py
│   ├── bronze_to_silver_entra.py
│   └── schema/
│       └── silver_purview_schema.md
├── infrastructure/
│   ├── main.bicep
│   └── parameters.json
├── scripts/
│   ├── run-pipeline-local.ps1        # Main pipeline orchestrator
│   ├── check-pax-version.ps1
│   └── generate-synthetic.ps1
├── dashboard/
│   ├── html/
│   │   ├── index.html                # Single-page dashboard (~1400 lines)
│   │   ├── staticwebapp.config.json  # SWA routing & security headers
│   │   └── robots.txt
│   ├── api/
│   │   ├── function_app.py           # Python Function App (7 endpoints)
│   │   ├── requirements.txt
│   │   └── host.json
│   └── powerbi/                      # (Future: Power BI reports)
└── resources/                        # Reference materials
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| PowerShell 7+ | For PAX script execution |
| Python 3.9+ | For explosion processor and transforms |
| Azure CLI | For infrastructure deployment |
| Power BI Desktop | For report development (optional) |
| Azure Subscription | `bmiddendorf@gmail.com` — Visual Studio Enterprise |
| M365 Demo Tenant | `M365CPI01318443.onmicrosoft.com` — Global Admin required |

---

## Quick Setup

1. **Copy the config template:**
   ```bash
   cp config/insight-harbor-config.template.json config/insight-harbor-config.json
   ```
2. **Fill in your real values** in `config/insight-harbor-config.json` (never commit this file).

3. **Complete App Registration** — see [docs/app-registration-setup.md](docs/app-registration-setup.md).

4. **Drop modified PAX scripts** into `ingestion/` — see [ingestion/README.md](ingestion/README.md).

5. **Deploy infrastructure** — see [infrastructure/main.bicep](infrastructure/main.bicep).

6. **Run the pipeline:**
   ```powershell
   # Full pipeline run
   .\scripts\run-pipeline-local.ps1

   # Skip PAX ingestion (reprocess existing data)
   .\scripts\run-pipeline-local.ps1 -SkipPAX

   # Dry run (no ADLS uploads)
   .\scripts\run-pipeline-local.ps1 -DryRun
   ```

---

## Dashboard

The dashboard is a single-page HTML application (~1400 lines, no build step) served from Azure Static Web Apps.

**Features:**
- Tab navigation: Overview + Teams Deep Dive
- KPI cards, trend line chart, department bar chart, workload doughnut
- Department adoption gauge cards (SVG rings)
- Hourly activity heatmap (CSS grid, 7×24)
- Entra license utilization with stats row
- Mobile responsive (768px + 480px breakpoints)
- PNG export via html2canvas
- MSAL 2.28.0 authentication (Entra ID, security group gated)
- App Insights client-side telemetry (page views, API timing, errors)

---

## API Endpoints

All endpoints require a Bearer token (Entra ID) except `/api/health`.

| Endpoint | Description |
|---|---|
| `/api/summary` | Top-level KPIs |
| `/api/trend?days=30` | Daily prompt trend |
| `/api/department` | Department breakdown |
| `/api/workload` | Workload breakdown |
| `/api/licensing` | License utilization |
| `/api/hourly` | Hour-of-day heatmap data |
| `/api/health` | Health check (anonymous) |

---

## Security

- **Authentication**: Entra ID via MSAL redirect flow (single-tenant, `InsightHarbor-Dashboard` app registration)
- **Authorization**: Security group `InsightHarbor-Viewers` gates dashboard access
- **API Protection**: JWT validation in Function App (PyJWT, RS256, validates audience/issuer/signature)
- **Secrets Management**: Key Vault (`ih-keyvault-poc01`) with `@KeyVault:` references in config
- **Crawler Blocking**: `robots.txt`, meta `noindex` tags, `X-Robots-Tag` header via SWA config

---

## Data Pipeline

- **Ingestion**: PAX PowerShell scripts pull raw audit data from Purview/Graph APIs
- **Explosion**: Python processor flattens nested AuditData JSON into 35-column flat CSV (~50x faster than PAX built-in)
- **Transform**: Bronze-to-Silver scripts cleanse, deduplicate, and enrich data
- **Storage**: ADLS Gen2 with date-partitioned paths (`bronze/purview/YYYY/MM/DD/`)
- **Notifications**: Teams webhook Adaptive Cards on pipeline success/failure
- **Orchestration**: `run-pipeline-local.ps1` — end-to-end pipeline with Key Vault secret resolution at runtime

**ADLS Lifecycle Policies:**
| Tier | Cool | Archive | Delete |
|---|---|---|---|
| Bronze | 30 days | — | 90 days |
| Silver | 60 days | 180 days | 365 days |
| Temp | — | — | 7 days |

---

## Data Sources

| Source | PAX Script | Data | Status |
|---|---|---|---|
| Purview Audit Logs | `PAX_Purview_Audit_Log_Processor` | CopilotInteraction events | Active |
| Entra User Profiles | `PAX_Purview_Audit_Log_Processor` | User metadata & licensing | Active |
| Graph Audit Logs | `PAX_Graph_Audit_Log_Processor` | Graph API audit events | Planned |
| Copilot Interactions | `PAX_CopilotInteractions_Content_Audit_Log_Processor` | Content-level audit | Planned |

---

## CI/CD

GitHub Actions workflows auto-deploy on push:

| Workflow | Trigger Path | Target | Required Secret |
|---|---|---|---|
| `deploy-dashboard.yml` | `dashboard/html/**` | Azure Static Web App (`ih-dashboard`) | `SWA_DEPLOY_TOKEN` |
| `deploy-api.yml` | `dashboard/api/**` | Azure Functions (`ih-api-poc01`) | `AZURE_FUNCTIONAPP_PUBLISH_PROFILE` |

---

## Azure Resources

All resources are in resource group: **`insight-harbor-rg`**
Azure Subscription: Visual Studio Enterprise (`bmiddendorf@gmail.com`)
Custom Domain: **`ih.data-analytics.tech`** (via Azure Front Door)

| Resource | Name | Type | Location | Purpose |
|---|---|---|---|---|
| ADLS Gen2 | `ihstoragepoc01` | Storage Account | East US 2 | Bronze & Silver data layers |
| Automation | `ih-automation` | Automation Account | Central US | Scheduled pipeline runbooks |
| Static Web App | `ih-dashboard` | Static Web App | Central US | HTML dashboard frontend |
| Function App | `ih-api-poc01` | Azure Functions (Python 3.11) | Central US | REST API (7 endpoints) |
| Key Vault | `ih-keyvault-poc01` | Key Vault | Central US | Secrets management |
| App Insights | `ih-app-insights` | Application Insights | Central US | Telemetry & monitoring |
| Log Analytics | `ih-log-analytics` | Log Analytics Workspace | Central US | Log storage for App Insights |
| Function Storage | `ihfuncstor01` | Storage Account | Central US | Function App backing store |

---

## App Registrations

Demo Tenant: `M365CPI01318443.onmicrosoft.com`

| Registration | Purpose | Auth Flow |
|---|---|---|
| `InsightHarbor-PAX` | Pipeline data ingestion | Client secret |
| `InsightHarbor-Dashboard` | SPA authentication | MSAL redirect (single-tenant) |

---

## Budget Target

**PoC monthly cap: $60** · Expected actual: ~$0.15–$2/month

App Insights first 5 GB/month free. Static Web App free tier. Function App consumption plan.
