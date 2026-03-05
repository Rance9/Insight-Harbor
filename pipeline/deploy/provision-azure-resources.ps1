#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Provisions Azure resources for the Insight Harbor Durable Functions Pipeline.

.DESCRIPTION
    Creates:
      1. Storage Account (ihpipelinestor01)   — Durable Functions task hub
      2. Function App (ih-pipeline-func01)    — Consumption Y1, Python 3.11
      3. IAM role assignments                 — Managed Identity → ADLS + Key Vault
      4. Function App Settings                — all IH_* configuration

    Prerequisites:
      - Azure CLI installed and logged in (az login)
      - Existing resources: insight-harbor-rg, ihstoragepoc01, ih-keyvault-poc01, ih-app-insights

.PARAMETER SkipRoleAssignments
    Skip IAM role assignment step (useful for re-runs where roles already exist).

.EXAMPLE
    .\provision-azure-resources.ps1
    .\provision-azure-resources.ps1 -SkipRoleAssignments
#>

param(
    [switch]$SkipRoleAssignments
)

$ErrorActionPreference = "Stop"

# ═══════════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════════

$ResourceGroup         = "insight-harbor-rg"
$Location              = "eastus"
$SubscriptionId        = "d5a9ef4c-6dd0-4edf-a1d2-a0c02d2b2376"

# New resources
$PipelineStorageName   = "ihpipelinestor01"
$FunctionAppName       = "ih-pipeline-func01"

# Existing resources
$ADLSAccountName       = "ihstoragepoc01"
$ADLSContainer         = "insight-harbor"
$KeyVaultName          = "ih-keyvault-poc01"
$AppInsightsName       = "ih-app-insights"

# App Registration (Graph API access)
$TenantId              = "579e8f66-10ec-4646-a923-b9dc013cc0a7"
$ClientId              = "01e187b7-264a-4ce1-8cc1-32c977e0a302"

Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Insight Harbor Pipeline — Azure Resource Provisioning" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── Set subscription ──────────────────────────────────────────────────────────
Write-Host "[1/6] Setting subscription..." -ForegroundColor Yellow
az account set --subscription $SubscriptionId
Write-Host "  Subscription: $SubscriptionId" -ForegroundColor Green

# ── Create Storage Account for Durable Functions ─────────────────────────────
Write-Host ""
Write-Host "[2/6] Creating storage account: $PipelineStorageName..." -ForegroundColor Yellow

$storageExists = az storage account show --name $PipelineStorageName --resource-group $ResourceGroup 2>$null
if ($storageExists) {
    Write-Host "  Storage account already exists — skipping creation" -ForegroundColor DarkYellow
} else {
    az storage account create `
        --name $PipelineStorageName `
        --resource-group $ResourceGroup `
        --location $Location `
        --sku Standard_LRS `
        --kind StorageV2 `
        --min-tls-version TLS1_2 `
        --allow-blob-public-access false `
        --https-only true
    Write-Host "  Created: $PipelineStorageName" -ForegroundColor Green
}

# ── Get App Insights connection string ────────────────────────────────────────
Write-Host ""
Write-Host "[3/6] Retrieving App Insights connection string..." -ForegroundColor Yellow
$appInsightsConnStr = az monitor app-insights component show `
    --app $AppInsightsName `
    --resource-group $ResourceGroup `
    --query "connectionString" -o tsv
Write-Host "  App Insights connected" -ForegroundColor Green

# ── Create Function App (Consumption / Python 3.11) ──────────────────────────
Write-Host ""
Write-Host "[4/6] Creating Function App: $FunctionAppName..." -ForegroundColor Yellow

$funcExists = az functionapp show --name $FunctionAppName --resource-group $ResourceGroup 2>$null
if ($funcExists) {
    Write-Host "  Function App already exists — skipping creation" -ForegroundColor DarkYellow
} else {
    az functionapp create `
        --name $FunctionAppName `
        --resource-group $ResourceGroup `
        --storage-account $PipelineStorageName `
        --consumption-plan-location $Location `
        --runtime python `
        --runtime-version 3.11 `
        --functions-version 4 `
        --os-type Linux `
        --app-insights $AppInsightsName `
        --assign-identity "[system]"
    Write-Host "  Created: $FunctionAppName (Python 3.11, Consumption Y1)" -ForegroundColor Green
}

# ── IAM Role Assignments ─────────────────────────────────────────────────────
if (-not $SkipRoleAssignments) {
    Write-Host ""
    Write-Host "[5/6] Configuring IAM role assignments..." -ForegroundColor Yellow

    # Get Function App's Managed Identity principal ID
    $principalId = az functionapp identity show `
        --name $FunctionAppName `
        --resource-group $ResourceGroup `
        --query "principalId" -o tsv

    if (-not $principalId) {
        Write-Host "  Enabling system-assigned managed identity..." -ForegroundColor DarkYellow
        $principalId = az functionapp identity assign `
            --name $FunctionAppName `
            --resource-group $ResourceGroup `
            --query "principalId" -o tsv
    }

    Write-Host "  Principal ID: $principalId" -ForegroundColor Gray

    # ADLS Gen2 — Storage Blob Data Contributor (read/write bronze + silver)
    $adlsResourceId = az storage account show `
        --name $ADLSAccountName `
        --resource-group $ResourceGroup `
        --query "id" -o tsv

    Write-Host "  Assigning Storage Blob Data Contributor on $ADLSAccountName..." -ForegroundColor Gray
    az role assignment create `
        --assignee $principalId `
        --role "Storage Blob Data Contributor" `
        --scope $adlsResourceId 2>$null
    Write-Host "    ✓ Storage Blob Data Contributor" -ForegroundColor Green

    # Key Vault — Secrets User (read client secret)
    $kvResourceId = az keyvault show `
        --name $KeyVaultName `
        --resource-group $ResourceGroup `
        --query "id" -o tsv

    Write-Host "  Assigning Key Vault Secrets User on $KeyVaultName..." -ForegroundColor Gray
    az role assignment create `
        --assignee $principalId `
        --role "Key Vault Secrets User" `
        --scope $kvResourceId 2>$null
    Write-Host "    ✓ Key Vault Secrets User" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "[5/6] Skipping IAM role assignments (--SkipRoleAssignments)" -ForegroundColor DarkYellow
}

# ── Function App Settings ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "[6/6] Configuring Function App settings..." -ForegroundColor Yellow

# Key Vault reference for client secret
$kvSecretRef = "@Microsoft.KeyVault(SecretUri=https://${KeyVaultName}.vault.azure.net/secrets/InsightHarbor-PAX-ClientSecret/)"

az functionapp config appsettings set `
    --name $FunctionAppName `
    --resource-group $ResourceGroup `
    --settings `
        "IH_TENANT_ID=$TenantId" `
        "IH_CLIENT_ID=$ClientId" `
        "IH_CLIENT_SECRET=$kvSecretRef" `
        "IH_ADLS_ACCOUNT_NAME=$ADLSAccountName" `
        "IH_ADLS_CONTAINER=$ADLSContainer" `
        "IH_DEFAULT_LOOKBACK_DAYS=1" `
        "IH_PARTITION_HOURS=6" `
        "IH_MAX_CONCURRENCY=4" `
        "IH_ACTIVITY_TYPES=CopilotInteraction" `
        "IH_SUBDIVISION_THRESHOLD=950000" `
        "IH_POLL_MIN_SECONDS=30" `
        "IH_POLL_MAX_SECONDS=90" `
        "IH_MAX_POLL_ATTEMPTS=120" `
        "IH_SCHEDULE_CRON=0 0 2 * * *" `
        "IH_DURABLE_TASK_HUB=ihpipelinehub" `
        "IH_EXPLOSION_MODE=raw" `
        "IH_INCLUDE_M365_USAGE=false" `
        "IH_INCLUDE_DSPM_AI=false" `
        "IH_EXCLUDE_COPILOT_INTERACTION=false" `
        "IH_AUTO_COMPLETENESS=false" `
        "APPLICATIONINSIGHTS_CONNECTION_STRING=$appInsightsConnStr" `
    --output none

Write-Host "  All IH_* settings configured" -ForegroundColor Green

# ═══════════════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Provisioning Complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Resources created:" -ForegroundColor White
Write-Host "    Storage Account: $PipelineStorageName" -ForegroundColor White
Write-Host "    Function App:    $FunctionAppName" -ForegroundColor White
Write-Host ""
Write-Host "  Next step: Deploy the pipeline code:" -ForegroundColor White
Write-Host "    cd pipeline" -ForegroundColor Gray
Write-Host "    func azure functionapp publish $FunctionAppName" -ForegroundColor Gray
Write-Host ""
