# Insight Harbor — Secret Rotation Plan

## Inventory of Secrets

| Secret | Location | Current Expiry | Rotation Cycle | Impact if Expired |
|---|---|---|---|---|
| `InsightHarbor-PAX` client secret | Entra App Reg → Key Vault `IH-CLIENT-SECRET` | 2027-03-04 | 365 days | Pipeline ingestion fails (PAX can't authenticate) |
| ADLS Account Key | Storage Account → Key Vault `IH-ADLS-ACCOUNT-KEY` | 2027-03-04 | 365 days | Pipeline uploads fail, API can't read Silver data |
| `AzureWebJobsStorage` | Key Vault | 2027-03-04 | 365 days | Function App fails to start |
| SWA Deploy Token | GitHub Secret `SWA_DEPLOY_TOKEN` | No expiry | Regenerate if compromised | Dashboard CI/CD deploy fails |
| Function App Publish Profile | GitHub Secret `AZURE_FUNCTIONAPP_PUBLISH_PROFILE` | No expiry | Regenerate if compromised | API CI/CD deploy fails |

---

## Rotation Procedures

### 1. Rotate PAX Client Secret (Every 90 Days)

```powershell
# Step 1: Create new client secret in Entra (demo tenant)
$appId = "01e187b7-264a-4ce1-8cc1-32c977e0a302"  # InsightHarbor-PAX

# In Azure Portal → Entra ID → App registrations → InsightHarbor-PAX
# → Certificates & secrets → New client secret
# Set description: "IH-PAX-YYYY-MM" and expiry: 365 days

# Step 2: Update Key Vault
az keyvault secret set `
  --vault-name ih-keyvault-poc01 `
  --name IH-CLIENT-SECRET `
  --value "<new-secret-value>"

# Step 3: Verify pipeline works
.\scripts\run-pipeline-local.ps1 -SkipExplosion -SkipTransform -DryRun

# Step 4: Delete the OLD client secret from Entra portal
```

### 2. Rotate ADLS Storage Account Key (Every 180 Days)

```powershell
# Step 1: Regenerate key2 (while key1 is still active)
az storage account keys renew `
  --account-name ihstoragepoc01 `
  --resource-group insight-harbor-rg `
  --key key2

# Step 2: Get the new key
$newKey = (az storage account keys list `
  --account-name ihstoragepoc01 `
  --resource-group insight-harbor-rg `
  --query "[1].value" -o tsv)

# Step 3: Update Key Vault
az keyvault secret set `
  --vault-name ih-keyvault-poc01 `
  --name IH-ADLS-ACCOUNT-KEY `
  --value $newKey

# Step 4: Update Function App env var
az functionapp config appsettings set `
  --name ih-api-poc01 `
  --resource-group insight-harbor-rg `
  --settings "IH_ADLS_ACCOUNT_KEY=$newKey"

# Step 5: Verify API health
Invoke-RestMethod -Uri "https://ih-api-poc01.azurewebsites.net/api/health"

# Step 6: Now regenerate key1 (so both keys are fresh)
az storage account keys renew `
  --account-name ihstoragepoc01 `
  --resource-group insight-harbor-rg `
  --key key1
```

### 3. Regenerate SWA Deploy Token (If Compromised)

```powershell
# Step 1: Reset token
az staticwebapp secrets reset-api-key `
  --name ih-dashboard `
  --resource-group insight-harbor-rg

# Step 2: Get new token
$newToken = az staticwebapp secrets list `
  --name ih-dashboard `
  --resource-group insight-harbor-rg `
  --query "properties.apiKey" -o tsv

# Step 3: Update GitHub Secret
# Go to GitHub repo → Settings → Secrets → SWA_DEPLOY_TOKEN → Update value
```

---

## Key Vault Expiration Monitoring

Key Vault has built-in near-expiry event notifications. For the PoC, we use a
simple PowerShell check that can be run manually or added to the Automation Account.

### Quick Check Script

```powershell
# List all secrets with their expiry dates
az keyvault secret list --vault-name ih-keyvault-poc01 `
  --query "[].{name:name, expires:attributes.expires, enabled:attributes.enabled}" `
  -o table
```

### Setting Expiry on Key Vault Secrets

When storing secrets, set an explicit expiry date so Key Vault can warn you:

```powershell
# Example: set a 365-day expiry
$expires = (Get-Date).AddDays(365).ToString("yyyy-MM-ddTHH:mm:ssZ")
az keyvault secret set `
  --vault-name ih-keyvault-poc01 `
  --name IH-CLIENT-SECRET `
  --value "<secret>" `
  --expires $expires
```

---

## Rotation Calendar

| Month | Action |
|---|---|
| Annually (March) | Rotate PAX client secret |
| Annually (March) | Rotate ADLS storage account keys |
| On compromise | Regenerate SWA deploy token, Function App publish profile |
| Annually | Run `az keyvault secret list` expiry check |

---

## Future Improvements

- **Azure Event Grid** — Subscribe to Key Vault `SecretNearExpiry` events → Teams notification
- **Managed Identity** — Eliminate ADLS account key entirely by using system-assigned MI on Function App
- **Certificate-based auth** — Replace PAX client secret with certificate (auto-rotation via Key Vault)
