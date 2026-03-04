# App Registration Setup

> **Account required:** `admin@M365CPI01318443.onmicrosoft.com` (M365 demo tenant — Global Admin)
>
> **NOT your Azure subscription account.** Log into the Microsoft Entra admin center or Azure portal using your demo tenant admin account for all steps in this document.
>
> Portal URLs:
> - Entra Admin Center: https://entra.microsoft.com (sign in as `admin@M365CPI01318443.onmicrosoft.com`)
> - Azure Portal (demo tenant context): https://portal.azure.com/?tenant=M365CPI01318443.onmicrosoft.com

---

## Overview

This App Registration creates a **non-interactive service principal** (daemon app) that Insight Harbor's PAX ingestion scripts use to authenticate to Microsoft Graph and Office 365 Management APIs. It uses client credentials (secret or certificate) — no user interaction required, suitable for scheduled/automated runs.

---

## Step 1 — Create the App Registration

1. Navigate to **Entra Admin Center** → **Applications** → **App registrations**.
2. Click **New registration**.
3. Fill in:
   - **Name:** `insight-harbor-ingestor`
   - **Supported account types:** `Accounts in this organizational directory only (M365CPI01318443 only - Single tenant)`
   - **Redirect URI:** Leave blank (not needed for daemon/service apps)
4. Click **Register**.
5. On the overview page, copy and save:
   - **Application (client) ID** → this is your `clientId`
   - **Directory (tenant) ID** → this is your `tenantId`

---

## Step 2 — Add API Permissions

1. In the app registration, go to **API permissions** → **Add a permission**.
2. Add the following permissions. All must be **Application** type (not Delegated):

### Microsoft Graph
| Permission | Type | Purpose |
|---|---|---|
| `AuditLog.Read.All` | Application | Read Purview audit logs via Graph API |
| `User.Read.All` | Application | Read Entra user profiles for enrichment |
| `Group.Read.All` | Application | Expand group membership for filtering |
| `Reports.Read.All` | Application | Read M365 usage reports |

### Office 365 Management APIs
| Permission | Type | Purpose |
|---|---|---|
| `ActivityFeed.Read` | Application | Read activity feed from Office 365 Management API |

3. After adding all permissions, click **Grant admin consent for M365CPI01318443**.
4. Confirm all permissions show a green ✓ **Granted for M365CPI01318443** status.

---

## Step 3 — Create a Client Secret

> **PoC uses client secret.** Certificate upgrade path is documented in Step 3b but not required for initial PoC.

### Step 3a — Client Secret (PoC)

1. Go to **Certificates & secrets** → **Client secrets** → **New client secret**.
2. Fill in:
   - **Description:** `insight-harbor-poc`
   - **Expires:** `24 months` (longest available — reduces maintenance for PoC)
3. Click **Add**.
4. **Immediately copy the secret Value** — it is only shown once. Paste it into your local `config/insight-harbor-config.json` as `auth.clientSecret`.

> **Important:** Set a calendar reminder before the secret expires. The `check-pax-version.ps1` notification mechanism can be extended to also alert on secret expiry.

### Step 3b — Certificate (Future Upgrade, Optional)

For production/customer deployments, replace the secret with a certificate:
1. Generate a self-signed cert:
   ```powershell
   $cert = New-SelfSignedCertificate `
       -Subject "CN=insight-harbor-ingestor" `
       -CertStoreLocation "Cert:\CurrentUser\My" `
       -KeyExportPolicy Exportable `
       -NotAfter (Get-Date).AddYears(2)
   Export-Certificate -Cert $cert -FilePath "insight-harbor-ingestor.cer"
   ```
2. In the app registration → **Certificates & secrets** → **Certificates** → **Upload certificate** → select the `.cer` file.
3. Note the certificate **Thumbprint** and update `auth.certificateThumbprint` in config.
4. Remove the client secret once the certificate is confirmed working.

---

## Step 4 — Assign Global Reader Role

The service principal needs read access to Purview audit logs and tenant data. Assign the **Global Reader** role:

1. Go to **Entra Admin Center** → **Roles & admins** → **Global Reader**.
2. Click **Add assignments**.
3. Search for `insight-harbor-ingestor` and select it.
4. Click **Add**.

> **Why Global Reader?** The PAX Purview script uses `Search-UnifiedAuditLog` and Exchange Online PowerShell, which requires at minimum the **View-Only Audit Logs** role. Global Reader is the cleanest single-role assignment that covers all required read operations including Graph API calls.

---

## Step 5 — Update Your Local Config

Fill in `config/insight-harbor-config.json` with the values collected above:

```json
"auth": {
  "tenantId": "<Paste Directory (tenant) ID here>",
  "clientId": "<Paste Application (client) ID here>",
  "clientSecret": "<Paste client secret Value here>",
  "certificateThumbprint": ""
}
```

---

## Step 6 — Verify Permissions (Optional Smoke Test)

Run this quick PowerShell verification from your local machine to confirm the app registration can authenticate:

```powershell
# Install if not present: Install-Module Microsoft.Graph -Scope CurrentUser
$tenantId     = "<your-tenant-id>"
$clientId     = "<your-client-id>"
$clientSecret = "<your-client-secret>"

$body = @{
    grant_type    = "client_credentials"
    scope         = "https://graph.microsoft.com/.default"
    client_id     = $clientId
    client_secret = $clientSecret
}
$token = Invoke-RestMethod `
    -Method Post `
    -Uri "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token" `
    -Body $body

if ($token.access_token) {
    Write-Host "SUCCESS: Token acquired. App Registration is configured correctly." -ForegroundColor Green
} else {
    Write-Host "FAILED: No token returned. Check your tenant ID, client ID, and secret." -ForegroundColor Red
}
```

Expected output: `SUCCESS: Token acquired. App Registration is configured correctly.`

---

## Summary — Values to Record

| Value | Where to Find | Config Key |
|---|---|---|
| Directory (tenant) ID | App registration overview | `auth.tenantId` |
| Application (client) ID | App registration overview | `auth.clientId` |
| Client secret Value | Certificates & secrets (copy immediately) | `auth.clientSecret` |
| Secret expiry date | Certificates & secrets | *(calendar reminder)* |

