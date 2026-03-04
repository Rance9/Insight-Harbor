<#
.SYNOPSIS
    Insight Harbor — Daily Pipeline Health Check Runbook
    Azure Automation Account: ih-automation
    Schedule: Run daily (e.g., 08:00 UTC)

.DESCRIPTION
    Checks ADLS Gen2 for fresh data in each pipeline layer.
    Reports stale layers (no new files in > 25 hours) to Teams webhook if configured.
    Uses the Automation Account's System-Assigned Managed Identity — no stored credentials.

    Layers checked:
        bronze/purview/       PAX raw output (PAX must run and upload manually or via local script)
        bronze/exploded/      Python explosion output
        silver/copilot-usage/ Bronze-to-Silver transform output

.NOTES
    REQUIRED AUTOMATION MODULES (install in ih-automation > Modules > Browse Gallery):
        Az.Accounts  >= 2.12.0
        Az.Storage   >= 5.0.0

    OPTIONAL AUTOMATION VARIABLES (Automation Account > Shared Resources > Variables):
        IH_StorageAccountName  — e.g., ihstoragepoc01
        IH_ContainerName       — e.g., insight-harbor
        IH_TeamsWebhookUrl     — Teams incoming webhook URL (leave blank to skip notifications)

    SETUP:
        1. In Automation Account portal:
           Modules > Browse gallery > install Az.Accounts, Az.Storage
        2. Shared Resources > Variables > create the three variables above
        3. Process Automation > Runbooks > Import runbook (this file, type: PowerShell)
        4. Publish the runbook, then link to a daily Schedule
#>

param()

# ─────────────────────────────────────────────────────────────────────────────
# Authenticate using System-Assigned Managed Identity
# ─────────────────────────────────────────────────────────────────────────────
Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Connecting with Managed Identity..."
try {
    Connect-AzAccount -Identity -ErrorAction Stop | Out-Null
    Write-Output "  Managed Identity auth successful."
} catch {
    Write-Error "Failed to authenticate with Managed Identity: $_"
    throw
}

# ─────────────────────────────────────────────────────────────────────────────
# Load config from Automation Variables (or fall back to defaults)
# ─────────────────────────────────────────────────────────────────────────────
function Get-AutomationVariableSafe {
    param([string]$Name, [string]$Default = '')
    try { return Get-AutomationVariable -Name $Name }
    catch { return $Default }
}

$StorageAccountName = Get-AutomationVariableSafe -Name 'IH_StorageAccountName' -Default 'ihstoragepoc01'
$ContainerName      = Get-AutomationVariableSafe -Name 'IH_ContainerName'      -Default 'insight-harbor'
$TeamsWebhookUrl    = Get-AutomationVariableSafe -Name 'IH_TeamsWebhookUrl'    -Default ''

Write-Output "  Storage account : $StorageAccountName"
Write-Output "  Container       : $ContainerName"
Write-Output "  Teams notify    : $($TeamsWebhookUrl -ne '')"

# ─────────────────────────────────────────────────────────────────────────────
# Get storage context
# ─────────────────────────────────────────────────────────────────────────────
$ctx = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

$checksHours = 25   # Flag as stale if no new file in this many hours
$now         = [DateTimeOffset]::UtcNow

# ─────────────────────────────────────────────────────────────────────────────
# Helper: get the most recent blob LastModified in a virtual folder prefix
# ─────────────────────────────────────────────────────────────────────────────
function Get-LatestBlobTime {
    param([string]$Prefix)
    try {
        $blobs = Get-AzStorageBlob -Container $ContainerName -Prefix $Prefix -Context $ctx -ErrorAction SilentlyContinue
        if (-not $blobs -or $blobs.Count -eq 0) { return $null }
        return ($blobs | Sort-Object { $_.LastModified } -Descending | Select-Object -First 1).LastModified
    } catch {
        Write-Warning "  Could not list blobs for prefix [$Prefix]: $_"
        return $null
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Check each pipeline layer
# ─────────────────────────────────────────────────────────────────────────────
$layers = @(
    @{ Name = 'Bronze / PAX Raw';       Prefix = 'bronze/purview/' },
    @{ Name = 'Bronze / Exploded';      Prefix = 'bronze/exploded/' },
    @{ Name = 'Silver / Copilot Usage'; Prefix = 'silver/copilot-usage/' }
)

$stale   = @()
$results = @()

foreach ($layer in $layers) {
    $latest = Get-LatestBlobTime -Prefix $layer.Prefix
    if ($null -eq $latest) {
        $status  = 'NO DATA'
        $age     = 'N/A'
        $isStale = $true
    } else {
        $ageHours = [math]::Round(($now - $latest).TotalHours, 1)
        $age      = "${ageHours}h ago"
        $isStale  = ($ageHours -gt $checksHours)
        $status   = if ($isStale) { "STALE ($age)" } else { "OK ($age)" }
    }

    if ($isStale) { $stale += $layer.Name }

    $results += [PSCustomObject]@{
        Layer  = $layer.Name
        Status = $status
    }

    Write-Output "  [$status]  $($layer.Name)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
Write-Output ""
if ($stale.Count -eq 0) {
    Write-Output "RESULT: All pipeline layers are fresh. No action required."
} else {
    $staleList = $stale -join ', '
    Write-Output "RESULT: Stale layers detected: $staleList"
    Write-Output "  ACTION: Run the local pipeline script (scripts/run-pipeline-local.ps1) to refresh data."
}

# ─────────────────────────────────────────────────────────────────────────────
# Optional: Send Teams notification if stale data detected and webhook configured
# ─────────────────────────────────────────────────────────────────────────────
if ($stale.Count -gt 0 -and $TeamsWebhookUrl -ne '') {
    $tableRows = ($results | ForEach-Object {
        "| $($_.Layer) | $($_.Status) |"
    }) -join "`n"

    $card = @{
        type        = 'message'
        attachments = @(@{
            contentType = 'application/vnd.microsoft.card.adaptive'
            content     = @{
                '$schema' = 'http://adaptivecards.io/schemas/adaptive-card.json'
                type      = 'AdaptiveCard'
                version   = '1.4'
                body      = @(
                    @{ type = 'TextBlock'; text = 'Insight Harbor — Pipeline Alert'; weight = 'Bolder'; size = 'Medium' },
                    @{ type = 'TextBlock'; text = "Stale layers: **$($stale -join ', ')**"; wrap = $true },
                    @{ type = 'TextBlock'; text = "Run ``scripts/run-pipeline-local.ps1`` to refresh."; wrap = $true }
                )
            }
        })
    }

    try {
        $body = $card | ConvertTo-Json -Depth 10
        Invoke-RestMethod -Uri $TeamsWebhookUrl -Method Post -Body $body -ContentType 'application/json' | Out-Null
        Write-Output "  Teams notification sent."
    } catch {
        Write-Warning "  Failed to send Teams notification: $_"
    }
}

Write-Output ""
Write-Output "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Health check complete."
