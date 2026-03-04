<#
.SYNOPSIS
    Insight Harbor — Local Pipeline Orchestrator
    Run this on demand to refresh Insight Harbor data end-to-end.

.DESCRIPTION
    Orchestrates the full Insight Harbor pipeline on your local machine:

    Stage 1  — PAX Purview ingestion
        Invokes the modified PAX Purview script.
        The PAX script must already be dropped into ingestion/ and
        configured per ingestion/README.md (ConfigFile param, ADLS upload).
        If the PAX script is not present, this stage is skipped with a warning.

    Stage 1B — PAX Entra user ingestion
        Invokes PAX Purview with -OnlyUserInfo to pull Entra directory data
        (users, licenses, org info). Output: EntraUsers_MAClicensing_*.csv

    Stage 2  — Python explosion
        Detects new raw CSVs in ingestion/output/ (or reads from ADLS bronze/purview/).
        Calls transform/explosion/pipeline_explode.py.

    Stage 2B — Entra Bronze → Silver transform
        Calls transform/bronze_to_silver_entra.py to produce the Silver Entra
        dimension table (silver_entra_users.csv).

    Stage 3  — Bronze → Silver transform (with Entra enrichment)
        Calls transform/bronze_to_silver_purview.py on the exploded output,
        enriching each usage row with Entra dimension columns via LEFT JOIN.

    Stage 4  — Summary
        Prints ADLS blob counts by layer and elapsed time.

.PARAMETER ConfigFile
    Path to the Insight Harbor config JSON (default: config/insight-harbor-config.json).

.PARAMETER SkipPAX
    Skip Stage 1 and Stage 1B (PAX ingestion, both Purview audit and Entra users).

.PARAMETER SkipEntra
    Skip Entra stages (1B and 2B). Use when Entra data is already up to date.

.PARAMETER SkipExplosion
    Skip Stage 2. Use when the exploded file was produced in a previous run.

.PARAMETER SkipTransform
    Skip Stage 3. Use when only re-running PAX ingestion.

.PARAMETER InputCsv
    Explicit path to an existing raw PAX CSV to process (skips PAX stage automatically).
    If omitted, the script looks for the most recent CSV in ingestion/output/.

.PARAMETER DryRun
    Pass --dry-run to Python stages (no ADLS uploads, prints what would happen).

.EXAMPLE
    .\scripts\run-pipeline-local.ps1
    # Full run: PAX → explosion → transform, using config/insight-harbor-config.json

.EXAMPLE
    .\scripts\run-pipeline-local.ps1 -SkipPAX -DryRun
    # Test explosion + transform on the latest raw CSV already in ingestion/output/

.EXAMPLE
    .\scripts\run-pipeline-local.ps1 -InputCsv "ingestion\output\purview_20250101.csv"
    # Run explosion + transform on a specific file (no PAX stage)
#>

[CmdletBinding()]
param(
    [string] $ConfigFile  = 'config\insight-harbor-config.json',
    [switch] $SkipPAX,
    [switch] $SkipEntra,
    [switch] $SkipExplosion,
    [switch] $SkipTransform,
    [string] $InputCsv    = '',
    [switch] $DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
$script:PipelineRoot = Split-Path -Parent $PSScriptRoot
Set-Location $script:PipelineRoot

function Write-Step {
    param([string]$Msg)
    Write-Host "`n── $Msg" -ForegroundColor Cyan
}

function Write-OK   { param([string]$Msg) Write-Host "  ✔ $Msg" -ForegroundColor Green }
function Write-Warn { param([string]$Msg) Write-Host "  ⚠ $Msg" -ForegroundColor Yellow }
function Write-Fail { param([string]$Msg) Write-Host "  ✘ $Msg" -ForegroundColor Red }

function Invoke-StageOrDie {
    param([string]$Name, [scriptblock]$Stage)
    Write-Step $Name
    try {
        & $Stage
        Write-OK "$Name completed."
    } catch {
        Write-Fail "$Name FAILED: $_"
        $elapsed = [math]::Round(((Get-Date) - $startTime).TotalSeconds, 1)
        Send-TeamsNotification -Status "Failed" -ElapsedSec $elapsed -ErrorMessage "$Name — $_" -ConfigPath $ConfigFile
        throw
    }
}

function Send-TeamsNotification {
    <#
    .SYNOPSIS
        Send a pipeline completion notification to a Teams channel via Workflows webhook.
    .DESCRIPTION
        Posts an Adaptive Card to the configured Teams webhook URL.
        If teamsWebhookUrl is empty/missing in config, this is a no-op.
    #>
    param(
        [string]$Status,        # "Success" or "Failed"
        [string]$ElapsedSec,
        [string]$ErrorMessage = "",
        [string]$ConfigPath
    )

    try {
        $cfg = Get-Content $ConfigPath -Raw -ErrorAction SilentlyContinue | ConvertFrom-Json -ErrorAction SilentlyContinue
        $webhookUrl = $cfg.notifications.teamsWebhookUrl

        if (-not $webhookUrl -or $webhookUrl -eq "") {
            Write-Warn "Teams webhook URL not configured — skipping notification."
            return
        }

        $statusColor = if ($Status -eq "Success") { "Good" } else { "Attention" }
        $statusEmoji = if ($Status -eq "Success") { "✅" } else { "❌" }
        $dashboardUrl = "https://ih.data-analytics.tech"
        $timestamp = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " UTC"

        # Adaptive Card payload (Teams Workflows webhook format)
        $card = @{
            type = "message"
            attachments = @(
                @{
                    contentType = "application/vnd.microsoft.card.adaptive"
                    contentUrl  = $null
                    content     = @{
                        '$schema' = "http://adaptivecards.io/schemas/adaptive-card.json"
                        type      = "AdaptiveCard"
                        version   = "1.4"
                        body      = @(
                            @{
                                type   = "TextBlock"
                                size   = "Medium"
                                weight = "Bolder"
                                text   = "$statusEmoji Insight Harbor Pipeline — $Status"
                            },
                            @{
                                type  = "FactSet"
                                facts = @(
                                    @{ title = "Status";   value = $Status },
                                    @{ title = "Duration"; value = "$ElapsedSec seconds" },
                                    @{ title = "Time";     value = $timestamp },
                                    @{ title = "Host";     value = $env:COMPUTERNAME }
                                )
                            }
                        )
                        actions   = @(
                            @{
                                type  = "Action.OpenUrl"
                                title = "Open Dashboard"
                                url   = $dashboardUrl
                            }
                        )
                    }
                }
            )
        } | ConvertTo-Json -Depth 10 -Compress

        # Add error details if failed
        if ($ErrorMessage -and $Status -ne "Success") {
            $cardObj = $card | ConvertFrom-Json
            $cardObj.attachments[0].content.body += @{
                type  = "TextBlock"
                text  = "Error: $($ErrorMessage.Substring(0, [Math]::Min($ErrorMessage.Length, 200)))"
                wrap  = $true
                color = "Attention"
            }
            $card = $cardObj | ConvertTo-Json -Depth 10 -Compress
        }

        Invoke-RestMethod -Uri $webhookUrl -Method Post -ContentType "application/json" -Body $card -TimeoutSec 10 | Out-Null
        Write-OK "Teams notification sent ($Status)."
    } catch {
        Write-Warn "Teams notification failed: $_"
        # Non-fatal — don't break the pipeline for a notification failure
    }
}

$startTime = Get-Date

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║   INSIGHT HARBOR — Local Pipeline Run                ║" -ForegroundColor Magenta
Write-Host "╚══════════════════════════════════════════════════════╝" -ForegroundColor Magenta
Write-Host "  Root   : $script:PipelineRoot"
Write-Host "  Config : $ConfigFile"
Write-Host "  DryRun : $DryRun"
Write-Host "  Time   : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') UTC"

# Verify config exists
if (-not (Test-Path $ConfigFile)) {
    Write-Fail "Config file not found: $ConfigFile"
    Write-Host ""
    Write-Host "  Copy 'config\insight-harbor-config.template.json' to 'config\insight-harbor-config.json'" -ForegroundColor Yellow
    Write-Host "  Fill in tenantId, clientId, clientSecret, and ADLS account info." -ForegroundColor Yellow
    exit 1
}

# ─────────────────────────────────────────────────────────────────────────────
# Resolve @KeyVault: references in config to a temp runtime config
# ─────────────────────────────────────────────────────────────────────────────
$cfgRaw = Get-Content $ConfigFile -Raw | ConvertFrom-Json
$vaultName = $cfgRaw.keyVault.vaultName
$resolvedConfigFile = $ConfigFile  # default: use original

if ($vaultName) {
    Write-Step "Resolving Key Vault secrets from '$vaultName'"
    $cfgJson = Get-Content $ConfigFile -Raw
    # Find all @KeyVault:<secret-name> values and resolve them
    $pattern = '@KeyVault:([A-Za-z0-9\-]+)'
    $resolvedSecrets = @{}
    [regex]::Matches($cfgJson, $pattern) | ForEach-Object {
        $secretName = $_.Groups[1].Value
        if (-not $resolvedSecrets.ContainsKey($secretName)) {
            try {
                $secretValue = az keyvault secret show --vault-name $vaultName --name $secretName --query "value" -o tsv 2>$null
                if ($LASTEXITCODE -eq 0 -and $secretValue) {
                    $resolvedSecrets[$secretName] = $secretValue.Trim()
                    Write-OK "Resolved: $secretName"
                } else {
                    Write-Warn "Could not resolve: $secretName"
                }
            } catch {
                Write-Warn "Key Vault lookup failed for $secretName`: $_"
            }
        }
    }
    # Write a temp runtime config with resolved secrets (never persisted/committed)
    if ($resolvedSecrets.Count -gt 0) {
        foreach ($kv in $resolvedSecrets.GetEnumerator()) {
            $cfgJson = $cfgJson -replace "@KeyVault:$($kv.Key)", $kv.Value
        }
        $resolvedConfigFile = Join-Path $env:TEMP "ih-config-resolved-$(Get-Date -Format 'yyyyMMddHHmmss').json"
        $cfgJson | Out-File -FilePath $resolvedConfigFile -Encoding utf8 -Force
        Write-OK "Runtime config written to $resolvedConfigFile (temp, not committed)"
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1 — PAX Purview Ingestion
# ─────────────────────────────────────────────────────────────────────────────
$rawCsvPath = ''
$script:explodedCsvPath = ''

if ($InputCsv -ne '') {
    Write-Warn "InputCsv specified — skipping PAX stage and using: $InputCsv"
    $rawCsvPath = $InputCsv
} elseif ($SkipPAX) {
    Write-Warn "PAX stage skipped by -SkipPAX flag. Looking for latest CSV in ingestion\output\..."
} else {
    Invoke-StageOrDie "Stage 1 — PAX Purview Ingestion" {
        $paxScript = Get-ChildItem 'ingestion\' -Filter 'PAX_Purview_Audit_Log_Processor_v*_IH.ps1' |
                     Sort-Object Name -Descending |
                     Select-Object -First 1

        if ($null -eq $paxScript) {
            Write-Warn "No PAX Purview script found in ingestion\. Stage 1 skipped."
            Write-Host "  → Drop the modified PAX script into ingestion\ (see ingestion\README.md)" -ForegroundColor Yellow
        } else {
            Write-OK "Found PAX script: $($paxScript.Name)"
            Write-Host "  → Running: pwsh -File `"$($paxScript.FullName)`" -ConfigFile `"$resolvedConfigFile`" -Auth AppRegistration" -ForegroundColor Gray
            & pwsh -File $paxScript.FullName -ConfigFile $resolvedConfigFile -Auth AppRegistration
        }
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1B — PAX Entra User Ingestion (-OnlyUserInfo)
# ─────────────────────────────────────────────────────────────────────────────
$script:entraLocalPath = ''

if (-not $SkipPAX -and -not $SkipEntra) {
    Invoke-StageOrDie "Stage 1B — PAX Entra User Ingestion" {
        $paxScript = Get-ChildItem 'ingestion\' -Filter 'PAX_Purview_Audit_Log_Processor_v*_IH.ps1' |
                     Sort-Object Name -Descending |
                     Select-Object -First 1

        if ($null -eq $paxScript) {
            Write-Warn "No PAX Purview script found — skipping Entra ingestion."
        } else {
            Write-OK "Running PAX -OnlyUserInfo: $($paxScript.Name)"
            Write-Host "  → pwsh -File `"$($paxScript.FullName)`" -ConfigFile `"$resolvedConfigFile`" -OnlyUserInfo -Auth AppRegistration" -ForegroundColor Gray
            & pwsh -File $paxScript.FullName -ConfigFile $resolvedConfigFile -OnlyUserInfo -Auth AppRegistration
        }
    }
} elseif ($SkipEntra) {
    Write-Warn "Entra ingestion skipped by -SkipEntra flag."
} else {
    Write-Warn "PAX skipped — Entra ingestion also skipped."
}

# Find latest Entra CSV for downstream stages
$latestEntra = Get-ChildItem 'ingestion\output\' -Filter 'EntraUsers_MAClicensing_*.csv' -ErrorAction SilentlyContinue |
               Sort-Object LastWriteTime -Descending |
               Select-Object -First 1
if ($null -ne $latestEntra) {
    $script:entraLocalPath = $latestEntra.FullName
    Write-OK "Entra CSV for transform: $($latestEntra.Name)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Find latest raw CSV (after PAX or manual drop)
# ─────────────────────────────────────────────────────────────────────────────
if ($rawCsvPath -eq '' -and -not $SkipExplosion) {
    $latestRaw = Get-ChildItem 'ingestion\output\' -Filter '*.csv' -ErrorAction SilentlyContinue |
                 Sort-Object LastWriteTime -Descending |
                 Select-Object -First 1
    if ($null -eq $latestRaw) {
        Write-Warn "No CSV files found in ingestion\output\. Explosion stage will be skipped."
        $SkipExplosion = $true
    } else {
        $rawCsvPath = $latestRaw.FullName
        Write-OK "Raw CSV to process: $rawCsvPath ($('{0:N0}' -f (Get-Item $rawCsvPath).Length) bytes)"
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2 — Python Explosion
# ─────────────────────────────────────────────────────────────────────────────
$explodedCsvPath = ''

if (-not $SkipExplosion) {
    Invoke-StageOrDie "Stage 2 — Python Explosion Processor" {
        $timestamp    = Get-Date -Format 'yyyyMMdd_HHmmss'
        $outputDir    = 'ingestion\output'
        $explodedFile = "$outputDir\exploded_$timestamp.csv"

        $pyArgs = @(
            'transform\explosion\pipeline_explode.py',
            '--input',  $rawCsvPath,
            '--output', $explodedFile,
            '--config', $resolvedConfigFile
        )
        if ($DryRun) { $pyArgs += '--dry-run' }

        Write-Host "  → python $($pyArgs -join ' ')" -ForegroundColor Gray
        python @pyArgs

        if (-not $DryRun -and (Test-Path $explodedFile)) {
            $script:explodedCsvPath = $explodedFile
            Write-OK "Exploded output: $explodedFile"
        } elseif ($DryRun) {
            Write-OK "Dry run — no file written."
        } else {
            throw "Exploded CSV not found at expected path: $explodedFile"
        }
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2B — Entra Bronze → Silver Transform
# ─────────────────────────────────────────────────────────────────────────────
if (-not $SkipEntra -and $script:entraLocalPath -ne '') {
    Invoke-StageOrDie "Stage 2B — Entra Bronze → Silver Transform" {
        $pyArgs = @(
            'transform\bronze_to_silver_entra.py',
            '--input',  $script:entraLocalPath,
            '--config', $resolvedConfigFile
        )
        if ($DryRun) { $pyArgs += '--dry-run' }

        Write-Host "  → python $($pyArgs -join ' ')" -ForegroundColor Gray
        python @pyArgs
    }
} elseif (-not $SkipEntra) {
    Write-Warn "Stage 2B skipped — no Entra CSV found in ingestion\output\."
}

# ─────────────────────────────────────────────────────────────────────────────
# STAGE 3 — Bronze → Silver Transform (with Entra enrichment)
# ─────────────────────────────────────────────────────────────────────────────
# Auto-detect latest exploded CSV if explosion was skipped
if ($script:explodedCsvPath -eq '' -and -not $SkipTransform) {
    $latestExploded = Get-ChildItem 'ingestion\output\' -Filter 'exploded_*.csv' -ErrorAction SilentlyContinue |
                      Sort-Object LastWriteTime -Descending |
                      Select-Object -First 1
    if ($null -ne $latestExploded) {
        $script:explodedCsvPath = $latestExploded.FullName
        Write-OK "Using existing exploded CSV: $($latestExploded.Name)"
    }
}

if (-not $SkipTransform -and $script:explodedCsvPath -ne '') {
    Invoke-StageOrDie "Stage 3 — Bronze → Silver Transform" {
        $pyArgs = @(
            'transform\bronze_to_silver_purview.py',
            '--config', $resolvedConfigFile
        )
        if ($script:explodedCsvPath -ne '') { $pyArgs += @('--input', $script:explodedCsvPath) }
        if ($DryRun)                  { $pyArgs += '--dry-run' }

        # Pass local Entra Silver for enrichment
        $entraLocal = Get-ChildItem 'ingestion\output\' -Filter 'silver_entra_users.csv' -ErrorAction SilentlyContinue |
                      Select-Object -First 1
        if ($null -ne $entraLocal) {
            $pyArgs += @('--entra-local', $entraLocal.FullName)
            Write-OK "Entra enrichment source: $($entraLocal.Name)"
        } else {
            Write-Warn "No local silver_entra_users.csv — enrichment will try ADLS or be empty."
        }

        Write-Host "  → python $($pyArgs -join ' ')" -ForegroundColor Gray
        python @pyArgs
    }
} elseif (-not $SkipTransform) {
    Write-Warn "Stage 3 skipped — no exploded CSV produced (explosion was skipped or dry-run)."
}

# ─────────────────────────────────────────────────────────────────────────────
# Cleanup — remove temp resolved config (secrets should not persist on disk)
# ─────────────────────────────────────────────────────────────────────────────
if ($resolvedConfigFile -ne $ConfigFile -and (Test-Path $resolvedConfigFile)) {
    Remove-Item $resolvedConfigFile -Force -ErrorAction SilentlyContinue
    Write-OK "Removed temp runtime config: $resolvedConfigFile"
}

# ─────────────────────────────────────────────────────────────────────────────
# Cleanup — remove intermediate pipeline artifacts from ingestion/output/
# ─────────────────────────────────────────────────────────────────────────────
# After successful ADLS upload, all data lives in the lake house. Local
# intermediate files (exploded CSVs, silver CSVs, metadata JSONs, synthetic
# data, PAX logs) are no longer needed.
#
# Preserved: PAX checkpoint files (.pax_checkpoint_*.json) — needed for
# incremental fetches on the next pipeline run.
Write-Step "Cleanup — Removing intermediate pipeline artifacts"

$outputDir = 'ingestion\output'
if (Test-Path $outputDir) {
    # Patterns to remove (intermediate/temp artifacts)
    $cleanupPatterns = @(
        'exploded_*.csv',
        '*_explosion_metadata.json',
        '*_transform_metadata.json',
        '*_entra_transform_metadata.json',
        'silver_copilot_usage.csv',
        'silver_entra_users.csv',
        'synthetic_purview_*.csv',
        'Purview_Audit_*.log',
        'Purview_Audit_*_PARTIAL.log'
    )

    $removedCount = 0
    foreach ($pattern in $cleanupPatterns) {
        $files = Get-ChildItem $outputDir -Filter $pattern -ErrorAction SilentlyContinue
        foreach ($file in $files) {
            try {
                Remove-Item $file.FullName -Force
                $removedCount++
            } catch {
                Write-Warn "Could not remove: $($file.Name) — $_"
            }
        }
    }

    if ($removedCount -gt 0) {
        Write-OK "Removed $removedCount intermediate file(s) from $outputDir"
    } else {
        Write-OK "No intermediate files to clean up."
    }

    # Show what remains (checkpoint files, source data)
    $remaining = Get-ChildItem $outputDir -ErrorAction SilentlyContinue
    if ($remaining) {
        Write-Host "  Remaining files (preserved):" -ForegroundColor Gray
        foreach ($item in $remaining) {
            Write-Host "    $($item.Name)" -ForegroundColor Gray
        }
    }
} else {
    Write-OK "No output directory to clean up."
}

# ─────────────────────────────────────────────────────────────────────────────
# STAGE 4 — Summary
# ─────────────────────────────────────────────────────────────────────────────
$elapsed = [math]::Round(((Get-Date) - $startTime).TotalSeconds, 1)

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║   PIPELINE COMPLETE — $elapsed seconds" + (' ' * [Math]::Max(0, 27 - "$elapsed".Length)) + "║" -ForegroundColor Magenta
Write-Host "╚══════════════════════════════════════════════════════╝" -ForegroundColor Magenta
Write-Host ""
Write-Host "  Next steps:"
Write-Host "  1. Open Power BI → Refresh dataset (ADLS silver/copilot-usage/)"
Write-Host "  2. Or visit https://ih.data-analytics.tech"
Write-Host ""

# ─────────────────────────────────────────────────────────────────────────────
# Teams notification — pipeline success
# ─────────────────────────────────────────────────────────────────────────────
Send-TeamsNotification -Status "Success" -ElapsedSec $elapsed -ConfigPath $ConfigFile
