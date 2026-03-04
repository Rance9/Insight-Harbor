<#
.SYNOPSIS
    Insight Harbor — PAX Script Version Checker
    Scans the ingestion\ folder for PAX scripts and compares to expected versions.

.DESCRIPTION
    PAX (PowerShell Audit eXtractor) scripts have version headers embedded in the file.
    This script:
        1. Finds all PAX scripts in ingestion\ matching expected naming patterns
        2. Reads the version from the script header comment
        3. Compares to the pinned "known-good" versions defined in this script
        4. Warns if:
            - A script is missing (not yet dropped into ingestion\)
            - The version is older than the pinned minimum (may lack ConfigFile support)
            - The version is newer than expected (test before relying on it)
        5. Optionally checks PAX GitHub Releases for any published newer versions

    The PAX scripts are NOT committed to this repo. They must be obtained from the
    PAX repository and modified per docs\pax-ai-prompts.md before dropping into ingestion\.

.PARAMETER ScriptsPath
    Path to the ingestion folder to inspect (default: ingestion\).

.PARAMETER CheckGitHub
    If set, queries the PAX GitHub Releases API for the latest published versions.
    Requires internet connectivity. Rate-limited to 60 unauthenticated requests/hour.

.EXAMPLE
    .\scripts\check-pax-version.ps1
    # Check local ingestion\ folder against pinned versions

.EXAMPLE
    .\scripts\check-pax-version.ps1 -CheckGitHub
    # Also check GitHub for newer versions
#>

[CmdletBinding()]
param(
    [string] $ScriptsPath  = 'ingestion',
    [switch] $CheckGitHub
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ─────────────────────────────────────────────────────────────────────────────
# Pinned minimum versions for Insight Harbor compatibility
# Update these when you validate a new PAX version with the pipeline.
# ─────────────────────────────────────────────────────────────────────────────
$PinnedVersions = @{
    # PAX Purview (Unified Audit Log)
    'PAX_Purview_Audit_Log_Processor' = @{
        Patterns    = @('PAX_Purview_Audit_Log_Processor_v*.ps1', 'PAX_Purview_Audit_Log_Processor_v*_IH.ps1')
        MinVersion  = [version]'1.10.7'
        Description = 'Purview Unified Audit Log extraction — primary data source'
        GitHub      = 'https://api.github.com/repos/o365scripts/PAX/releases/latest'
    }
    # PAX Graph (Entra user profiles)
    'PAX_Graph_Audit_Log_Processor' = @{
        Patterns    = @('PAX_Graph_Audit_Log_Processor_v*.ps1', 'PAX_Graph_Audit_Log_Processor_v*_IH.ps1')
        MinVersion  = [version]'1.0.1'
        Description = 'Entra ID / Graph user profile export — enrichment data'
        GitHub      = $null
    }
    # PAX Copilot Interactions
    'PAX_CopilotInteractions_Content_Audit_Log_Processor' = @{
        Patterns    = @('PAX_CopilotInteractions_Content_Audit_Log_Processor_v*.ps1', 'PAX_CopilotInteractions_Content_Audit_Log_Processor_v*_IH.ps1')
        MinVersion  = [version]'1.2.0'
        Description = 'Copilot interaction metadata export (optional in Phase 1)'
        GitHub      = $null
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
function Get-ScriptVersion {
    param([string]$FilePath)
    # PAX scripts embed version in the filename: Script_Name_v1.10.7.ps1 or Script_Name_v1.10.7_IH.ps1
    if ($FilePath -match '_v(\d+\.\d+\.\d+)(?:_IH)?\.ps1$') {
        return [version]$Matches[1]
    }
    # Fall back to searching first 30 lines for # Version: x.y.z
    $lines = Get-Content $FilePath -TotalCount 30 -ErrorAction SilentlyContinue
    foreach ($line in $lines) {
        if ($line -match '#.*[Vv]ersion[:\s]+(\d+\.\d+\.\d+)') {
            return [version]$Matches[1]
        }
    }
    return $null
}

function Get-LatestGitHubVersion {
    param([string]$ApiUrl)
    try {
        $response = Invoke-RestMethod -Uri $ApiUrl -Headers @{ 'User-Agent' = 'InsightHarbor/1.0' } -TimeoutSec 10
        $tag = $response.tag_name -replace '^v', ''
        if ($tag -match '^\d+\.\d+\.\d+$') { return [version]$tag }
        return $null
    } catch {
        return $null
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Main check
# ─────────────────────────────────────────────────────────────────────────────
Push-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path $ScriptsPath)) {
    Write-Warning "Scripts path not found: $ScriptsPath"
    exit 1
}

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   PAX Script Version Checker                         ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host "  Checking: $ScriptsPath\"
Write-Host "  Date    : $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
Write-Host ""

$allOk     = $true
$results   = @()

foreach ($scriptKey in $PinnedVersions.Keys | Sort-Object) {
    $info    = $PinnedVersions[$scriptKey]
    $matches = $info.Patterns | ForEach-Object {
        Get-ChildItem (Join-Path $ScriptsPath $_) -ErrorAction SilentlyContinue
    } | Where-Object { $null -ne $_ }

    if ($null -eq $matches -or @($matches).Count -eq 0) {
        $status = 'MISSING'
        $color  = 'Yellow'
        $note   = "Not found in $ScriptsPath\. See docs\pax-ai-prompts.md to prepare this script."
        $allOk  = $false
    } else {
        # If multiple versions exist, take the highest
        $latest    = $matches | Sort-Object {
            if ($_.Name -match '_v(\d+\.\d+\.\d+)(?:_IH)?\.ps1$') { [version]$Matches[1] } else { [version]'0.0.0' }
        } -Descending | Select-Object -First 1

        $found = Get-ScriptVersion -FilePath $latest.FullName

        if ($null -eq $found) {
            $status = 'UNKNOWN'
            $color  = 'Yellow'
            $note   = "Could not parse version from filename or header."
            $allOk  = $false
        } elseif ($found -lt $info.MinVersion) {
            $status = 'OLD'
            $color  = 'Red'
            $note   = "Version $found < minimum $($info.MinVersion). This version may not support -ConfigFile. Update and re-run PAX AI prompts."
            $allOk  = $false
        } else {
            $status = 'OK'
            $color  = 'Green'
            $note   = "v$found ≥ minimum v$($info.MinVersion)"

            # Check if it's been modified for Insight Harbor (look for ConfigFile param usage)
            $content = Get-Content $latest.FullName -Raw -ErrorAction SilentlyContinue
            if ($content -and ($content -notmatch '\$ConfigFile|\-ConfigFile')) {
                $status = 'UNMODIFIED'
                $color  = 'Yellow'
                $note   = "v$found is present but ConfigFile param not found. Run docs\pax-ai-prompts.md Prompt 1 on this script."
                $allOk  = $false
            }
        }
    }

    # Optionally fetch latest GitHub version
    $ghVersion = $null
    if ($CheckGitHub -and $null -ne $info.GitHub) {
        $ghVersion = Get-LatestGitHubVersion -ApiUrl $info.GitHub
    }

    $row = [PSCustomObject]@{
        Script      = $scriptKey
        Status      = $status
        Local       = if ($found) { "v$found" } else { '—' }
        Minimum     = "v$($info.MinVersion)"
        GitHubLatest = if ($ghVersion) { "v$ghVersion" } else { if ($CheckGitHub -and $info.GitHub) { 'fetch failed' } else { '(not checked)' } }
        Note        = $note
    }
    $results += $row

    Write-Host "  [$status]  $scriptKey" -ForegroundColor $color
    Write-Host "        $note" -ForegroundColor Gray
    Write-Host ""
}

# ─────────────────────────────────────────────────────────────────────────────
# Summary table
# ─────────────────────────────────────────────────────────────────────────────
$results | Format-Table -Property Script, Status, Local, Minimum, GitHubLatest -AutoSize

if ($allOk) {
    Write-Host "RESULT: All PAX scripts are present and ready." -ForegroundColor Green
    exit 0
} else {
    Write-Host "RESULT: One or more PAX scripts need attention (see above)." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  NEXT STEPS:"
    Write-Host "  1. Obtain missing scripts from the PAX repository"
    Write-Host "  2. Follow docs\pax-ai-prompts.md to apply Insight Harbor modifications"
    Write-Host "  3. Drop the modified script into ingestion\ and re-run this check"
    exit 1
}
