<#
.SYNOPSIS
    Insight Harbor — Synthetic Data Generator
    Produces realistic fake PAX Purview CSV output for pipeline testing.

.DESCRIPTION
    Generates a CSV that matches the exact schema and column set produced by
    the modified PAX Purview script in RAW mode (pre-explosion).

    The raw PAX output contains one row per audit log record with all
    AuditData fields stored as a packed JSON string in 'AuditData' column.
    The Python explosion processor then flattens that JSON into 153 columns.

    Generated data mimics:
        • M365 Copilot interactions (SendMessage, CopilotResponse, etc.)
        • Purview DLP policy events
        • SharePoint file access events (for baseline diversity)

    Use this to:
        • Test the explosion processor without real M365 data
        • Validate Power BI schema before first real ingest
        • Demo the dashboard to stakeholders before App Registration is ready
        • Regression-test the pipeline automatically

.PARAMETER OutputPath
    Where to write the generated CSV (default: ingestion\output\synthetic_purview_<timestamp>.csv).

.PARAMETER RowCount
    Number of audit records to generate (default: 500).

.PARAMETER StartDate
    Earliest date for generated records. Default: 30 days ago.

.PARAMETER EndDate
    Latest date for generated records. Default: now.

.PARAMETER UserCount
    Number of distinct synthetic users to distribute records across (default: 25).

.PARAMETER Seed
    Random seed for reproducible output. Omit for random data each run.

.EXAMPLE
    .\scripts\generate-synthetic.ps1
    # 500 rows, last 30 days, written to ingestion\output\synthetic_purview_<ts>.csv

.EXAMPLE
    .\scripts\generate-synthetic.ps1 -RowCount 2000 -UserCount 50 -Seed 42
    # Reproducible large dataset for load testing
#>

[CmdletBinding()]
param(
    [string]   $OutputPath = '',
    [int]      $RowCount   = 500,
    [datetime] $StartDate  = [datetime]::UtcNow.AddDays(-30),
    [datetime] $EndDate    = [datetime]::UtcNow,
    [int]      $UserCount  = 25,
    [int]      $Seed       = -1
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────
$rng = if ($Seed -ge 0) { [System.Random]::new($Seed) } else { [System.Random]::new() }

Push-Location (Split-Path -Parent $PSScriptRoot)

if ($OutputPath -eq '') {
    $ts         = Get-Date -Format 'yyyyMMdd_HHmmss'
    $OutputPath = "ingestion\output\synthetic_purview_$ts.csv"
}
$outputDir = Split-Path -Parent $OutputPath
if (-not (Test-Path $outputDir)) { New-Item -ItemType Directory -Path $outputDir -Force | Out-Null }

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║   Insight Harbor — Synthetic Data Generator          ║" -ForegroundColor Magenta
Write-Host "╚══════════════════════════════════════════════════════╝" -ForegroundColor Magenta
Write-Host "  Rows      : $RowCount"
Write-Host "  Users     : $UserCount"
Write-Host "  Date range: $($StartDate.ToString('yyyy-MM-dd')) → $($EndDate.ToString('yyyy-MM-dd'))"
Write-Host "  Output    : $OutputPath"
Write-Host "  Seed      : $(if ($Seed -ge 0) { $Seed } else { 'random' })"
Write-Host ""

# ─────────────────────────────────────────────────────────────────────────────
# Fake data pools
# ─────────────────────────────────────────────────────────────────────────────
$domains = @('contoso.com', 'fabrikam.onmicrosoft.com', 'woodgrove.com')

$firstNames = @('Alex','Jordan','Morgan','Taylor','Casey','Riley','Drew','Parker',
                'Sam','Avery','Quinn','Blair','Reese','Logan','Skyler','Finley',
                'Robin','Sage','Hayden','Peyton','Cameron','Dakota','Emerson','Harley','Indigo')
$lastNames  = @('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
                'Wilson','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin',
                'Thompson','Young','Allen','King','Wright','Scott','Torres','Hill','Green')

# Generate synthetic users
$users = 1..$UserCount | ForEach-Object {
    $fn     = $firstNames[$rng.Next($firstNames.Count)]
    $ln     = $lastNames[$rng.Next($lastNames.Count)]
    $domain = $domains[$rng.Next($domains.Count)]
    [PSCustomObject]@{
        DisplayName = "$fn $ln"
        UPN         = "$($fn.ToLower()).$($ln.ToLower())@$domain"
        ObjectId    = [System.Guid]::NewGuid().ToString()
        Department  = (@('Engineering','Sales','Marketing','Finance','HR','IT','Legal','Operations'))[$rng.Next(8)]
        Country     = (@('US','GB','CA','AU','DE','FR','JP','IN'))[$rng.Next(8)]
    }
}

$operations = @(
    @{ Op = 'SendMessage';          Weight = 40; Type = 'MicrosoftTeams' }
    @{ Op = 'CopilotResponse';      Weight = 35; Type = 'MicrosoftTeams' }
    @{ Op = 'MessageCreatedHasLink';Weight = 10; Type = 'MicrosoftTeams' }
    @{ Op = 'FileAccessed';         Weight = 8;  Type = 'SharePoint' }
    @{ Op = 'DlpRuleMatch';         Weight = 4;  Type = 'SecurityComplianceCenter' }
    @{ Op = 'MeetingParticipantDetail'; Weight = 3; Type = 'MicrosoftTeams' }
)

# Expand by weight
$weightedOps = @()
foreach ($op in $operations) {
    for ($w = 0; $w -lt $op.Weight; $w++) { $weightedOps += $op }
}

$agentNames = @(
    'Microsoft Copilot', 'Word Copilot', 'Excel Copilot', 'PowerPoint Copilot',
    'Teams Copilot', 'Outlook Copilot', 'Loop Copilot', $null, $null, $null
)

$samplePrompts = @(
    'Summarize the last 10 emails from the project team',
    'Draft a reply to the Q4 budget review',
    'Create a slide deck for the product launch',
    'What are the key action items from this meeting?',
    'Generate a status report for the sprint',
    'Translate this document to Spanish',
    'Identify the main risks in this proposal',
    'Write a job description for a senior engineer',
    'What were the highlights from last week?',
    'Help me prepare for my 1:1 with my manager'
)

# ─────────────────────────────────────────────────────────────────────────────
# Generate rows
# ─────────────────────────────────────────────────────────────────────────────
$dateRange = ($EndDate - $StartDate).TotalSeconds
$rows      = [System.Collections.Generic.List[object]]::new()

for ($i = 0; $i -lt $RowCount; $i++) {
    $user       = $users[$rng.Next($users.Count)]
    $opInfo     = $weightedOps[$rng.Next($weightedOps.Count)]
    $ts         = $StartDate.AddSeconds($rng.NextDouble() * $dateRange)
    $recordId   = [System.Guid]::NewGuid().ToString()
    $messageId  = [System.Guid]::NewGuid().ToString()
    $agentName  = $agentNames[$rng.Next($agentNames.Count)]

    # Build AuditData JSON (subset of real PAX fields)
    $auditData = [ordered]@{
        Id              = $recordId
        RecordType      = if ($opInfo.Type -eq 'MicrosoftTeams') { 25 } elseif ($opInfo.Type -eq 'SharePoint') { 6 } else { 11 }
        CreationTime    = $ts.ToString('yyyy-MM-ddTHH:mm:ssZ')
        Operation       = $opInfo.Op
        OrganizationId  = 'org-' + $domains[$rng.Next($domains.Count)].Replace('.','')
        UserType        = 0
        UserKey         = $user.UPN
        Workload        = $opInfo.Type
        ClientIP        = "$($rng.Next(10,200)).$($rng.Next(0,255)).$($rng.Next(0,255)).$($rng.Next(1,254))"
        UserId          = $user.UPN
        AadAppId        = $null
        ExtendedProperties = @(
            @{ Name = 'UserAgent'; Value = 'Mozilla/5.0 (compatible; SyntheticData/1.0)' }
        )
        ModifiedProperties = @()
    }

    if ($opInfo.Op -in @('SendMessage','CopilotResponse')) {
        $prompt = $samplePrompts[$rng.Next($samplePrompts.Count)]
        $isPrompt = if ($opInfo.Op -eq 'SendMessage') { $true } else { $false }
        $auditData['MessageId']       = $messageId
        $auditData['CommunicationType'] = 'OneOnOneCall'
        $auditData['AISystemPrompt']  = '[SYSTEM] You are a helpful Microsoft 365 Copilot assistant.'
        $auditData['PromptMessage']   = $prompt
        $auditData['ThreadId']        = 'th-' + [System.Guid]::NewGuid().ToString('N').Substring(0,12)
        $auditData['IsTeamsMeeting']  = ($rng.Next(10) -lt 3).ToString().ToLower()
        # CopilotEventData with Messages array — required for explosion processor Path B
        $copilotEventData = [ordered]@{
            AppHost   = (@('Word','Excel','PowerPoint','Teams','Outlook','Loop'))[$rng.Next(6)]
            Messages  = @(
                [ordered]@{
                    Id        = $messageId
                    isPrompt  = $isPrompt
                }
            )
            Contexts  = @(
                [ordered]@{
                    Id   = 'ctx-' + [System.Guid]::NewGuid().ToString('N').Substring(0,8)
                    Type = (@('File','Email','Meeting','Chat'))[$rng.Next(4)]
                }
            )
        }
        if ($null -ne $agentName) {
            $agentIdVal = 'agent-' + [System.Guid]::NewGuid().ToString('N').Substring(0,8)
            # Top-level AuditData — where explosion processor reads AgentId/AgentName
            $auditData['AgentId']   = $agentIdVal
            $auditData['AgentName'] = $agentName
            # Also in CED for completeness
            $copilotEventData['AgentId']   = $agentIdVal
            $copilotEventData['AgentName'] = $agentName
        }
        $auditData['CopilotEventData'] = $copilotEventData
    }

    if ($opInfo.Op -eq 'DlpRuleMatch') {
        $auditData['PolicyDetails'] = @(@{
            PolicyId   = 'pol-' + [System.Guid]::NewGuid().ToString('N').Substring(0,8)
            PolicyName = (@('PII Policy','Financial Data','GDPR Baseline'))[$rng.Next(3)]
            RuleDetails = @(@{
                RuleId    = 'rule-' + [System.Guid]::NewGuid().ToString('N').Substring(0,8)
                RuleName  = 'Block External Share'
                Severity  = (@('Low','Medium','High'))[$rng.Next(3)]
            })
        })
    }

    $auditJson = $auditData | ConvertTo-Json -Compress -Depth 5

    $row = [PSCustomObject]@{
        RecordId        = $recordId
        CreationTime    = $ts.ToString('yyyy-MM-ddTHH:mm:ssZ')
        UserId          = $user.UPN
        Operation       = $opInfo.Op
        Workload        = $opInfo.Type
        RecordType      = $auditData.RecordType
        AuditData       = $auditJson
        # Additional PAX output columns
        UserDisplayName = $user.DisplayName
        Department      = $user.Department
        Country         = $user.Country
        ObjectId        = $user.ObjectId
        IngestionTimestamp = [datetime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
        SourceFile      = [System.IO.Path]::GetFileName($OutputPath)
    }
    $rows.Add($row)
}

# ─────────────────────────────────────────────────────────────────────────────
# Write CSV
# ─────────────────────────────────────────────────────────────────────────────
$rows | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8

$fileSize = (Get-Item $OutputPath).Length
Write-Host "  Generated $RowCount rows → $OutputPath ($([math]::Round($fileSize/1KB, 1)) KB)" -ForegroundColor Green
Write-Host ""
Write-Host "  Run the pipeline on this file:"
Write-Host "  .\scripts\run-pipeline-local.ps1 -InputCsv `"$OutputPath`"" -ForegroundColor Cyan
Write-Host ""
